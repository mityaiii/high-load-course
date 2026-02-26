package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.min


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val actualRequestAverageProcessingTime = (properties.averageProcessingTime.toMillis().toDouble()*1.0).toLong()
    private val rateLimitPerSec = properties.rateLimitPerSec.toDouble()
    private val parallelRequests = properties.parallelRequests
    private val parallelLimitPerSec = properties.parallelRequests.toDouble()/(properties.averageProcessingTime.toMillis() / 1000.0)

    private val minimalLimitPerSec = min(rateLimitPerSec, parallelLimitPerSec)

    private val httpClient = HttpClient.newBuilder()
        .executor(Executors.newFixedThreadPool(100))
        .version(HttpClient.Version.HTTP_2)
        .build()

    private val executorScope = CoroutineScope(
        SupervisorJob() + Dispatchers.IO.limitedParallelism(32)
    )

    private val dbScope = CoroutineScope(
        SupervisorJob() + Dispatchers.IO.limitedParallelism(16)
    )

    val retryManager = RetryManager(
        maxRetries = 2,
        backoff = 1.0,
        avgProcessingTime = (actualRequestAverageProcessingTime)
    )

    var requestDuration = DistributionSummary.builder("avg_payment_processing_time")
        .description("request_latency")
        .publishPercentiles(0.5, 0.75, 0.9, 0.95, 0.99)
        .publishPercentileHistogram()
        .register(meterRegistry)

    data class RetryRequestData(
        var attempt: Int,
        var delays: LongArray? = null,
        var startTime: Long
    )

    private val maxQueueSize = 1000
    private val queue = PriorityBlockingQueue<PaymentRequest>(maxQueueSize)
    private val outgoingRateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1L))
    private val inFlightRequests = AtomicInteger(0)

    val executorInFlight = AtomicInteger(0)
    val dbInFlight = AtomicInteger(0)

    val actualTimeout = Counter.builder("payment_actual_timeout_count")
        .description("Count total amount of actual timeouts")
        .tag("accountName", accountName)
        .register(Metrics.globalRegistry)

    val theoreticalTimeout = Counter.builder("payment_theoretical_timeout_count")
        .description("Count total amount of theoretical timeouts")
        .tag("accountName", accountName)
        .register(Metrics.globalRegistry)

    val externalPaymentRequests = Counter.builder("external_payment_requests")
        .description("Count total amount of external payment requests")
        .tag("accountName", accountName)
        .register(Metrics.globalRegistry)

    val externalPaymentRequestsWithRetires = Counter.builder("external_payment_requests_with_retries")
        .description("Count total amount of external payment requests with retires")
        .tag("accountName", accountName)
        .register(Metrics.globalRegistry)

    private val q50 = AtomicReference<Double>(0.0)
    private val q80 = AtomicReference<Double>(0.0)
    private val q95 = AtomicReference<Double>(0.0)
    private val q99 = AtomicReference<Double>(0.0)

    init {
        Gauge.builder("external_request_latency", q50) { it.get() }
            .description("External request latency quantiles")
            .tag("accountName", accountName)
            .tag("quantile", "0.5")
            .register(Metrics.globalRegistry)

        Gauge.builder("external_request_latency", q80) { it.get() }
            .description("External request latency quantiles")
            .tag("accountName", accountName)
            .tag("quantile", "0.8")
            .register(Metrics.globalRegistry)

        Gauge.builder("external_request_latency", q95) { it.get() }
            .description("External request latency quantiles")
            .tag("accountName", accountName)
            .tag("quantile", "0.95")
            .register(Metrics.globalRegistry)

        Gauge.builder("external_request_latency", q99) { it.get() }
            .description("External request latency quantiles")
            .tag("accountName", accountName)
            .tag("quantile", "0.99")
            .register(Metrics.globalRegistry)
    }

    override fun canAcceptPayment(deadline: Long): Pair<Boolean, Long> {
        val estimatedWaitMs = (queue.size / minimalLimitPerSec) * 1000
        val additionalTimeMs = 1000
        val willCompleteAt = now() + estimatedWaitMs + additionalTimeMs + requestAverageProcessingTime.toMillis()

        val canMeetDeadline = willCompleteAt < deadline
        val queueOk = queue.size < maxQueueSize

        return Pair(canMeetDeadline && queueOk, estimatedWaitMs.toLong())
    }

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ) {
        val paymentRequest = PaymentRequest(deadline, paymentId, amount, paymentStartedAt)
        var canAccept = Pair(true,0L)
        var accepted = false

        canAccept = canAcceptPayment(deadline)
        if (canAccept.first) {
            accepted = queue.offer(paymentRequest)
        }

        if (!canAccept.first) {
            logger.error("429 from PaymentExternalSystemAdapterImpl")
            val delaySeconds = (canAccept.second - System.currentTimeMillis()) / 1000
            throw ResponseStatusException(
                HttpStatus.TOO_MANY_REQUESTS,
                delaySeconds.toString(),
            )
        }

        if (!accepted) {
            logger.error("429 from PaymentExternalSystemAdapterImpl (queue reason)")
            logger.error("[$accountName] Queue overflow! Rejecting payment $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), UUID.randomUUID(), reason = "Queue overflow (back pressure).")
            }
            val retryTimeMs = 5L
            throw ResponseStatusException(
                HttpStatus.TOO_MANY_REQUESTS,
                retryTimeMs.toString()
            )
        }
    }

    private fun performPaymentWithRetry(paymentId: UUID,
                                        amount: Int, paymentStartedAt: Long,
                                        deadline: Long,
                                        request: PaymentRequest,
                                        retryData: RetryRequestData = RetryRequestData(0, null, now())) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        dbScope.launch {
            dbInFlight.incrementAndGet()
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
            dbInFlight.decrementAndGet()
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        val url = "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
        val timeout = computeDynamicTimeout(deadline) ?: 30000L
        val httpRequest = HttpRequest.newBuilder()
            .uri(URI(url))
            .timeout(Duration.ofMillis(timeout))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        externalPaymentRequests.increment()

        httpClient.sendAsync(httpRequest, java.net.http.HttpResponse.BodyHandlers.ofString())
            .thenApply { resp ->

                val body = try {
                    mapper.readValue(resp.body(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    ExternalSysResponse(
                        transactionId.toString(),
                        paymentId.toString(),
                        false,
                        "Bad JSON: ${e.message}"
                    )
                }

                dbScope.launch {
                    dbInFlight.incrementAndGet()
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId)
                    }
                    dbInFlight.decrementAndGet()
                }

                if (body.result) {
                    inFlightRequests.decrementAndGet()
                    executorInFlight.decrementAndGet()
                    return@thenApply null
                }

                handleRetry(
                    resp.statusCode(),
                    paymentId,
                    amount,
                    paymentStartedAt,
                    deadline,
                    request,
                    retryData
                )

                null
            }
            .exceptionally { ex ->

                val msg = ex.cause?.message ?: ex.message ?: "Unknown exception"

                dbScope.launch {
                    dbInFlight.incrementAndGet()
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = msg)
                    }
                    dbInFlight.decrementAndGet()
                }

                handleRetry(500, paymentId, amount, paymentStartedAt, deadline, request, retryData)

                null
            }
    }

    private fun handleRetry(
        code: Int,
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        request: PaymentRequest,
        retryData: RetryRequestData
    ) {
        externalPaymentRequestsWithRetires.increment()


        if (!retryManager.shouldRetry(retryData.startTime, deadline, retryData.attempt)) {

            dbScope.launch {
                dbInFlight.incrementAndGet()
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), UUID.randomUUID())
                }
                dbInFlight.decrementAndGet()
            }

            inFlightRequests.decrementAndGet()
            executorInFlight.decrementAndGet()
            return
        }

        retryData.delays = retryManager.computeDelays(retryData.startTime, deadline)
        retryData.attempt = retryManager.onFailure(retryData.attempt, retryData.delays, retryData.startTime)

        performPaymentWithRetry(paymentId, amount, paymentStartedAt, deadline, request, retryData)
    }

    override fun name() = properties.accountName

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    private fun computeDynamicTimeout(deadline: Long): Long? {
        return actualRequestAverageProcessingTime.coerceAtMost(deadline - now())
    }
}

public fun now() = System.currentTimeMillis()