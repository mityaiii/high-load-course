package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingDeque


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
    private val rateLimitPerSec = properties.rateLimitPerSec.toLong()
    private val parallelRequests = properties.parallelRequests

    private val httpClient = HttpClient.newBuilder()
        .executor(Executors.newFixedThreadPool(100))
        .version(HttpClient.Version.HTTP_2)
        .build()

    private val responseTimes = LinkedBlockingDeque<Long>(1024)


    var requestDuration = DistributionSummary.builder("avg_payment_processing_time")
        .description("request_latency")
        .publishPercentiles(0.5, 0.75, 0.9, 0.95, 0.99)
        .publishPercentileHistogram()
        .register(meterRegistry)


    private val retryCounter = Counter.builder("retry_count")
        .description("retry_count")
        .register(meterRegistry)

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec, Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(parallelRequests)

    private val executorScope = CoroutineScope(Dispatchers.IO)

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        val request = HttpRequest.newBuilder()
            .uri(URI("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        sendRequest(request, transactionId, paymentId, retryCount = 3, deadline = deadline)
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private suspend fun sendRequest(
        request: HttpRequest,
        transactionId: UUID,
        paymentId: UUID,
        retryCount: Int = 1,
        deadline: Long = 1
    ) {
        var x = 0
        var shouldTry = true
        while (shouldTry) {
            shouldTry = false
            x++
            httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply { response ->
                val body = try {
                    mapper.readValue(response.body(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
                if (!body.result && body.message == "Temporary error" && deadline - now() > requestAverageProcessingTime.toMillis() && x < retryCount)
                    shouldTry = true

            }
        }

    }

        private fun addResponseTime(durationMs: Long) {
            if (responseTimes.remainingCapacity() == 0) {
                responseTimes.pollFirst()
            }

            responseTimes.offer(durationMs)
        }


        private fun computeQuantile(quantile: Double): Long {
            val current = responseTimes.toTypedArray()
            if (current.isEmpty()) {
                return (requestAverageProcessingTime.toMillis() * quantile).toLong()
            }

            Arrays.sort(current)
            val idx = ((current.size - 1) * quantile).toInt()
            return current[idx].toLong()
        }
    }

    public fun now() = System.currentTimeMillis()