package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.future.await
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.AsyncOngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    val retryCounter: Counter = Counter.builder("payment.retry.attempts")
        .description("Number of retry attempts for payment requests")
        .register(meterRegistry)

    val retriesPerRequestSummary: DistributionSummary = DistributionSummary.builder("payment.retries.per.request")
        .description("Average number of retries per payment request")
        .register(meterRegistry)

    val requestDurationTimer: Timer = Timer.builder("payment.request.duration")
        .publishPercentiles(0.5, 0.75, 0.9, 0.95, 0.99)
        .publishPercentileHistogram(true)
        .description("Time taken to execute payment request")
        .register(meterRegistry)

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName

    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val slidingWindow = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = AsyncOngoingWindow(parallelRequests)

    private val client = HttpClient.newBuilder()
        .executor(Executors.newFixedThreadPool(25))
        .version(HttpClient.Version.HTTP_2)
        .build()

    class RetryAfterException(val interval: Long) : Exception("Retry after $interval ms")

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        // Sending a POST with a non-empty body over h2c (HTTP/2 cleartext) can fall back to HTTP/1.1 if the server or client doesn’t properly support request body framing in h2c mode.
        val request = HttpRequest.newBuilder()
            .uri(URI.create("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .POST(HttpRequest.BodyPublishers.noBody())
            //  .POST(HttpRequest.BodyPublishers.ofString(emptyBody.toString()))
            .timeout(Duration.ofSeconds(2))
            .build()

        ongoingWindow.acquire()
        try {
            trySendRequest(request, paymentId, transactionId, deadline)
        } finally {
            ongoingWindow.release()
        }
    }

    suspend private fun trySendRequest(request: HttpRequest, paymentId: UUID, transactionId: UUID, deadline: Long) {
        var attemptIndex = 0
        val maxAttempts = 3

        repeat(maxAttempts) {
            slidingWindow.tickAsync()
            attemptIndex += 1

            retryCounter.increment()

            val sample = Timer.start(meterRegistry)


            try {
                var response = client
                    .sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .await()

                val body = try {
                    mapper.readValue(response.body(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] txId=$transactionId payment=$paymentId code=${response.statusCode()} reason=${response.body()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                retriesPerRequestSummary.record((attemptIndex).toDouble())

                if (response.statusCode() in 200..299) {
                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }

                    return
                }
            } catch (e: Exception) {
                when {
                    e is TimeoutCancellationException || e is HttpTimeoutException -> {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }

                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
            } finally {
                sample.stop(requestDurationTimer)
            }
        }

        retriesPerRequestSummary.record(maxAttempts.toDouble())
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = "request failed")
        }

    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()