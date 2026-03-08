package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.delay
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.ConcurrentRateLimiter
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry
) : PaymentExternalSystemAdapter, AutoCloseable {

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

    private val paymentUpdateExecutor: ExecutorService =
        Executors.newFixedThreadPool(10)

    var requestDuration = DistributionSummary.builder("avg_payment_processing_time")
        .description("request_latency")
        .publishPercentiles(0.5, 0.75, 0.9, 0.95, 0.99)
        .publishPercentileHistogram()
        .register(meterRegistry)

    private val rateLimiter = ConcurrentRateLimiter(rateLimitPerSec, Duration.ofSeconds(1))
    private val ongoingWindow = NonBlockingOngoingWindow(parallelRequests)

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        logSubmissionAsync(paymentId, transactionId, paymentStartedAt)

        logger.info("[$accountName] Submit: $paymentId, txId: $transactionId")

        val request = HttpRequest.newBuilder()
            .uri(
                URI(
                    "http://$paymentProviderHostPort/external/process" +
                            "?serviceName=$serviceName" +
                            "&token=$token" +
                            "&accountName=$accountName" +
                            "&transactionId=$transactionId" +
                            "&paymentId=$paymentId" +
                            "&amount=$amount"
                )
            )
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
            if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
                logResultAsync(paymentId, transactionId, "Deadline exceeded")
                return
            }

            shouldTry = false
            x++

            try {
                while (ongoingWindow.putIntoWindow() is NonBlockingOngoingWindow.WindowResponse.Fail) {
                    delay(10)
                }

                if (!rateLimiter.tryTick(deadline)) {
                    logResultAsync(paymentId, transactionId, "Deadline exceeded")
                    return
                }

                httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenAccept { response ->
                        val body = try {
                            mapper.readValue(response.body(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error(
                                "[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, " +
                                        "result code: ${response.statusCode()}, reason: ${response.body()}",
                                e
                            )
                            ExternalSysResponse(
                                transactionId = transactionId.toString(),
                                paymentId = paymentId.toString(),
                                result = false,
                                message = e.message
                            )
                        } finally {
                            ongoingWindow.releaseWindow()
                        }

                        logger.warn(
                            "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, " +
                                    "succeeded: ${body.result}, message: ${body.message}"
                        )

                        logProcessingAsync(
                            paymentId = paymentId,
                            transactionId = transactionId,
                            result = body.result,
                            reason = body.message
                        )
                    }
                    .exceptionally { ex ->
                        ongoingWindow.releaseWindow()
                        logger.error("[$accountName] Payment failed for $paymentId", ex)
                        logResultAsync(paymentId, transactionId, ex.message)
                        null
                    }

            } catch (e: Exception) {
                logger.error("[$accountName] Payment failed for $paymentId", e)
                logResultAsync(paymentId, transactionId, e.message)
            }
        }
    }

    private fun logSubmissionAsync(
        paymentId: UUID,
        transactionId: UUID,
        paymentStartedAt: Long
    ) {
        paymentUpdateExecutor.submit {
            runCatching {
                val now = now()
                paymentESService.update(paymentId) {
                    it.logSubmission(
                        success = true,
                        transactionId = transactionId,
                        startedAt = now,
                        spentInQueueDuration = Duration.ofMillis(now - paymentStartedAt)
                    )
                }
            }.onFailure { e ->
                logger.error("[$accountName] failed to save submission for $paymentId", e)
            }
        }
    }

    private fun logProcessingAsync(
        paymentId: UUID,
        transactionId: UUID,
        result: Boolean,
        reason: String?
    ) {
        paymentUpdateExecutor.submit {
            runCatching {
                paymentESService.update(paymentId) {
                    it.logProcessing(result, now(), transactionId, reason = reason)
                }
            }.onFailure { e ->
                logger.error("[$accountName] failed to save processing result for $paymentId", e)
            }
        }
    }

    private fun logResultAsync(
        paymentId: UUID,
        transactionId: UUID,
        message: String?
    ) {
        paymentUpdateExecutor.submit {
            runCatching {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = message)
                }
            }.onFailure { e ->
                logger.error("[$accountName] failed to save result for $paymentId", e)
            }
        }
    }

    override fun close() {
        paymentUpdateExecutor.shutdown()
    }
}

fun now() = System.currentTimeMillis()