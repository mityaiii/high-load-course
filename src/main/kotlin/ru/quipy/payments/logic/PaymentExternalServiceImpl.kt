package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeout
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.ConcurrentRateLimiter
import ru.quipy.common.utils.NonBlockingOngoingWindow
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
//    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
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

    var requestDuration = DistributionSummary.builder("avg_payment_processing_time")
        .description("request_latency")
        .publishPercentiles(0.5, 0.75, 0.8, 0.85, 0.9, 0.95, 0.99)
        .publishPercentileHistogram()
        .register(meterRegistry)

    private val rateLimiter = ConcurrentRateLimiter(rateLimitPerSec, Duration.ofSeconds(1))
    private val ongoingWindow = NonBlockingOngoingWindow(parallelRequests)

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()


        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
//        paymentESService.update(paymentId) {
//            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
//        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        val idempotencyKey = UUID.randomUUID().toString()
        val request = HttpRequest.newBuilder()
            .uri(URI("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .POST(HttpRequest.BodyPublishers.noBody())
            .header("x-idempotency-key", idempotencyKey)
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
//            if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
////                logResult(paymentId, transactionId, "Deadline exceeded")
//                return
//            }
            shouldTry = false
            x++

            try {
                while (ongoingWindow.putIntoWindow() is NonBlockingOngoingWindow.WindowResponse.Fail) {
                    delay(10)
                }

//                if (!rateLimiter.tryTick(deadline)) {
//                    with(Dispatchers.IO) {
//                        paymentESService.update(paymentId) {
//                            it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded")
//                        }
//                    }
//                    return
//                }
                //   logger.info("lets send")
                val result = try {
                    withTimeout(10000000) {
                        coroutineScope {
                            val firstJob = async {
                                sendSingleRequest(request, transactionId, paymentId)
                            }

                            val secondJob = async {
                                delay(100)
                                sendSingleRequest(request, transactionId, paymentId)
                            }
//                            val thirdJob = async {
//                                delay(100)
//                                sendSingleRequest(request, transactionId, paymentId)
//                            }

                            select {
                                firstJob.onAwait { it }
                                secondJob.onAwait { it }
                                //thirdJob.onAwait { it }
                            }
                        }
                    }
                } catch (e: TimeoutCancellationException) {
                    logger.error("[$accountName] Request timed out after timeout for payment $paymentId")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "timeout")
                } finally {
                    ongoingWindow.releaseWindow()
                }

                if (!result.result && x < retryCount) {
                    shouldTry = true
                    delay(10 * x.toLong())
                }
            } catch (e: Exception) {
                logger.error("[$accountName] Payment failed for $paymentId", e)
//                logResult(paymentId, transactionId, e.message)
            }
        }
    }

    private suspend fun sendSingleRequest(
        request: HttpRequest,
        transactionId: UUID,
        paymentId: UUID
    ): ExternalSysResponse {
        return suspendCancellableCoroutine { continuation ->
            val start = now()

            httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply { response ->
                    try {
                        val body = try {
                            mapper.readValue(response.body(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }

                        requestDuration.record((now() - start).toDouble())

                        if (continuation.isActive) {
                            continuation.resume(body)
                        }
                    } catch (e: Exception) {
                        if (continuation.isActive) {
                            continuation.resumeWithException(e)
                        }
                    }
                }
                .exceptionally { throwable ->
                    if (continuation.isActive) {
                        continuation.resumeWithException(throwable)
                    }
                }
        }
    }

//    private fun logResult(paymentId: UUID, transactionId: UUID, message: String?) {
//        try {
//            with(Dispatchers.IO) {
//                paymentESService.update(paymentId) {
//                    it.logProcessing(false, now(), transactionId, reason = message)
//                }
//            }
//        } catch(e: Exception) {
//            logger.error("[$accountName] failed to save result for $paymentId", e)
//        }
//    }
}

public fun now() = System.currentTimeMillis()