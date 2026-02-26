package ru.quipy.payments.logic

import kotlin.math.pow

class RetryManager (
    private val maxRetries: Int,
    private val backoff: Double = 2.0,
    private val avgProcessingTime: Long = 1000L,
) {
    fun shouldRetry(currentTime: Long, deadline: Long, attempt: Int): Boolean {
        if (currentTime >= deadline - avgProcessingTime*1.02) return false
        if (attempt == maxRetries) return false
        return true
    }

    fun computeDelays(startTime: Long, deadline: Long): LongArray? {
        val availableTime = deadline - startTime - avgProcessingTime
        if (availableTime <= 0) {
            return null
        }

        val sumFactor = (backoff.pow(maxRetries - 1) - 1) / (backoff - 1)
        val baseDelay = availableTime / sumFactor
        return LongArray(maxRetries) { i ->
            if (i == maxRetries - 1) 0
            else (baseDelay * baseDelay.pow(i.toDouble())).toLong()
        }
    }

    fun onFailure(localAttempt: Int, delays: LongArray?, startTime: Long): Int {
        val attempt = localAttempt + 1
        return attempt
    }
}