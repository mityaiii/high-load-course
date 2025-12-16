package ru.quipy.common.utils

import kotlinx.coroutines.delay
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong

class ConcurrentRateLimiter(
    private val rate: Long,
    private val window: Duration,
) {
    private val queue = ConcurrentLinkedQueue<Long>()
    private val sum = AtomicLong(0)

    suspend fun tryTick(deadline: Long): Boolean {
        while (System.currentTimeMillis() < deadline) {
            deleteOldTimes()
            if (sum.get() < rate) {
                if (tryAdd(System.currentTimeMillis())) {
                    return true
                }
            }
            delay(10)
        }
        return false
    }

    private fun tryAdd(timeToAdd: Long): Boolean {
        if (sum.incrementAndGet() <= rate) {
            queue.add(timeToAdd)
            return true
        } else {
            sum.decrementAndGet()
            return false
        }
    }

    private fun deleteOldTimes() {
        val leftIntervalTime = System.currentTimeMillis() - window.toMillis()
        while (true) {
            val peekTime = queue.peek() ?: break
            if (peekTime < leftIntervalTime) {
                queue.poll()
                sum.decrementAndGet()
            } else {
                break
            }
        }
    }
}