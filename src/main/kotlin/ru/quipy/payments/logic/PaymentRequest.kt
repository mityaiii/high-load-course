package ru.quipy.payments.logic

import java.util.UUID

class PaymentRequest(
    val deadline: Long,
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long
) : Comparable<PaymentRequest> {
    override fun compareTo(other: PaymentRequest): Int = deadline.compareTo(other.deadline)
}