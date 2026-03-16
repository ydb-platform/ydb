# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class ReaderCollectPaymentMethodParams(RequestOptions):
    collect_config: NotRequired[
        "ReaderCollectPaymentMethodParamsCollectConfig"
    ]
    """
    Configuration overrides for this collection, such as tipping, surcharging, and customer cancellation settings.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    payment_intent: str
    """
    The ID of the PaymentIntent to collect a payment method for.
    """


class ReaderCollectPaymentMethodParamsCollectConfig(TypedDict):
    allow_redisplay: NotRequired[Literal["always", "limited", "unspecified"]]
    """
    This field indicates whether this payment method can be shown again to its customer in a checkout flow. Stripe products such as Checkout and Elements use this field to determine whether a payment method can be shown as a saved payment method in a checkout flow.
    """
    enable_customer_cancellation: NotRequired[bool]
    """
    Enables cancel button on transaction screens.
    """
    skip_tipping: NotRequired[bool]
    """
    Override showing a tipping selection screen on this transaction.
    """
    tipping: NotRequired[
        "ReaderCollectPaymentMethodParamsCollectConfigTipping"
    ]
    """
    Tipping configuration for this transaction.
    """


class ReaderCollectPaymentMethodParamsCollectConfigTipping(TypedDict):
    amount_eligible: NotRequired[int]
    """
    Amount used to calculate tip suggestions on tipping selection screen for this transaction. Must be a positive integer in the smallest currency unit (e.g., 100 cents to represent $1.00 or 100 to represent ¥100, a zero-decimal currency).
    """
