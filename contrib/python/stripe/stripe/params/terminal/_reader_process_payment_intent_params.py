# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class ReaderProcessPaymentIntentParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    payment_intent: str
    """
    The ID of the PaymentIntent to process on the reader.
    """
    process_config: NotRequired[
        "ReaderProcessPaymentIntentParamsProcessConfig"
    ]
    """
    Configuration overrides for this transaction, such as tipping and customer cancellation settings.
    """


class ReaderProcessPaymentIntentParamsProcessConfig(TypedDict):
    allow_redisplay: NotRequired[Literal["always", "limited", "unspecified"]]
    """
    This field indicates whether this payment method can be shown again to its customer in a checkout flow. Stripe products such as Checkout and Elements use this field to determine whether a payment method can be shown as a saved payment method in a checkout flow.
    """
    enable_customer_cancellation: NotRequired[bool]
    """
    Enables cancel button on transaction screens.
    """
    return_url: NotRequired[str]
    """
    The URL to redirect your customer back to after they authenticate or cancel their payment on the payment method's app or site. If you'd prefer to redirect to a mobile application, you can alternatively supply an application URI scheme.
    """
    skip_tipping: NotRequired[bool]
    """
    Override showing a tipping selection screen on this transaction.
    """
    tipping: NotRequired[
        "ReaderProcessPaymentIntentParamsProcessConfigTipping"
    ]
    """
    Tipping configuration for this transaction.
    """


class ReaderProcessPaymentIntentParamsProcessConfigTipping(TypedDict):
    amount_eligible: NotRequired[int]
    """
    Amount used to calculate tip suggestions on tipping selection screen for this transaction. Must be a positive integer in the smallest currency unit (e.g., 100 cents to represent $1.00 or 100 to represent ¥100, a zero-decimal currency).
    """
