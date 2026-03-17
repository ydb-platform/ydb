# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import NotRequired, TypedDict


class ReaderRefundPaymentParams(RequestOptions):
    amount: NotRequired[int]
    """
    A positive integer in __cents__ representing how much of this charge to refund.
    """
    charge: NotRequired[str]
    """
    ID of the Charge to refund.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    payment_intent: NotRequired[str]
    """
    ID of the PaymentIntent to refund.
    """
    refund_application_fee: NotRequired[bool]
    """
    Boolean indicating whether the application fee should be refunded when refunding this charge. If a full charge refund is given, the full application fee will be refunded. Otherwise, the application fee will be refunded in an amount proportional to the amount of the charge refunded. An application fee can be refunded only by the application that created the charge.
    """
    refund_payment_config: NotRequired[
        "ReaderRefundPaymentParamsRefundPaymentConfig"
    ]
    """
    Configuration overrides for this refund, such as customer cancellation settings.
    """
    reverse_transfer: NotRequired[bool]
    """
    Boolean indicating whether the transfer should be reversed when refunding this charge. The transfer will be reversed proportionally to the amount being refunded (either the entire or partial amount). A transfer can be reversed only by the application that created the charge.
    """


class ReaderRefundPaymentParamsRefundPaymentConfig(TypedDict):
    enable_customer_cancellation: NotRequired[bool]
    """
    Enables cancel button on transaction screens.
    """
