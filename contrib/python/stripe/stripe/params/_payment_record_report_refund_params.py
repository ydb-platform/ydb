# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class PaymentRecordReportRefundParams(RequestOptions):
    amount: NotRequired["PaymentRecordReportRefundParamsAmount"]
    """
    A positive integer in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal) representing how much of this payment to refund. Can refund only up to the remaining, unrefunded amount of the payment.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    initiated_at: NotRequired[int]
    """
    When the reported refund was initiated. Measured in seconds since the Unix epoch.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    outcome: Literal["refunded"]
    """
    The outcome of the reported refund.
    """
    processor_details: "PaymentRecordReportRefundParamsProcessorDetails"
    """
    Processor information for this refund.
    """
    refunded: "PaymentRecordReportRefundParamsRefunded"
    """
    Information about the payment attempt refund.
    """


class PaymentRecordReportRefundParamsAmount(TypedDict):
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    value: int
    """
    A positive integer representing the amount in the currency's [minor unit](https://docs.stripe.com/currencies#zero-decimal). For example, `100` can represent 1 USD or 100 JPY.
    """


class PaymentRecordReportRefundParamsProcessorDetails(TypedDict):
    custom: NotRequired[
        "PaymentRecordReportRefundParamsProcessorDetailsCustom"
    ]
    """
    Information about the custom processor used to make this refund.
    """
    type: Literal["custom"]
    """
    The type of the processor details. An additional hash is included on processor_details with a name matching this value. It contains additional information specific to the processor.
    """


class PaymentRecordReportRefundParamsProcessorDetailsCustom(TypedDict):
    refund_reference: str
    """
    A reference to the external refund. This field must be unique across all refunds.
    """


class PaymentRecordReportRefundParamsRefunded(TypedDict):
    refunded_at: int
    """
    When the reported refund completed. Measured in seconds since the Unix epoch.
    """
