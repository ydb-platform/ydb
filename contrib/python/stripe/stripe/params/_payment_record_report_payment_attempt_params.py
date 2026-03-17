# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class PaymentRecordReportPaymentAttemptParams(RequestOptions):
    description: NotRequired[str]
    """
    An arbitrary string attached to the object. Often useful for displaying to users.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    failed: NotRequired["PaymentRecordReportPaymentAttemptParamsFailed"]
    """
    Information about the payment attempt failure.
    """
    guaranteed: NotRequired[
        "PaymentRecordReportPaymentAttemptParamsGuaranteed"
    ]
    """
    Information about the payment attempt guarantee.
    """
    initiated_at: int
    """
    When the reported payment was initiated. Measured in seconds since the Unix epoch.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    outcome: NotRequired[Literal["failed", "guaranteed"]]
    """
    The outcome of the reported payment.
    """
    payment_method_details: NotRequired[
        "PaymentRecordReportPaymentAttemptParamsPaymentMethodDetails"
    ]
    """
    Information about the Payment Method debited for this payment.
    """
    shipping_details: NotRequired[
        "PaymentRecordReportPaymentAttemptParamsShippingDetails"
    ]
    """
    Shipping information for this payment.
    """


class PaymentRecordReportPaymentAttemptParamsFailed(TypedDict):
    failed_at: int
    """
    When the reported payment failed. Measured in seconds since the Unix epoch.
    """


class PaymentRecordReportPaymentAttemptParamsGuaranteed(TypedDict):
    guaranteed_at: int
    """
    When the reported payment was guaranteed. Measured in seconds since the Unix epoch.
    """


class PaymentRecordReportPaymentAttemptParamsPaymentMethodDetails(TypedDict):
    billing_details: NotRequired[
        "PaymentRecordReportPaymentAttemptParamsPaymentMethodDetailsBillingDetails"
    ]
    """
    The billing details associated with the method of payment.
    """
    custom: NotRequired[
        "PaymentRecordReportPaymentAttemptParamsPaymentMethodDetailsCustom"
    ]
    """
    Information about the custom (user-defined) payment method used to make this payment.
    """
    payment_method: NotRequired[str]
    """
    ID of the Stripe Payment Method used to make this payment.
    """
    type: NotRequired[Literal["custom"]]
    """
    The type of the payment method details. An additional hash is included on the payment_method_details with a name matching this value. It contains additional information specific to the type.
    """


class PaymentRecordReportPaymentAttemptParamsPaymentMethodDetailsBillingDetails(
    TypedDict,
):
    address: NotRequired[
        "PaymentRecordReportPaymentAttemptParamsPaymentMethodDetailsBillingDetailsAddress"
    ]
    """
    The billing address associated with the method of payment.
    """
    email: NotRequired[str]
    """
    The billing email associated with the method of payment.
    """
    name: NotRequired[str]
    """
    The billing name associated with the method of payment.
    """
    phone: NotRequired[str]
    """
    The billing phone number associated with the method of payment.
    """


class PaymentRecordReportPaymentAttemptParamsPaymentMethodDetailsBillingDetailsAddress(
    TypedDict,
):
    city: NotRequired[str]
    """
    City, district, suburb, town, or village.
    """
    country: NotRequired[str]
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    line1: NotRequired[str]
    """
    Address line 1, such as the street, PO Box, or company name.
    """
    line2: NotRequired[str]
    """
    Address line 2, such as the apartment, suite, unit, or building.
    """
    postal_code: NotRequired[str]
    """
    ZIP or postal code.
    """
    state: NotRequired[str]
    """
    State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
    """


class PaymentRecordReportPaymentAttemptParamsPaymentMethodDetailsCustom(
    TypedDict,
):
    display_name: NotRequired[str]
    """
    Display name for the custom (user-defined) payment method type used to make this payment.
    """
    type: NotRequired[str]
    """
    The custom payment method type associated with this payment.
    """


class PaymentRecordReportPaymentAttemptParamsShippingDetails(TypedDict):
    address: NotRequired[
        "PaymentRecordReportPaymentAttemptParamsShippingDetailsAddress"
    ]
    """
    The physical shipping address.
    """
    name: NotRequired[str]
    """
    The shipping recipient's name.
    """
    phone: NotRequired[str]
    """
    The shipping recipient's phone number.
    """


class PaymentRecordReportPaymentAttemptParamsShippingDetailsAddress(TypedDict):
    city: NotRequired[str]
    """
    City, district, suburb, town, or village.
    """
    country: NotRequired[str]
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    line1: NotRequired[str]
    """
    Address line 1, such as the street, PO Box, or company name.
    """
    line2: NotRequired[str]
    """
    Address line 2, such as the apartment, suite, unit, or building.
    """
    postal_code: NotRequired[str]
    """
    ZIP or postal code.
    """
    state: NotRequired[str]
    """
    State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
    """
