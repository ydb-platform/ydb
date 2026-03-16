# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class PaymentRecordReportPaymentParams(RequestOptions):
    amount_requested: "PaymentRecordReportPaymentParamsAmountRequested"
    """
    The amount you initially requested for this payment.
    """
    customer_details: NotRequired[
        "PaymentRecordReportPaymentParamsCustomerDetails"
    ]
    """
    Customer information for this payment.
    """
    customer_presence: NotRequired[Literal["off_session", "on_session"]]
    """
    Indicates whether the customer was present in your checkout flow during this payment.
    """
    description: NotRequired[str]
    """
    An arbitrary string attached to the object. Often useful for displaying to users.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    failed: NotRequired["PaymentRecordReportPaymentParamsFailed"]
    """
    Information about the payment attempt failure.
    """
    guaranteed: NotRequired["PaymentRecordReportPaymentParamsGuaranteed"]
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
    payment_method_details: (
        "PaymentRecordReportPaymentParamsPaymentMethodDetails"
    )
    """
    Information about the Payment Method debited for this payment.
    """
    processor_details: NotRequired[
        "PaymentRecordReportPaymentParamsProcessorDetails"
    ]
    """
    Processor information for this payment.
    """
    shipping_details: NotRequired[
        "PaymentRecordReportPaymentParamsShippingDetails"
    ]
    """
    Shipping information for this payment.
    """


class PaymentRecordReportPaymentParamsAmountRequested(TypedDict):
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    value: int
    """
    A positive integer representing the amount in the currency's [minor unit](https://docs.stripe.com/currencies#zero-decimal). For example, `100` can represent 1 USD or 100 JPY.
    """


class PaymentRecordReportPaymentParamsCustomerDetails(TypedDict):
    customer: NotRequired[str]
    """
    The customer who made the payment.
    """
    email: NotRequired[str]
    """
    The customer's phone number.
    """
    name: NotRequired[str]
    """
    The customer's name.
    """
    phone: NotRequired[str]
    """
    The customer's phone number.
    """


class PaymentRecordReportPaymentParamsFailed(TypedDict):
    failed_at: int
    """
    When the reported payment failed. Measured in seconds since the Unix epoch.
    """


class PaymentRecordReportPaymentParamsGuaranteed(TypedDict):
    guaranteed_at: int
    """
    When the reported payment was guaranteed. Measured in seconds since the Unix epoch.
    """


class PaymentRecordReportPaymentParamsPaymentMethodDetails(TypedDict):
    billing_details: NotRequired[
        "PaymentRecordReportPaymentParamsPaymentMethodDetailsBillingDetails"
    ]
    """
    The billing details associated with the method of payment.
    """
    custom: NotRequired[
        "PaymentRecordReportPaymentParamsPaymentMethodDetailsCustom"
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


class PaymentRecordReportPaymentParamsPaymentMethodDetailsBillingDetails(
    TypedDict,
):
    address: NotRequired[
        "PaymentRecordReportPaymentParamsPaymentMethodDetailsBillingDetailsAddress"
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


class PaymentRecordReportPaymentParamsPaymentMethodDetailsBillingDetailsAddress(
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


class PaymentRecordReportPaymentParamsPaymentMethodDetailsCustom(TypedDict):
    display_name: NotRequired[str]
    """
    Display name for the custom (user-defined) payment method type used to make this payment.
    """
    type: NotRequired[str]
    """
    The custom payment method type associated with this payment.
    """


class PaymentRecordReportPaymentParamsProcessorDetails(TypedDict):
    custom: NotRequired[
        "PaymentRecordReportPaymentParamsProcessorDetailsCustom"
    ]
    """
    Information about the custom processor used to make this payment.
    """
    type: Literal["custom"]
    """
    The type of the processor details. An additional hash is included on processor_details with a name matching this value. It contains additional information specific to the processor.
    """


class PaymentRecordReportPaymentParamsProcessorDetailsCustom(TypedDict):
    payment_reference: str
    """
    An opaque string for manual reconciliation of this payment, for example a check number or a payment processor ID.
    """


class PaymentRecordReportPaymentParamsShippingDetails(TypedDict):
    address: NotRequired[
        "PaymentRecordReportPaymentParamsShippingDetailsAddress"
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


class PaymentRecordReportPaymentParamsShippingDetailsAddress(TypedDict):
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
