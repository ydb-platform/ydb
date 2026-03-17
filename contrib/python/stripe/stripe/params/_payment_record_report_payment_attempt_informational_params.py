# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class PaymentRecordReportPaymentAttemptInformationalParams(RequestOptions):
    customer_details: NotRequired[
        "PaymentRecordReportPaymentAttemptInformationalParamsCustomerDetails"
    ]
    """
    Customer information for this payment.
    """
    description: NotRequired["Literal['']|str"]
    """
    An arbitrary string attached to the object. Often useful for displaying to users.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    shipping_details: NotRequired[
        "Literal['']|PaymentRecordReportPaymentAttemptInformationalParamsShippingDetails"
    ]
    """
    Shipping information for this payment.
    """


class PaymentRecordReportPaymentAttemptInformationalParamsCustomerDetails(
    TypedDict,
):
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


class PaymentRecordReportPaymentAttemptInformationalParamsShippingDetails(
    TypedDict,
):
    address: NotRequired[
        "PaymentRecordReportPaymentAttemptInformationalParamsShippingDetailsAddress"
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


class PaymentRecordReportPaymentAttemptInformationalParamsShippingDetailsAddress(
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
