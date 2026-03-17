# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class PaymentEvaluationCreateParams(RequestOptions):
    client_device_metadata_details: NotRequired[
        "PaymentEvaluationCreateParamsClientDeviceMetadataDetails"
    ]
    """
    Details about the Client Device Metadata to associate with the payment evaluation.
    """
    customer_details: "PaymentEvaluationCreateParamsCustomerDetails"
    """
    Details about the customer associated with the payment evaluation.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    payment_details: "PaymentEvaluationCreateParamsPaymentDetails"
    """
    Details about the payment.
    """


class PaymentEvaluationCreateParamsClientDeviceMetadataDetails(TypedDict):
    radar_session: str
    """
    ID for the Radar Session to associate with the payment evaluation. A [Radar Session](https://docs.stripe.com/radar/radar-session) is a snapshot of the browser metadata and device details that help Radar make more accurate predictions on your payments.
    """


class PaymentEvaluationCreateParamsCustomerDetails(TypedDict):
    customer: NotRequired[str]
    """
    The ID of the customer associated with the payment evaluation.
    """
    customer_account: NotRequired[str]
    """
    The ID of the Account representing the customer associated with the payment evaluation.
    """
    email: NotRequired[str]
    """
    The customer's email address.
    """
    name: NotRequired[str]
    """
    The customer's full name or business name.
    """
    phone: NotRequired[str]
    """
    The customer's phone number.
    """


class PaymentEvaluationCreateParamsPaymentDetails(TypedDict):
    amount: int
    """
    The intended amount to collect with this payment. A positive integer representing how much to charge in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal) (for example, 100 cents to charge 1.00 USD or 100 to charge 100 Yen, a zero-decimal currency).
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    description: NotRequired[str]
    """
    An arbitrary string attached to the object. Often useful for displaying to users.
    """
    money_movement_details: NotRequired[
        "PaymentEvaluationCreateParamsPaymentDetailsMoneyMovementDetails"
    ]
    """
    Details about the payment's customer presence and type.
    """
    payment_method_details: (
        "PaymentEvaluationCreateParamsPaymentDetailsPaymentMethodDetails"
    )
    """
    Details about the payment method to use for the payment.
    """
    shipping_details: NotRequired[
        "PaymentEvaluationCreateParamsPaymentDetailsShippingDetails"
    ]
    """
    Shipping details for the payment evaluation.
    """
    statement_descriptor: NotRequired[str]
    """
    Payment statement descriptor.
    """


class PaymentEvaluationCreateParamsPaymentDetailsMoneyMovementDetails(
    TypedDict,
):
    card: NotRequired[
        "PaymentEvaluationCreateParamsPaymentDetailsMoneyMovementDetailsCard"
    ]
    """
    Describes card money movement details for the payment evaluation.
    """
    money_movement_type: Literal["card"]
    """
    Describes the type of money movement. Currently only `card` is supported.
    """


class PaymentEvaluationCreateParamsPaymentDetailsMoneyMovementDetailsCard(
    TypedDict,
):
    customer_presence: NotRequired[Literal["off_session", "on_session"]]
    """
    Describes the presence of the customer during the payment.
    """
    payment_type: NotRequired[
        Literal["one_off", "recurring", "setup_one_off", "setup_recurring"]
    ]
    """
    Describes the type of payment.
    """


class PaymentEvaluationCreateParamsPaymentDetailsPaymentMethodDetails(
    TypedDict,
):
    billing_details: NotRequired[
        "PaymentEvaluationCreateParamsPaymentDetailsPaymentMethodDetailsBillingDetails"
    ]
    """
    Billing information associated with the payment evaluation.
    """
    payment_method: str
    """
    ID of the payment method used in this payment evaluation.
    """


class PaymentEvaluationCreateParamsPaymentDetailsPaymentMethodDetailsBillingDetails(
    TypedDict,
):
    address: NotRequired[
        "PaymentEvaluationCreateParamsPaymentDetailsPaymentMethodDetailsBillingDetailsAddress"
    ]
    """
    Billing address.
    """
    email: NotRequired[str]
    """
    Email address.
    """
    name: NotRequired[str]
    """
    Full name.
    """
    phone: NotRequired[str]
    """
    Billing phone number (including extension).
    """


class PaymentEvaluationCreateParamsPaymentDetailsPaymentMethodDetailsBillingDetailsAddress(
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


class PaymentEvaluationCreateParamsPaymentDetailsShippingDetails(TypedDict):
    address: NotRequired[
        "PaymentEvaluationCreateParamsPaymentDetailsShippingDetailsAddress"
    ]
    """
    Shipping address.
    """
    name: NotRequired[str]
    """
    Shipping name.
    """
    phone: NotRequired[str]
    """
    Shipping phone number.
    """


class PaymentEvaluationCreateParamsPaymentDetailsShippingDetailsAddress(
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
