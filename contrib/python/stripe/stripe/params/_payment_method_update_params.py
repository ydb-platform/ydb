# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class PaymentMethodUpdateParams(TypedDict):
    allow_redisplay: NotRequired[Literal["always", "limited", "unspecified"]]
    """
    This field indicates whether this payment method can be shown again to its customer in a checkout flow. Stripe products such as Checkout and Elements use this field to determine whether a payment method can be shown as a saved payment method in a checkout flow. The field defaults to `unspecified`.
    """
    billing_details: NotRequired["PaymentMethodUpdateParamsBillingDetails"]
    """
    Billing information associated with the PaymentMethod that may be used or required by particular types of payment methods.
    """
    card: NotRequired["PaymentMethodUpdateParamsCard"]
    """
    If this is a `card` PaymentMethod, this hash contains the user's card details.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    payto: NotRequired["PaymentMethodUpdateParamsPayto"]
    """
    If this is a `payto` PaymentMethod, this hash contains details about the PayTo payment method.
    """
    us_bank_account: NotRequired["PaymentMethodUpdateParamsUsBankAccount"]
    """
    If this is an `us_bank_account` PaymentMethod, this hash contains details about the US bank account payment method.
    """


class PaymentMethodUpdateParamsBillingDetails(TypedDict):
    address: NotRequired[
        "Literal['']|PaymentMethodUpdateParamsBillingDetailsAddress"
    ]
    """
    Billing address.
    """
    email: NotRequired["Literal['']|str"]
    """
    Email address.
    """
    name: NotRequired["Literal['']|str"]
    """
    Full name.
    """
    phone: NotRequired["Literal['']|str"]
    """
    Billing phone number (including extension).
    """
    tax_id: NotRequired[str]
    """
    Taxpayer identification number. Used only for transactions between LATAM buyers and non-LATAM sellers.
    """


class PaymentMethodUpdateParamsBillingDetailsAddress(TypedDict):
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


class PaymentMethodUpdateParamsCard(TypedDict):
    exp_month: NotRequired[int]
    """
    Two-digit number representing the card's expiration month.
    """
    exp_year: NotRequired[int]
    """
    Four-digit number representing the card's expiration year.
    """
    networks: NotRequired["PaymentMethodUpdateParamsCardNetworks"]
    """
    Contains information about card networks used to process the payment.
    """


class PaymentMethodUpdateParamsCardNetworks(TypedDict):
    preferred: NotRequired[
        "Literal['']|Literal['cartes_bancaires', 'mastercard', 'visa']"
    ]
    """
    The customer's preferred card network for co-branded cards. Supports `cartes_bancaires`, `mastercard`, or `visa`. Selection of a network that does not apply to the card will be stored as `invalid_preference` on the card.
    """


class PaymentMethodUpdateParamsPayto(TypedDict):
    account_number: NotRequired[str]
    """
    The account number for the bank account.
    """
    bsb_number: NotRequired[str]
    """
    Bank-State-Branch number of the bank account.
    """
    pay_id: NotRequired[str]
    """
    The PayID alias for the bank account.
    """


class PaymentMethodUpdateParamsUsBankAccount(TypedDict):
    account_holder_type: NotRequired[Literal["company", "individual"]]
    """
    Bank account holder type.
    """
    account_type: NotRequired[Literal["checking", "savings"]]
    """
    Bank account type.
    """
