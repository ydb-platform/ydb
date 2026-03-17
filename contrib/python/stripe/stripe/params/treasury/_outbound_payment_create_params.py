# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class OutboundPaymentCreateParams(RequestOptions):
    amount: int
    """
    Amount (in cents) to be transferred.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    customer: NotRequired[str]
    """
    ID of the customer to whom the OutboundPayment is sent. Must match the Customer attached to the `destination_payment_method` passed in.
    """
    description: NotRequired[str]
    """
    An arbitrary string attached to the object. Often useful for displaying to users.
    """
    destination_payment_method: NotRequired[str]
    """
    The PaymentMethod to use as the payment instrument for the OutboundPayment. Exclusive with `destination_payment_method_data`.
    """
    destination_payment_method_data: NotRequired[
        "OutboundPaymentCreateParamsDestinationPaymentMethodData"
    ]
    """
    Hash used to generate the PaymentMethod to be used for this OutboundPayment. Exclusive with `destination_payment_method`.
    """
    destination_payment_method_options: NotRequired[
        "OutboundPaymentCreateParamsDestinationPaymentMethodOptions"
    ]
    """
    Payment method-specific configuration for this OutboundPayment.
    """
    end_user_details: NotRequired["OutboundPaymentCreateParamsEndUserDetails"]
    """
    End user details.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    financial_account: str
    """
    The FinancialAccount to pull funds from.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    statement_descriptor: NotRequired[str]
    """
    The description that appears on the receiving end for this OutboundPayment (for example, bank statement for external bank transfer). Maximum 10 characters for `ach` payments, 140 characters for `us_domestic_wire` payments, or 500 characters for `stripe` network transfers. Can only include -#.$&*, spaces, and alphanumeric characters. The default value is "payment".
    """


class OutboundPaymentCreateParamsDestinationPaymentMethodData(TypedDict):
    billing_details: NotRequired[
        "OutboundPaymentCreateParamsDestinationPaymentMethodDataBillingDetails"
    ]
    """
    Billing information associated with the PaymentMethod that may be used or required by particular types of payment methods.
    """
    financial_account: NotRequired[str]
    """
    Required if type is set to `financial_account`. The FinancialAccount ID to send funds to.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    type: Literal["financial_account", "us_bank_account"]
    """
    The type of the PaymentMethod. An additional hash is included on the PaymentMethod with a name matching this value. It contains additional information specific to the PaymentMethod type.
    """
    us_bank_account: NotRequired[
        "OutboundPaymentCreateParamsDestinationPaymentMethodDataUsBankAccount"
    ]
    """
    Required hash if type is set to `us_bank_account`.
    """


class OutboundPaymentCreateParamsDestinationPaymentMethodDataBillingDetails(
    TypedDict,
):
    address: NotRequired[
        "Literal['']|OutboundPaymentCreateParamsDestinationPaymentMethodDataBillingDetailsAddress"
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


class OutboundPaymentCreateParamsDestinationPaymentMethodDataBillingDetailsAddress(
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


class OutboundPaymentCreateParamsDestinationPaymentMethodDataUsBankAccount(
    TypedDict,
):
    account_holder_type: NotRequired[Literal["company", "individual"]]
    """
    Account holder type: individual or company.
    """
    account_number: NotRequired[str]
    """
    Account number of the bank account.
    """
    account_type: NotRequired[Literal["checking", "savings"]]
    """
    Account type: checkings or savings. Defaults to checking if omitted.
    """
    financial_connections_account: NotRequired[str]
    """
    The ID of a Financial Connections Account to use as a payment method.
    """
    routing_number: NotRequired[str]
    """
    Routing number of the bank account.
    """


class OutboundPaymentCreateParamsDestinationPaymentMethodOptions(TypedDict):
    us_bank_account: NotRequired[
        "Literal['']|OutboundPaymentCreateParamsDestinationPaymentMethodOptionsUsBankAccount"
    ]
    """
    Optional fields for `us_bank_account`.
    """


class OutboundPaymentCreateParamsDestinationPaymentMethodOptionsUsBankAccount(
    TypedDict,
):
    network: NotRequired[Literal["ach", "us_domestic_wire"]]
    """
    Specifies the network rails to be used. If not set, will default to the PaymentMethod's preferred network. See the [docs](https://docs.stripe.com/treasury/money-movement/timelines) to learn more about money movement timelines for each network type.
    """


class OutboundPaymentCreateParamsEndUserDetails(TypedDict):
    ip_address: NotRequired[str]
    """
    IP address of the user initiating the OutboundPayment. Must be supplied if `present` is set to `true`.
    """
    present: bool
    """
    `True` if the OutboundPayment creation request is being made on behalf of an end user by a platform. Otherwise, `false`.
    """
