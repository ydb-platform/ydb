# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class ReceivedDebitCreateParams(TypedDict):
    amount: int
    """
    Amount (in cents) to be transferred.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    description: NotRequired[str]
    """
    An arbitrary string attached to the object. Often useful for displaying to users.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    financial_account: str
    """
    The FinancialAccount to pull funds from.
    """
    initiating_payment_method_details: NotRequired[
        "ReceivedDebitCreateParamsInitiatingPaymentMethodDetails"
    ]
    """
    Initiating payment method details for the object.
    """
    network: Literal["ach"]
    """
    Specifies the network rails to be used. If not set, will default to the PaymentMethod's preferred network. See the [docs](https://docs.stripe.com/treasury/money-movement/timelines) to learn more about money movement timelines for each network type.
    """


class ReceivedDebitCreateParamsInitiatingPaymentMethodDetails(TypedDict):
    type: Literal["us_bank_account"]
    """
    The source type.
    """
    us_bank_account: NotRequired[
        "ReceivedDebitCreateParamsInitiatingPaymentMethodDetailsUsBankAccount"
    ]
    """
    Optional fields for `us_bank_account`.
    """


class ReceivedDebitCreateParamsInitiatingPaymentMethodDetailsUsBankAccount(
    TypedDict,
):
    account_holder_name: NotRequired[str]
    """
    The bank account holder's name.
    """
    account_number: NotRequired[str]
    """
    The bank account number.
    """
    routing_number: NotRequired[str]
    """
    The bank account's routing number.
    """
