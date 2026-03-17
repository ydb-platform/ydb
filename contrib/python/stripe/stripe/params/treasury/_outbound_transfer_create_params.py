# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class OutboundTransferCreateParams(RequestOptions):
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
    destination_payment_method: NotRequired[str]
    """
    The PaymentMethod to use as the payment instrument for the OutboundTransfer.
    """
    destination_payment_method_data: NotRequired[
        "OutboundTransferCreateParamsDestinationPaymentMethodData"
    ]
    """
    Hash used to generate the PaymentMethod to be used for this OutboundTransfer. Exclusive with `destination_payment_method`.
    """
    destination_payment_method_options: NotRequired[
        "OutboundTransferCreateParamsDestinationPaymentMethodOptions"
    ]
    """
    Hash describing payment method configuration details.
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
    Statement descriptor to be shown on the receiving end of an OutboundTransfer. Maximum 10 characters for `ach` transfers or 140 characters for `us_domestic_wire` transfers. The default value is "transfer". Can only include -#.$&*, spaces, and alphanumeric characters.
    """


class OutboundTransferCreateParamsDestinationPaymentMethodData(TypedDict):
    financial_account: NotRequired[str]
    """
    Required if type is set to `financial_account`. The FinancialAccount ID to send funds to.
    """
    type: Literal["financial_account"]
    """
    The type of the destination.
    """


class OutboundTransferCreateParamsDestinationPaymentMethodOptions(TypedDict):
    us_bank_account: NotRequired[
        "Literal['']|OutboundTransferCreateParamsDestinationPaymentMethodOptionsUsBankAccount"
    ]
    """
    Optional fields for `us_bank_account`.
    """


class OutboundTransferCreateParamsDestinationPaymentMethodOptionsUsBankAccount(
    TypedDict,
):
    network: NotRequired[Literal["ach", "us_domestic_wire"]]
    """
    Specifies the network rails to be used. If not set, will default to the PaymentMethod's preferred network. See the [docs](https://docs.stripe.com/treasury/money-movement/timelines) to learn more about money movement timelines for each network type.
    """
