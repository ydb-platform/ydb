# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class FinancialAccountModifyParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    features: NotRequired["FinancialAccountModifyParamsFeatures"]
    """
    Encodes whether a FinancialAccount has access to a particular feature, with a status enum and associated `status_details`. Stripe or the platform may control features via the requested field.
    """
    forwarding_settings: NotRequired[
        "FinancialAccountModifyParamsForwardingSettings"
    ]
    """
    A different bank account where funds can be deposited/debited in order to get the closing FA's balance to $0
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    nickname: NotRequired["Literal['']|str"]
    """
    The nickname for the FinancialAccount.
    """
    platform_restrictions: NotRequired[
        "FinancialAccountModifyParamsPlatformRestrictions"
    ]
    """
    The set of functionalities that the platform can restrict on the FinancialAccount.
    """


class FinancialAccountModifyParamsFeatures(TypedDict):
    card_issuing: NotRequired[
        "FinancialAccountModifyParamsFeaturesCardIssuing"
    ]
    """
    Encodes the FinancialAccount's ability to be used with the Issuing product, including attaching cards to and drawing funds from the FinancialAccount.
    """
    deposit_insurance: NotRequired[
        "FinancialAccountModifyParamsFeaturesDepositInsurance"
    ]
    """
    Represents whether this FinancialAccount is eligible for deposit insurance. Various factors determine the insurance amount.
    """
    financial_addresses: NotRequired[
        "FinancialAccountModifyParamsFeaturesFinancialAddresses"
    ]
    """
    Contains Features that add FinancialAddresses to the FinancialAccount.
    """
    inbound_transfers: NotRequired[
        "FinancialAccountModifyParamsFeaturesInboundTransfers"
    ]
    """
    Contains settings related to adding funds to a FinancialAccount from another Account with the same owner.
    """
    intra_stripe_flows: NotRequired[
        "FinancialAccountModifyParamsFeaturesIntraStripeFlows"
    ]
    """
    Represents the ability for the FinancialAccount to send money to, or receive money from other FinancialAccounts (for example, via OutboundPayment).
    """
    outbound_payments: NotRequired[
        "FinancialAccountModifyParamsFeaturesOutboundPayments"
    ]
    """
    Includes Features related to initiating money movement out of the FinancialAccount to someone else's bucket of money.
    """
    outbound_transfers: NotRequired[
        "FinancialAccountModifyParamsFeaturesOutboundTransfers"
    ]
    """
    Contains a Feature and settings related to moving money out of the FinancialAccount into another Account with the same owner.
    """


class FinancialAccountModifyParamsFeaturesCardIssuing(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountModifyParamsFeaturesDepositInsurance(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountModifyParamsFeaturesFinancialAddresses(TypedDict):
    aba: NotRequired[
        "FinancialAccountModifyParamsFeaturesFinancialAddressesAba"
    ]
    """
    Adds an ABA FinancialAddress to the FinancialAccount.
    """


class FinancialAccountModifyParamsFeaturesFinancialAddressesAba(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountModifyParamsFeaturesInboundTransfers(TypedDict):
    ach: NotRequired["FinancialAccountModifyParamsFeaturesInboundTransfersAch"]
    """
    Enables ACH Debits via the InboundTransfers API.
    """


class FinancialAccountModifyParamsFeaturesInboundTransfersAch(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountModifyParamsFeaturesIntraStripeFlows(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountModifyParamsFeaturesOutboundPayments(TypedDict):
    ach: NotRequired["FinancialAccountModifyParamsFeaturesOutboundPaymentsAch"]
    """
    Enables ACH transfers via the OutboundPayments API.
    """
    us_domestic_wire: NotRequired[
        "FinancialAccountModifyParamsFeaturesOutboundPaymentsUsDomesticWire"
    ]
    """
    Enables US domestic wire transfers via the OutboundPayments API.
    """


class FinancialAccountModifyParamsFeaturesOutboundPaymentsAch(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountModifyParamsFeaturesOutboundPaymentsUsDomesticWire(
    TypedDict,
):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountModifyParamsFeaturesOutboundTransfers(TypedDict):
    ach: NotRequired[
        "FinancialAccountModifyParamsFeaturesOutboundTransfersAch"
    ]
    """
    Enables ACH transfers via the OutboundTransfers API.
    """
    us_domestic_wire: NotRequired[
        "FinancialAccountModifyParamsFeaturesOutboundTransfersUsDomesticWire"
    ]
    """
    Enables US domestic wire transfers via the OutboundTransfers API.
    """


class FinancialAccountModifyParamsFeaturesOutboundTransfersAch(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountModifyParamsFeaturesOutboundTransfersUsDomesticWire(
    TypedDict,
):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountModifyParamsForwardingSettings(TypedDict):
    financial_account: NotRequired[str]
    """
    The financial_account id
    """
    payment_method: NotRequired[str]
    """
    The payment_method or bank account id. This needs to be a verified bank account.
    """
    type: Literal["financial_account", "payment_method"]
    """
    The type of the bank account provided. This can be either "financial_account" or "payment_method"
    """


class FinancialAccountModifyParamsPlatformRestrictions(TypedDict):
    inbound_flows: NotRequired[Literal["restricted", "unrestricted"]]
    """
    Restricts all inbound money movement.
    """
    outbound_flows: NotRequired[Literal["restricted", "unrestricted"]]
    """
    Restricts all outbound money movement.
    """
