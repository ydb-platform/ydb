# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class FinancialAccountCreateParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    features: NotRequired["FinancialAccountCreateParamsFeatures"]
    """
    Encodes whether a FinancialAccount has access to a particular feature. Stripe or the platform can control features via the requested field.
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
        "FinancialAccountCreateParamsPlatformRestrictions"
    ]
    """
    The set of functionalities that the platform can restrict on the FinancialAccount.
    """
    supported_currencies: List[str]
    """
    The currencies the FinancialAccount can hold a balance in.
    """


class FinancialAccountCreateParamsFeatures(TypedDict):
    card_issuing: NotRequired[
        "FinancialAccountCreateParamsFeaturesCardIssuing"
    ]
    """
    Encodes the FinancialAccount's ability to be used with the Issuing product, including attaching cards to and drawing funds from the FinancialAccount.
    """
    deposit_insurance: NotRequired[
        "FinancialAccountCreateParamsFeaturesDepositInsurance"
    ]
    """
    Represents whether this FinancialAccount is eligible for deposit insurance. Various factors determine the insurance amount.
    """
    financial_addresses: NotRequired[
        "FinancialAccountCreateParamsFeaturesFinancialAddresses"
    ]
    """
    Contains Features that add FinancialAddresses to the FinancialAccount.
    """
    inbound_transfers: NotRequired[
        "FinancialAccountCreateParamsFeaturesInboundTransfers"
    ]
    """
    Contains settings related to adding funds to a FinancialAccount from another Account with the same owner.
    """
    intra_stripe_flows: NotRequired[
        "FinancialAccountCreateParamsFeaturesIntraStripeFlows"
    ]
    """
    Represents the ability for the FinancialAccount to send money to, or receive money from other FinancialAccounts (for example, via OutboundPayment).
    """
    outbound_payments: NotRequired[
        "FinancialAccountCreateParamsFeaturesOutboundPayments"
    ]
    """
    Includes Features related to initiating money movement out of the FinancialAccount to someone else's bucket of money.
    """
    outbound_transfers: NotRequired[
        "FinancialAccountCreateParamsFeaturesOutboundTransfers"
    ]
    """
    Contains a Feature and settings related to moving money out of the FinancialAccount into another Account with the same owner.
    """


class FinancialAccountCreateParamsFeaturesCardIssuing(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountCreateParamsFeaturesDepositInsurance(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountCreateParamsFeaturesFinancialAddresses(TypedDict):
    aba: NotRequired[
        "FinancialAccountCreateParamsFeaturesFinancialAddressesAba"
    ]
    """
    Adds an ABA FinancialAddress to the FinancialAccount.
    """


class FinancialAccountCreateParamsFeaturesFinancialAddressesAba(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountCreateParamsFeaturesInboundTransfers(TypedDict):
    ach: NotRequired["FinancialAccountCreateParamsFeaturesInboundTransfersAch"]
    """
    Enables ACH Debits via the InboundTransfers API.
    """


class FinancialAccountCreateParamsFeaturesInboundTransfersAch(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountCreateParamsFeaturesIntraStripeFlows(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountCreateParamsFeaturesOutboundPayments(TypedDict):
    ach: NotRequired["FinancialAccountCreateParamsFeaturesOutboundPaymentsAch"]
    """
    Enables ACH transfers via the OutboundPayments API.
    """
    us_domestic_wire: NotRequired[
        "FinancialAccountCreateParamsFeaturesOutboundPaymentsUsDomesticWire"
    ]
    """
    Enables US domestic wire transfers via the OutboundPayments API.
    """


class FinancialAccountCreateParamsFeaturesOutboundPaymentsAch(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountCreateParamsFeaturesOutboundPaymentsUsDomesticWire(
    TypedDict,
):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountCreateParamsFeaturesOutboundTransfers(TypedDict):
    ach: NotRequired[
        "FinancialAccountCreateParamsFeaturesOutboundTransfersAch"
    ]
    """
    Enables ACH transfers via the OutboundTransfers API.
    """
    us_domestic_wire: NotRequired[
        "FinancialAccountCreateParamsFeaturesOutboundTransfersUsDomesticWire"
    ]
    """
    Enables US domestic wire transfers via the OutboundTransfers API.
    """


class FinancialAccountCreateParamsFeaturesOutboundTransfersAch(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountCreateParamsFeaturesOutboundTransfersUsDomesticWire(
    TypedDict,
):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountCreateParamsPlatformRestrictions(TypedDict):
    inbound_flows: NotRequired[Literal["restricted", "unrestricted"]]
    """
    Restricts all inbound money movement.
    """
    outbound_flows: NotRequired[Literal["restricted", "unrestricted"]]
    """
    Restricts all outbound money movement.
    """
