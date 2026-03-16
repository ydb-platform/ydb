# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import List
from typing_extensions import NotRequired, TypedDict


class FinancialAccountFeaturesUpdateParams(TypedDict):
    card_issuing: NotRequired[
        "FinancialAccountFeaturesUpdateParamsCardIssuing"
    ]
    """
    Encodes the FinancialAccount's ability to be used with the Issuing product, including attaching cards to and drawing funds from the FinancialAccount.
    """
    deposit_insurance: NotRequired[
        "FinancialAccountFeaturesUpdateParamsDepositInsurance"
    ]
    """
    Represents whether this FinancialAccount is eligible for deposit insurance. Various factors determine the insurance amount.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    financial_addresses: NotRequired[
        "FinancialAccountFeaturesUpdateParamsFinancialAddresses"
    ]
    """
    Contains Features that add FinancialAddresses to the FinancialAccount.
    """
    inbound_transfers: NotRequired[
        "FinancialAccountFeaturesUpdateParamsInboundTransfers"
    ]
    """
    Contains settings related to adding funds to a FinancialAccount from another Account with the same owner.
    """
    intra_stripe_flows: NotRequired[
        "FinancialAccountFeaturesUpdateParamsIntraStripeFlows"
    ]
    """
    Represents the ability for the FinancialAccount to send money to, or receive money from other FinancialAccounts (for example, via OutboundPayment).
    """
    outbound_payments: NotRequired[
        "FinancialAccountFeaturesUpdateParamsOutboundPayments"
    ]
    """
    Includes Features related to initiating money movement out of the FinancialAccount to someone else's bucket of money.
    """
    outbound_transfers: NotRequired[
        "FinancialAccountFeaturesUpdateParamsOutboundTransfers"
    ]
    """
    Contains a Feature and settings related to moving money out of the FinancialAccount into another Account with the same owner.
    """


class FinancialAccountFeaturesUpdateParamsCardIssuing(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountFeaturesUpdateParamsDepositInsurance(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountFeaturesUpdateParamsFinancialAddresses(TypedDict):
    aba: NotRequired[
        "FinancialAccountFeaturesUpdateParamsFinancialAddressesAba"
    ]
    """
    Adds an ABA FinancialAddress to the FinancialAccount.
    """


class FinancialAccountFeaturesUpdateParamsFinancialAddressesAba(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountFeaturesUpdateParamsInboundTransfers(TypedDict):
    ach: NotRequired["FinancialAccountFeaturesUpdateParamsInboundTransfersAch"]
    """
    Enables ACH Debits via the InboundTransfers API.
    """


class FinancialAccountFeaturesUpdateParamsInboundTransfersAch(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountFeaturesUpdateParamsIntraStripeFlows(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountFeaturesUpdateParamsOutboundPayments(TypedDict):
    ach: NotRequired["FinancialAccountFeaturesUpdateParamsOutboundPaymentsAch"]
    """
    Enables ACH transfers via the OutboundPayments API.
    """
    us_domestic_wire: NotRequired[
        "FinancialAccountFeaturesUpdateParamsOutboundPaymentsUsDomesticWire"
    ]
    """
    Enables US domestic wire transfers via the OutboundPayments API.
    """


class FinancialAccountFeaturesUpdateParamsOutboundPaymentsAch(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountFeaturesUpdateParamsOutboundPaymentsUsDomesticWire(
    TypedDict,
):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountFeaturesUpdateParamsOutboundTransfers(TypedDict):
    ach: NotRequired[
        "FinancialAccountFeaturesUpdateParamsOutboundTransfersAch"
    ]
    """
    Enables ACH transfers via the OutboundTransfers API.
    """
    us_domestic_wire: NotRequired[
        "FinancialAccountFeaturesUpdateParamsOutboundTransfersUsDomesticWire"
    ]
    """
    Enables US domestic wire transfers via the OutboundTransfers API.
    """


class FinancialAccountFeaturesUpdateParamsOutboundTransfersAch(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountFeaturesUpdateParamsOutboundTransfersUsDomesticWire(
    TypedDict,
):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """
