# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import NotRequired, TypedDict


class FinancialAccountUpdateFeaturesParams(RequestOptions):
    card_issuing: NotRequired[
        "FinancialAccountUpdateFeaturesParamsCardIssuing"
    ]
    """
    Encodes the FinancialAccount's ability to be used with the Issuing product, including attaching cards to and drawing funds from the FinancialAccount.
    """
    deposit_insurance: NotRequired[
        "FinancialAccountUpdateFeaturesParamsDepositInsurance"
    ]
    """
    Represents whether this FinancialAccount is eligible for deposit insurance. Various factors determine the insurance amount.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    financial_addresses: NotRequired[
        "FinancialAccountUpdateFeaturesParamsFinancialAddresses"
    ]
    """
    Contains Features that add FinancialAddresses to the FinancialAccount.
    """
    inbound_transfers: NotRequired[
        "FinancialAccountUpdateFeaturesParamsInboundTransfers"
    ]
    """
    Contains settings related to adding funds to a FinancialAccount from another Account with the same owner.
    """
    intra_stripe_flows: NotRequired[
        "FinancialAccountUpdateFeaturesParamsIntraStripeFlows"
    ]
    """
    Represents the ability for the FinancialAccount to send money to, or receive money from other FinancialAccounts (for example, via OutboundPayment).
    """
    outbound_payments: NotRequired[
        "FinancialAccountUpdateFeaturesParamsOutboundPayments"
    ]
    """
    Includes Features related to initiating money movement out of the FinancialAccount to someone else's bucket of money.
    """
    outbound_transfers: NotRequired[
        "FinancialAccountUpdateFeaturesParamsOutboundTransfers"
    ]
    """
    Contains a Feature and settings related to moving money out of the FinancialAccount into another Account with the same owner.
    """


class FinancialAccountUpdateFeaturesParamsCardIssuing(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountUpdateFeaturesParamsDepositInsurance(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountUpdateFeaturesParamsFinancialAddresses(TypedDict):
    aba: NotRequired[
        "FinancialAccountUpdateFeaturesParamsFinancialAddressesAba"
    ]
    """
    Adds an ABA FinancialAddress to the FinancialAccount.
    """


class FinancialAccountUpdateFeaturesParamsFinancialAddressesAba(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountUpdateFeaturesParamsInboundTransfers(TypedDict):
    ach: NotRequired["FinancialAccountUpdateFeaturesParamsInboundTransfersAch"]
    """
    Enables ACH Debits via the InboundTransfers API.
    """


class FinancialAccountUpdateFeaturesParamsInboundTransfersAch(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountUpdateFeaturesParamsIntraStripeFlows(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountUpdateFeaturesParamsOutboundPayments(TypedDict):
    ach: NotRequired["FinancialAccountUpdateFeaturesParamsOutboundPaymentsAch"]
    """
    Enables ACH transfers via the OutboundPayments API.
    """
    us_domestic_wire: NotRequired[
        "FinancialAccountUpdateFeaturesParamsOutboundPaymentsUsDomesticWire"
    ]
    """
    Enables US domestic wire transfers via the OutboundPayments API.
    """


class FinancialAccountUpdateFeaturesParamsOutboundPaymentsAch(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountUpdateFeaturesParamsOutboundPaymentsUsDomesticWire(
    TypedDict,
):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountUpdateFeaturesParamsOutboundTransfers(TypedDict):
    ach: NotRequired[
        "FinancialAccountUpdateFeaturesParamsOutboundTransfersAch"
    ]
    """
    Enables ACH transfers via the OutboundTransfers API.
    """
    us_domestic_wire: NotRequired[
        "FinancialAccountUpdateFeaturesParamsOutboundTransfersUsDomesticWire"
    ]
    """
    Enables US domestic wire transfers via the OutboundTransfers API.
    """


class FinancialAccountUpdateFeaturesParamsOutboundTransfersAch(TypedDict):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """


class FinancialAccountUpdateFeaturesParamsOutboundTransfersUsDomesticWire(
    TypedDict,
):
    requested: bool
    """
    Whether the FinancialAccount should have the Feature.
    """
