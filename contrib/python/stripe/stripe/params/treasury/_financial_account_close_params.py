# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class FinancialAccountCloseParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    forwarding_settings: NotRequired[
        "FinancialAccountCloseParamsForwardingSettings"
    ]
    """
    A different bank account where funds can be deposited/debited in order to get the closing FA's balance to $0
    """


class FinancialAccountCloseParamsForwardingSettings(TypedDict):
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
