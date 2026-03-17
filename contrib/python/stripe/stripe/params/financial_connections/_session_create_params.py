# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class SessionCreateParams(RequestOptions):
    account_holder: "SessionCreateParamsAccountHolder"
    """
    The account holder to link accounts for.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    filters: NotRequired["SessionCreateParamsFilters"]
    """
    Filters to restrict the kinds of accounts to collect.
    """
    permissions: List[
        Literal["balances", "ownership", "payment_method", "transactions"]
    ]
    """
    List of data features that you would like to request access to.

    Possible values are `balances`, `transactions`, `ownership`, and `payment_method`.
    """
    prefetch: NotRequired[
        List[Literal["balances", "ownership", "transactions"]]
    ]
    """
    List of data features that you would like to retrieve upon account creation.
    """
    return_url: NotRequired[str]
    """
    For webview integrations only. Upon completing OAuth login in the native browser, the user will be redirected to this URL to return to your app.
    """


class SessionCreateParamsAccountHolder(TypedDict):
    account: NotRequired[str]
    """
    The ID of the Stripe account whose accounts you will retrieve. Only available when `type` is `account`.
    """
    customer: NotRequired[str]
    """
    The ID of the Stripe customer whose accounts you will retrieve. Only available when `type` is `customer`.
    """
    customer_account: NotRequired[str]
    """
    The ID of Account representing a customer whose accounts you will retrieve. Only available when `type` is `customer`.
    """
    type: Literal["account", "customer"]
    """
    Type of account holder to collect accounts for.
    """


class SessionCreateParamsFilters(TypedDict):
    account_subcategories: NotRequired[
        List[
            Literal[
                "checking",
                "credit_card",
                "line_of_credit",
                "mortgage",
                "savings",
            ]
        ]
    ]
    """
    Restricts the Session to subcategories of accounts that can be linked. Valid subcategories are: `checking`, `savings`, `mortgage`, `line_of_credit`, `credit_card`.
    """
    countries: NotRequired[List[str]]
    """
    List of countries from which to collect accounts.
    """
