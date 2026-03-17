# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class CustomerModifyCashBalanceParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    settings: NotRequired["CustomerModifyCashBalanceParamsSettings"]
    """
    A hash of settings for this cash balance.
    """


class CustomerModifyCashBalanceParamsSettings(TypedDict):
    reconciliation_mode: NotRequired[
        Literal["automatic", "manual", "merchant_default"]
    ]
    """
    Controls how funds transferred by the customer are applied to payment intents and invoices. Valid options are `automatic`, `manual`, or `merchant_default`. For more information about these reconciliation modes, see [Reconciliation](https://docs.stripe.com/payments/customer-balance/reconciliation).
    """
