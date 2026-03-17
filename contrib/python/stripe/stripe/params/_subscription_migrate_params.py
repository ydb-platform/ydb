# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class SubscriptionMigrateParams(RequestOptions):
    billing_mode: "SubscriptionMigrateParamsBillingMode"
    """
    Controls how prorations and invoices for subscriptions are calculated and orchestrated.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """


class SubscriptionMigrateParamsBillingMode(TypedDict):
    flexible: NotRequired["SubscriptionMigrateParamsBillingModeFlexible"]
    """
    Configure behavior for flexible billing mode.
    """
    type: Literal["flexible"]
    """
    Controls the calculation and orchestration of prorations and invoices for subscriptions.
    """


class SubscriptionMigrateParamsBillingModeFlexible(TypedDict):
    proration_discounts: NotRequired[Literal["included", "itemized"]]
    """
    Controls how invoices and invoice items display proration amounts and discount amounts.
    """
