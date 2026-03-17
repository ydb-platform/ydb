# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import NotRequired


class SubscriptionScheduleCancelParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    invoice_now: NotRequired[bool]
    """
    If the subscription schedule is `active`, indicates if a final invoice will be generated that contains any un-invoiced metered usage and new/pending proration invoice items. Defaults to `true`.
    """
    prorate: NotRequired[bool]
    """
    If the subscription schedule is `active`, indicates if the cancellation should be prorated. Defaults to `true`.
    """
