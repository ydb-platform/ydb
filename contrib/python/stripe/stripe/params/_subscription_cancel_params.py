# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class SubscriptionCancelParams(RequestOptions):
    cancellation_details: NotRequired[
        "SubscriptionCancelParamsCancellationDetails"
    ]
    """
    Details about why this subscription was cancelled
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    invoice_now: NotRequired[bool]
    """
    Will generate a final invoice that invoices for any un-invoiced metered usage and new/pending proration invoice items. Defaults to `false`.
    """
    prorate: NotRequired[bool]
    """
    Will generate a proration invoice item that credits remaining unused time until the subscription period end. Defaults to `false`.
    """


class SubscriptionCancelParamsCancellationDetails(TypedDict):
    comment: NotRequired["Literal['']|str"]
    """
    Additional comments about why the user canceled the subscription, if the subscription was canceled explicitly by the user.
    """
    feedback: NotRequired[
        "Literal['']|Literal['customer_service', 'low_quality', 'missing_features', 'other', 'switched_service', 'too_complex', 'too_expensive', 'unused']"
    ]
    """
    The customer submitted reason for why they canceled, if the subscription was canceled explicitly by the user.
    """
