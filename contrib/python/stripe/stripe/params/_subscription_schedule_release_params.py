# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import NotRequired


class SubscriptionScheduleReleaseParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    preserve_cancel_date: NotRequired[bool]
    """
    Keep any cancellation on the subscription that the schedule has set
    """
