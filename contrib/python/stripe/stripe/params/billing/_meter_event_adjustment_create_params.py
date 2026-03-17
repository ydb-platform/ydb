# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class MeterEventAdjustmentCreateParams(RequestOptions):
    cancel: NotRequired["MeterEventAdjustmentCreateParamsCancel"]
    """
    Specifies which event to cancel.
    """
    event_name: str
    """
    The name of the meter event. Corresponds with the `event_name` field on a meter.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    type: Literal["cancel"]
    """
    Specifies whether to cancel a single event or a range of events for a time period. Time period cancellation is not supported yet.
    """


class MeterEventAdjustmentCreateParamsCancel(TypedDict):
    identifier: NotRequired[str]
    """
    Unique identifier for the event. You can only cancel events within 24 hours of Stripe receiving them.
    """
