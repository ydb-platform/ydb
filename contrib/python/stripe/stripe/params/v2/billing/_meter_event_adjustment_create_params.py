# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing_extensions import Literal, TypedDict


class MeterEventAdjustmentCreateParams(TypedDict):
    cancel: "MeterEventAdjustmentCreateParamsCancel"
    """
    Specifies which event to cancel.
    """
    event_name: str
    """
    The name of the meter event. Corresponds with the `event_name` field on a meter.
    """
    type: Literal["cancel"]
    """
    Specifies whether to cancel a single event or a range of events for a time period. Time period cancellation is not supported yet.
    """


class MeterEventAdjustmentCreateParamsCancel(TypedDict):
    identifier: str
    """
    Unique identifier for the event. You can only cancel events within 24 hours of Stripe receiving them.
    """
