# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_object import StripeObject
from typing import ClassVar
from typing_extensions import Literal


class MeterEventSummary(StripeObject):
    """
    A billing meter event summary represents an aggregated view of a customer's billing meter events within a specified timeframe. It indicates how much
    usage was accrued by a customer for that period.

    Note: Meters events are aggregated asynchronously so the meter event summaries provide an eventually consistent view of the reported usage.
    """

    OBJECT_NAME: ClassVar[Literal["billing.meter_event_summary"]] = (
        "billing.meter_event_summary"
    )
    aggregated_value: float
    """
    Aggregated value of all the events within `start_time` (inclusive) and `end_time` (inclusive). The aggregation strategy is defined on meter via `default_aggregation`.
    """
    end_time: int
    """
    End timestamp for this event summary (exclusive). Must be aligned with minute boundaries.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    meter: str
    """
    The meter associated with this event summary.
    """
    object: Literal["billing.meter_event_summary"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    start_time: int
    """
    Start timestamp for this event summary (inclusive). Must be aligned with minute boundaries.
    """
