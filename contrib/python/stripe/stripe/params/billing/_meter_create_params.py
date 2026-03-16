# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class MeterCreateParams(RequestOptions):
    customer_mapping: NotRequired["MeterCreateParamsCustomerMapping"]
    """
    Fields that specify how to map a meter event to a customer.
    """
    default_aggregation: "MeterCreateParamsDefaultAggregation"
    """
    The default settings to aggregate a meter's events with.
    """
    display_name: str
    """
    The meter's name. Not visible to the customer.
    """
    event_name: str
    """
    The name of the meter event to record usage for. Corresponds with the `event_name` field on meter events.
    """
    event_time_window: NotRequired[Literal["day", "hour"]]
    """
    The time window which meter events have been pre-aggregated for, if any.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    value_settings: NotRequired["MeterCreateParamsValueSettings"]
    """
    Fields that specify how to calculate a meter event's value.
    """


class MeterCreateParamsCustomerMapping(TypedDict):
    event_payload_key: str
    """
    The key in the meter event payload to use for mapping the event to a customer.
    """
    type: Literal["by_id"]
    """
    The method for mapping a meter event to a customer. Must be `by_id`.
    """


class MeterCreateParamsDefaultAggregation(TypedDict):
    formula: Literal["count", "last", "sum"]
    """
    Specifies how events are aggregated. Allowed values are `count` to count the number of events, `sum` to sum each event's value and `last` to take the last event's value in the window.
    """


class MeterCreateParamsValueSettings(TypedDict):
    event_payload_key: str
    """
    The key in the usage event payload to use as the value for this meter. For example, if the event payload contains usage on a `bytes_used` field, then set the event_payload_key to "bytes_used".
    """
