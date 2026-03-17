# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired


class MeterListEventSummariesParams(RequestOptions):
    customer: str
    """
    The customer for which to fetch event summaries.
    """
    end_time: int
    """
    The timestamp from when to stop aggregating meter events (exclusive). Must be aligned with minute boundaries.
    """
    ending_before: NotRequired[str]
    """
    A cursor for use in pagination. `ending_before` is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, starting with `obj_bar`, your subsequent call can include `ending_before=obj_bar` in order to fetch the previous page of the list.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    limit: NotRequired[int]
    """
    A limit on the number of objects to be returned. Limit can range between 1 and 100, and the default is 10.
    """
    start_time: int
    """
    The timestamp from when to start aggregating meter events (inclusive). Must be aligned with minute boundaries.
    """
    starting_after: NotRequired[str]
    """
    A cursor for use in pagination. `starting_after` is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, ending with `obj_foo`, your subsequent call can include `starting_after=obj_foo` in order to fetch the next page of the list.
    """
    value_grouping_window: NotRequired[Literal["day", "hour"]]
    """
    Specifies what granularity to use when generating event summaries. If not specified, a single event summary would be returned for the specified time range. For hourly granularity, start and end times must align with hour boundaries (e.g., 00:00, 01:00, ..., 23:00). For daily granularity, start and end times must align with UTC day boundaries (00:00 UTC).
    """
