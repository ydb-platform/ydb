# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import List
from typing_extensions import NotRequired, TypedDict


class EventListParams(TypedDict):
    created: NotRequired["EventListParamsCreated"]
    """
    Set of filters to query events within a range of `created` timestamps.
    """
    limit: NotRequired[int]
    """
    The page size.
    """
    object_id: NotRequired[str]
    """
    Primary object ID used to retrieve related events.
    """
    types: NotRequired[List[str]]
    """
    An array of up to 20 strings containing specific event names.
    """


class EventListParamsCreated(TypedDict):
    gt: NotRequired[str]
    """
    Filter for events created after the specified timestamp.
    """
    gte: NotRequired[str]
    """
    Filter for events created at or after the specified timestamp.
    """
    lt: NotRequired[str]
    """
    Filter for events created before the specified timestamp.
    """
    lte: NotRequired[str]
    """
    Filter for events created at or before the specified timestamp.
    """
