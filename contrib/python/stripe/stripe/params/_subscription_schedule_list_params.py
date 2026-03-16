# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import NotRequired, TypedDict


class SubscriptionScheduleListParams(RequestOptions):
    canceled_at: NotRequired["SubscriptionScheduleListParamsCanceledAt|int"]
    """
    Only return subscription schedules that were created canceled the given date interval.
    """
    completed_at: NotRequired["SubscriptionScheduleListParamsCompletedAt|int"]
    """
    Only return subscription schedules that completed during the given date interval.
    """
    created: NotRequired["SubscriptionScheduleListParamsCreated|int"]
    """
    Only return subscription schedules that were created during the given date interval.
    """
    customer: NotRequired[str]
    """
    Only return subscription schedules for the given customer.
    """
    customer_account: NotRequired[str]
    """
    Only return subscription schedules for the given account.
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
    released_at: NotRequired["SubscriptionScheduleListParamsReleasedAt|int"]
    """
    Only return subscription schedules that were released during the given date interval.
    """
    scheduled: NotRequired[bool]
    """
    Only return subscription schedules that have not started yet.
    """
    starting_after: NotRequired[str]
    """
    A cursor for use in pagination. `starting_after` is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, ending with `obj_foo`, your subsequent call can include `starting_after=obj_foo` in order to fetch the next page of the list.
    """


class SubscriptionScheduleListParamsCanceledAt(TypedDict):
    gt: NotRequired[int]
    """
    Minimum value to filter by (exclusive)
    """
    gte: NotRequired[int]
    """
    Minimum value to filter by (inclusive)
    """
    lt: NotRequired[int]
    """
    Maximum value to filter by (exclusive)
    """
    lte: NotRequired[int]
    """
    Maximum value to filter by (inclusive)
    """


class SubscriptionScheduleListParamsCompletedAt(TypedDict):
    gt: NotRequired[int]
    """
    Minimum value to filter by (exclusive)
    """
    gte: NotRequired[int]
    """
    Minimum value to filter by (inclusive)
    """
    lt: NotRequired[int]
    """
    Maximum value to filter by (exclusive)
    """
    lte: NotRequired[int]
    """
    Maximum value to filter by (inclusive)
    """


class SubscriptionScheduleListParamsCreated(TypedDict):
    gt: NotRequired[int]
    """
    Minimum value to filter by (exclusive)
    """
    gte: NotRequired[int]
    """
    Minimum value to filter by (inclusive)
    """
    lt: NotRequired[int]
    """
    Maximum value to filter by (exclusive)
    """
    lte: NotRequired[int]
    """
    Maximum value to filter by (inclusive)
    """


class SubscriptionScheduleListParamsReleasedAt(TypedDict):
    gt: NotRequired[int]
    """
    Minimum value to filter by (exclusive)
    """
    gte: NotRequired[int]
    """
    Minimum value to filter by (inclusive)
    """
    lt: NotRequired[int]
    """
    Maximum value to filter by (exclusive)
    """
    lte: NotRequired[int]
    """
    Maximum value to filter by (inclusive)
    """
