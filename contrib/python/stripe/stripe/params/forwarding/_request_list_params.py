# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import NotRequired, TypedDict


class RequestListParams(RequestOptions):
    created: NotRequired["RequestListParamsCreated"]
    """
    Similar to other List endpoints, filters results based on created timestamp. You can pass gt, gte, lt, and lte timestamp values.
    """
    ending_before: NotRequired[str]
    """
    A pagination cursor to fetch the previous page of the list. The value must be a ForwardingRequest ID.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    limit: NotRequired[int]
    """
    A limit on the number of objects to be returned. Limit can range between 1 and 100, and the default is 10.
    """
    starting_after: NotRequired[str]
    """
    A pagination cursor to fetch the next page of the list. The value must be a ForwardingRequest ID.
    """


class RequestListParamsCreated(TypedDict):
    gt: NotRequired[int]
    """
    Return results where the `created` field is greater than this value.
    """
    gte: NotRequired[int]
    """
    Return results where the `created` field is greater than or equal to this value.
    """
    lt: NotRequired[int]
    """
    Return results where the `created` field is less than this value.
    """
    lte: NotRequired[int]
    """
    Return results where the `created` field is less than or equal to this value.
    """
