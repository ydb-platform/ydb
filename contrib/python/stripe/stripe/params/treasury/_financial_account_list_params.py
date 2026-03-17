# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class FinancialAccountListParams(RequestOptions):
    created: NotRequired["FinancialAccountListParamsCreated|int"]
    """
    Only return FinancialAccounts that were created during the given date interval.
    """
    ending_before: NotRequired[str]
    """
    An object ID cursor for use in pagination.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    limit: NotRequired[int]
    """
    A limit ranging from 1 to 100 (defaults to 10).
    """
    starting_after: NotRequired[str]
    """
    An object ID cursor for use in pagination.
    """
    status: NotRequired[Literal["closed", "open"]]
    """
    Only return FinancialAccounts that have the given status: `open` or `closed`
    """


class FinancialAccountListParamsCreated(TypedDict):
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
