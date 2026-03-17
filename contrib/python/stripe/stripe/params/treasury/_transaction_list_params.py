# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class TransactionListParams(RequestOptions):
    created: NotRequired["TransactionListParamsCreated|int"]
    """
    Only return Transactions that were created during the given date interval.
    """
    ending_before: NotRequired[str]
    """
    A cursor for use in pagination. `ending_before` is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, starting with `obj_bar`, your subsequent call can include `ending_before=obj_bar` in order to fetch the previous page of the list.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    financial_account: str
    """
    Returns objects associated with this FinancialAccount.
    """
    limit: NotRequired[int]
    """
    A limit on the number of objects to be returned. Limit can range between 1 and 100, and the default is 10.
    """
    order_by: NotRequired[Literal["created", "posted_at"]]
    """
    The results are in reverse chronological order by `created` or `posted_at`. The default is `created`.
    """
    starting_after: NotRequired[str]
    """
    A cursor for use in pagination. `starting_after` is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, ending with `obj_foo`, your subsequent call can include `starting_after=obj_foo` in order to fetch the next page of the list.
    """
    status: NotRequired[Literal["open", "posted", "void"]]
    """
    Only return Transactions that have the given status: `open`, `posted`, or `void`.
    """
    status_transitions: NotRequired["TransactionListParamsStatusTransitions"]
    """
    A filter for the `status_transitions.posted_at` timestamp. When using this filter, `status=posted` and `order_by=posted_at` must also be specified.
    """


class TransactionListParamsCreated(TypedDict):
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


class TransactionListParamsStatusTransitions(TypedDict):
    posted_at: NotRequired[
        "TransactionListParamsStatusTransitionsPostedAt|int"
    ]
    """
    Returns Transactions with `posted_at` within the specified range.
    """


class TransactionListParamsStatusTransitionsPostedAt(TypedDict):
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
