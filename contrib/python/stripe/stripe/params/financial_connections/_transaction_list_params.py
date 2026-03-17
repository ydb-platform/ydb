# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import NotRequired, TypedDict


class TransactionListParams(RequestOptions):
    account: str
    """
    The ID of the Financial Connections Account whose transactions will be retrieved.
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
    starting_after: NotRequired[str]
    """
    A cursor for use in pagination. `starting_after` is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, ending with `obj_foo`, your subsequent call can include `starting_after=obj_foo` in order to fetch the next page of the list.
    """
    transacted_at: NotRequired["TransactionListParamsTransactedAt|int"]
    """
    A filter on the list based on the object `transacted_at` field. The value can be a string with an integer Unix timestamp, or it can be a dictionary with the following options:
    """
    transaction_refresh: NotRequired["TransactionListParamsTransactionRefresh"]
    """
    A filter on the list based on the object `transaction_refresh` field. The value can be a dictionary with the following options:
    """


class TransactionListParamsTransactedAt(TypedDict):
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


class TransactionListParamsTransactionRefresh(TypedDict):
    after: str
    """
    Return results where the transactions were created or updated by a refresh that took place after this refresh (non-inclusive).
    """
