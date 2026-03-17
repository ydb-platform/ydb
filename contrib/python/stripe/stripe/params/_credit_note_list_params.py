# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import NotRequired, TypedDict


class CreditNoteListParams(RequestOptions):
    created: NotRequired["CreditNoteListParamsCreated|int"]
    """
    Only return credit notes that were created during the given date interval.
    """
    customer: NotRequired[str]
    """
    Only return credit notes for the customer specified by this customer ID.
    """
    customer_account: NotRequired[str]
    """
    Only return credit notes for the account representing the customer specified by this account ID.
    """
    ending_before: NotRequired[str]
    """
    A cursor for use in pagination. `ending_before` is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, starting with `obj_bar`, your subsequent call can include `ending_before=obj_bar` in order to fetch the previous page of the list.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    invoice: NotRequired[str]
    """
    Only return credit notes for the invoice specified by this invoice ID.
    """
    limit: NotRequired[int]
    """
    A limit on the number of objects to be returned. Limit can range between 1 and 100, and the default is 10.
    """
    starting_after: NotRequired[str]
    """
    A cursor for use in pagination. `starting_after` is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, ending with `obj_foo`, your subsequent call can include `starting_after=obj_foo` in order to fetch the next page of the list.
    """


class CreditNoteListParamsCreated(TypedDict):
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
