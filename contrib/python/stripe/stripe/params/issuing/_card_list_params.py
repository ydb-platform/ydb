# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class CardListParams(RequestOptions):
    cardholder: NotRequired[str]
    """
    Only return cards belonging to the Cardholder with the provided ID.
    """
    created: NotRequired["CardListParamsCreated|int"]
    """
    Only return cards that were issued during the given date interval.
    """
    ending_before: NotRequired[str]
    """
    A cursor for use in pagination. `ending_before` is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, starting with `obj_bar`, your subsequent call can include `ending_before=obj_bar` in order to fetch the previous page of the list.
    """
    exp_month: NotRequired[int]
    """
    Only return cards that have the given expiration month.
    """
    exp_year: NotRequired[int]
    """
    Only return cards that have the given expiration year.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    last4: NotRequired[str]
    """
    Only return cards that have the given last four digits.
    """
    limit: NotRequired[int]
    """
    A limit on the number of objects to be returned. Limit can range between 1 and 100, and the default is 10.
    """
    personalization_design: NotRequired[str]
    starting_after: NotRequired[str]
    """
    A cursor for use in pagination. `starting_after` is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, ending with `obj_foo`, your subsequent call can include `starting_after=obj_foo` in order to fetch the next page of the list.
    """
    status: NotRequired[Literal["active", "canceled", "inactive"]]
    """
    Only return cards that have the given status. One of `active`, `inactive`, or `canceled`.
    """
    type: NotRequired[Literal["physical", "virtual"]]
    """
    Only return cards that have the given type. One of `virtual` or `physical`.
    """


class CardListParamsCreated(TypedDict):
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
