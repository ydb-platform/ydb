# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class PriceListParams(RequestOptions):
    active: NotRequired[bool]
    """
    Only return prices that are active or inactive (e.g., pass `false` to list all inactive prices).
    """
    created: NotRequired["PriceListParamsCreated|int"]
    """
    A filter on the list, based on the object `created` field. The value can be a string with an integer Unix timestamp, or it can be a dictionary with a number of different query options.
    """
    currency: NotRequired[str]
    """
    Only return prices for the given currency.
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
    lookup_keys: NotRequired[List[str]]
    """
    Only return the price with these lookup_keys, if any exist. You can specify up to 10 lookup_keys.
    """
    product: NotRequired[str]
    """
    Only return prices for the given product.
    """
    recurring: NotRequired["PriceListParamsRecurring"]
    """
    Only return prices with these recurring fields.
    """
    starting_after: NotRequired[str]
    """
    A cursor for use in pagination. `starting_after` is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, ending with `obj_foo`, your subsequent call can include `starting_after=obj_foo` in order to fetch the next page of the list.
    """
    type: NotRequired[Literal["one_time", "recurring"]]
    """
    Only return prices of type `recurring` or `one_time`.
    """


class PriceListParamsCreated(TypedDict):
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


class PriceListParamsRecurring(TypedDict):
    interval: NotRequired[Literal["day", "month", "week", "year"]]
    """
    Filter by billing frequency. Either `day`, `week`, `month` or `year`.
    """
    meter: NotRequired[str]
    """
    Filter by the price's meter.
    """
    usage_type: NotRequired[Literal["licensed", "metered"]]
    """
    Filter by the usage type for this price. Can be either `metered` or `licensed`.
    """
