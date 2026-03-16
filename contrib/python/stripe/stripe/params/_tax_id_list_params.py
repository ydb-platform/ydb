# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class TaxIdListParams(RequestOptions):
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
    owner: NotRequired["TaxIdListParamsOwner"]
    """
    The account or customer the tax ID belongs to. Defaults to `owner[type]=self`.
    """
    starting_after: NotRequired[str]
    """
    A cursor for use in pagination. `starting_after` is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, ending with `obj_foo`, your subsequent call can include `starting_after=obj_foo` in order to fetch the next page of the list.
    """


class TaxIdListParamsOwner(TypedDict):
    account: NotRequired[str]
    """
    Connected Account the tax ID belongs to. Required when `type=account`
    """
    customer: NotRequired[str]
    """
    Customer the tax ID belongs to. Required when `type=customer`
    """
    customer_account: NotRequired[str]
    """
    ID of the Account representing the customer that the tax ID belongs to. Can be used in place of `customer` when `type=customer`
    """
    type: Literal["account", "application", "customer", "self"]
    """
    Type of owner referenced.
    """
