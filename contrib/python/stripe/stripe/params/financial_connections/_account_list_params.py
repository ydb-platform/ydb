# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import NotRequired, TypedDict


class AccountListParams(RequestOptions):
    account_holder: NotRequired["AccountListParamsAccountHolder"]
    """
    If present, only return accounts that belong to the specified account holder. `account_holder[customer]` and `account_holder[account]` are mutually exclusive.
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
    session: NotRequired[str]
    """
    If present, only return accounts that were collected as part of the given session.
    """
    starting_after: NotRequired[str]
    """
    A cursor for use in pagination. `starting_after` is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, ending with `obj_foo`, your subsequent call can include `starting_after=obj_foo` in order to fetch the next page of the list.
    """


class AccountListParamsAccountHolder(TypedDict):
    account: NotRequired[str]
    """
    The ID of the Stripe account whose accounts you will retrieve.
    """
    customer: NotRequired[str]
    """
    The ID of the Stripe customer whose accounts you will retrieve.
    """
    customer_account: NotRequired[str]
    """
    The ID of the Account representing a customer whose accounts you will retrieve.
    """
