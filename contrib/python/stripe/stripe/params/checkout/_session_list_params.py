# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class SessionListParams(RequestOptions):
    created: NotRequired["SessionListParamsCreated|int"]
    """
    Only return Checkout Sessions that were created during the given date interval.
    """
    customer: NotRequired[str]
    """
    Only return the Checkout Sessions for the Customer specified.
    """
    customer_account: NotRequired[str]
    """
    Only return the Checkout Sessions for the Account specified.
    """
    customer_details: NotRequired["SessionListParamsCustomerDetails"]
    """
    Only return the Checkout Sessions for the Customer details specified.
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
    payment_intent: NotRequired[str]
    """
    Only return the Checkout Session for the PaymentIntent specified.
    """
    payment_link: NotRequired[str]
    """
    Only return the Checkout Sessions for the Payment Link specified.
    """
    starting_after: NotRequired[str]
    """
    A cursor for use in pagination. `starting_after` is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, ending with `obj_foo`, your subsequent call can include `starting_after=obj_foo` in order to fetch the next page of the list.
    """
    status: NotRequired[Literal["complete", "expired", "open"]]
    """
    Only return the Checkout Sessions matching the given status.
    """
    subscription: NotRequired[str]
    """
    Only return the Checkout Session for the subscription specified.
    """


class SessionListParamsCreated(TypedDict):
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


class SessionListParamsCustomerDetails(TypedDict):
    email: str
    """
    Customer's email address.
    """
