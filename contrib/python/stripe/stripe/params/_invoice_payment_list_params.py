# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class InvoicePaymentListParams(RequestOptions):
    created: NotRequired["InvoicePaymentListParamsCreated|int"]
    """
    Only return invoice payments that were created during the given date interval.
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
    The identifier of the invoice whose payments to return.
    """
    limit: NotRequired[int]
    """
    A limit on the number of objects to be returned. Limit can range between 1 and 100, and the default is 10.
    """
    payment: NotRequired["InvoicePaymentListParamsPayment"]
    """
    The payment details of the invoice payments to return.
    """
    starting_after: NotRequired[str]
    """
    A cursor for use in pagination. `starting_after` is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, ending with `obj_foo`, your subsequent call can include `starting_after=obj_foo` in order to fetch the next page of the list.
    """
    status: NotRequired[Literal["canceled", "open", "paid"]]
    """
    The status of the invoice payments to return.
    """


class InvoicePaymentListParamsCreated(TypedDict):
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


class InvoicePaymentListParamsPayment(TypedDict):
    payment_intent: NotRequired[str]
    """
    Only return invoice payments associated by this payment intent ID.
    """
    payment_record: NotRequired[str]
    """
    Only return invoice payments associated by this payment record ID.
    """
    type: Literal["payment_intent", "payment_record"]
    """
    Only return invoice payments associated by this payment type.
    """
