# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class InvoiceListParams(RequestOptions):
    collection_method: NotRequired[
        Literal["charge_automatically", "send_invoice"]
    ]
    """
    The collection method of the invoice to retrieve. Either `charge_automatically` or `send_invoice`.
    """
    created: NotRequired["InvoiceListParamsCreated|int"]
    """
    Only return invoices that were created during the given date interval.
    """
    customer: NotRequired[str]
    """
    Only return invoices for the customer specified by this customer ID.
    """
    customer_account: NotRequired[str]
    """
    Only return invoices for the account representing the customer specified by this account ID.
    """
    due_date: NotRequired["InvoiceListParamsDueDate|int"]
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
    status: NotRequired[
        Literal["draft", "open", "paid", "uncollectible", "void"]
    ]
    """
    The status of the invoice, one of `draft`, `open`, `paid`, `uncollectible`, or `void`. [Learn more](https://docs.stripe.com/billing/invoices/workflow#workflow-overview)
    """
    subscription: NotRequired[str]
    """
    Only return invoices for the subscription specified by this subscription ID.
    """


class InvoiceListParamsCreated(TypedDict):
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


class InvoiceListParamsDueDate(TypedDict):
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
