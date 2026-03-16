# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class ReaderSetReaderDisplayParams(RequestOptions):
    cart: NotRequired["ReaderSetReaderDisplayParamsCart"]
    """
    Cart details to display on the reader screen, including line items, amounts, and currency.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    type: Literal["cart"]
    """
    Type of information to display. Only `cart` is currently supported.
    """


class ReaderSetReaderDisplayParamsCart(TypedDict):
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    line_items: List["ReaderSetReaderDisplayParamsCartLineItem"]
    """
    Array of line items to display.
    """
    tax: NotRequired[int]
    """
    The amount of tax in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
    """
    total: int
    """
    Total balance of cart due in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
    """


class ReaderSetReaderDisplayParamsCartLineItem(TypedDict):
    amount: int
    """
    The price of the item in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
    """
    description: str
    """
    The description or name of the item.
    """
    quantity: int
    """
    The quantity of the line item being purchased.
    """
