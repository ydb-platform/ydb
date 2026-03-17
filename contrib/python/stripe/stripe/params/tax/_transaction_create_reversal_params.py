# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class TransactionCreateReversalParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    flat_amount: NotRequired[int]
    """
    A flat amount to reverse across the entire transaction, in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal) in negative. This value represents the total amount to refund from the transaction, including taxes.
    """
    line_items: NotRequired[List["TransactionCreateReversalParamsLineItem"]]
    """
    The line item amounts to reverse.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    mode: Literal["full", "partial"]
    """
    If `partial`, the provided line item or shipping cost amounts are reversed. If `full`, the original transaction is fully reversed.
    """
    original_transaction: str
    """
    The ID of the Transaction to partially or fully reverse.
    """
    reference: str
    """
    A custom identifier for this reversal, such as `myOrder_123-refund_1`, which must be unique across all transactions. The reference helps identify this reversal transaction in exported [tax reports](https://docs.stripe.com/tax/reports).
    """
    shipping_cost: NotRequired["TransactionCreateReversalParamsShippingCost"]
    """
    The shipping cost to reverse.
    """


class TransactionCreateReversalParamsLineItem(TypedDict):
    amount: int
    """
    The amount to reverse, in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal) in negative.
    """
    amount_tax: int
    """
    The amount of tax to reverse, in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal) in negative.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    original_line_item: str
    """
    The `id` of the line item to reverse in the original transaction.
    """
    quantity: NotRequired[int]
    """
    The quantity reversed. Appears in [tax exports](https://docs.stripe.com/tax/reports), but does not affect the amount of tax reversed.
    """
    reference: str
    """
    A custom identifier for this line item in the reversal transaction, such as 'L1-refund'.
    """


class TransactionCreateReversalParamsShippingCost(TypedDict):
    amount: int
    """
    The amount to reverse, in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal) in negative.
    """
    amount_tax: int
    """
    The amount of tax to reverse, in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal) in negative.
    """
