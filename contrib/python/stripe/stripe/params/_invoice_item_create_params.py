# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class InvoiceItemCreateParams(RequestOptions):
    amount: NotRequired[int]
    """
    The integer amount in cents (or local equivalent) of the charge to be applied to the upcoming invoice. Passing in a negative `amount` will reduce the `amount_due` on the invoice.
    """
    currency: NotRequired[str]
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    customer: NotRequired[str]
    """
    The ID of the customer to bill for this invoice item.
    """
    customer_account: NotRequired[str]
    """
    The ID of the account representing the customer to bill for this invoice item.
    """
    description: NotRequired[str]
    """
    An arbitrary string which you can attach to the invoice item. The description is displayed in the invoice for easy tracking.
    """
    discountable: NotRequired[bool]
    """
    Controls whether discounts apply to this invoice item. Defaults to false for prorations or negative invoice items, and true for all other invoice items.
    """
    discounts: NotRequired["Literal['']|List[InvoiceItemCreateParamsDiscount]"]
    """
    The coupons and promotion codes to redeem into discounts for the invoice item or invoice line item.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    invoice: NotRequired[str]
    """
    The ID of an existing invoice to add this invoice item to. For subscription invoices, when left blank, the invoice item will be added to the next upcoming scheduled invoice. For standalone invoices, the invoice item won't be automatically added unless you pass `pending_invoice_item_behavior: 'include'` when creating the invoice. This is useful when adding invoice items in response to an invoice.created webhook. You can only add invoice items to draft invoices and there is a maximum of 250 items per invoice.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    period: NotRequired["InvoiceItemCreateParamsPeriod"]
    """
    The period associated with this invoice item. When set to different values, the period will be rendered on the invoice. If you have [Stripe Revenue Recognition](https://docs.stripe.com/revenue-recognition) enabled, the period will be used to recognize and defer revenue. See the [Revenue Recognition documentation](https://docs.stripe.com/revenue-recognition/methodology/subscriptions-and-invoicing) for details.
    """
    price_data: NotRequired["InvoiceItemCreateParamsPriceData"]
    """
    Data used to generate a new [Price](https://docs.stripe.com/api/prices) object inline.
    """
    pricing: NotRequired["InvoiceItemCreateParamsPricing"]
    """
    The pricing information for the invoice item.
    """
    quantity: NotRequired[int]
    """
    Non-negative integer. The quantity of units for the invoice item.
    """
    subscription: NotRequired[str]
    """
    The ID of a subscription to add this invoice item to. When left blank, the invoice item is added to the next upcoming scheduled invoice. When set, scheduled invoices for subscriptions other than the specified subscription will ignore the invoice item. Use this when you want to express that an invoice item has been accrued within the context of a particular subscription.
    """
    tax_behavior: NotRequired[Literal["exclusive", "inclusive", "unspecified"]]
    """
    Only required if a [default tax behavior](https://docs.stripe.com/tax/products-prices-tax-categories-tax-behavior#setting-a-default-tax-behavior-(recommended)) was not provided in the Stripe Tax settings. Specifies whether the price is considered inclusive of taxes or exclusive of taxes. One of `inclusive`, `exclusive`, or `unspecified`. Once specified as either `inclusive` or `exclusive`, it cannot be changed.
    """
    tax_code: NotRequired["Literal['']|str"]
    """
    A [tax code](https://docs.stripe.com/tax/tax-categories) ID.
    """
    tax_rates: NotRequired[List[str]]
    """
    The tax rates which apply to the invoice item. When set, the `default_tax_rates` on the invoice do not apply to this invoice item.
    """
    unit_amount_decimal: NotRequired[str]
    """
    The decimal unit amount in cents (or local equivalent) of the charge to be applied to the upcoming invoice. This `unit_amount_decimal` will be multiplied by the quantity to get the full amount. Passing in a negative `unit_amount_decimal` will reduce the `amount_due` on the invoice. Accepts at most 12 decimal places.
    """


class InvoiceItemCreateParamsDiscount(TypedDict):
    coupon: NotRequired[str]
    """
    ID of the coupon to create a new discount for.
    """
    discount: NotRequired[str]
    """
    ID of an existing discount on the object (or one of its ancestors) to reuse.
    """
    promotion_code: NotRequired[str]
    """
    ID of the promotion code to create a new discount for.
    """


class InvoiceItemCreateParamsPeriod(TypedDict):
    end: int
    """
    The end of the period, which must be greater than or equal to the start. This value is inclusive.
    """
    start: int
    """
    The start of the period. This value is inclusive.
    """


class InvoiceItemCreateParamsPriceData(TypedDict):
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    product: str
    """
    The ID of the [Product](https://docs.stripe.com/api/products) that this [Price](https://docs.stripe.com/api/prices) will belong to.
    """
    tax_behavior: NotRequired[Literal["exclusive", "inclusive", "unspecified"]]
    """
    Only required if a [default tax behavior](https://docs.stripe.com/tax/products-prices-tax-categories-tax-behavior#setting-a-default-tax-behavior-(recommended)) was not provided in the Stripe Tax settings. Specifies whether the price is considered inclusive of taxes or exclusive of taxes. One of `inclusive`, `exclusive`, or `unspecified`. Once specified as either `inclusive` or `exclusive`, it cannot be changed.
    """
    unit_amount: NotRequired[int]
    """
    A positive integer in cents (or local equivalent) (or 0 for a free price) representing how much to charge.
    """
    unit_amount_decimal: NotRequired[str]
    """
    Same as `unit_amount`, but accepts a decimal value in cents (or local equivalent) with at most 12 decimal places. Only one of `unit_amount` and `unit_amount_decimal` can be set.
    """


class InvoiceItemCreateParamsPricing(TypedDict):
    price: NotRequired[str]
    """
    The ID of the price object.
    """
