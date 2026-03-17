# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List, Union
from typing_extensions import Literal, NotRequired, TypedDict


class PriceModifyParams(RequestOptions):
    active: NotRequired[bool]
    """
    Whether the price can be used for new purchases. Defaults to `true`.
    """
    currency_options: NotRequired[
        "Literal['']|Dict[str, PriceModifyParamsCurrencyOptions]"
    ]
    """
    Prices defined in each available currency option. Each key must be a three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html) and a [supported currency](https://stripe.com/docs/currencies).
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    lookup_key: NotRequired[str]
    """
    A lookup key used to retrieve prices dynamically from a static string. This may be up to 200 characters.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    nickname: NotRequired[str]
    """
    A brief description of the price, hidden from customers.
    """
    tax_behavior: NotRequired[Literal["exclusive", "inclusive", "unspecified"]]
    """
    Only required if a [default tax behavior](https://docs.stripe.com/tax/products-prices-tax-categories-tax-behavior#setting-a-default-tax-behavior-(recommended)) was not provided in the Stripe Tax settings. Specifies whether the price is considered inclusive of taxes or exclusive of taxes. One of `inclusive`, `exclusive`, or `unspecified`. Once specified as either `inclusive` or `exclusive`, it cannot be changed.
    """
    transfer_lookup_key: NotRequired[bool]
    """
    If set to true, will atomically remove the lookup key from the existing price, and assign it to this price.
    """


class PriceModifyParamsCurrencyOptions(TypedDict):
    custom_unit_amount: NotRequired[
        "PriceModifyParamsCurrencyOptionsCustomUnitAmount"
    ]
    """
    When set, provides configuration for the amount to be adjusted by the customer during Checkout Sessions and Payment Links.
    """
    tax_behavior: NotRequired[Literal["exclusive", "inclusive", "unspecified"]]
    """
    Only required if a [default tax behavior](https://docs.stripe.com/tax/products-prices-tax-categories-tax-behavior#setting-a-default-tax-behavior-(recommended)) was not provided in the Stripe Tax settings. Specifies whether the price is considered inclusive of taxes or exclusive of taxes. One of `inclusive`, `exclusive`, or `unspecified`. Once specified as either `inclusive` or `exclusive`, it cannot be changed.
    """
    tiers: NotRequired[List["PriceModifyParamsCurrencyOptionsTier"]]
    """
    Each element represents a pricing tier. This parameter requires `billing_scheme` to be set to `tiered`. See also the documentation for `billing_scheme`.
    """
    unit_amount: NotRequired[int]
    """
    A positive integer in cents (or local equivalent) (or 0 for a free price) representing how much to charge.
    """
    unit_amount_decimal: NotRequired[str]
    """
    Same as `unit_amount`, but accepts a decimal value in cents (or local equivalent) with at most 12 decimal places. Only one of `unit_amount` and `unit_amount_decimal` can be set.
    """


class PriceModifyParamsCurrencyOptionsCustomUnitAmount(TypedDict):
    enabled: bool
    """
    Pass in `true` to enable `custom_unit_amount`, otherwise omit `custom_unit_amount`.
    """
    maximum: NotRequired[int]
    """
    The maximum unit amount the customer can specify for this item.
    """
    minimum: NotRequired[int]
    """
    The minimum unit amount the customer can specify for this item. Must be at least the minimum charge amount.
    """
    preset: NotRequired[int]
    """
    The starting unit amount which can be updated by the customer.
    """


class PriceModifyParamsCurrencyOptionsTier(TypedDict):
    flat_amount: NotRequired[int]
    """
    The flat billing amount for an entire tier, regardless of the number of units in the tier.
    """
    flat_amount_decimal: NotRequired[str]
    """
    Same as `flat_amount`, but accepts a decimal value representing an integer in the minor units of the currency. Only one of `flat_amount` and `flat_amount_decimal` can be set.
    """
    unit_amount: NotRequired[int]
    """
    The per unit billing amount for each individual unit for which this tier applies.
    """
    unit_amount_decimal: NotRequired[str]
    """
    Same as `unit_amount`, but accepts a decimal value in cents (or local equivalent) with at most 12 decimal places. Only one of `unit_amount` and `unit_amount_decimal` can be set.
    """
    up_to: Union[Literal["inf"], int]
    """
    Specifies the upper bound of this tier. The lower bound of a tier is the upper bound of the previous tier adding one. Use `inf` to define a fallback tier.
    """
