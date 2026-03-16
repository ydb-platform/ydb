# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List, Union
from typing_extensions import Literal, NotRequired, TypedDict


class PriceCreateParams(RequestOptions):
    active: NotRequired[bool]
    """
    Whether the price can be used for new purchases. Defaults to `true`.
    """
    billing_scheme: NotRequired[Literal["per_unit", "tiered"]]
    """
    Describes how to compute the price per period. Either `per_unit` or `tiered`. `per_unit` indicates that the fixed amount (specified in `unit_amount` or `unit_amount_decimal`) will be charged per unit in `quantity` (for prices with `usage_type=licensed`), or per unit of total usage (for prices with `usage_type=metered`). `tiered` indicates that the unit pricing will be computed using a tiering strategy as defined using the `tiers` and `tiers_mode` attributes.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    currency_options: NotRequired[
        Dict[str, "PriceCreateParamsCurrencyOptions"]
    ]
    """
    Prices defined in each available currency option. Each key must be a three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html) and a [supported currency](https://stripe.com/docs/currencies).
    """
    custom_unit_amount: NotRequired["PriceCreateParamsCustomUnitAmount"]
    """
    When set, provides configuration for the amount to be adjusted by the customer during Checkout Sessions and Payment Links.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    lookup_key: NotRequired[str]
    """
    A lookup key used to retrieve prices dynamically from a static string. This may be up to 200 characters.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    nickname: NotRequired[str]
    """
    A brief description of the price, hidden from customers.
    """
    product: NotRequired[str]
    """
    The ID of the [Product](https://docs.stripe.com/api/products) that this [Price](https://docs.stripe.com/api/prices) will belong to.
    """
    product_data: NotRequired["PriceCreateParamsProductData"]
    """
    These fields can be used to create a new product that this price will belong to.
    """
    recurring: NotRequired["PriceCreateParamsRecurring"]
    """
    The recurring components of a price such as `interval` and `usage_type`.
    """
    tax_behavior: NotRequired[Literal["exclusive", "inclusive", "unspecified"]]
    """
    Only required if a [default tax behavior](https://docs.stripe.com/tax/products-prices-tax-categories-tax-behavior#setting-a-default-tax-behavior-(recommended)) was not provided in the Stripe Tax settings. Specifies whether the price is considered inclusive of taxes or exclusive of taxes. One of `inclusive`, `exclusive`, or `unspecified`. Once specified as either `inclusive` or `exclusive`, it cannot be changed.
    """
    tiers: NotRequired[List["PriceCreateParamsTier"]]
    """
    Each element represents a pricing tier. This parameter requires `billing_scheme` to be set to `tiered`. See also the documentation for `billing_scheme`.
    """
    tiers_mode: NotRequired[Literal["graduated", "volume"]]
    """
    Defines if the tiering price should be `graduated` or `volume` based. In `volume`-based tiering, the maximum quantity within a period determines the per unit price, in `graduated` tiering pricing can successively change as the quantity grows.
    """
    transfer_lookup_key: NotRequired[bool]
    """
    If set to true, will atomically remove the lookup key from the existing price, and assign it to this price.
    """
    transform_quantity: NotRequired["PriceCreateParamsTransformQuantity"]
    """
    Apply a transformation to the reported usage or set quantity before computing the billed price. Cannot be combined with `tiers`.
    """
    unit_amount: NotRequired[int]
    """
    A positive integer in cents (or local equivalent) (or 0 for a free price) representing how much to charge. One of `unit_amount`, `unit_amount_decimal`, or `custom_unit_amount` is required, unless `billing_scheme=tiered`.
    """
    unit_amount_decimal: NotRequired[str]
    """
    Same as `unit_amount`, but accepts a decimal value in cents (or local equivalent) with at most 12 decimal places. Only one of `unit_amount` and `unit_amount_decimal` can be set.
    """


class PriceCreateParamsCurrencyOptions(TypedDict):
    custom_unit_amount: NotRequired[
        "PriceCreateParamsCurrencyOptionsCustomUnitAmount"
    ]
    """
    When set, provides configuration for the amount to be adjusted by the customer during Checkout Sessions and Payment Links.
    """
    tax_behavior: NotRequired[Literal["exclusive", "inclusive", "unspecified"]]
    """
    Only required if a [default tax behavior](https://docs.stripe.com/tax/products-prices-tax-categories-tax-behavior#setting-a-default-tax-behavior-(recommended)) was not provided in the Stripe Tax settings. Specifies whether the price is considered inclusive of taxes or exclusive of taxes. One of `inclusive`, `exclusive`, or `unspecified`. Once specified as either `inclusive` or `exclusive`, it cannot be changed.
    """
    tiers: NotRequired[List["PriceCreateParamsCurrencyOptionsTier"]]
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


class PriceCreateParamsCurrencyOptionsCustomUnitAmount(TypedDict):
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


class PriceCreateParamsCurrencyOptionsTier(TypedDict):
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


class PriceCreateParamsCustomUnitAmount(TypedDict):
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


class PriceCreateParamsProductData(TypedDict):
    active: NotRequired[bool]
    """
    Whether the product is currently available for purchase. Defaults to `true`.
    """
    id: NotRequired[str]
    """
    The identifier for the product. Must be unique. If not provided, an identifier will be randomly generated.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    name: str
    """
    The product's name, meant to be displayable to the customer.
    """
    statement_descriptor: NotRequired[str]
    """
    An arbitrary string to be displayed on your customer's credit card or bank statement. While most banks display this information consistently, some may display it incorrectly or not at all.

    This may be up to 22 characters. The statement description may not include `<`, `>`, `\\`, `"`, `'` characters, and will appear on your customer's statement in capital letters. Non-ASCII characters are automatically stripped.
    """
    tax_code: NotRequired[str]
    """
    A [tax code](https://docs.stripe.com/tax/tax-categories) ID.
    """
    unit_label: NotRequired[str]
    """
    A label that represents units of this product. When set, this will be included in customers' receipts, invoices, Checkout, and the customer portal.
    """


class PriceCreateParamsRecurring(TypedDict):
    interval: Literal["day", "month", "week", "year"]
    """
    Specifies billing frequency. Either `day`, `week`, `month` or `year`.
    """
    interval_count: NotRequired[int]
    """
    The number of intervals between subscription billings. For example, `interval=month` and `interval_count=3` bills every 3 months. Maximum of three years interval allowed (3 years, 36 months, or 156 weeks).
    """
    meter: NotRequired[str]
    """
    The meter tracking the usage of a metered price
    """
    trial_period_days: NotRequired[int]
    """
    Default number of trial days when subscribing a customer to this price using [`trial_from_plan=true`](https://docs.stripe.com/api#create_subscription-trial_from_plan).
    """
    usage_type: NotRequired[Literal["licensed", "metered"]]
    """
    Configures how the quantity per period should be determined. Can be either `metered` or `licensed`. `licensed` automatically bills the `quantity` set when adding it to a subscription. `metered` aggregates the total usage based on usage records. Defaults to `licensed`.
    """


class PriceCreateParamsTier(TypedDict):
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


class PriceCreateParamsTransformQuantity(TypedDict):
    divide_by: int
    """
    Divide usage by this number.
    """
    round: Literal["down", "up"]
    """
    After division, either round the result `up` or `down`.
    """
