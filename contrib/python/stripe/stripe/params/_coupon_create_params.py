# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class CouponCreateParams(RequestOptions):
    amount_off: NotRequired[int]
    """
    A positive integer representing the amount to subtract from an invoice total (required if `percent_off` is not passed).
    """
    applies_to: NotRequired["CouponCreateParamsAppliesTo"]
    """
    A hash containing directions for what this Coupon will apply discounts to.
    """
    currency: NotRequired[str]
    """
    Three-letter [ISO code for the currency](https://stripe.com/docs/currencies) of the `amount_off` parameter (required if `amount_off` is passed).
    """
    currency_options: NotRequired[
        Dict[str, "CouponCreateParamsCurrencyOptions"]
    ]
    """
    Coupons defined in each available currency option (only supported if `amount_off` is passed). Each key must be a three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html) and a [supported currency](https://stripe.com/docs/currencies).
    """
    duration: NotRequired[Literal["forever", "once", "repeating"]]
    """
    Specifies how long the discount will be in effect if used on a subscription. Defaults to `once`.
    """
    duration_in_months: NotRequired[int]
    """
    Required only if `duration` is `repeating`, in which case it must be a positive integer that specifies the number of months the discount will be in effect.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    id: NotRequired[str]
    """
    Unique string of your choice that will be used to identify this coupon when applying it to a customer. If you don't want to specify a particular code, you can leave the ID blank and we'll generate a random code for you.
    """
    max_redemptions: NotRequired[int]
    """
    A positive integer specifying the number of times the coupon can be redeemed before it's no longer valid. For example, you might have a 50% off coupon that the first 20 readers of your blog can use.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    name: NotRequired[str]
    """
    Name of the coupon displayed to customers on, for instance invoices, or receipts. By default the `id` is shown if `name` is not set.
    """
    percent_off: NotRequired[float]
    """
    A positive float larger than 0, and smaller or equal to 100, that represents the discount the coupon will apply (required if `amount_off` is not passed).
    """
    redeem_by: NotRequired[int]
    """
    Unix timestamp specifying the last time at which the coupon can be redeemed (cannot be set to more than 5 years in the future). After the redeem_by date, the coupon can no longer be applied to new customers.
    """


class CouponCreateParamsAppliesTo(TypedDict):
    products: NotRequired[List[str]]
    """
    An array of Product IDs that this Coupon will apply to.
    """


class CouponCreateParamsCurrencyOptions(TypedDict):
    amount_off: int
    """
    A positive integer representing the amount to subtract from an invoice total.
    """
