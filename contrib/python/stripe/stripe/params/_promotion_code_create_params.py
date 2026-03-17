# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class PromotionCodeCreateParams(RequestOptions):
    active: NotRequired[bool]
    """
    Whether the promotion code is currently active.
    """
    code: NotRequired[str]
    """
    The customer-facing code. Regardless of case, this code must be unique across all active promotion codes for a specific customer. Valid characters are lower case letters (a-z), upper case letters (A-Z), digits (0-9), and dashes (-).

    If left blank, we will generate one automatically.
    """
    customer: NotRequired[str]
    """
    The customer who can use this promotion code. If not set, all customers can use the promotion code.
    """
    customer_account: NotRequired[str]
    """
    The account representing the customer who can use this promotion code. If not set, all customers can use the promotion code.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    expires_at: NotRequired[int]
    """
    The timestamp at which this promotion code will expire. If the coupon has specified a `redeems_by`, then this value cannot be after the coupon's `redeems_by`.
    """
    max_redemptions: NotRequired[int]
    """
    A positive integer specifying the number of times the promotion code can be redeemed. If the coupon has specified a `max_redemptions`, then this value cannot be greater than the coupon's `max_redemptions`.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    promotion: "PromotionCodeCreateParamsPromotion"
    """
    The promotion referenced by this promotion code.
    """
    restrictions: NotRequired["PromotionCodeCreateParamsRestrictions"]
    """
    Settings that restrict the redemption of the promotion code.
    """


class PromotionCodeCreateParamsPromotion(TypedDict):
    coupon: NotRequired[str]
    """
    If promotion `type` is `coupon`, the coupon for this promotion code.
    """
    type: Literal["coupon"]
    """
    Specifies the type of promotion.
    """


class PromotionCodeCreateParamsRestrictions(TypedDict):
    currency_options: NotRequired[
        Dict[str, "PromotionCodeCreateParamsRestrictionsCurrencyOptions"]
    ]
    """
    Promotion codes defined in each available currency option. Each key must be a three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html) and a [supported currency](https://stripe.com/docs/currencies).
    """
    first_time_transaction: NotRequired[bool]
    """
    A Boolean indicating if the Promotion Code should only be redeemed for Customers without any successful payments or invoices
    """
    minimum_amount: NotRequired[int]
    """
    Minimum amount required to redeem this Promotion Code into a Coupon (e.g., a purchase must be $100 or more to work).
    """
    minimum_amount_currency: NotRequired[str]
    """
    Three-letter [ISO code](https://stripe.com/docs/currencies) for minimum_amount
    """


class PromotionCodeCreateParamsRestrictionsCurrencyOptions(TypedDict):
    minimum_amount: NotRequired[int]
    """
    Minimum amount required to redeem this Promotion Code into a Coupon (e.g., a purchase must be $100 or more to work).
    """
