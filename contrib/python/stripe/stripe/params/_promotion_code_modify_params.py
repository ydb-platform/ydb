# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class PromotionCodeModifyParams(RequestOptions):
    active: NotRequired[bool]
    """
    Whether the promotion code is currently active. A promotion code can only be reactivated when the coupon is still valid and the promotion code is otherwise redeemable.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    restrictions: NotRequired["PromotionCodeModifyParamsRestrictions"]
    """
    Settings that restrict the redemption of the promotion code.
    """


class PromotionCodeModifyParamsRestrictions(TypedDict):
    currency_options: NotRequired[
        Dict[str, "PromotionCodeModifyParamsRestrictionsCurrencyOptions"]
    ]
    """
    Promotion codes defined in each available currency option. Each key must be a three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html) and a [supported currency](https://stripe.com/docs/currencies).
    """


class PromotionCodeModifyParamsRestrictionsCurrencyOptions(TypedDict):
    minimum_amount: NotRequired[int]
    """
    Minimum amount required to redeem this Promotion Code into a Coupon (e.g., a purchase must be $100 or more to work).
    """
