# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class ShippingRateModifyParams(RequestOptions):
    active: NotRequired[bool]
    """
    Whether the shipping rate can be used for new purchases. Defaults to `true`.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    fixed_amount: NotRequired["ShippingRateModifyParamsFixedAmount"]
    """
    Describes a fixed amount to charge for shipping. Must be present if type is `fixed_amount`.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    tax_behavior: NotRequired[Literal["exclusive", "inclusive", "unspecified"]]
    """
    Specifies whether the rate is considered inclusive of taxes or exclusive of taxes. One of `inclusive`, `exclusive`, or `unspecified`.
    """


class ShippingRateModifyParamsFixedAmount(TypedDict):
    currency_options: NotRequired[
        Dict[str, "ShippingRateModifyParamsFixedAmountCurrencyOptions"]
    ]
    """
    Shipping rates defined in each available currency option. Each key must be a three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html) and a [supported currency](https://stripe.com/docs/currencies).
    """


class ShippingRateModifyParamsFixedAmountCurrencyOptions(TypedDict):
    amount: NotRequired[int]
    """
    A non-negative integer in cents representing how much to charge.
    """
    tax_behavior: NotRequired[Literal["exclusive", "inclusive", "unspecified"]]
    """
    Specifies whether the rate is considered inclusive of taxes or exclusive of taxes. One of `inclusive`, `exclusive`, or `unspecified`.
    """
