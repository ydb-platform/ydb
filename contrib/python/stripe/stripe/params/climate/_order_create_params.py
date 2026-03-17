# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import NotRequired, TypedDict


class OrderCreateParams(RequestOptions):
    amount: NotRequired[int]
    """
    Requested amount of carbon removal units. Either this or `metric_tons` must be specified.
    """
    beneficiary: NotRequired["OrderCreateParamsBeneficiary"]
    """
    Publicly sharable reference for the end beneficiary of carbon removal. Assumed to be the Stripe account if not set.
    """
    currency: NotRequired[str]
    """
    Request currency for the order as a three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a supported [settlement currency for your account](https://stripe.com/docs/currencies). If omitted, the account's default currency will be used.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    metric_tons: NotRequired[str]
    """
    Requested number of tons for the order. Either this or `amount` must be specified.
    """
    product: str
    """
    Unique identifier of the Climate product.
    """


class OrderCreateParamsBeneficiary(TypedDict):
    public_name: str
    """
    Publicly displayable name for the end beneficiary of carbon removal.
    """
