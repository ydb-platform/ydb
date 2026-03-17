# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class SettingsModifyParams(RequestOptions):
    defaults: NotRequired["SettingsModifyParamsDefaults"]
    """
    Default configuration to be used on Stripe Tax calculations.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    head_office: NotRequired["SettingsModifyParamsHeadOffice"]
    """
    The place where your business is located.
    """


class SettingsModifyParamsDefaults(TypedDict):
    tax_behavior: NotRequired[
        Literal["exclusive", "inclusive", "inferred_by_currency"]
    ]
    """
    Specifies the default [tax behavior](https://stripe.com/docs/tax/products-prices-tax-categories-tax-behavior#tax-behavior) to be used when the item's price has unspecified tax behavior. One of inclusive, exclusive, or inferred_by_currency. Once specified, it cannot be changed back to null.
    """
    tax_code: NotRequired[str]
    """
    A [tax code](https://docs.stripe.com/tax/tax-categories) ID.
    """


class SettingsModifyParamsHeadOffice(TypedDict):
    address: "SettingsModifyParamsHeadOfficeAddress"
    """
    The location of the business for tax purposes.
    """


class SettingsModifyParamsHeadOfficeAddress(TypedDict):
    city: NotRequired[str]
    """
    City, district, suburb, town, or village.
    """
    country: NotRequired[str]
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    line1: NotRequired[str]
    """
    Address line 1, such as the street, PO Box, or company name.
    """
    line2: NotRequired[str]
    """
    Address line 2, such as the apartment, suite, unit, or building.
    """
    postal_code: NotRequired[str]
    """
    ZIP or postal code.
    """
    state: NotRequired[str]
    """
    State/province as an [ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2) subdivision code, without country prefix, such as "NY" or "TX".
    """
