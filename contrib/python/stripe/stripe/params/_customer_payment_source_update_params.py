# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class CustomerPaymentSourceUpdateParams(TypedDict):
    account_holder_name: NotRequired[str]
    """
    The name of the person or business that owns the bank account.
    """
    account_holder_type: NotRequired[Literal["company", "individual"]]
    """
    The type of entity that holds the account. This can be either `individual` or `company`.
    """
    address_city: NotRequired[str]
    """
    City/District/Suburb/Town/Village.
    """
    address_country: NotRequired[str]
    """
    Billing address country, if provided when creating card.
    """
    address_line1: NotRequired[str]
    """
    Address line 1 (Street address/PO Box/Company name).
    """
    address_line2: NotRequired[str]
    """
    Address line 2 (Apartment/Suite/Unit/Building).
    """
    address_state: NotRequired[str]
    """
    State/County/Province/Region.
    """
    address_zip: NotRequired[str]
    """
    ZIP or postal code.
    """
    exp_month: NotRequired[str]
    """
    Two digit number representing the card's expiration month.
    """
    exp_year: NotRequired[str]
    """
    Four digit number representing the card's expiration year.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    name: NotRequired[str]
    """
    Cardholder name.
    """
    owner: NotRequired["CustomerPaymentSourceUpdateParamsOwner"]


class CustomerPaymentSourceUpdateParamsOwner(TypedDict):
    address: NotRequired["CustomerPaymentSourceUpdateParamsOwnerAddress"]
    """
    Owner's address.
    """
    email: NotRequired[str]
    """
    Owner's email address.
    """
    name: NotRequired[str]
    """
    Owner's full name.
    """
    phone: NotRequired[str]
    """
    Owner's phone number.
    """


class CustomerPaymentSourceUpdateParamsOwnerAddress(TypedDict):
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
    State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
    """
