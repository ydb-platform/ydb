# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import Dict, List, Union
from typing_extensions import Literal, NotRequired, TypedDict


class AccountExternalAccountCreateParams(TypedDict):
    default_for_currency: NotRequired[bool]
    """
    When set to true, or if this is the first external account added in this currency, this account becomes the default external account for its currency.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    external_account: Union[
        str,
        "AccountExternalAccountCreateParamsCard",
        "AccountExternalAccountCreateParamsBankAccount",
        "AccountExternalAccountCreateParamsCardToken",
    ]
    """
    A token, like the ones returned by [Stripe.js](https://docs.stripe.com/js) or a dictionary containing a user's external account details (with the options shown below). Please refer to full [documentation](https://stripe.com/docs/api/external_accounts) instead.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """


class AccountExternalAccountCreateParamsCard(TypedDict):
    object: Literal["card"]
    address_city: NotRequired[str]
    address_country: NotRequired[str]
    address_line1: NotRequired[str]
    address_line2: NotRequired[str]
    address_state: NotRequired[str]
    address_zip: NotRequired[str]
    currency: NotRequired[str]
    cvc: NotRequired[str]
    exp_month: int
    exp_year: int
    name: NotRequired[str]
    number: str
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://stripe.com/docs/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """


class AccountExternalAccountCreateParamsBankAccount(TypedDict):
    object: Literal["bank_account"]
    account_holder_name: NotRequired[str]
    """
    The name of the person or business that owns the bank account.This field is required when attaching the bank account to a `Customer` object.
    """
    account_holder_type: NotRequired[Literal["company", "individual"]]
    """
    The type of entity that holds the account. It can be `company` or `individual`. This field is required when attaching the bank account to a `Customer` object.
    """
    account_number: str
    """
    The account number for the bank account, in string form. Must be a checking account.
    """
    country: str
    """
    The country in which the bank account is located.
    """
    currency: NotRequired[str]
    """
    The currency the bank account is in. This must be a country/currency pairing that [Stripe supports.](docs/payouts)
    """
    routing_number: NotRequired[str]
    """
    The routing number, sort code, or other country-appropriate institution number for the bank account. For US bank accounts, this is required and should be the ACH routing number, not the wire routing number. If you are providing an IBAN for `account_number`, this field is not required.
    """


class AccountExternalAccountCreateParamsCardToken(TypedDict):
    object: Literal["card"]
    currency: NotRequired[str]
    token: str
