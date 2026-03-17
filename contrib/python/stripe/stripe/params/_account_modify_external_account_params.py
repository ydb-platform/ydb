# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class AccountModifyExternalAccountParams(RequestOptions):
    account_holder_name: NotRequired[str]
    """
    The name of the person or business that owns the bank account.
    """
    account_holder_type: NotRequired[
        "Literal['']|Literal['company', 'individual']"
    ]
    """
    The type of entity that holds the account. This can be either `individual` or `company`.
    """
    account_type: NotRequired[Literal["checking", "futsu", "savings", "toza"]]
    """
    The bank account type. This can only be `checking` or `savings` in most countries. In Japan, this can only be `futsu` or `toza`.
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
    default_for_currency: NotRequired[bool]
    """
    When set to true, this becomes the default external account for its currency.
    """
    documents: NotRequired["AccountModifyExternalAccountParamsDocuments"]
    """
    Documents that may be submitted to satisfy various informational requests.
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


class AccountModifyExternalAccountParamsDocuments(TypedDict):
    bank_account_ownership_verification: NotRequired[
        "AccountModifyExternalAccountParamsDocumentsBankAccountOwnershipVerification"
    ]
    """
    One or more documents that support the [Bank account ownership verification](https://support.stripe.com/questions/bank-account-ownership-verification) requirement. Must be a document associated with the bank account that displays the last 4 digits of the account number, either a statement or a check.
    """


class AccountModifyExternalAccountParamsDocumentsBankAccountOwnershipVerification(
    TypedDict,
):
    files: NotRequired[List[str]]
    """
    One or more document ids returned by a [file upload](https://api.stripe.com#create_file) with a `purpose` value of `account_requirement`.
    """
