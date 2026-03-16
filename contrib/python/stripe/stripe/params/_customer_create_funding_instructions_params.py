# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class CustomerCreateFundingInstructionsParams(RequestOptions):
    bank_transfer: "CustomerCreateFundingInstructionsParamsBankTransfer"
    """
    Additional parameters for `bank_transfer` funding types
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    funding_type: Literal["bank_transfer"]
    """
    The `funding_type` to get the instructions for.
    """


class CustomerCreateFundingInstructionsParamsBankTransfer(TypedDict):
    eu_bank_transfer: NotRequired[
        "CustomerCreateFundingInstructionsParamsBankTransferEuBankTransfer"
    ]
    """
    Configuration for eu_bank_transfer funding type.
    """
    requested_address_types: NotRequired[
        List[Literal["iban", "sort_code", "spei", "zengin"]]
    ]
    """
    List of address types that should be returned in the financial_addresses response. If not specified, all valid types will be returned.

    Permitted values include: `sort_code`, `zengin`, `iban`, or `spei`.
    """
    type: Literal[
        "eu_bank_transfer",
        "gb_bank_transfer",
        "jp_bank_transfer",
        "mx_bank_transfer",
        "us_bank_transfer",
    ]
    """
    The type of the `bank_transfer`
    """


class CustomerCreateFundingInstructionsParamsBankTransferEuBankTransfer(
    TypedDict,
):
    country: str
    """
    The desired country code of the bank account information. Permitted values include: `DE`, `FR`, `IE`, or `NL`.
    """
