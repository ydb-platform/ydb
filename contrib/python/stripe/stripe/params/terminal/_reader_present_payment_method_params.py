# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class ReaderPresentPaymentMethodParams(RequestOptions):
    amount_tip: NotRequired[int]
    """
    Simulated on-reader tip amount.
    """
    card: NotRequired["ReaderPresentPaymentMethodParamsCard"]
    """
    Simulated data for the card payment method.
    """
    card_present: NotRequired["ReaderPresentPaymentMethodParamsCardPresent"]
    """
    Simulated data for the card_present payment method.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    interac_present: NotRequired[
        "ReaderPresentPaymentMethodParamsInteracPresent"
    ]
    """
    Simulated data for the interac_present payment method.
    """
    type: NotRequired[Literal["card", "card_present", "interac_present"]]
    """
    Simulated payment type.
    """


class ReaderPresentPaymentMethodParamsCard(TypedDict):
    cvc: NotRequired[str]
    """
    Card security code.
    """
    exp_month: int
    """
    Two-digit number representing the card's expiration month.
    """
    exp_year: int
    """
    Two- or four-digit number representing the card's expiration year.
    """
    number: str
    """
    The card number, as a string without any separators.
    """


class ReaderPresentPaymentMethodParamsCardPresent(TypedDict):
    number: NotRequired[str]
    """
    The card number, as a string without any separators.
    """


class ReaderPresentPaymentMethodParamsInteracPresent(TypedDict):
    number: NotRequired[str]
    """
    The Interac card number.
    """
