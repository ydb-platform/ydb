# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class OutboundPaymentReturnOutboundPaymentParams(TypedDict):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    returned_details: NotRequired[
        "OutboundPaymentReturnOutboundPaymentParamsReturnedDetails"
    ]
    """
    Optional hash to set the return code.
    """


class OutboundPaymentReturnOutboundPaymentParamsReturnedDetails(TypedDict):
    code: NotRequired[
        Literal[
            "account_closed",
            "account_frozen",
            "bank_account_restricted",
            "bank_ownership_changed",
            "declined",
            "incorrect_account_holder_name",
            "invalid_account_number",
            "invalid_currency",
            "no_account",
            "other",
        ]
    ]
    """
    The return code to be set on the OutboundPayment object.
    """
