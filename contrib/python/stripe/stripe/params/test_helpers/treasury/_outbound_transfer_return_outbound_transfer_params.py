# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class OutboundTransferReturnOutboundTransferParams(TypedDict):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    returned_details: NotRequired[
        "OutboundTransferReturnOutboundTransferParamsReturnedDetails"
    ]
    """
    Details about a returned OutboundTransfer.
    """


class OutboundTransferReturnOutboundTransferParamsReturnedDetails(TypedDict):
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
    Reason for the return.
    """
