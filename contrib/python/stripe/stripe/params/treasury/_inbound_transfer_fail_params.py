# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class InboundTransferFailParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    failure_details: NotRequired["InboundTransferFailParamsFailureDetails"]
    """
    Details about a failed InboundTransfer.
    """


class InboundTransferFailParamsFailureDetails(TypedDict):
    code: NotRequired[
        Literal[
            "account_closed",
            "account_frozen",
            "bank_account_restricted",
            "bank_ownership_changed",
            "debit_not_authorized",
            "incorrect_account_holder_address",
            "incorrect_account_holder_name",
            "incorrect_account_holder_tax_id",
            "insufficient_funds",
            "invalid_account_number",
            "invalid_currency",
            "no_account",
            "other",
        ]
    ]
    """
    Reason for the failure.
    """
