# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import NotRequired


class PaymentIntentVerifyMicrodepositsParams(RequestOptions):
    amounts: NotRequired[List[int]]
    """
    Two positive integers, in *cents*, equal to the values of the microdeposits sent to the bank account.
    """
    descriptor_code: NotRequired[str]
    """
    A six-character code starting with SM present in the microdeposit sent to the bank account.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
