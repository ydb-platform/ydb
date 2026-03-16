# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import List
from typing_extensions import NotRequired, TypedDict


class CustomerPaymentSourceVerifyParams(TypedDict):
    amounts: NotRequired[List[int]]
    """
    Two positive integers, in *cents*, equal to the values of the microdeposits sent to the bank account.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
