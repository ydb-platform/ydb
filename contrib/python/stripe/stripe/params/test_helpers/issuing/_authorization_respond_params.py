# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import List
from typing_extensions import NotRequired, TypedDict


class AuthorizationRespondParams(TypedDict):
    confirmed: bool
    """
    Whether to simulate the user confirming that the transaction was legitimate (true) or telling Stripe that it was fraudulent (false).
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
