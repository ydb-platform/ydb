# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class AccountCloseParams(TypedDict):
    applied_configurations: NotRequired[
        List[Literal["customer", "merchant", "recipient"]]
    ]
    """
    Configurations on the Account to be closed. All configurations on the Account must be passed in for this request to succeed.
    """
