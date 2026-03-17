# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired


class AccountRefreshAccountParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    features: List[Literal["balance", "ownership", "transactions"]]
    """
    The list of account features that you would like to refresh.
    """
