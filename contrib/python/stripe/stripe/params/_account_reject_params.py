# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import NotRequired


class AccountRejectParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    reason: str
    """
    The reason for rejecting the account. Can be `fraud`, `terms_of_service`, or `other`.
    """
