# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import NotRequired


class MeterModifyParams(RequestOptions):
    display_name: NotRequired[str]
    """
    The meter's name. Not visible to the customer.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
