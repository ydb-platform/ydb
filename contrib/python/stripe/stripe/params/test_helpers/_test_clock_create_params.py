# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import NotRequired


class TestClockCreateParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    frozen_time: int
    """
    The initial frozen time for this test clock.
    """
    name: NotRequired[str]
    """
    The name for this test clock.
    """
