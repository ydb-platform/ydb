# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import NotRequired


class ValueListItemCreateParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    value: str
    """
    The value of the item (whose type must match the type of the parent value list).
    """
    value_list: str
    """
    The identifier of the value list which the created item will be added to.
    """
