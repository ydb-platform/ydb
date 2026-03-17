# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import NotRequired


class FeatureCreateParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    lookup_key: str
    """
    A unique key you provide as your own system identifier. This may be up to 80 characters.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of key-value pairs that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    name: str
    """
    The feature's name, for your own purpose, not meant to be displayable to the customer.
    """
