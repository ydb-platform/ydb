# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired


class FeatureModifyParams(RequestOptions):
    active: NotRequired[bool]
    """
    Inactive features cannot be attached to new products and will not be returned from the features list endpoint.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of key-value pairs that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    name: NotRequired[str]
    """
    The feature's name, for your own purpose, not meant to be displayable to the customer.
    """
