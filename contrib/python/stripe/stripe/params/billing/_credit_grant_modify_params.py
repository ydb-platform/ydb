# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired


class CreditGrantModifyParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    expires_at: NotRequired["Literal['']|int"]
    """
    The time when the billing credits created by this credit grant expire. If set to empty, the billing credits never expire.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of key-value pairs you can attach to an object. You can use this to store additional information about the object (for example, cost basis) in a structured format.
    """
