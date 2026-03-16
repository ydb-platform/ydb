# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import NotRequired


class SourceRetrieveParams(RequestOptions):
    client_secret: NotRequired[str]
    """
    The client secret of the source. Required if a publishable key is used to retrieve the source.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
