# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import NotRequired


class SetupIntentRetrieveParams(RequestOptions):
    client_secret: NotRequired[str]
    """
    The client secret of the SetupIntent. We require this string if you use a publishable key to retrieve the SetupIntent.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
