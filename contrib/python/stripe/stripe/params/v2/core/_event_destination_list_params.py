# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class EventDestinationListParams(TypedDict):
    include: NotRequired[List[Literal["webhook_endpoint.url"]]]
    """
    Additional fields to include in the response. Currently supports `webhook_endpoint.url`.
    """
    limit: NotRequired[int]
    """
    The page size.
    """
