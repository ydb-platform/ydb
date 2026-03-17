# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class EventDestinationRetrieveParams(TypedDict):
    include: NotRequired[List[Literal["webhook_endpoint.url"]]]
    """
    Additional fields to include in the response.
    """
