# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing_extensions import NotRequired, TypedDict


class PersonListParams(TypedDict):
    limit: NotRequired[int]
    """
    The upper limit on the number of accounts returned by the List Account request.
    """
