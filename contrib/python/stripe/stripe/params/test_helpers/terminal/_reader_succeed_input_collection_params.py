# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class ReaderSucceedInputCollectionParams(TypedDict):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    skip_non_required_inputs: NotRequired[Literal["all", "none"]]
    """
    This parameter defines the skip behavior for input collection.
    """
