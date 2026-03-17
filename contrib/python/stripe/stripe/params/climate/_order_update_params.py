# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import Dict, List, Union
from typing_extensions import Literal, NotRequired, TypedDict


class OrderUpdateParams(TypedDict):
    beneficiary: NotRequired["Literal['']|OrderUpdateParamsBeneficiary"]
    """
    Publicly sharable reference for the end beneficiary of carbon removal. Assumed to be the Stripe account if not set.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """


class OrderUpdateParamsBeneficiary(TypedDict):
    public_name: Union[Literal[""], str]
    """
    Publicly displayable name for the end beneficiary of carbon removal.
    """
