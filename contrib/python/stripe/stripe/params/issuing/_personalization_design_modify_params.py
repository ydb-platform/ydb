# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class PersonalizationDesignModifyParams(RequestOptions):
    card_logo: NotRequired["Literal['']|str"]
    """
    The file for the card logo, for use with physical bundles that support card logos. Must have a `purpose` value of `issuing_logo`.
    """
    carrier_text: NotRequired[
        "Literal['']|PersonalizationDesignModifyParamsCarrierText"
    ]
    """
    Hash containing carrier text, for use with physical bundles that support carrier text.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    lookup_key: NotRequired["Literal['']|str"]
    """
    A lookup key used to retrieve personalization designs dynamically from a static string. This may be up to 200 characters.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    name: NotRequired["Literal['']|str"]
    """
    Friendly display name. Providing an empty string will set the field to null.
    """
    physical_bundle: NotRequired[str]
    """
    The physical bundle object belonging to this personalization design.
    """
    preferences: NotRequired["PersonalizationDesignModifyParamsPreferences"]
    """
    Information on whether this personalization design is used to create cards when one is not specified.
    """
    transfer_lookup_key: NotRequired[bool]
    """
    If set to true, will atomically remove the lookup key from the existing personalization design, and assign it to this personalization design.
    """


class PersonalizationDesignModifyParamsCarrierText(TypedDict):
    footer_body: NotRequired["Literal['']|str"]
    """
    The footer body text of the carrier letter.
    """
    footer_title: NotRequired["Literal['']|str"]
    """
    The footer title text of the carrier letter.
    """
    header_body: NotRequired["Literal['']|str"]
    """
    The header body text of the carrier letter.
    """
    header_title: NotRequired["Literal['']|str"]
    """
    The header title text of the carrier letter.
    """


class PersonalizationDesignModifyParamsPreferences(TypedDict):
    is_default: bool
    """
    Whether we use this personalization design to create cards when one isn't specified. A connected account uses the Connect platform's default design if no personalization design is set as the default design.
    """
