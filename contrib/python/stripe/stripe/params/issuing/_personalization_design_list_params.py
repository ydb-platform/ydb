# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class PersonalizationDesignListParams(RequestOptions):
    ending_before: NotRequired[str]
    """
    A cursor for use in pagination. `ending_before` is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, starting with `obj_bar`, your subsequent call can include `ending_before=obj_bar` in order to fetch the previous page of the list.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    limit: NotRequired[int]
    """
    A limit on the number of objects to be returned. Limit can range between 1 and 100, and the default is 10.
    """
    lookup_keys: NotRequired[List[str]]
    """
    Only return personalization designs with the given lookup keys.
    """
    preferences: NotRequired["PersonalizationDesignListParamsPreferences"]
    """
    Only return personalization designs with the given preferences.
    """
    starting_after: NotRequired[str]
    """
    A cursor for use in pagination. `starting_after` is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, ending with `obj_foo`, your subsequent call can include `starting_after=obj_foo` in order to fetch the next page of the list.
    """
    status: NotRequired[Literal["active", "inactive", "rejected", "review"]]
    """
    Only return personalization designs with the given status.
    """


class PersonalizationDesignListParamsPreferences(TypedDict):
    is_default: NotRequired[bool]
    """
    Only return the personalization design that's set as the default. A connected account uses the Connect platform's default design if no personalization design is set as the default.
    """
    is_platform_default: NotRequired[bool]
    """
    Only return the personalization design that is set as the Connect platform's default. This parameter is only applicable to connected accounts.
    """
