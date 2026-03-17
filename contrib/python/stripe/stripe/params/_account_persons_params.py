# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import NotRequired, TypedDict


class AccountPersonsParams(RequestOptions):
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
    relationship: NotRequired["AccountPersonsParamsRelationship"]
    """
    Filters on the list of people returned based on the person's relationship to the account's company.
    """
    starting_after: NotRequired[str]
    """
    A cursor for use in pagination. `starting_after` is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, ending with `obj_foo`, your subsequent call can include `starting_after=obj_foo` in order to fetch the next page of the list.
    """


class AccountPersonsParamsRelationship(TypedDict):
    authorizer: NotRequired[bool]
    """
    A filter on the list of people returned based on whether these people are authorizers of the account's representative.
    """
    director: NotRequired[bool]
    """
    A filter on the list of people returned based on whether these people are directors of the account's company.
    """
    executive: NotRequired[bool]
    """
    A filter on the list of people returned based on whether these people are executives of the account's company.
    """
    legal_guardian: NotRequired[bool]
    """
    A filter on the list of people returned based on whether these people are legal guardians of the account's representative.
    """
    owner: NotRequired[bool]
    """
    A filter on the list of people returned based on whether these people are owners of the account's company.
    """
    representative: NotRequired[bool]
    """
    A filter on the list of people returned based on whether these people are the representative of the account's company.
    """
