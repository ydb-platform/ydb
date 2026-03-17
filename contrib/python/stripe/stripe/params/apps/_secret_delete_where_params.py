# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class SecretDeleteWhereParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    name: str
    """
    A name for the secret that's unique within the scope.
    """
    scope: "SecretDeleteWhereParamsScope"
    """
    Specifies the scoping of the secret. Requests originating from UI extensions can only access account-scoped secrets or secrets scoped to their own user.
    """


class SecretDeleteWhereParamsScope(TypedDict):
    type: Literal["account", "user"]
    """
    The secret scope type.
    """
    user: NotRequired[str]
    """
    The user ID. This field is required if `type` is set to `user`, and should not be provided if `type` is set to `account`.
    """
