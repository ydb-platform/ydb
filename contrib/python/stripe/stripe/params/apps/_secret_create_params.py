# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class SecretCreateParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    expires_at: NotRequired[int]
    """
    The Unix timestamp for the expiry time of the secret, after which the secret deletes.
    """
    name: str
    """
    A name for the secret that's unique within the scope.
    """
    payload: str
    """
    The plaintext secret value to be stored.
    """
    scope: "SecretCreateParamsScope"
    """
    Specifies the scoping of the secret. Requests originating from UI extensions can only access account-scoped secrets or secrets scoped to their own user.
    """


class SecretCreateParamsScope(TypedDict):
    type: Literal["account", "user"]
    """
    The secret scope type.
    """
    user: NotRequired[str]
    """
    The user ID. This field is required if `type` is set to `user`, and should not be provided if `type` is set to `account`.
    """
