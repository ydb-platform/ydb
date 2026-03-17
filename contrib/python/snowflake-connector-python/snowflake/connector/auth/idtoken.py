#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

from typing import Any

from ..network import ID_TOKEN_AUTHENTICATOR
from .by_plugin import AuthByPlugin, AuthType


class AuthByIdToken(AuthByPlugin):
    """Internal IdToken Based Authentication.

    Works by accepting an id_toke and use that to authenticate. Only be used when users are using EXTERNAL_BROWSER_AUTHENTICATOR
    """

    @property
    def type_(self) -> AuthType:
        return AuthType.ID_TOKEN

    @property
    def assertion_content(self) -> str:
        return self._id_token

    def __init__(self, id_token: str) -> None:
        """Initialized an instance with an IdToken."""
        super().__init__()
        self._id_token: str | None = id_token

    def reset_secrets(self) -> None:
        self._id_token = None

    def prepare(self, **kwargs: Any) -> None:
        pass

    def reauthenticate(self, **kwargs: Any) -> dict[str, bool]:
        return {"success": False}

    def update_body(self, body: dict[Any, Any]) -> None:
        """Idtoken needs the authenticator and token attributes set."""
        body["data"]["AUTHENTICATOR"] = ID_TOKEN_AUTHENTICATOR
        body["data"]["TOKEN"] = self._id_token
