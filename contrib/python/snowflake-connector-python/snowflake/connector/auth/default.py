#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

from typing import Any

from .by_plugin import AuthByPlugin, AuthType


class AuthByDefault(AuthByPlugin):
    """Default username and password authenticator."""

    @property
    def type_(self) -> AuthType:
        return AuthType.DEFAULT

    @property
    def assertion_content(self) -> str:
        return "*********"

    def __init__(self, password: str) -> None:
        """Initializes an instance with a password."""
        super().__init__()
        self._password: str | None = password

    def reset_secrets(self) -> None:
        self._password = None

    def prepare(self, **kwargs: Any) -> None:
        pass

    def reauthenticate(self, **kwargs: Any) -> dict[str, bool]:
        return {"success": False}

    def update_body(self, body: dict[Any, Any]) -> None:
        """Sets the password if available."""
        body["data"]["PASSWORD"] = self._password
