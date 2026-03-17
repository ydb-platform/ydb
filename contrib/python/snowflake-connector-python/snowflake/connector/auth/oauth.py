#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

from typing import Any

from ..network import OAUTH_AUTHENTICATOR
from .by_plugin import AuthByPlugin, AuthType


class AuthByOAuth(AuthByPlugin):
    """OAuth Based Authentication.

    Works by accepting an OAuth token and using that to authenticate.
    """

    @property
    def type_(self) -> AuthType:
        return AuthType.OAUTH

    @property
    def assertion_content(self) -> str:
        """Returns the token."""
        return self._oauth_token

    def __init__(self, oauth_token: str) -> None:
        """Initializes an instance with an OAuth Token."""
        super().__init__()
        self._oauth_token: str | None = oauth_token

    def reset_secrets(self) -> None:
        self._oauth_token = None

    def prepare(
        self,
        **kwargs: Any,
    ) -> None:
        """Nothing to do here, token should be obtained outside the driver."""
        pass

    def reauthenticate(self, **kwargs: Any) -> dict[str, bool]:
        return {"success": False}

    def update_body(self, body: dict[Any, Any]) -> None:
        """Update some information required by OAuth.

        OAuth needs the authenticator and token attributes set, as well as loginname, which is set already in auth.py.
        """
        body["data"]["AUTHENTICATOR"] = OAUTH_AUTHENTICATOR
        body["data"]["TOKEN"] = self._oauth_token
