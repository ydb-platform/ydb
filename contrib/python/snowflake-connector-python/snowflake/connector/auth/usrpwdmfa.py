#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from ..errorcode import ER_NO_PASSWORD
from ..errors import ProgrammingError
from .by_plugin import AuthByPlugin, AuthType

if TYPE_CHECKING:
    from .. import SnowflakeConnection

logger = logging.getLogger(__name__)

MFA_TOKEN = "MFATOKEN"


class AuthByUsrPwdMfa(AuthByPlugin):
    """Username & password & mfa authenticator."""

    @property
    def assertion_content(self):
        return "*********"

    def __init__(
        self,
        password: str,
        mfa_token: str | None = None,
    ) -> None:
        """Initializes and instance with a password and a mfa token."""
        super().__init__()
        self._password: str | None = password
        self._mfa_token: str | None = mfa_token

    def reset_secrets(self) -> None:
        self._password = None
        self._mfa_token = None

    @property
    def type_(self) -> AuthType:
        return AuthType.USR_PWD_MFA

    def prepare(
        self,
        *,
        conn: SnowflakeConnection,
        **kwargs: Any,
    ) -> None:
        if conn._rest and conn._rest.mfa_token:
            self._mfa_token = conn._rest.mfa_token

    def reauthenticate(self, **kwargs) -> dict[str, bool]:
        return {"success": False}

    def update_body(self, body: dict[Any, Any]) -> None:
        """Sets the password and mfa_token if available.

        Don't set body['data']['AUTHENTICATOR'], since this is still snowflake default authenticator.
        """
        if not self._password:
            raise ProgrammingError(
                msg="Password for username password authenticator is empty.",
                errno=ER_NO_PASSWORD,
            )
        body["data"]["PASSWORD"] = self._password
        if self._mfa_token:
            body["data"]["TOKEN"] = self._mfa_token
