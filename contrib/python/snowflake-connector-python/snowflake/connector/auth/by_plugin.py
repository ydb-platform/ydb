#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

"""This module implements the base class for authenticator classes.

Note:
 **kwargs are added to most functions so that child classes can safely ignore extra in
  arguments in case of a caller API change and named arguments are enforced to prevent
  issues with argument being sent in out of order.
"""

import logging
import time
from abc import ABC, abstractmethod
from enum import Enum, unique
from os import getenv
from typing import TYPE_CHECKING, Any

from ..errorcode import ER_FAILED_TO_CONNECT_TO_DB
from ..errors import DatabaseError, Error, OperationalError
from ..sqlstate import SQLSTATE_CONNECTION_WAS_NOT_ESTABLISHED
from ..time_util import DecorrelateJitterBackoff

if TYPE_CHECKING:
    from .. import SnowflakeConnection

logger = logging.getLogger(__name__)

DEFAULT_MAX_CON_RETRY_ATTEMPTS = 1


class AuthRetryCtx:
    def __init__(self) -> None:
        self._current_retry_count = 0
        self._max_retry_attempts = int(
            getenv("MAX_CON_RETRY_ATTEMPTS", DEFAULT_MAX_CON_RETRY_ATTEMPTS)
        )
        self._backoff = DecorrelateJitterBackoff(1, 16)
        self._current_sleep_time = 1

    def get_current_retry_count(self) -> int:
        return self._current_retry_count

    def increment_retry(self) -> None:
        self._current_retry_count += 1

    def should_retry(self) -> bool:
        """Decides whether to retry connection.

        Default value for max retry is 1 because
        Python requests module already tries twice
        by default. Unlike JWT where we need to refresh
        token every 10 seconds, general authenticators
        wait for 60 seconds before connection timeout
        per attempt totaling a 240 sec wait time for a non
        JWT based authenticator which is more than enough.
        This can be changed ofcourse using MAX_CNXN_RETRY_ATTEMPTS
        env variable.
        """
        return self._current_retry_count < self._max_retry_attempts

    def next_sleep_duration(self) -> int:
        self._current_sleep_time = self._backoff.next_sleep(
            self._current_retry_count, self._current_sleep_time
        )
        logger.debug(f"Sleeping for {self._current_sleep_time} seconds")
        return self._current_sleep_time

    def reset(self):
        self._current_retry_count = 0
        self._current_sleep_time = 1


@unique
class AuthType(Enum):
    DEFAULT = "SNOWFLAKE"  # default authenticator name
    EXTERNAL_BROWSER = "EXTERNALBROWSER"
    KEY_PAIR = "SNOWFLAKE_JWT"
    OAUTH = "OAUTH"
    ID_TOKEN = "ID_TOKEN"
    USR_PWD_MFA = "USERNAME_PASSWORD_MFA"
    OKTA = "OKTA"


class AuthByPlugin(ABC):
    """External Authenticator interface."""

    def __init__(self) -> None:
        self._retry_ctx = AuthRetryCtx()
        self.consent_cache_id_token = False
        self._timeout = 120

    @property
    def timeout(self) -> int:
        return self._timeout

    @timeout.setter
    def timeout(self, value: Any) -> None:
        self._timeout = int(value)

    @property
    @abstractmethod
    def type_(self) -> AuthType:
        """Return the Snowflake friendly name of auth class."""
        raise NotImplementedError

    @property
    @abstractmethod
    def assertion_content(self) -> str:
        """Return a safe version of the information used to authenticate with Snowflake.

        This is used for logging, useful for printing temporary tokens, but make sure to
        mask secrets.
        """
        raise NotImplementedError

    @abstractmethod
    def prepare(
        self,
        *,
        conn: SnowflakeConnection,
        authenticator: str,
        service_name: str | None,
        account: str,
        user: str,
        password: str | None,
        **kwargs: Any,
    ) -> str | None:
        """Prepare for authentication.

        This function is useful for situations where we need to reach out to a 3rd-party
        service before authenticating with Snowflake.
        """
        raise NotImplementedError

    @abstractmethod
    def update_body(self, body: dict[Any, Any]) -> None:
        """Update the body of the authentication request."""
        raise NotImplementedError

    @abstractmethod
    def reset_secrets(self) -> None:
        """Reset secret members."""
        raise NotImplementedError

    @abstractmethod
    def reauthenticate(
        self,
        *,
        conn: SnowflakeConnection,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Re-perform authentication.

        The difference between this and authentication is that secrets will be removed
        from memory by the time this gets called.
        """
        raise NotImplementedError

    def _handle_failure(
        self,
        *,
        conn: SnowflakeConnection,
        ret: dict[Any, Any],
        **kwargs: Any,
    ) -> None:
        """Handles a failure when an issue happens while connecting to Snowflake.

        If the user returns from this function execution will continue. The argument
        data can be manipulated from within this function and so recovery is possible
        from here.
        """
        Error.errorhandler_wrapper(
            conn,
            None,
            DatabaseError,
            {
                "msg": "Failed to connect to DB: {host}:{port}, {message}".format(
                    host=conn._rest._host,
                    port=conn._rest._port,
                    message=ret["message"],
                ),
                "errno": int(ret.get("code", -1)),
                "sqlstate": SQLSTATE_CONNECTION_WAS_NOT_ESTABLISHED,
            },
        )

    def handle_timeout(
        self,
        *,
        authenticator: str,
        service_name: str | None,
        account: str,
        user: str,
        password: str,
        **kwargs: Any,
    ) -> None:
        """Default timeout handler.

        This will trigger if the authenticator
        hasn't implemented one. By default we retry on timeouts and use
        jitter to deduce the time to sleep before retrying. The sleep
        time ranges between 1 and 16 seconds.
        """

        del authenticator, service_name, account, user, password
        logger.debug("Default timeout handler invoked for authenticator")
        if not self._retry_ctx.should_retry():
            self._retry_ctx.reset()
            raise OperationalError(
                msg=f"Could not connect to Snowflake backend after {self._retry_ctx.get_current_retry_count()} attempt(s)."
                "Aborting",
                errno=ER_FAILED_TO_CONNECT_TO_DB,
            )
        else:
            logger.debug(
                f"Hit connection timeout, attempt number {self._retry_ctx.get_current_retry_count()}."
                " Will retry in a bit..."
            )
            self._retry_ctx.increment_retry()
            time.sleep(self._retry_ctx.next_sleep_duration())
