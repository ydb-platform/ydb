# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

import abc
import time
from dataclasses import dataclass

# ignore TC003 to make sphinx not completely drop the ball
from os import PathLike  # noqa: TC003

from . import _typing as t
from .api import (
    _TAuth,
    Auth,
)
from .exceptions import (
    ConfigurationError,
    Neo4jError,
)


if t.TYPE_CHECKING:
    from ._typing import Protocol as _Protocol
else:
    _Protocol = object


@dataclass
class ExpiringAuth:
    """
    Represents potentially expiring authentication information.

    This class is used with :meth:`.AuthManagers.bearer` and
    :meth:`.AsyncAuthManagers.bearer`.

    :param auth: The authentication information.
    :param expires_at:
        Unix timestamp (seconds since 1970-01-01 00:00:00 UTC)
        indicating when the authentication information expires.
        If :data:`None`, the authentication information is considered to not
        expire until the server explicitly indicates so.

    .. seealso::
        :meth:`.AuthManagers.bearer`,
        :meth:`.AsyncAuthManagers.bearer`

    .. versionadded:: 5.8

    .. versionchanged:: 5.9

        * Removed parameter and attribute ``expires_in`` (relative expiration
          time). Replaced with ``expires_at`` (absolute expiration time).
        * :meth:`.expires_in` can be used to create an :class:`.ExpiringAuth`
          with a relative expiration time.

    .. versionchanged:: 5.14 Stabilized from preview.
    """

    auth: _TAuth
    expires_at: float | None = None

    def expires_in(self, seconds: float) -> ExpiringAuth:
        """
        Return a (flat) copy of this object with a new expiration time.

        This is a convenience method for creating an :class:`.ExpiringAuth`
        for a relative expiration time ("expires in" instead of "expires at").

            >>> import time, freezegun
            >>> with freezegun.freeze_time("1970-01-01 00:00:40"):
            ...     ExpiringAuth(("user", "pass")).expires_in(2)
            ExpiringAuth(auth=('user', 'pass'), expires_at=42.0)
            >>> with freezegun.freeze_time("1970-01-01 00:00:40"):
            ...     ExpiringAuth(("user", "pass"), time.time() + 2)
            ExpiringAuth(auth=('user', 'pass'), expires_at=42.0)

        :param seconds:
            The number of seconds from now until the authentication information
            expires.

        .. versionadded:: 5.9
        """
        return ExpiringAuth(self.auth, time.time() + seconds)


def expiring_auth_has_expired(auth: ExpiringAuth) -> bool:
    expires_at = auth.expires_at
    return expires_at is not None and expires_at < time.time()


class AuthManager(metaclass=abc.ABCMeta):
    """
    Abstract base class for authentication information managers.

    The driver provides some default implementations of this class in
    :class:`.AuthManagers` for convenience.

    Custom implementations of this class can be used to provide more complex
    authentication refresh functionality.

    .. warning::

        The manager **must not** interact with the driver in any way as this
        can cause deadlocks and undefined behaviour.

        Furthermore, the manager is expected to be thread-safe.

        The token returned must always belong to the same identity.
        Switching identities using the ``AuthManager`` is undefined behavior.
        You may use :ref:`session-level authentication<session-auth-ref>`
        for such use-cases.

    .. seealso:: :class:`.AuthManagers`

    .. versionadded:: 5.8

    .. versionchanged:: 5.12
        ``on_auth_expired`` was removed from the interface and replaced by
        :meth:`handle_security_exception`. The new method is called when the
        server returns any ``Neo.ClientError.Security.*`` error. Its signature
        differs in that it additionally receives the error returned by the
        server and returns a boolean indicating whether the error was handled.

    .. versionchanged:: 5.14 Stabilized from preview.
    """

    @abc.abstractmethod
    def get_auth(self) -> _TAuth:
        """
        Return the current authentication information.

        The driver will call this method very frequently. It is recommended
        to implement some form of caching to avoid unnecessary overhead.

        .. warning::

            The method must only ever return auth information belonging to the
            same identity.
            Switching identities using the ``AuthManager`` is undefined
            behavior. You may use
            :ref:`session-level authentication<session-auth-ref>` for such
            use-cases.
        """
        ...

    @abc.abstractmethod
    def handle_security_exception(
        self, auth: _TAuth, error: Neo4jError
    ) -> bool:
        """
        Handle the server indicating authentication failure.

        The driver will call this method when the server returns any
        ``Neo.ClientError.Security.*`` error. The error will then be processed
        further as usual.

        :param auth:
            The authentication information that was used when the server
            returned the error.
        :param error:
            The error returned by the server.

        :returns:
            Whether the error was handled (:data:`True`), in which case the
            driver will mark the error as retryable
            (see :meth:`.Neo4jError.is_retryable`).

        .. versionadded:: 5.12
        """
        ...


class AsyncAuthManager(_Protocol, metaclass=abc.ABCMeta):
    """
    Async version of :class:`.AuthManager`.

    .. seealso:: :class:`.AuthManager`

    .. versionadded:: 5.8

    .. versionchanged:: 5.12
        ``on_auth_expired`` was removed from the interface and replaced by
         :meth:`handle_security_exception`. See :class:`.AuthManager`.

    .. versionchanged:: 5.14 Stabilized from preview.
    """

    @abc.abstractmethod
    async def get_auth(self) -> _TAuth:
        """
        Async version of :meth:`.AuthManager.get_auth`.

        .. seealso:: :meth:`.AuthManager.get_auth`
        """
        ...

    @abc.abstractmethod
    async def handle_security_exception(
        self, auth: _TAuth, error: Neo4jError
    ) -> bool:
        """
        Async version of :meth:`.AuthManager.handle_security_exception`.

        .. seealso:: :meth:`.AuthManager.handle_security_exception`
        """
        ...


@dataclass
class ClientCertificate:
    """
    Simple data class to hold client certificate information.

    The attributes are the same as the arguments to
    :meth:`ssl.SSLContext.load_cert_chain()`.

    .. versionadded:: 5.19

    .. versionchanged:: 5.27 Stabilized from preview.
    """

    certfile: str | bytes | PathLike[str] | PathLike[bytes]
    keyfile: str | bytes | PathLike[str] | PathLike[bytes] | None = None
    password: t.Callable[[], str | bytes] | str | bytes | None = None


class ClientCertificateProvider(_Protocol, metaclass=abc.ABCMeta):
    """
    Interface for providing a client certificate to the driver for mutual TLS.

    This is an abstract base class (:class:`abc.ABC`) as well as a protocol
    (:class:`typing.Protocol`). Meaning you can either inherit from it or just
    implement all required method on a class to satisfy the type constraints.

    The package provides some default implementations of this class in
    :class:`.ClientCertificateProviders` for convenience.

    The driver will call :meth:`.get_certificate` to check if the client wants
    the driver to use as new certificate for mutual TLS.

    The certificate is only used as a second factor for authenticating the
    client.
    The DBMS user still needs to authenticate with an authentication token.

    Note that the work done in the methods of this interface count towards the
    connection acquisition affected by the respective timeout setting
    :ref:`connection-acquisition-timeout-ref`.
    Should fetching the certificate be particularly slow, it might be necessary
    to increase the timeout.

    .. warning::

        The provider **must not** interact with the driver in any way as this
        can cause deadlocks and undefined behaviour.

    .. versionadded:: 5.19

    .. versionchanged:: 5.27 Stabilized from preview.
    """

    @abc.abstractmethod
    def get_certificate(self) -> ClientCertificate | None:
        """
        Return the new certificate (if present) to use for new connections.

        If no new certificate is available, return :data:`None`.
        This will make the driver continue using the current certificate.

        Note that a new certificate will only be used for new connections.
        Already established connections will continue using the old
        certificate as TLS is established during connection setup.

        :returns: The new certificate to use for new connections.
        """
        ...


class AsyncClientCertificateProvider(_Protocol, metaclass=abc.ABCMeta):
    """
    Async version of :class:`.ClientCertificateProvider`.

    The package provides some default implementations of this class in
    :class:`.AsyncClientCertificateProviders` for convenience.

    .. seealso::
        :class:`.ClientCertificateProvider`,
        :class:`.AsyncClientCertificateProviders`

    .. versionadded:: 5.19

    .. versionchanged:: 5.27 Stabilized from preview.
    """

    @abc.abstractmethod
    async def get_certificate(self) -> ClientCertificate | None:
        """
        Return the new certificate (if present) to use for new connections.

        .. seealso:: :meth:`.ClientCertificateProvider.get_certificate`
        """
        ...


def to_auth_dict(auth: _TAuth) -> dict[str, t.Any]:
    # Determine auth details
    if not auth:
        return {}
    elif isinstance(auth, tuple) and 2 <= len(auth) <= 3:
        return vars(Auth("basic", *auth))
    else:
        try:
            return vars(auth)
        except (KeyError, TypeError) as e:
            raise ConfigurationError(
                f"Cannot determine auth details from {auth!r}"
            ) from e
