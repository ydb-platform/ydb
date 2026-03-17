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
from logging import getLogger

from .. import _typing as t
from .._async_compat.concurrency import (
    CooperativeLock,
    Lock,
)
from .._auth_management import (
    AuthManager,
    ClientCertificate,
    ClientCertificateProvider,
    expiring_auth_has_expired,
    ExpiringAuth,
)

# ignore TC001 to make sphinx not completely drop the ball
from ..api import _TAuth  # noqa: TC001


if t.TYPE_CHECKING:
    from ..exceptions import Neo4jError


log = getLogger("neo4j.auth_management")


class StaticAuthManager(AuthManager):
    _auth: _TAuth

    def __init__(self, auth: _TAuth) -> None:
        self._auth = auth

    def get_auth(self) -> _TAuth:
        return self._auth

    def handle_security_exception(
        self, auth: _TAuth, error: Neo4jError
    ) -> bool:
        return False


class Neo4jAuthTokenManager(AuthManager):
    _current_auth: ExpiringAuth | None
    _provider: t.Callable[[], t.Union[ExpiringAuth]]
    _handled_codes: frozenset[str]
    _lock: Lock

    def __init__(
        self,
        provider: t.Callable[[], t.Union[ExpiringAuth]],
        handled_codes: frozenset[str],
    ) -> None:
        self._provider = provider
        self._handled_codes = handled_codes
        self._current_auth = None
        self._lock = Lock()

    def _refresh_auth(self):
        try:
            self._current_auth = self._provider()
        except BaseException as e:
            log.error("[     ]  _: <AUTH MANAGER> provider failed: %r", e)
            raise
        if self._current_auth is None:
            raise TypeError(
                "Auth provider function passed to expiration_based "
                "AuthManager returned None, expected ExpiringAuth"
            )

    def get_auth(self) -> _TAuth:
        with self._lock:
            auth = self._current_auth
            if auth is None or expiring_auth_has_expired(auth):
                log.debug(
                    "[     ]  _: <AUTH MANAGER> refreshing (%s)",
                    "init" if auth is None else "time out",
                )
                self._refresh_auth()
                auth = self._current_auth
                assert auth is not None
            return auth.auth

    def handle_security_exception(
        self, auth: _TAuth, error: Neo4jError
    ) -> bool:
        if error.code not in self._handled_codes:
            return False
        with self._lock:
            cur_auth = self._current_auth
            if cur_auth is not None and cur_auth.auth == auth:
                log.debug(
                    "[     ]  _: <AUTH MANAGER> refreshing (error %s)",
                    error.code,
                )
                self._refresh_auth()
            return True


class AuthManagers:
    """
    A collection of :class:`.AuthManager` factories.

    .. versionadded:: 5.8

    .. versionchanged:: 5.12

        * Method ``expiration_based()`` was renamed to :meth:`bearer`.
        * Added :meth:`basic`.

    .. versionchanged:: 5.14 Stabilized from preview.
    """

    @staticmethod
    def static(auth: _TAuth) -> AuthManager:
        """
        Create a static auth manager.

        The manager will always return the auth info provided at its creation.

        Example::

            # NOTE: this example is for illustration purposes only.
            #       The driver will automatically wrap static auth info in a
            #       static auth manager.

            import neo4j
            from neo4j.auth_management import AuthManagers


            auth = neo4j.basic_auth("neo4j", "password")

            with neo4j.GraphDatabase.driver(
                "neo4j://example.com:7687",
                auth=AuthManagers.static(auth)
                # auth=auth  # this is equivalent
            ) as driver:
                ...  # do stuff

        :param auth: The auth to return.

        :returns:
            An instance of an implementation of :class:`.AuthManager` that
            always returns the same auth.

        .. versionadded:: 5.8

        .. versionchanged:: 5.14 Stabilized from preview.
        """
        return StaticAuthManager(auth)

    @staticmethod
    def basic(
        provider: t.Callable[[], t.Union[_TAuth]],
    ) -> AuthManager:
        """
        Create an auth manager handling basic auth password rotation.

        This factory wraps the provider function in an auth manager
        implementation that caches the provided auth info until the server
        notifies the driver that the auth info has expired (by returning
        an error that indicates that the password is invalid).

        Note that this implies that the provider function will be called again
        if it provides wrong auth info, potentially deferring failure due to a
        wrong password or username.

        .. warning::

            The provider function **must not** interact with the driver in any
            way as this can cause deadlocks and undefined behaviour.

            The provider function must only ever return auth information
            belonging to the same identity.
            Switching identities is undefined behavior.
            You may use :ref:`session-level authentication<session-auth-ref>`
            for such use-cases.

        Example::

            import neo4j
            from neo4j.auth_management import (
                AuthManagers,
                ExpiringAuth,
            )


            def auth_provider():
                # some way of getting a token
                user, password = get_current_auth()
                return (user, password)


            with neo4j.GraphDatabase.driver(
                "neo4j://example.com:7687",
                auth=AuthManagers.basic(auth_provider)
            ) as driver:
                ...  # do stuff

        :param provider:
            A callable that provides new auth info whenever the server notifies
            the driver that the previous auth info is invalid.

        :returns:
            An instance of an implementation of :class:`.AuthManager` that
            returns auth info from the given provider and refreshes it, calling
            the provider again, when the auth info was rejected by the server.

        .. versionadded:: 5.12

        .. versionchanged:: 5.14 Stabilized from preview.
        """
        handled_codes = frozenset(("Neo.ClientError.Security.Unauthorized",))

        def wrapped_provider() -> ExpiringAuth:
            return ExpiringAuth(provider())

        return Neo4jAuthTokenManager(wrapped_provider, handled_codes)

    @staticmethod
    def bearer(
        provider: t.Callable[[], t.Union[ExpiringAuth]],
    ) -> AuthManager:
        """
        Create an auth manager for potentially expiring bearer auth tokens.

        This factory wraps the provider function in an auth manager
        implementation that caches the provided auth info until either the
        :attr:`.ExpiringAuth.expires_at` exceeded or the server notified the
        driver that the auth info has expired (by returning an error that
        indicates that the bearer auth token has expired).

        .. warning::

            The provider function **must not** interact with the driver in any
            way as this can cause deadlocks and undefined behaviour.

            The provider function must only ever return auth information
            belonging to the same identity.
            Switching identities is undefined behavior.
            You may use :ref:`session-level authentication<session-auth-ref>`
            for such use-cases.

        Example::

            import neo4j
            from neo4j.auth_management import (
                AuthManagers,
                ExpiringAuth,
            )


            def auth_provider():
                # some way of getting a token
                sso_token = get_sso_token()
                # assume we know our tokens expire every 60 seconds
                expires_in = 60

                # Include a little buffer so that we fetch a new token
                # *before* the old one expires
                expires_in -= 10

                auth = neo4j.bearer_auth(sso_token)
                return ExpiringAuth(auth=auth).expires_in(expires_in)


            with neo4j.GraphDatabase.driver(
                "neo4j://example.com:7687",
                auth=AuthManagers.bearer(auth_provider)
            ) as driver:
                ...  # do stuff

        :param provider:
            A callable that provides a :class:`.ExpiringAuth` instance.

        :returns:
            An instance of an implementation of :class:`.AuthManager` that
            returns auth info from the given provider and refreshes it, calling
            the provider again, when the auth info expires (either because it's
            reached its expiry time or because the server flagged it as
            expired).

        .. versionadded:: 5.12

        .. versionchanged:: 5.14 Stabilized from preview.
        """
        handled_codes = frozenset(
            (
                "Neo.ClientError.Security.TokenExpired",
                "Neo.ClientError.Security.Unauthorized",
            )
        )
        return Neo4jAuthTokenManager(provider, handled_codes)


class _StaticClientCertificateProvider(ClientCertificateProvider):
    _cert: ClientCertificate | None

    def __init__(self, cert: ClientCertificate) -> None:
        self._cert = cert

    def get_certificate(self) -> ClientCertificate | None:
        cert, self._cert = self._cert, None
        return cert


class RotatingClientCertificateProvider(ClientCertificateProvider):
    """
    Abstract base class for certificate providers that can rotate certificates.

    The provider will make the driver use the initial certificate for all
    connections until the certificate is updated using the
    :meth:`update_certificate` method.
    From that point on, the new certificate will be used for all new
    connections until :meth:`update_certificate` is called again and so on.

    Example::

        from neo4j import GraphDatabase
        from neo4j.auth_management import (
            ClientCertificate,
            ClientCertificateProviders,
        )


        provider = ClientCertificateProviders.rotating(
            ClientCertificate(
                certfile="path/to/certfile.pem",
                keyfile="path/to/keyfile.pem",
                password=lambda: "super_secret_password"
            )
        )
        driver = GraphDatabase.driver(
           # secure driver must be configured for client certificate
           # to be used: (...+s[sc] scheme or encrypted=True)
           "neo4j+s://example.com:7687",
           # auth still required as before, unless server is configured to not
           # use authentication
           auth=("neo4j", "password"),
           client_certificate=provider
        )

        # do work with the driver, until the certificate needs to be rotated
        ...

        provider.update_certificate(
            ClientCertificate(
                certfile="path/to/new/certfile.pem",
                keyfile="path/to/new/keyfile.pem",
                password=lambda: "new_super_secret_password"
            )
        )

        # do more work with the driver, until the certificate needs to be
        # rotated again
        ...

    .. versionadded:: 5.19

    .. versionchanged:: 5.24

        Turned this class into an abstract class to make the actual
        implementation internal. This entails removing the possibility to
        directly instantiate this class. Please use the factory method
        :meth:`.ClientCertificateProviders.rotating` instead.

    .. versionchanged:: 5.27 Stabilized from preview.
    """

    @abc.abstractmethod
    def update_certificate(self, cert: ClientCertificate) -> None:
        """Update the certificate to use for new connections."""


class _Neo4jRotatingClientCertificateProvider(
    RotatingClientCertificateProvider
):
    def __init__(self, initial_cert: ClientCertificate) -> None:
        self._cert: ClientCertificate | None = initial_cert
        self._lock = CooperativeLock()

    def get_certificate(self) -> ClientCertificate | None:
        with self._lock:
            cert, self._cert = self._cert, None
            return cert

    def update_certificate(self, cert: ClientCertificate) -> None:
        with self._lock:
            self._cert = cert


class ClientCertificateProviders:
    """
    A collection of :class:`.ClientCertificateProvider` factories.

    .. versionadded:: 5.19

    .. versionchanged:: 5.27 Stabilized from preview.
    """

    @staticmethod
    def static(cert: ClientCertificate) -> ClientCertificateProvider:
        """
        Create a static client certificate provider.

        The provider simply makes the driver use the given certificate for all
        connections.
        """
        return _StaticClientCertificateProvider(cert)

    @staticmethod
    def rotating(
        initial_cert: ClientCertificate,
    ) -> RotatingClientCertificateProvider:
        """
        Create certificate provider that allows for rotating certificates.

        .. seealso:: :class:`.RotatingClientCertificateProvider`
        """
        return _Neo4jRotatingClientCertificateProvider(initial_cert)
