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

import asyncio
from types import NoneType

from .. import _typing as t
from .._addressing import Address
from .._api import (
    DRIVER_BOLT,
    DRIVER_NEO4J,
    NotificationMinimumSeverity,
    parse_neo4j_uri,
    parse_routing_context,
    RoutingControl,
    SECURITY_TYPE_SECURE,
    SECURITY_TYPE_SELF_SIGNED_CERTIFICATE,
    TelemetryAPI,
)
from .._conf import (
    Config,
    ConfigurationError,
    SessionConfig,
    TrustAll,
    TrustStore,
    WorkspaceConfig,
)
from .._debug import ENABLED as DEBUG_ENABLED
from .._warnings import (
    deprecation_warn,
    preview_warn,
    unclosed_resource_warn,
)
from .._work import (
    EagerResult,
    Query,
    unit_of_work,
)
from ..api import (
    _TAuth,
    AsyncBookmarkManager,
    Auth,
    BookmarkManager,
    Bookmarks,
    READ_ACCESS,
    ServerInfo,
    URI_SCHEME_BOLT,
    URI_SCHEME_BOLT_SECURE,
    URI_SCHEME_BOLT_SELF_SIGNED_CERTIFICATE,
    URI_SCHEME_NEO4J,
    URI_SCHEME_NEO4J_SECURE,
    URI_SCHEME_NEO4J_SELF_SIGNED_CERTIFICATE,
    WRITE_ACCESS,
)
from ..auth_management import (
    AsyncAuthManager,
    AsyncAuthManagers,
    AsyncClientCertificateProvider,
    ClientCertificate,
)
from ..exceptions import (
    DriverError,
    Neo4jError,
)
from .auth_management import _AsyncStaticClientCertificateProvider
from .bookmark_manager import (
    AsyncNeo4jBookmarkManager,
    TBmConsumer as _TBmConsumer,
    TBmSupplier as _TBmSupplier,
)
from .config import AsyncPoolConfig
from .work import (
    AsyncManagedTransaction,
    AsyncResult,
    AsyncSession,
)


if t.TYPE_CHECKING:
    import ssl
    from enum import Enum

    from .._api import (
        T_NotificationDisabledClassification,
        T_NotificationMinimumSeverity,
        T_RoutingControl,
    )

    class _DefaultEnum(Enum):
        default = "default"

    _default = _DefaultEnum.default

else:
    _default = object()

_T = t.TypeVar("_T")


class AsyncGraphDatabase:
    """Accessor for :class:`neo4j.AsyncDriver` construction."""

    if t.TYPE_CHECKING:

        @classmethod
        def driver(
            cls,
            uri: str,
            *,
            auth: _TAuth | AsyncAuthManager = ...,
            max_connection_lifetime: float = ...,
            liveness_check_timeout: float | None = ...,
            max_connection_pool_size: int = ...,
            connection_timeout: float = ...,
            connection_write_timeout: float = ...,
            resolver: (
                t.Callable[[Address], t.Iterable[Address]]
                | t.Callable[[Address], t.Awaitable[t.Iterable[Address]]]
            ) = ...,
            encrypted: bool = ...,
            trusted_certificates: TrustStore = ...,
            client_certificate: (
                ClientCertificate | AsyncClientCertificateProvider | None
            ) = ...,
            ssl_context: ssl.SSLContext | None = ...,
            user_agent: str = ...,
            keep_alive: bool = ...,
            notifications_min_severity: (
                T_NotificationMinimumSeverity | None
            ) = ...,
            # deprecated in favor of notifications_disabled_classifications
            notifications_disabled_categories: (
                t.Iterable[T_NotificationDisabledClassification] | None
            ) = ...,
            notifications_disabled_classifications: (
                t.Iterable[T_NotificationDisabledClassification] | None
            ) = ...,
            warn_notification_severity: (
                T_NotificationMinimumSeverity | None
            ) = ...,
            telemetry_disabled: bool = ...,
            max_transaction_retry_time: float = ...,
            connection_acquisition_timeout: float = ...,
            # undocumented/unsupported options
            # they may be changed or removed any time without prior notice
            initial_retry_delay: float = ...,
            retry_delay_multiplier: float = ...,
            retry_delay_jitter_factor: float = ...,
            database: str | None = ...,
            fetch_size: int = ...,
            impersonated_user: str | None = ...,
            bookmark_manager: (
                AsyncBookmarkManager | BookmarkManager | None
            ) = ...,
        ) -> AsyncDriver: ...

    else:

        @classmethod
        def driver(
            cls,
            uri: str,
            *,
            auth: _TAuth | AsyncAuthManager = None,
            **config,
        ) -> AsyncDriver:
            """
            Create a driver.

            :param uri: the connection URI for the driver,
                see :ref:`async-uri-ref` for available URIs.
            :param auth: the authentication details,
                see :ref:`auth-ref` for available authentication details.
            :param config: driver configuration key-word arguments,
                see :ref:`async-driver-configuration-ref` for available
                key-word arguments.
            """
            driver_type, security_type, parsed = parse_neo4j_uri(uri)

            if not isinstance(auth, AsyncAuthManager):
                auth = AsyncAuthManagers.static(auth)
            config["auth"] = auth

            client_certificate = config.get("client_certificate")
            if isinstance(client_certificate, ClientCertificate):
                # using internal class until public factory is GA:
                # AsyncClientCertificateProviders.static
                config["client_certificate"] = (
                    _AsyncStaticClientCertificateProvider(client_certificate)
                )

            if "trusted_certificates" in config and not isinstance(
                config["trusted_certificates"], TrustStore
            ):
                raise ConfigurationError(
                    'The config setting "trusted_certificates" must be of '
                    "type neo4j.TrustAll, neo4j.TrustCustomCAs, or"
                    "neo4j.TrustSystemCAs but was {}".format(
                        type(config["trusted_certificates"])
                    )
                )

            if security_type in {
                SECURITY_TYPE_SELF_SIGNED_CERTIFICATE,
                SECURITY_TYPE_SECURE,
            } and (
                "encrypted" in config
                or "trusted_certificates" in config
                or "ssl_context" in config
            ):
                raise ConfigurationError(
                    'The config settings "encrypted", "trusted_certificates", '
                    'and "ssl_context" can only be used with the URI schemes '
                    "{!r}. Use the other URI schemes {!r} for setting "
                    "encryption settings.".format(
                        [
                            URI_SCHEME_BOLT,
                            URI_SCHEME_NEO4J,
                        ],
                        [
                            URI_SCHEME_BOLT_SELF_SIGNED_CERTIFICATE,
                            URI_SCHEME_BOLT_SECURE,
                            URI_SCHEME_NEO4J_SELF_SIGNED_CERTIFICATE,
                            URI_SCHEME_NEO4J_SECURE,
                        ],
                    )
                )

            if security_type == SECURITY_TYPE_SECURE:
                config["encrypted"] = True
            elif security_type == SECURITY_TYPE_SELF_SIGNED_CERTIFICATE:
                config["encrypted"] = True
                config["trusted_certificates"] = TrustAll()

            if "notifications_disabled_categories" in config:
                deprecation_warn(
                    "notifications_disabled_categories is deprecated, "
                    "use notifications_disabled_classifications instead.",
                    stack_level=2,
                )
            _normalize_notifications_config(config, driver_level=True)

            liveness_check_timeout = config.get("liveness_check_timeout")
            if (
                liveness_check_timeout is not None
                and liveness_check_timeout < 0
            ):
                raise ConfigurationError(
                    'The config setting "liveness_check_timeout" must be '
                    "greater than or equal to 0 but was "
                    f"{liveness_check_timeout}."
                )

            assert driver_type in {DRIVER_BOLT, DRIVER_NEO4J}
            if driver_type == DRIVER_BOLT:
                if parse_routing_context(parsed.query):
                    raise ConfigurationError(
                        "Routing context (URI query parameters) are not "
                        "supported by direct drivers "
                        f'("bolt[+s[sc]]://" scheme). Given URI: {uri!r}.'
                    )
                return cls._bolt_driver(parsed.netloc, **config)
            # else driver_type == DRIVER_NEO4J
            routing_context = parse_routing_context(parsed.query)
            return cls._neo4j_driver(
                parsed.netloc, routing_context=routing_context, **config
            )

    @classmethod
    def bookmark_manager(
        cls,
        initial_bookmarks: Bookmarks | t.Iterable[str] | None = None,
        bookmarks_supplier: _TBmSupplier | None = None,
        bookmarks_consumer: _TBmConsumer | None = None,
    ) -> AsyncBookmarkManager:
        """
        Create a :class:`.AsyncBookmarkManager` with default implementation.

        Basic usage example to configure sessions with the built-in bookmark
        manager implementation so that all work is automatically causally
        chained (i.e., all reads can observe all previous writes even in a
        clustered setup)::

            import neo4j


            # omitting closing the driver for brevity
            driver = neo4j.AsyncGraphDatabase.driver(...)
            bookmark_manager = neo4j.AsyncGraphDatabase.bookmark_manager(...)

            async with (
                driver.session(bookmark_manager=bookmark_manager) as session1,
                driver.session(
                    bookmark_manager=bookmark_manager,
                    default_access_mode=neo4j.READ_ACCESS,
                ) as session2,
            ):
                result1 = await session1.run("<WRITE_QUERY>")
                await result1.consume()
                # READ_QUERY is guaranteed to see what WRITE_QUERY wrote.
                result2 = await session2.run("<READ_QUERY>")
                await result2.consume()

        This is a very contrived example, and in this particular case, having
        both queries in the same session has the exact same effect and might
        even be more performant. However, when dealing with sessions spanning
        multiple threads, async Tasks, processes, or even hosts, the bookmark
        manager can come in handy as sessions are not safe to be used
        concurrently.

        :param initial_bookmarks:
            The initial set of bookmarks. The returned bookmark manager will
            use this to initialize its internal bookmarks.

            .. deprecated:: 6.0
                Passing raw string bookmarks is deprecated.
                Use a :class:`.Bookmarks` object instead.

        :param bookmarks_supplier:
            Function which will be called every time the default bookmark
            manager's method :meth:`.AsyncBookmarkManager.get_bookmarks`
            gets called.
            The function takes no arguments and must return a
            :class:`.Bookmarks` object. The result of ``bookmarks_supplier``
            will then be concatenated with the internal set of bookmarks and
            used to configure the session in creation. It will, however, not
            update the internal set of bookmarks.
        :param bookmarks_consumer:
            Function which will be called whenever the set of bookmarks
            handled by the bookmark manager gets updated with the new
            internal bookmark set. It will receive the new set of bookmarks
            as a :class:`.Bookmarks` object and return :data:`None`.

        :returns: A default implementation of :class:`.AsyncBookmarkManager`.

        .. versionadded:: 5.0

        .. versionchanged:: 5.3
            The bookmark manager no longer tracks bookmarks per database.
            This effectively changes the signature of almost all bookmark
            manager related methods:

            * ``initial_bookmarks`` is no longer a mapping from database name
              to bookmarks but plain bookmarks.
            * ``bookmarks_supplier`` no longer receives the database name as
              an argument.
            * ``bookmarks_consumer`` no longer receives the database name as
              an argument.

        .. versionchanged:: 5.8 Stabilized from experimental.

        .. versionchanged:: 6.0
            Deprecated passing raw string bookmarks as initial_bookmarks.
        """
        cast_initial_bookmarks: Bookmarks | None
        # TODO: 7.0 - remove raw bookmark support
        if not isinstance(initial_bookmarks, (Bookmarks, NoneType)):
            deprecation_warn(
                (
                    "Passing raw strings as initial_bookmarks is deprecated. "
                    "Use a Bookmarks object instead."
                ),
                stack_level=2,
            )
            cast_initial_bookmarks = Bookmarks.from_raw_values(
                t.cast(t.Iterable[str], initial_bookmarks)
            )
        else:
            cast_initial_bookmarks = initial_bookmarks
        return AsyncNeo4jBookmarkManager(
            initial_bookmarks=cast_initial_bookmarks,
            bookmarks_supplier=bookmarks_supplier,
            bookmarks_consumer=bookmarks_consumer,
        )

    @classmethod
    def _bolt_driver(cls, target, **config):
        """
        Create a direct driver.

        Create a driver for direct Bolt server access that uses
        socket I/O and thread-based concurrency.
        """
        from .._exceptions import (
            BoltHandshakeError,
            BoltSecurityError,
        )

        try:
            return AsyncBoltDriver._open(target, **config)
        except (BoltHandshakeError, BoltSecurityError) as error:
            from ..exceptions import ServiceUnavailable

            raise ServiceUnavailable(str(error)) from error

    @classmethod
    def _neo4j_driver(cls, target, routing_context=None, **config):
        """
        Create a routing driver.

        Create a driver for routing-capable Neo4j service access
        that uses socket I/O and thread-based concurrency.
        """
        from .._exceptions import (
            BoltHandshakeError,
            BoltSecurityError,
        )

        try:
            return AsyncNeo4jDriver._open(
                target, routing_context=routing_context, **config
            )
        except (BoltHandshakeError, BoltSecurityError) as error:
            from ..exceptions import ServiceUnavailable

            raise ServiceUnavailable(str(error)) from error


class _Direct:
    _default_host = "localhost"
    _default_port = 7687
    _default_target = ":"

    def __init__(self, address):
        self._address = address

    @property
    def address(self):
        return self._address

    @classmethod
    def _parse_target(cls, target):
        """Parse a target string to produce an address."""
        if not target:
            target = cls._default_target
        return Address.parse(
            target,
            default_host=cls._default_host,
            default_port=cls._default_port,
        )


class _Routing:
    _default_host = "localhost"
    _default_port = 7687
    _default_targets = ": :17601 :17687"

    def __init__(self, initial_addresses):
        self._initial_addresses = initial_addresses

    @property
    def initial_addresses(self):
        return self._initial_addresses

    @classmethod
    def _parse_targets(cls, *targets):
        """Parse a sequence of target strings to produce an address list."""
        targets = " ".join(targets)
        if not targets:
            targets = cls._default_targets
        return Address.parse_list(
            targets,
            default_host=cls._default_host,
            default_port=cls._default_port,
        )


class AsyncDriver:
    """
    Base class for all driver types.

    Drivers are used as the primary access point to Neo4j.
    """

    #: Connection pool
    _pool: t.Any = None

    #: Flag if the driver has been closed
    _closed = False

    def __init__(self, pool, default_workspace_config):
        assert pool is not None
        assert default_workspace_config is not None
        self._pool = pool
        self._default_workspace_config = default_workspace_config
        self._query_bookmark_manager = AsyncGraphDatabase.bookmark_manager()

    async def __aenter__(self) -> AsyncDriver:
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    # Copy globals as function locals to make sure that they are available
    # during Python shutdown when the Pool is destroyed.
    def __del__(
        self,
        _unclosed_resource_warn=unclosed_resource_warn,
    ):
        if not self._closed:
            _unclosed_resource_warn(self)

    def _check_state(self):
        if self._closed:
            raise DriverError("Driver closed")

    @property
    def encrypted(self) -> bool:
        """Indicate whether the driver was configured to use encryption."""
        return bool(self._pool.pool_config.encrypted)

    if t.TYPE_CHECKING:

        def session(
            self,
            *,
            connection_acquisition_timeout: float = ...,
            max_transaction_retry_time: float = ...,
            database: str | None = ...,
            fetch_size: int = ...,
            impersonated_user: str | None = ...,
            bookmarks: Bookmarks | None = ...,
            default_access_mode: str = ...,
            bookmark_manager: (
                AsyncBookmarkManager | BookmarkManager | None
            ) = ...,
            auth: _TAuth = ...,
            notifications_min_severity: (
                T_NotificationMinimumSeverity | None
            ) = ...,
            # deprecated in favor of notifications_disabled_classifications
            notifications_disabled_categories: (
                t.Iterable[T_NotificationDisabledClassification] | None
            ) = ...,
            notifications_disabled_classifications: (
                t.Iterable[T_NotificationDisabledClassification] | None
            ) = ...,
            # undocumented/unsupported options
            # they may be change or removed any time without prior notice
            initial_retry_delay: float = ...,
            retry_delay_multiplier: float = ...,
            retry_delay_jitter_factor: float = ...,
        ) -> AsyncSession: ...

    else:

        def session(self, **config) -> AsyncSession:
            """
            Create a session.

            See :ref:`async-session-construction-ref` for details.

            :param config: session configuration key-word arguments,
                see :ref:`async-session-configuration-ref` for available
                key-word arguments.

            :returns: new :class:`neo4j.AsyncSession` object

            :raises DriverError: if the driver has been closed.

            .. versionchanged:: 6.0

                * Raise :exc:`DriverError` if the driver has been closed.
                * Deprecated ``notifications_disabled_categories`` in favor of
                  ``notifications_disabled_classifications``.
            """
            # Would work just fine, but we don't want to introduce yet
            # another undocumented/unsupported config option.
            config.pop("warn_notification_severity", None)

            self._check_state()
            if "notifications_disabled_categories" in config:
                deprecation_warn(
                    "notifications_disabled_categories is deprecated, "
                    "use notifications_disabled_classifications instead.",
                    stack_level=2,
                )
            session_config = self._read_session_config(config)
            return self._session(session_config)

    def _session(self, session_config) -> AsyncSession:
        return AsyncSession(self._pool, session_config)

    def _read_session_config(self, config_kwargs):
        config = self._prepare_session_config(config_kwargs)
        return SessionConfig(self._default_workspace_config, config)

    @classmethod
    def _prepare_session_config(cls, config_kwargs):
        _normalize_notifications_config(config_kwargs)
        return config_kwargs

    async def close(self) -> None:
        """Shut down, closing any open connections in the pool.

        .. warning::

            While the driver object is concurrency-safe, ``close`` is *not*.
            Make sure you are not using the driver object or any resources
            spawned from it (such as sessions or transactions) while calling
            this method. Failing to do so results in unspecified behavior.
        """
        if self._closed:
            return
        try:
            await self._pool.close()
        except asyncio.CancelledError:
            self._closed = True
            raise
        self._closed = True

    # overloads to work around https://github.com/python/mypy/issues/3737
    @t.overload
    async def execute_query(
        self,
        query_: t.LiteralString | Query,
        parameters_: dict[str, t.Any] | None = None,
        routing_: T_RoutingControl = RoutingControl.WRITE,
        database_: str | None = None,
        impersonated_user_: str | None = None,
        bookmark_manager_: (
            AsyncBookmarkManager | BookmarkManager | None
        ) = ...,
        auth_: _TAuth = None,
        result_transformer_: t.Callable[
            [AsyncResult], t.Awaitable[EagerResult]
        ] = ...,
        **kwargs: t.Any,
    ) -> EagerResult: ...

    @t.overload
    async def execute_query(
        self,
        query_: t.LiteralString | Query,
        parameters_: dict[str, t.Any] | None = None,
        routing_: T_RoutingControl = RoutingControl.WRITE,
        database_: str | None = None,
        impersonated_user_: str | None = None,
        bookmark_manager_: (
            AsyncBookmarkManager | BookmarkManager | None
        ) = ...,
        auth_: _TAuth = None,
        result_transformer_: t.Callable[[AsyncResult], t.Awaitable[_T]] = ...,
        **kwargs: t.Any,
    ) -> _T: ...

    async def execute_query(
        self,
        query_: t.LiteralString | Query,
        parameters_: dict[str, t.Any] | None = None,
        routing_: T_RoutingControl = RoutingControl.WRITE,
        database_: str | None = None,
        impersonated_user_: str | None = None,
        bookmark_manager_: (
            AsyncBookmarkManager
            | BookmarkManager
            | t.Literal[_DefaultEnum.default]
            | None
        ) = _default,
        auth_: _TAuth = None,
        result_transformer_: t.Callable[
            [AsyncResult], t.Awaitable[t.Any]
        ] = AsyncResult.to_eager_result,
        **kwargs: t.Any,
    ) -> t.Any:
        '''
        Execute a query in a transaction function and return all results.

        This method is a handy wrapper for lower-level driver APIs like
        sessions, transactions, and transaction functions. It is intended
        for simple use cases where there is no need for managing all possible
        options.

        The internal usage of transaction functions provides a retry-mechanism
        for appropriate errors. Furthermore, this means that queries using
        ``CALL {} IN TRANSACTIONS`` or the older ``USING PERIODIC COMMIT``
        will not work (use :meth:`AsyncSession.run` for these).

        The method is roughly equivalent to::

            async def execute_query(
                query_, parameters_, routing_, database_, impersonated_user_,
                bookmark_manager_, auth_, result_transformer_, **kwargs
            ):
                @unit_of_work(query_.metadata, query_.timeout)
                async def work(tx):
                    result = await tx.run(query_.text, parameters_, **kwargs)
                    return await result_transformer_(result)

                async with driver.session(
                    database=database_,
                    impersonated_user=impersonated_user_,
                    bookmark_manager=bookmark_manager_,
                    auth=auth_,
                ) as session:
                    if routing_ == RoutingControl.WRITE:
                        return await session.execute_write(work)
                    elif routing_ == RoutingControl.READ:
                        return await session.execute_read(work)

        Usage example::

            from typing import List

            import neo4j


            async def example(driver: neo4j.AsyncDriver) -> List[str]:
                """Get the name of all 42 year-olds."""
                records, summary, keys = await driver.execute_query(
                    "MATCH (p:Person {age: $age}) RETURN p.name",
                    {"age": 42},
                    routing_=neo4j.RoutingControl.READ,  # or just "r"
                    database_="neo4j",
                )
                assert keys == ["p.name"]  # not needed, just for illustration
                # log_summary(summary)  # log some metadata
                return [str(record["p.name"]) for record in records]
                # or: return [str(record[0]) for record in records]
                # or even: return list(map(lambda r: str(r[0]), records))

        Another example::

            import neo4j


            async def example(driver: neo4j.AsyncDriver) -> int:
                """Call all young people "My dear" and get their count."""
                record = await driver.execute_query(
                    "MATCH (p:Person) WHERE p.age <= $age "
                    "SET p.nickname = 'My dear' "
                    "RETURN count(*)",
                    # optional routing parameter, as write is default
                    # routing_=neo4j.RoutingControl.WRITE,  # or just "w",
                    database_="neo4j",
                    result_transformer_=neo4j.AsyncResult.single,
                    age=15,
                )
                assert record is not None  # for typechecking and illustration
                count = record[0]
                assert isinstance(count, int)
                return count

        :param query_:
            Cypher query to execute.
            Use a :class:`.Query` object to pass a query with additional
            transaction configuration.
        :type query_: typing.LiteralString | Query
        :param parameters_: parameters to use in the query
        :type parameters_: typing.Optional[typing.Dict[str, typing.Any]]
        :param routing_:
            Whether to route the query to a reader (follower/read replica) or
            a writer (leader) in the cluster. Default is to route to a writer.
        :type routing_: RoutingControl
        :param database_:
            Database to execute the query against.

            :data:`None` (default) uses the database configured on the server
            side.

            .. Note::
                It is recommended to always specify the database explicitly
                when possible. This allows the driver to work more efficiently,
                as it will not have to resolve the default database first.

            See also the Session config :ref:`database-ref`.
        :type database_: typing.Optional[str]
        :param impersonated_user_:
            Name of the user to impersonate.

            This means that all query will be executed in the security context
            of the impersonated user. For this, the user for which the
            :class:`Driver` has been created needs to have the appropriate
            permissions.

            See also the Session config :ref:`impersonated-user-ref`.
        :type impersonated_user_: typing.Optional[str]
        :param auth_:
            Authentication information to use for this query.

            By default, the driver configuration is used.

            See also the Session config :ref:`session-auth-ref`.
        :type auth_: typing.Tuple[typing.Any, typing.Any] | Auth | None
        :param result_transformer_:
            A function that gets passed the :class:`neo4j.AsyncResult` object
            resulting from the query and converts it to a different type. The
            result of the transformer function is returned by this method.

            .. warning::

                The transformer function must **not** return the
                :class:`neo4j.AsyncResult` itself.

            .. warning::

                N.B. the driver might retry the underlying transaction so the
                transformer might get invoked more than once (with different
                :class:`neo4j.AsyncResult` objects).
                Therefore, it needs to be idempotent (i.e., have the same
                effect, regardless if called once or many times).

            Example transformer that checks that exactly one record is in the
            result stream, then returns the record and the result summary::

                from typing import Tuple

                import neo4j


                async def transformer(
                    result: neo4j.AsyncResult
                ) -> Tuple[neo4j.Record, neo4j.ResultSummary]:
                    record = await result.single(strict=True)
                    summary = await result.consume()
                    return record, summary

            Note that methods of :class:`neo4j.AsyncResult` that don't take
            mandatory arguments can be used directly as transformer functions.
            For example::

                import neo4j


                async def example(driver: neo4j.AsyncDriver) -> neo4j.Record::
                    record = await driver.execute_query(
                        "SOME QUERY",
                        result_transformer_=neo4j.AsyncResult.single
                    )


                # is equivalent to:


                async def transformer(result: neo4j.AsyncResult) -> neo4j.Record:
                    return await result.single()


                async def example(driver: neo4j.AsyncDriver) -> neo4j.Record::
                    record = await driver.execute_query(
                        "SOME QUERY",
                        result_transformer_=transformer
                    )

        :type result_transformer_:
            typing.Callable[[AsyncResult], typing.Awaitable[T]]
        :param bookmark_manager_:
            Specify a bookmark manager to use.

            If present, the bookmark manager is used to keep the query causally
            consistent with all work executed using the same bookmark manager.

            Defaults to the driver's :attr:`.execute_query_bookmark_manager`.

            Pass :data:`None` to disable causal consistency.
        :type bookmark_manager_: AsyncBookmarkManager | BookmarkManager | None
        :param kwargs: additional keyword parameters. None of these can end
            with a single underscore. This is to avoid collisions with the
            keyword configuration parameters of this method. If you need to
            pass such a parameter, use the ``parameters_`` parameter instead.
            Parameters passed as kwargs take precedence over those passed in
            ``parameters_``.
        :type kwargs: typing.Any

        :returns: the result of the ``result_transformer_``
        :rtype: T

        :raises DriverError: if the driver has been closed.

        .. versionadded:: 5.5

        .. versionchanged:: 5.8

            * Added ``auth_`` parameter in preview.
            * Stabilized from experimental.

        .. versionchanged:: 5.14
            Stabilized ``auth_`` parameter from preview.

        .. versionchanged:: 5.15
            The ``query_`` parameter now also accepts a :class:`.Query` object
            instead of only :class:`str`.

        .. versionchanged:: 6.0
            Raise :exc:`DriverError` if the driver has been closed.
        '''  # noqa: E501 example code isn't too long
        self._check_state()
        invalid_kwargs = [
            k for k in kwargs if k[-2:-1] != "_" and k[-1:] == "_"
        ]
        if invalid_kwargs:
            raise ValueError(
                "keyword parameters must not end with a single '_'. "
                f"Found: {invalid_kwargs!r}\n"
                "\nYou either misspelled an existing configuration parameter "
                "or tried to send a query parameter that is reserved. In the "
                "latter case, use the `parameters_` dictionary instead."
            )
        if isinstance(query_, Query):
            timeout = query_.timeout
            metadata = query_.metadata
            query_str = query_.text
            work = unit_of_work(metadata, timeout)(_work)
        else:
            query_str = query_
            work = _work
        parameters = dict(parameters_ or {}, **kwargs)

        if bookmark_manager_ is _default:
            bookmark_manager_ = self._query_bookmark_manager
        assert bookmark_manager_ is not _default

        session_config = self._read_session_config(
            {
                "database": database_,
                "impersonated_user": impersonated_user_,
                "bookmark_manager": bookmark_manager_,
                "auth": auth_,
            }
        )
        session = self._session(session_config)
        async with session:
            if routing_ == RoutingControl.WRITE:
                access_mode = WRITE_ACCESS
            elif routing_ == RoutingControl.READ:
                access_mode = READ_ACCESS
            else:
                raise ValueError(
                    f"Invalid routing control value: {routing_!r}"
                )
            with session._pipelined_begin:
                return await session._run_transaction(
                    access_mode,
                    TelemetryAPI.DRIVER,
                    work,
                    (query_str, parameters, result_transformer_),
                    {},
                )

    @property
    def execute_query_bookmark_manager(self) -> AsyncBookmarkManager:
        """
        The driver's default query bookmark manager.

        This is the default :class:`.AsyncBookmarkManager` used by
        :meth:`.execute_query`. This can be used to causally chain
        :meth:`.execute_query` calls and sessions. Example::

            async def example(driver: neo4j.AsyncDriver) -> None:
                await driver.execute_query("<QUERY 1>")
                async with driver.session(
                    bookmark_manager=driver.execute_query_bookmark_manager
                ) as session:
                    # every query inside this session will be causally chained
                    # (i.e., can read what was written by <QUERY 1>)
                    await session.run("<QUERY 2>")
                # subsequent execute_query calls will be causally chained
                # (i.e., can read what was written by <QUERY 2>)
                await driver.execute_query("<QUERY 3>")

        .. versionadded:: 5.5

        .. versionchanged:: 5.8

            * Renamed from ``query_bookmark_manager`` to
              ``execute_query_bookmark_manager``.
            * Stabilized from experimental.
        """
        return self._query_bookmark_manager

    if t.TYPE_CHECKING:

        async def verify_connectivity(
            self,
            *,
            # all arguments are experimental
            # they may be change or removed any time without prior notice
            session_connection_timeout: float = ...,
            connection_acquisition_timeout: float = ...,
            max_transaction_retry_time: float = ...,
            database: str | None = ...,
            fetch_size: int = ...,
            impersonated_user: str | None = ...,
            bookmarks: t.Iterable[str] | Bookmarks | None = ...,
            default_access_mode: str = ...,
            bookmark_manager: (
                AsyncBookmarkManager | BookmarkManager | None
            ) = ...,
            auth: Auth | tuple[str, str] = ...,
            notifications_min_severity: (
                T_NotificationMinimumSeverity | None
            ) = ...,
            notifications_disabled_categories: (
                t.Iterable[T_NotificationDisabledClassification] | None
            ) = ...,
            notifications_disabled_classifications: (
                t.Iterable[T_NotificationDisabledClassification] | None
            ) = ...,
            # undocumented/unsupported options
            initial_retry_delay: float = ...,
            retry_delay_multiplier: float = ...,
            retry_delay_jitter_factor: float = ...,
        ) -> None: ...

    else:

        async def verify_connectivity(self, **config) -> None:
            """
            Verify that the driver can establish a connection to the server.

            This verifies if the driver can establish a reading connection to a
            remote server or a cluster. Some data will be exchanged.

            .. note::
                Even if this method raises an exception, the driver still needs
                to be closed via :meth:`close` to free up all resources.

            :param config: accepts the same configuration key-word arguments as
                :meth:`session`.

                .. warning::
                    Passing key-word arguments is a preview feature.
                    It might be changed or removed in any future version
                    without prior notice.

            :raises Exception: if the driver cannot connect to the remote.
                Use the exception to further understand the cause of the
                connectivity problem.
            :raises DriverError: if the driver has been closed.

            .. versionchanged:: 5.0
                The undocumented return value has been removed.
                If you need information about the remote server, use
                :meth:`get_server_info` instead.

            .. versionchanged:: 6.0
                Raise :exc:`DriverError` if the driver has been closed.
            """
            self._check_state()
            if config:
                preview_warn(
                    "Passing key-word arguments to verify_connectivity() is a "
                    "preview feature."
                )
            session_config = self._read_session_config(config)
            await self._get_server_info(session_config)

    if t.TYPE_CHECKING:

        async def get_server_info(
            self,
            *,
            # all arguments are experimental
            # they may be change or removed any time without prior notice
            session_connection_timeout: float = ...,
            connection_acquisition_timeout: float = ...,
            max_transaction_retry_time: float = ...,
            database: str | None = ...,
            fetch_size: int = ...,
            impersonated_user: str | None = ...,
            bookmarks: t.Iterable[str] | Bookmarks | None = ...,
            default_access_mode: str = ...,
            bookmark_manager: (
                AsyncBookmarkManager | BookmarkManager | None
            ) = ...,
            auth: Auth | tuple[str, str] = ...,
            notifications_min_severity: (
                T_NotificationMinimumSeverity | None
            ) = ...,
            notifications_disabled_categories: (
                t.Iterable[T_NotificationDisabledClassification] | None
            ) = ...,
            notifications_disabled_classifications: (
                t.Iterable[T_NotificationDisabledClassification] | None
            ) = ...,
            # undocumented/unsupported options
            initial_retry_delay: float = ...,
            retry_delay_multiplier: float = ...,
            retry_delay_jitter_factor: float = ...,
        ) -> ServerInfo: ...

    else:

        async def get_server_info(self, **config) -> ServerInfo:
            """
            Get information about the connected Neo4j server.

            Try to establish a working read connection to the remote server or
            a member of a cluster and exchange some data. Then return the
            contacted server's information.

            In a cluster, there is no guarantee about which server will be
            contacted.

            .. note::
                Even if this method raises an exception, the driver still needs
                to be closed via :meth:`close` to free up all resources.

            :param config: accepts the same configuration key-word arguments as
                :meth:`session`.

                .. warning::
                    Passing key-word arguments is a preview feature.
                    It might be changed or removed in any future
                    version without prior notice.

            :raises Exception: if the driver cannot connect to the remote.
                Use the exception to further understand the cause of the
                connectivity problem.
            :raises DriverError: if the driver has been closed.

            .. versionadded:: 5.0

            .. versionchanged:: 6.0
                Raise :exc:`DriverError` if the driver has been closed.
            """
            self._check_state()
            if config:
                preview_warn(
                    "Passing key-word arguments to get_server_info() is a "
                    "preview feature."
                )
            session_config = self._read_session_config(config)
            return await self._get_server_info(session_config)

    async def supports_multi_db(self) -> bool:
        """
        Check if the server or cluster supports multi-databases.

        .. note::
            Feature support query based solely on the Bolt protocol version.
            The feature might still be disabled on the server side even if this
            function return :data:`True`. It just guarantees that the driver
            won't throw a :exc:`.ConfigurationError` when trying to use this
            driver feature.

        :returns: Returns true if the server or cluster the driver connects to
            supports multi-databases, otherwise false.

        :raises DriverError: if the driver has been closed.

        .. versionchanged:: 6.0
            Raise :exc:`DriverError` if the driver has been closed.
        """
        self._check_state()
        session_config = self._read_session_config({})
        async with self._session(session_config) as session:
            await session._connect(READ_ACCESS)
            assert session._connection
            return session._connection.supports_multiple_databases

    if t.TYPE_CHECKING:

        async def verify_authentication(
            self,
            auth: Auth | tuple[str, str] | None = None,
            # all other arguments are experimental
            # they may be change or removed any time without prior notice
            session_connection_timeout: float = ...,
            connection_acquisition_timeout: float = ...,
            max_transaction_retry_time: float = ...,
            database: str | None = ...,
            fetch_size: int = ...,
            impersonated_user: str | None = ...,
            bookmarks: t.Iterable[str] | Bookmarks | None = ...,
            default_access_mode: str = ...,
            bookmark_manager: (
                AsyncBookmarkManager | BookmarkManager | None
            ) = ...,
            # undocumented/unsupported options
            initial_retry_delay: float = ...,
            retry_delay_multiplier: float = ...,
            retry_delay_jitter_factor: float = ...,
        ) -> bool: ...

    else:

        async def verify_authentication(
            self,
            auth: Auth | tuple[str, str] | None = None,
            **config,
        ) -> bool:
            """
            Verify that the authentication information is valid.

            Like :meth:`.verify_connectivity`, but for checking authentication.

            Try to establish a working read connection to the remote server or
            a member of a cluster and exchange some data. In a cluster, there
            is no guarantee about which server will be contacted. If the data
            exchange is successful and the authentication information is valid,
            :data:`True` is returned. Otherwise, the error will be matched
            against a list of known authentication errors. If the error is on
            that list, :data:`False` is returned indicating that the
            authentication information is invalid. Otherwise, the error is
            re-raised.

            :param auth: authentication information to verify.
                Same as the session config :ref:`auth-ref`.
            :param config: accepts the same configuration key-word arguments as
                :meth:`session`.

                .. warning::
                    Passing key-word arguments (except ``auth``) is a preview
                    feature. It might be changed or removed in any future
                    version without prior notice.

            :raises Exception: if the driver cannot connect to the remote.
                Use the exception to further understand the cause of the
                connectivity problem.
            :raises DriverError: if the driver has been closed.

            .. versionadded:: 5.8

            .. versionchanged:: 5.14 Stabilized from experimental.

            .. versionchanged:: 6.0
                Raise :exc:`DriverError` if the driver has been closed.
            """
            self._check_state()
            if config:
                preview_warn(
                    "Passing key-word arguments except 'auth' to "
                    "verify_authentication() is a preview feature."
                )
            if "database" not in config:
                config["database"] = "system"
            session_config = self._read_session_config(config)
            session_config = SessionConfig(session_config, {"auth": auth})
            async with self._session(session_config) as session:
                try:
                    await session._verify_authentication()
                except Neo4jError as exc:
                    if exc.code in {
                        "Neo.ClientError.Security.CredentialsExpired",
                        "Neo.ClientError.Security.Forbidden",
                        "Neo.ClientError.Security.TokenExpired",
                        "Neo.ClientError.Security.Unauthorized",
                    }:
                        return False
                    raise
            return True

    async def supports_session_auth(self) -> bool:
        """
        Check if the remote supports connection re-authentication.

        .. note::
            Feature support query based solely on the Bolt protocol version.
            The feature might still be disabled on the server side even if this
            function return :data:`True`. It just guarantees that the driver
            won't throw a :exc:`.ConfigurationError` when trying to use this
            driver feature.

        :returns: Returns true if the server or cluster the driver connects to
            supports re-authentication of existing connections, otherwise
            false.

        :raises DriverError: if the driver has been closed.

        .. versionadded:: 5.8

        .. versionchanged:: 6.0
            Raise :exc:`DriverError` if the driver has been closed.
        """
        self._check_state()
        session_config = self._read_session_config({})
        async with self._session(session_config) as session:
            await session._connect(READ_ACCESS)
            assert session._connection
            return session._connection.supports_re_auth

    async def _get_server_info(self, session_config) -> ServerInfo:
        async with self._session(session_config) as session:
            return await session._get_server_info()


async def _work(
    tx: AsyncManagedTransaction,
    query: t.LiteralString,
    parameters: dict[str, t.Any],
    transformer: t.Callable[[AsyncResult], t.Awaitable[_T]],
) -> _T:
    res = await tx.run(query, parameters)
    return await transformer(res)


class AsyncBoltDriver(_Direct, AsyncDriver):
    """
    :class:`.AsyncBoltDriver` is instantiated for ``bolt`` URIs.

    It addresses a single database machine. This may be a standalone server or
    could be a specific member of a cluster.

    Connections established by a :class:`.AsyncBoltDriver` are always made to
    the exact host and port detailed in the URI.

    This class is not supposed to be instantiated externally. Use
    :meth:`AsyncGraphDatabase.driver` instead.
    """

    @classmethod
    def _open(cls, target, **config):
        from .io import AsyncBoltPool

        address = cls._parse_target(target)
        pool_config, default_workspace_config = Config.consume_chain(
            config, AsyncPoolConfig, WorkspaceConfig
        )
        pool = AsyncBoltPool.open(
            address,
            pool_config=pool_config,
            workspace_config=default_workspace_config,
        )
        return cls(pool, default_workspace_config)

    def __init__(self, pool, default_workspace_config):
        _Direct.__init__(self, pool.address)
        AsyncDriver.__init__(self, pool, default_workspace_config)
        self._default_workspace_config = default_workspace_config


class AsyncNeo4jDriver(_Routing, AsyncDriver):
    """
    :class:`.AsyncNeo4jDriver` is instantiated for ``neo4j`` URIs.

    The routing behaviour works in tandem with Neo4j's `Causal Clustering
    <https://neo4j.com/docs/operations-manual/current/clustering/>`_
    feature by directing read and write behaviour to appropriate
    cluster members.

    This class is not supposed to be instantiated externally. Use
    :meth:`AsyncGraphDatabase.driver` instead.
    """

    @classmethod
    def _open(cls, *targets, routing_context=None, **config):
        from .io import AsyncNeo4jPool

        addresses = cls._parse_targets(*targets)
        pool_config, default_workspace_config = Config.consume_chain(
            config, AsyncPoolConfig, WorkspaceConfig
        )
        pool = AsyncNeo4jPool.open(
            *addresses,
            routing_context=routing_context,
            pool_config=pool_config,
            workspace_config=default_workspace_config,
        )
        return cls(pool, default_workspace_config)

    def __init__(self, pool, default_workspace_config):
        _Routing.__init__(self, [pool.address])
        AsyncDriver.__init__(self, pool, default_workspace_config)


def _normalize_notifications_config(config_kwargs, *, driver_level=False):
    list_config_keys = (
        "notifications_disabled_categories",
        "notifications_disabled_classifications",
    )
    for key in list_config_keys:
        value = config_kwargs.get(key)
        if value is not None:
            config_kwargs[key] = [getattr(e, "value", e) for e in value]

    disabled_categories = config_kwargs.pop(
        "notifications_disabled_categories", None
    )
    if disabled_categories is not None:
        disabled_classifications = config_kwargs.get(
            "notifications_disabled_classifications"
        )
        if disabled_classifications is None:
            disabled_classifications = disabled_categories
        else:
            disabled_classifications = list(
                {*disabled_categories, *disabled_classifications}
            )
        config_kwargs["notifications_disabled_classifications"] = (
            disabled_classifications
        )

    single_config_keys = (
        "notifications_min_severity",
        "warn_notification_severity",
    )
    for key in single_config_keys:
        value = config_kwargs.get(key)
        if value is not None:
            config_kwargs[key] = getattr(value, "value", value)

    value = config_kwargs.get("warn_notification_severity")
    if value not in {*NotificationMinimumSeverity, None}:
        raise ValueError(
            f"Invalid value for configuration "
            f"warn_notification_severity: {value}. Should be None, a "
            f"NotificationMinimumSeverity, or a string representing a "
            f"NotificationMinimumSeverity."
        )
    if driver_level:
        if value is None:
            if DEBUG_ENABLED:
                config_kwargs["warn_notification_severity"] = (
                    NotificationMinimumSeverity.INFORMATION
                )
        elif value == NotificationMinimumSeverity.OFF:
            config_kwargs["warn_notification_severity"] = None
