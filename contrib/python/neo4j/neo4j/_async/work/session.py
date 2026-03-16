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
from logging import getLogger
from random import random
from time import monotonic

from ... import _typing as t
from ..._api import TelemetryAPI
from ..._async_compat import async_sleep
from ..._async_compat.util import AsyncUtil
from ..._conf import SessionConfig
from ..._util import ContextBool
from ..._work import Query
from ...api import (
    Bookmarks,
    READ_ACCESS,
    WRITE_ACCESS,
)
from ...exceptions import (
    DriverError,
    Neo4jError,
    ServiceUnavailable,
    SessionExpired,
    TransactionError,
)
from .._debug import AsyncNonConcurrentMethodChecker
from ..auth_management import AsyncAuthManagers
from .result import AsyncResult
from .transaction import (
    AsyncManagedTransaction,
    AsyncTransaction,
)
from .workspace import AsyncWorkspace


if t.TYPE_CHECKING:
    from ..io import AsyncBolt

    _R = t.TypeVar("_R")
    _P = t.ParamSpec("_P")


log = getLogger("neo4j.session")


class AsyncSession(AsyncWorkspace):
    """
    Context for executing work.

    A :class:`.AsyncSession` is a logical context for transactional units
    of work. Connections are drawn from the :class:`.AsyncDriver` connection
    pool as required.

    Session creation is a lightweight operation and sessions are not safe to
    be used in concurrent contexts (multiple threads/coroutines).
    Therefore, a session should generally be short-lived, and must not
    span multiple threads/asynchronous Tasks.

    In general, sessions will be created and destroyed within a ``with``
    context. For example::

        async with driver.session(database="neo4j") as session:
            result = await session.run("MATCH (n:Person) RETURN n.name AS name")
            ...  # do something with the result
    """  # noqa: E501 example code isn't too long

    # The current connection.
    _connection: AsyncBolt | None = None

    # The current transaction instance, if any.
    _transaction: AsyncTransaction | AsyncManagedTransaction | None = None

    # The current auto-commit transaction result, if any.
    _auto_result = None

    # The state this session is in.
    _state_failed = False

    _config: SessionConfig
    _bookmark_manager: Bookmarks | None
    _pipelined_begin: ContextBool

    def __init__(self, pool, session_config):
        assert isinstance(session_config, SessionConfig)
        if session_config.auth is not None:
            session_config.auth = AsyncAuthManagers.static(session_config.auth)
        super().__init__(pool, session_config)
        self._config = session_config
        self._initialize_bookmarks(session_config.bookmarks)
        self._bookmark_manager = session_config.bookmark_manager
        self._pipelined_begin = ContextBool()

    async def __aenter__(self) -> AsyncSession:
        return self

    async def __aexit__(self, exception_type, exception_value, traceback):
        if exception_type:
            if issubclass(exception_type, asyncio.CancelledError):
                self._handle_cancellation(message="__aexit__")
                self._closed = True
                return
            self._state_failed = True
        await self.close()

    async def _connect(self, access_mode, **acquire_kwargs):
        if access_mode is None:
            access_mode = self._config.default_access_mode
        try:
            await super()._connect(
                access_mode, auth=self._config.auth, **acquire_kwargs
            )
        except asyncio.CancelledError:
            self._handle_cancellation(message="_connect")
            raise

    async def _disconnect(self, sync=False):
        try:
            return await super()._disconnect(sync=sync)
        except asyncio.CancelledError:
            self._handle_cancellation(message="_disconnect")
            raise

    def _handle_cancellation(self, message="General"):
        self._transaction = None
        self._auto_result = None
        connection = self._connection
        self._connection = None
        if connection:
            log.debug(
                "[#%04X]  _: <SESSION> %s cancellation clean-up",
                connection.local_port,
                message,
            )
            self._pool.kill_and_release(connection)
        else:
            log.debug(
                "[#0000]  _: <SESSION> %s cancellation clean-up", message
            )

    async def _result_closed(self):
        if self._auto_result:
            await self._update_bookmark(self._auto_result._bookmark)
            self._auto_result = None
            await self._disconnect()

    async def _result_error(self, error):
        if isinstance(error, asyncio.CancelledError):
            self._handle_cancellation(message="_result_error")
            return
        if self._auto_result:
            self._auto_result = None
            await self._disconnect()

    async def _get_server_info(self):
        assert not self._connection
        await self._connect(
            READ_ACCESS, liveness_check_timeout=0, unprepared=True
        )
        server_info = self._connection.server_info
        await self._disconnect()
        return server_info

    async def _verify_authentication(self):
        assert not self._connection
        await self._connect(READ_ACCESS, force_auth=True, unprepared=True)
        await self._disconnect()

    @AsyncNonConcurrentMethodChecker._non_concurrent_method
    async def close(self) -> None:
        """
        Close the session.

        This will release any borrowed resources, such as connections, and will
        roll back any outstanding transactions.
        """
        if self._closed:
            return
        if self._connection:
            if self._auto_result and self._state_failed is False:
                try:
                    await self._auto_result.consume()
                    await self._update_bookmark(self._auto_result._bookmark)
                except Exception:
                    # TODO: Investigate potential non graceful close states
                    self._auto_result = None
                    self._state_failed = True

            if self._transaction:
                if self._transaction._closed() is False:
                    # roll back the transaction if it is not closed
                    await self._transaction._rollback()
                self._transaction = None

            try:
                if self._connection:
                    await self._connection.send_all()
                    await self._connection.fetch_all()
                    # TODO: Investigate potential non graceful close states
            except Neo4jError:
                pass
            except TransactionError:
                pass
            except ServiceUnavailable:
                pass
            except SessionExpired:
                pass
            finally:
                await self._disconnect()

            self._state_failed = False
        self._closed = True

    if AsyncUtil.is_async_code:

        def cancel(self) -> None:
            """
            Cancel this session.

            If the session is already closed, this method does nothing.
            Else, it will if present, forcefully close the connection the
            session holds. This will violently kill all work in flight.

            The primary purpose of this function is to handle
            :class:`asyncio.CancelledError`.

            ::

                session = driver.session()
                try:
                    ...  # do some work
                except asyncio.CancelledError:
                    session.cancel()
                    raise
            """
            self._handle_cancellation(message="manual cancel")

    @AsyncNonConcurrentMethodChecker._non_concurrent_method
    async def run(
        self,
        query: t.LiteralString | Query,
        parameters: dict[str, t.Any] | None = None,
        **kwargs: t.Any,
    ) -> AsyncResult:
        """
        Run a Cypher query within an auto-commit transaction.

        The query is sent and the result header received
        immediately but the :class:`neo4j.Result` content is
        fetched lazily as consumed by the client application.

        If a query is executed before a previous
        :class:`neo4j.AsyncResult` in the same :class:`.AsyncSession` has
        been fully consumed, the first result will be fully fetched
        and buffered. Note therefore that the generally recommended
        pattern of usage is to fully consume one result before
        executing a subsequent query. If two results need to be
        consumed in parallel, multiple :class:`.AsyncSession` objects
        can be used as an alternative to result buffering.

        For more usage details, see :meth:`.AsyncTransaction.run`.

        :param query: cypher query
        :param parameters: dictionary of parameters
        :param kwargs: additional keyword parameters.
            These take precedence over parameters passed as ``parameters``.

        :returns: a new :class:`neo4j.AsyncResult` object

        :raises TransactionError: if a transaction is already open.
        :raises SessionError: if the session has been closed.
        """
        self._check_state()
        if not query:
            raise ValueError("Cannot run an empty query")
        if not isinstance(query, (str, Query)):
            raise TypeError("query must be a string or a Query instance")

        if self._transaction:
            raise TransactionError(
                self._transaction, "Explicit transaction already open"
            )

        if self._auto_result:
            # This will buffer upp all records for the previous auto-commit tx
            await self._auto_result._buffer_all()

        if not self._connection:
            await self._connect(self._config.default_access_mode)
            assert self._connection is not None
        cx = self._connection

        cx.telemetry(TelemetryAPI.AUTO_COMMIT)
        self._auto_result = AsyncResult(
            cx,
            self._config.fetch_size,
            self._config.warn_notification_severity,
            self._result_closed,
            self._result_error,
            self._make_db_resolution_callback(),
        )
        bookmarks = await self._get_bookmarks()
        parameters = dict(parameters or {}, **kwargs)
        await self._auto_result._run(
            query,
            parameters,
            self._config.database,
            self._config.impersonated_user,
            self._config.default_access_mode,
            bookmarks,
            self._config.notifications_min_severity,
            self._config.notifications_disabled_classifications,
        )

        return self._auto_result

    @AsyncNonConcurrentMethodChecker._non_concurrent_method
    async def last_bookmarks(self) -> Bookmarks:
        """
        Return most recent bookmarks of the session.

        Bookmarks can be used to causally chain sessions. For example,
        if a session (``session1``) wrote something, that another session
        (``session2``) needs to read, use
        ``session2 = driver.session(bookmarks=session1.last_bookmarks())`` to
        achieve this.

        Combine the bookmarks of multiple sessions like so::

            bookmarks1 = await session1.last_bookmarks()
            bookmarks2 = await session2.last_bookmarks()
            session3 = driver.session(bookmarks=bookmarks1 + bookmarks2)

        A session automatically manages bookmarks, so this method is rarely
        needed. If you need causal consistency, try to run the relevant queries
        in the same session.

        "Most recent bookmarks" are either the bookmarks passed to the session
        on creation, or the last bookmark the session received after committing
        a transaction to the server.

        Note: For auto-commit transactions (:meth:`Session.run`), this will
        trigger :meth:`Result.consume` for the current result.

        :returns: the session's last known bookmarks
        """
        # The set of bookmarks to be passed into the next transaction.

        if self._auto_result:
            await self._auto_result.consume()

        if self._transaction and self._transaction._closed():
            await self._update_bookmark(self._transaction._bookmark)
            self._transaction = None

        return Bookmarks.from_raw_values(self._bookmarks)

    async def _transaction_closed_handler(self):
        if self._transaction:
            await self._update_bookmark(self._transaction._bookmark)
            self._transaction = None
            await self._disconnect()

    async def _transaction_error_handler(self, error):
        if self._transaction:
            self._transaction = None
            await self._disconnect()

    def _transaction_cancel_handler(self):
        return self._handle_cancellation(message="_transaction_cancel_handler")

    async def _open_transaction(
        self,
        *,
        tx_cls: t.Callable[..., AsyncTransaction | AsyncManagedTransaction],
        access_mode,
        api: TelemetryAPI | None,
        metadata=None,
        timeout=None,
        api_success_cb: t.Callable[[dict], None] | None = None,
    ) -> None:
        await self._connect(access_mode=access_mode)
        assert self._connection is not None
        if api is not None:
            self._connection.telemetry(api, on_success=api_success_cb)
        self._transaction = tx_cls(
            self._connection,
            self._config.fetch_size,
            self._config.warn_notification_severity,
            self._transaction_closed_handler,
            self._transaction_error_handler,
            self._transaction_cancel_handler,
            self._make_db_resolution_callback(),
        )
        bookmarks = await self._get_bookmarks()
        await self._transaction._begin(
            self._config.database,
            self._config.impersonated_user,
            bookmarks,
            access_mode,
            metadata,
            timeout,
            self._config.notifications_min_severity,
            self._config.notifications_disabled_classifications,
            pipelined=self._pipelined_begin,
        )

    @AsyncNonConcurrentMethodChecker._non_concurrent_method
    async def begin_transaction(
        self,
        metadata: dict[str, t.Any] | None = None,
        timeout: float | None = None,
    ) -> AsyncTransaction:
        """
        Begin a new unmanaged transaction.

        Creates a new :class:`.AsyncTransaction` within this session.
        At most one transaction may exist in a session at any point in time.
        To maintain multiple concurrent transactions, use multiple concurrent
        sessions.

        Note: For auto-commit transactions (:meth:`.AsyncSession.run`), this
        will trigger a :meth:`.AsyncResult.consume` for the current result.

        :param metadata:
            a dictionary with metadata.
            Specified metadata will be attached to the executing transaction
            and visible in the output of ``SHOW TRANSACTIONS YIELD *``
            It will also get logged to the ``query.log``.
            This functionality makes it easier to tag transactions and is
            equivalent to the ``dbms.setTXMetaData`` procedure, see
            https://neo4j.com/docs/cypher-manual/current/clauses/transaction-clauses/#query-listing-transactions
            and https://neo4j.com/docs/operations-manual/current/reference/procedures/
            for reference.

        :param timeout:
            the transaction timeout in seconds.
            Transactions that execute longer than the configured timeout will
            be terminated by the database.
            This functionality allows user code to limit query/transaction
            execution time.
            The specified timeout overrides the default timeout configured in
            the database using the ``db.transaction.timeout`` setting
            (``dbms.transaction.timeout`` before Neo4j 5.0).
            Values higher than ``db.transaction.timeout`` will be ignored and
            will fall back to the default for server versions between 4.2 and
            5.2 (inclusive).
            The value should not represent a negative duration.
            A ``0`` duration will make the transaction execute indefinitely.
            :data:`None` will use the default timeout configured on the server.

        :returns: A new transaction instance.

        :raises TransactionError: if a transaction is already open.
        :raises SessionError: if the session has been closed.
        """
        self._check_state()
        # TODO: Implement TransactionConfig consumption

        if self._auto_result:
            await self._auto_result.consume()

        if self._transaction:
            raise TransactionError(
                self._transaction, "Explicit transaction already open"
            )

        await self._open_transaction(
            tx_cls=AsyncTransaction,
            api=TelemetryAPI.TX,
            access_mode=self._config.default_access_mode,
            metadata=metadata,
            timeout=timeout,
        )

        return t.cast(AsyncTransaction, self._transaction)

    async def _run_transaction(
        self,
        access_mode: str,
        api: TelemetryAPI,
        transaction_function: t.Callable[
            t.Concatenate[AsyncManagedTransaction, _P], t.Awaitable[_R]
        ],
        # *args: _P.args, **kwargs: _P.kwargs
        # gives more type safety, but is less performant and makes for harder
        # to read call sites
        args: t.Any,
        kwargs: t.Any,
    ) -> _R:
        self._check_state()
        if not callable(transaction_function):
            raise TypeError("Unit of work is not callable")

        metadata = getattr(transaction_function, "metadata", None)
        timeout = getattr(transaction_function, "timeout", None)

        retry_delay = retry_delay_generator(
            self._config.initial_retry_delay,
            self._config.retry_delay_multiplier,
            self._config.retry_delay_jitter_factor,
        )

        telemetry_sent = False

        def api_success_cb(meta):
            nonlocal telemetry_sent
            telemetry_sent = True

        errors = []

        t0: float = -1  # Timer

        while True:
            try:
                await self._open_transaction(
                    tx_cls=AsyncManagedTransaction,
                    api=None if telemetry_sent else api,
                    access_mode=access_mode,
                    metadata=metadata,
                    timeout=timeout,
                    api_success_cb=api_success_cb,
                )
                assert isinstance(self._transaction, AsyncManagedTransaction)
                tx = self._transaction
                try:
                    result = await transaction_function(tx, *args, **kwargs)
                except asyncio.CancelledError:
                    # if cancellation callback has not been called yet:
                    if self._transaction is not None:
                        self._handle_cancellation(
                            message="transaction function"
                        )
                    raise
                except Exception:
                    await tx._close()
                    raise
                else:
                    await tx._commit()
            except (DriverError, Neo4jError) as error:
                await self._disconnect()
                if not error.is_retryable():
                    raise
                errors.append(error)
            else:
                return result
            if t0 == -1:
                # The timer should be started after the first attempt
                t0 = monotonic()
            t1 = monotonic()
            if t1 - t0 > self._config.max_transaction_retry_time:
                break
            delay = next(retry_delay)
            log.warning(
                "Transaction failed and will be retried in %ss (%s)",
                delay,
                "; ".join(errors[-1].args),
            )
            try:
                await async_sleep(delay)
            except asyncio.CancelledError:
                log.debug("[#0000]  _: <SESSION> retry cancelled")
                raise

        if errors:
            # TODO: 7.0 - when Python 3.11+ is the minimum,
            #             use exception groups
            raise errors[-1]
        else:
            raise ServiceUnavailable("Transaction failed")

    @AsyncNonConcurrentMethodChecker._non_concurrent_method
    async def execute_read(
        self,
        transaction_function: t.Callable[
            t.Concatenate[AsyncManagedTransaction, _P], t.Awaitable[_R]
        ],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _R:
        """
        Execute a unit of work in a managed read transaction.

        .. note::
            This does not necessarily imply access control, see the session
            configuration option :ref:`default-access-mode-ref`.

        This transaction will automatically be committed when the function
        returns, unless an exception is thrown during query execution or by
        the user code. Note, that this function performs retries and that the
        supplied ``transaction_function`` might get invoked more than once.
        Therefore, it needs to be idempotent (i.e., have the same effect,
        regardless if called once or many times).

        Example::

            async def do_cypher_tx(tx, cypher):
                result = await tx.run(cypher)
                values = [record.values() async for record in result]
                return values

            async with driver.session() as session:
                values = await session.execute_read(do_cypher_tx, "RETURN 1 AS x")

        Example::

            async def get_two_tx(tx):
                result = await tx.run("UNWIND [1,2,3,4] AS x RETURN x")
                values = []
                async for record in result:
                    if len(values) >= 2:
                        break
                    values.append(record.values())
                # or shorter: values = [record.values()
                #                       for record in await result.fetch(2)]

                # discard the remaining records if there are any
                summary = await result.consume()
                # use the summary for logging etc.
                return values

            async with driver.session() as session:
                values = await session.execute_read(get_two_tx)

        :param transaction_function: a function that takes a transaction as an
            argument and does work with the transaction.
            ``transaction_function(tx, *args, **kwargs)`` where ``tx`` is a
            :class:`.AsyncManagedTransaction`.
        :type transaction_function:
            typing.Callable[[AsyncManagedTransaction, P], typing.Awaitable[R]]
        :param args: additional arguments for the ``transaction_function``
        :type args: P
        :param kwargs: key word arguments for the ``transaction_function``
        :type kwargs: P

        :returns: whatever the given ``transaction_function`` returns
        :rtype: R

        :raises SessionError: if the session has been closed.

        .. versionadded:: 5.0
        """  # noqa: E501 example code isn't too long
        return await self._run_transaction(
            READ_ACCESS,
            TelemetryAPI.TX_FUNC,
            transaction_function,
            args,
            kwargs,
        )

    @AsyncNonConcurrentMethodChecker._non_concurrent_method
    async def execute_write(
        self,
        transaction_function: t.Callable[
            t.Concatenate[AsyncManagedTransaction, _P], t.Awaitable[_R]
        ],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _R:
        """
        Execute a unit of work in a managed write transaction.

        .. note::
            This does not necessarily imply access control, see the session
            configuration option :ref:`default-access-mode-ref`.

        This transaction will automatically be committed when the function
        returns unless, an exception is thrown during query execution or by
        the user code. Note, that this function performs retries and that the
        supplied ``transaction_function`` might get invoked more than once.
        Therefore, it needs to be idempotent (i.e., have the same effect,
        regardless if called once or many times).

        Example::

            async def create_node_tx(tx, name):
                query = ("CREATE (n:NodeExample {name: $name, id: randomUUID()}) "
                         "RETURN n.id AS node_id")
                result = await tx.run(query, name=name)
                record = await result.single()
                return record["node_id"]

            async with driver.session() as session:
                node_id = await session.execute_write(create_node_tx, "Bob")

        :param transaction_function: a function that takes a transaction as an
            argument and does work with the transaction.
            ``transaction_function(tx, *args, **kwargs)`` where ``tx`` is a
            :class:`.AsyncManagedTransaction`.
        :type transaction_function:
            typing.Callable[[AsyncManagedTransaction, P], typing.Awaitable[R]]
        :param args: additional arguments for the ``transaction_function``
        :type args: P
        :param kwargs: key word arguments for the ``transaction_function``
        :type kwargs: P

        :returns: a result as returned by the given unit of work
        :rtype: R

        :raises SessionError: if the session has been closed.

        .. versionadded:: 5.0
        """  # noqa: E501 example code isn't too long
        return await self._run_transaction(
            WRITE_ACCESS,
            TelemetryAPI.TX_FUNC,
            transaction_function,
            args,
            kwargs,
        )


def retry_delay_generator(initial_delay, multiplier, jitter_factor):
    delay = initial_delay
    while True:
        jitter = jitter_factor * delay
        yield delay - jitter + (2 * jitter * random())
        delay *= multiplier
