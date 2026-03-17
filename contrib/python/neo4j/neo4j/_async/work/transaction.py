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

from ... import _typing as t  # noqa: TC001
from ..._async_compat.util import AsyncUtil
from ..._work import Query
from ...exceptions import TransactionError
from .._debug import AsyncNonConcurrentMethodChecker
from ..io import ConnectionErrorHandler
from .result import AsyncResult


__all__ = (
    "AsyncManagedTransaction",
    "AsyncTransaction",
    "AsyncTransactionBase",
)


class AsyncTransactionBase(AsyncNonConcurrentMethodChecker):
    def __init__(
        self,
        connection,
        fetch_size,
        warn_notification_severity,
        on_closed,
        on_error,
        on_cancel,
        on_database,
    ):
        self._connection = connection
        self._error_handling_connection = ConnectionErrorHandler(
            connection, self._error_handler
        )
        self._bookmark = None
        self._database = None
        self._results = []
        self._closed_flag = False
        self._last_error = None
        self._fetch_size = fetch_size
        self._warn_notification_severity = warn_notification_severity
        self._on_closed = on_closed
        self._on_error = on_error
        self._on_cancel = on_cancel
        self._on_database = on_database
        super().__init__()

    async def _enter(self) -> t.Self:
        return self

    @AsyncNonConcurrentMethodChecker._non_concurrent_method
    async def _exit(self, exception_type, exception_value, traceback):
        if self._closed_flag:
            return
        success = not bool(exception_type)
        if success:
            await self._commit()
        elif issubclass(exception_type, asyncio.CancelledError):
            self._cancel()
            return
        await self._close()

    @AsyncNonConcurrentMethodChecker._non_concurrent_method
    async def _begin(
        self,
        database,
        imp_user,
        bookmarks,
        access_mode,
        metadata,
        timeout,
        notifications_min_severity,
        notifications_disabled_classifications,
        pipelined=False,
    ):
        async def on_begin_success(metadata_):
            db = metadata_.get("db")
            if isinstance(db, str):
                await AsyncUtil.callback(self._on_database, db)

        self._database = database
        self._connection.begin(
            bookmarks=bookmarks,
            metadata=metadata,
            timeout=timeout,
            mode=access_mode,
            db=database,
            imp_user=imp_user,
            notifications_min_severity=notifications_min_severity,
            notifications_disabled_classifications=(
                notifications_disabled_classifications
            ),
            on_success=on_begin_success,
        )
        if not pipelined:
            await self._error_handling_connection.send_all()
            await self._error_handling_connection.fetch_all()

    async def _result_on_closed_handler(self):
        pass

    async def _error_handler(self, exc):
        self._last_error = exc
        for result in self._results:
            result._tx_failure(exc)
        if isinstance(exc, asyncio.CancelledError):
            self._cancel()
            return
        await AsyncUtil.callback(self._on_error, exc)

    async def _consume_results(self):
        for result in self._results:
            await result._tx_end()
        self._results = []

    @AsyncNonConcurrentMethodChecker._non_concurrent_method
    async def run(
        self,
        query: t.LiteralString,
        parameters: dict[str, t.Any] | None = None,
        **kwparameters: t.Any,
    ) -> AsyncResult:
        """
        Run a Cypher query within the context of this transaction.

        Cypher is typically expressed as a query template plus a
        set of named parameters. In Python, parameters may be expressed
        through a dictionary of parameters, through individual parameter
        arguments, or as a mixture of both. For example, the ``run``
        calls below are all equivalent::

            query = "CREATE (a:Person { name: $name, age: $age })"
            result = await tx.run(query, {"name": "Alice", "age": 33})
            result = await tx.run(query, {"name": "Alice"}, age=33)
            result = await tx.run(query, name="Alice", age=33)

        Parameter values can be of any type supported by the Neo4j type
        system. In Python, this includes :class:`bool`, :class:`int`,
        :class:`str`, :class:`list` and :class:`dict`. Note however that
        :class:`list` properties must be homogenous.

        :param query: cypher query
        :type query: typing.LiteralString
        :param parameters: dictionary of parameters
        :type parameters: typing.Dict[str, typing.Any] | None
        :param kwparameters: additional keyword parameters.
            These take precedence over parameters passed as ``parameters``.
        :type kwparameters: typing.Any

        :raise TransactionError: if the transaction is already closed

        :returns: a new :class:`neo4j.AsyncResult` object
        """
        if isinstance(query, Query):
            raise TypeError("Query object is only supported for session.run")

        if self._closed_flag:
            raise TransactionError(self, "Transaction closed")
        if self._last_error:
            raise TransactionError(
                self, "Transaction failed"
            ) from self._last_error

        if (
            self._results
            and self._connection.supports_multiple_results is False
        ):
            # Bolt 3 Support
            # Buffer up all records for the previous Result because it does not
            # have any qid to fetch in batches.
            await self._results[-1]._buffer_all()

        result = AsyncResult(
            self._connection,
            self._fetch_size,
            self._warn_notification_severity,
            self._result_on_closed_handler,
            self._error_handler,
            None,
        )
        self._results.append(result)

        parameters = dict(parameters or {}, **kwparameters)
        await result._tx_ready_run(query, parameters)

        return result

    @AsyncNonConcurrentMethodChecker._non_concurrent_method
    async def _commit(self):
        if self._closed_flag:
            raise TransactionError(self, "Transaction closed")
        if self._last_error:
            raise TransactionError(
                self, "Transaction failed"
            ) from self._last_error

        metadata = {}
        try:
            # DISCARD pending records then do a commit.
            await self._consume_results()
            self._connection.commit(on_success=metadata.update)
            await self._connection.send_all()
            await self._connection.fetch_all()
            self._bookmark = metadata.get("bookmark")
            self._database = metadata.get("db", self._database)
        except asyncio.CancelledError:
            self._on_cancel()
            raise
        finally:
            self._closed_flag = True
            await AsyncUtil.callback(self._on_closed)

        return self._bookmark

    @AsyncNonConcurrentMethodChecker._non_concurrent_method
    async def _rollback(self):
        if self._closed_flag:
            raise TransactionError(self, "Transaction closed")

        metadata = {}
        try:
            if not (
                self._connection.defunct()
                or self._connection.closed()
                or self._connection.is_reset
            ):
                # DISCARD pending records then do a rollback.
                await self._consume_results()
                self._connection.rollback(on_success=metadata.update)
                await self._connection.send_all()
                await self._connection.fetch_all()
        except asyncio.CancelledError:
            self._on_cancel()
            raise
        finally:
            self._closed_flag = True
            await AsyncUtil.callback(self._on_closed)

    @AsyncNonConcurrentMethodChecker._non_concurrent_method
    async def _close(self):
        if self._closed_flag:
            return
        await self._rollback()

    if AsyncUtil.is_async_code:

        def _cancel(self) -> None:
            if self._closed_flag:
                return
            try:
                self._on_cancel()
            finally:
                self._closed_flag = True

    def _closed(self) -> bool:
        return self._closed_flag


class AsyncTransaction(AsyncTransactionBase):
    """
    Fully user-managed transaction.

    Container for multiple Cypher queries to be executed within a single
    context. :class:`AsyncTransaction` objects can be used as a context
    manager (``async with`` block) where the transaction is committed
    or rolled back based on whether an exception is raised::

        async with await session.begin_transaction() as tx:
            ...
    """

    async def __aenter__(self) -> AsyncTransaction:
        return await self._enter()

    async def __aexit__(
        self, exception_type, exception_value, traceback
    ) -> None:
        await self._exit(exception_type, exception_value, traceback)

    async def commit(self) -> None:
        """
        Commit the transaction and close it.

        Marks this transaction as successful and closes in order to trigger a
        COMMIT.

        :raise TransactionError: if the transaction is already closed
        """
        return await self._commit()

    async def rollback(self) -> None:
        """
        Rollback the transaction and close it.

        Marks the transaction as unsuccessful and closes in order to trigger
        a ROLLBACK.

        :raise TransactionError: if the transaction is already closed
        """
        return await self._rollback()

    async def close(self) -> None:
        """Close this transaction, triggering a ROLLBACK if not closed."""
        return await self._close()

    def closed(self) -> bool:
        """
        Indicate whether the transaction has been closed or cancelled.

        :returns:
            :data:`True` if closed or cancelled, :data:`False` otherwise.
        :rtype: bool
        """
        return self._closed()

    if AsyncUtil.is_async_code:

        def cancel(self) -> None:
            """
            Cancel this transaction.

            If the transaction is already closed, this method does nothing.
            Else, it will close the connection without ROLLBACK or COMMIT in
            a non-blocking manner.

            The primary purpose of this function is to handle
            :class:`asyncio.CancelledError`.

            ::

                tx = await session.begin_transaction()
                try:
                    ...  # do some work
                except asyncio.CancelledError:
                    tx.cancel()
                    raise
            """
            return self._cancel()


class AsyncManagedTransaction(AsyncTransactionBase):
    """
    Transaction object provided to transaction functions.

    Inside a transaction function, the driver is responsible for managing
    (committing / rolling back) the transaction. Therefore,
    AsyncManagedTransactions don't offer such methods.
    Otherwise, they behave like :class:`.AsyncTransaction`.

    * To commit the transaction,
      return anything from the transaction function.
    * To rollback the transaction, raise any exception.

    Note that transaction functions have to be idempotent (i.e., the result
    of running the function once has to be the same as running it any number
    of times). This is, because the driver will retry the transaction function
    if the error is classified as retryable.

    .. versionadded:: 5.0

        Prior, transaction functions used :class:`AsyncTransaction` objects,
        but would cause hard to interpret errors when managed explicitly
        (committed or rolled back by user code).
    """
