import asyncio
import contextlib
import weakref

from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.ddl import DDLElement
from sqlalchemy.sql.dml import UpdateBase

from ..utils import _ContextManager, _IterableContextManager
from . import exc
from .result import ResultProxy
from .transaction import (
    NestedTransaction,
    RootTransaction,
    Transaction,
    TwoPhaseTransaction,
)


async def _commit_transaction_if_active(t: Transaction) -> None:
    if t.is_active:
        await t.commit()


async def _rollback_transaction(t: Transaction) -> None:
    await t.rollback()


async def _close_result_proxy(c: "ResultProxy") -> None:
    c.close()


class SAConnection:
    _QUERY_COMPILE_KWARGS = (("render_postcompile", True),)

    __slots__ = (
        "_connection",
        "_transaction",
        "_savepoint_seq",
        "_engine",
        "_dialect",
        "_cursors",
        "_query_compile_kwargs",
    )

    def __init__(self, connection, engine):
        self._connection = connection
        self._transaction = None
        self._savepoint_seq = 0
        self._engine = engine
        self._dialect = engine.dialect
        self._cursors = weakref.WeakSet()
        self._query_compile_kwargs = dict(self._QUERY_COMPILE_KWARGS)

    def execute(self, query, *multiparams, **params):
        """Executes a SQL query with optional parameters.

        query - a SQL query string or any sqlalchemy expression.

        *multiparams/**params - represent bound parameter values to be
        used in the execution.  Typically, the format is a dictionary
        passed to *multiparams:

            await conn.execute(
                table.insert(),
                {"id":1, "value":"v1"},
            )

        ...or individual key/values interpreted by **params::

            await conn.execute(
                table.insert(), id=1, value="v1"
            )

        In the case that a plain SQL string is passed, a tuple or
        individual values in \\*multiparams may be passed::

            await conn.execute(
                "INSERT INTO table (id, value) VALUES (%d, %s)",
                (1, "v1")
            )

            await conn.execute(
                "INSERT INTO table (id, value) VALUES (%s, %s)",
                1, "v1"
            )

        Returns ResultProxy instance with results of SQL query
        execution.

        """
        coro = self._execute(query, *multiparams, **params)
        return _IterableContextManager[ResultProxy](coro, _close_result_proxy)

    async def _open_cursor(self):
        if self._connection is None:
            raise exc.ResourceClosedError("This connection is closed.")
        cursor = await self._connection.cursor()
        self._cursors.add(cursor)
        return cursor

    def _close_cursor(self, cursor):
        self._cursors.remove(cursor)
        cursor.close()

    async def _execute(self, query, *multiparams, **params):
        cursor = await self._open_cursor()
        dp = _distill_params(multiparams, params)
        if len(dp) > 1:
            raise exc.ArgumentError("aiopg doesn't support executemany")
        elif dp:
            dp = dp[0]

        result_map = None

        if isinstance(query, str):
            await cursor.execute(query, dp)
        elif isinstance(query, ClauseElement):
            # parameters = compiled.params
            if not isinstance(query, DDLElement):
                compiled = query.compile(
                    dialect=self._dialect,
                    compile_kwargs=self._query_compile_kwargs,
                )
                if dp and isinstance(dp, (list, tuple)):
                    if isinstance(query, UpdateBase):
                        dp = {
                            c.key: pval for c, pval in zip(query.table.c, dp)
                        }
                    else:
                        raise exc.ArgumentError(
                            "Don't mix sqlalchemy SELECT "
                            "clause with positional "
                            "parameters"
                        )

                compiled_parameters = [compiled.construct_params(dp)]
                processed_parameters = []
                processors = compiled._bind_processors
                for compiled_params in compiled_parameters:
                    params = {
                        key: (
                            processors[key](compiled_params[key])
                            if key in processors
                            else compiled_params[key]
                        )
                        for key in compiled_params
                    }
                    processed_parameters.append(params)
                post_processed_params = self._dialect.execute_sequence_format(
                    processed_parameters
                )

                # _result_columns is a private API of Compiled,
                # but I couldn't find any public API exposing this data.
                result_map = compiled._result_columns

            else:
                compiled = query.compile(dialect=self._dialect)
                if dp:
                    raise exc.ArgumentError(
                        "Don't mix sqlalchemy DDL clause "
                        "and execution with parameters"
                    )
                post_processed_params = [compiled.construct_params()]
                result_map = None

            await cursor.execute(str(compiled), post_processed_params[0])
        else:
            raise exc.ArgumentError(
                "sql statement should be str or "
                "SQLAlchemy data "
                "selection/modification clause"
            )

        return ResultProxy(self, cursor, self._dialect, result_map)

    async def scalar(self, query, *multiparams, **params):
        """Executes a SQL query and returns a scalar value."""
        res = await self.execute(query, *multiparams, **params)
        return await res.scalar()

    @property
    def closed(self):
        """The readonly property that returns True if connections is closed."""
        return self.connection is None or self.connection.closed

    @property
    def connection(self):
        return self._connection

    def begin(self, isolation_level=None, readonly=False, deferrable=False):
        """Begin a transaction and return a transaction handle.

        isolation_level - The isolation level of the transaction,
        should be one of 'SERIALIZABLE', 'REPEATABLE READ', 'READ COMMITTED',
        'READ UNCOMMITTED', default (None) is 'READ COMMITTED'

        readonly - The transaction is read only

        deferrable - The transaction may block when acquiring data before
        running without overhead of SERLIALIZABLE, has no effect unless
        transaction is both SERIALIZABLE and readonly

        The returned object is an instance of Transaction.  This
        object represents the "scope" of the transaction, which
        completes when either the .rollback or .commit method is
        called.

        Nested calls to .begin on the same SAConnection instance will
        return new Transaction objects that represent an emulated
        transaction within the scope of the enclosing transaction,
        that is::

            trans = await conn.begin()   # outermost transaction
            trans2 = await conn.begin()  # "nested"
            await trans2.commit()        # does nothing
            await trans.commit()         # actually commits

        Calls to .commit only have an effect when invoked via the
        outermost Transaction object, though the .rollback method of
        any of the Transaction objects will roll back the transaction.

        See also:
          .begin_nested - use a SAVEPOINT
          .begin_twophase - use a two phase/XA transaction

        """
        coro = self._begin(isolation_level, readonly, deferrable)
        return _ContextManager[Transaction](
            coro, _commit_transaction_if_active, _rollback_transaction
        )

    async def _begin(self, isolation_level, readonly, deferrable):
        if self._transaction is None:
            self._transaction = RootTransaction(self)
            await self._begin_impl(isolation_level, readonly, deferrable)
            return self._transaction
        return Transaction(self, self._transaction)

    async def _begin_impl(self, isolation_level, readonly, deferrable):
        stmt = "BEGIN"
        if isolation_level is not None:
            stmt += f" ISOLATION LEVEL {isolation_level}"
        if readonly:
            stmt += " READ ONLY"
        if deferrable:
            stmt += " DEFERRABLE"

        cursor = await self._open_cursor()
        try:
            await cursor.execute(stmt)
        finally:
            self._close_cursor(cursor)

    async def _commit_impl(self):
        cursor = await self._open_cursor()
        try:
            await cursor.execute("COMMIT")
        finally:
            self._close_cursor(cursor)
            self._transaction = None

    async def _rollback_impl(self):
        try:
            if self._connection.closed:
                return
            cursor = await self._open_cursor()
            try:
                await cursor.execute("ROLLBACK")
            finally:
                self._close_cursor(cursor)
        finally:
            self._transaction = None

    def begin_nested(self):
        """Begin a nested transaction and return a transaction handle.

        The returned object is an instance of :class:`.NestedTransaction`.

        Nested transactions require SAVEPOINT support in the
        underlying database.  Any transaction in the hierarchy may
        .commit() and .rollback(), however the outermost transaction
        still controls the overall .commit() or .rollback() of the
        transaction of a whole.
        """
        coro = self._begin_nested()
        return _ContextManager(
            coro, _commit_transaction_if_active, _rollback_transaction
        )

    async def _begin_nested(self):
        if self._transaction is None:
            self._transaction = RootTransaction(self)
            await self._begin_impl(None, False, False)
        else:
            self._transaction = NestedTransaction(self, self._transaction)
            self._transaction._savepoint = await self._savepoint_impl()
        return self._transaction

    async def _savepoint_impl(self):
        self._savepoint_seq += 1
        name = f"aiopg_sa_savepoint_{self._savepoint_seq}"

        cursor = await self._open_cursor()
        try:
            await cursor.execute(f"SAVEPOINT {name}")
            return name
        finally:
            self._close_cursor(cursor)

    async def _rollback_to_savepoint_impl(self, name, parent):
        try:
            if self._connection.closed:
                return
            cursor = await self._open_cursor()
            try:
                await cursor.execute(f"ROLLBACK TO SAVEPOINT {name}")
            finally:
                self._close_cursor(cursor)
        finally:
            self._transaction = parent

    async def _release_savepoint_impl(self, name, parent):
        cursor = await self._open_cursor()
        try:
            await cursor.execute(f"RELEASE SAVEPOINT {name}")
        finally:
            self._close_cursor(cursor)

        self._transaction = parent

    async def begin_twophase(self, xid=None):
        """Begin a two-phase or XA transaction and return a transaction
        handle.

        The returned object is an instance of
        TwoPhaseTransaction, which in addition to the
        methods provided by Transaction, also provides a
        TwoPhaseTransaction.prepare() method.

        xid - the two phase transaction id.  If not supplied, a
        random id will be generated.
        """

        if self._transaction is not None:
            raise exc.InvalidRequestError(
                "Cannot start a two phase transaction when a transaction "
                "is already in progress."
            )
        if xid is None:
            xid = self._dialect.create_xid()
        self._transaction = TwoPhaseTransaction(self, xid)
        await self._begin_impl(None, False, False)
        return self._transaction

    async def _prepare_twophase_impl(self, xid):
        await self.execute(f"PREPARE TRANSACTION {xid!r}")

    async def recover_twophase(self):
        """Return a list of prepared twophase transaction ids."""
        result = await self.execute("SELECT gid FROM pg_prepared_xacts")
        return [row[0] for row in result]

    async def rollback_prepared(self, xid, *, is_prepared=True):
        """Rollback prepared twophase transaction."""
        if is_prepared:
            await self.execute(f"ROLLBACK PREPARED {xid:!r}")
        else:
            await self._rollback_impl()

    async def commit_prepared(self, xid, *, is_prepared=True):
        """Commit prepared twophase transaction."""
        if is_prepared:
            await self.execute(f"COMMIT PREPARED {xid!r}")
        else:
            await self._commit_impl()

    @property
    def in_transaction(self):
        """Return True if a transaction is in progress."""
        return self._transaction is not None and self._transaction.is_active

    async def close(self):
        """Close this SAConnection.

        This results in a release of the underlying database
        resources, that is, the underlying connection referenced
        internally. The underlying connection is typically restored
        back to the connection-holding Pool referenced by the Engine
        that produced this SAConnection. Any transactional state
        present on the underlying connection is also unconditionally
        released via calling Transaction.rollback() method.

        After .close() is called, the SAConnection is permanently in a
        closed state, and will allow no further operations.
        """

        if self.connection is None:
            return

        await asyncio.shield(self._close())

    async def _close(self):
        if self._transaction is not None:
            with contextlib.suppress(Exception):
                await self._transaction.rollback()
            self._transaction = None

        for cursor in self._cursors:
            cursor.close()
        self._cursors.clear()

        if self._engine is not None:
            with contextlib.suppress(Exception):
                await self._engine.release(self)
            self._connection = None
            self._engine = None


def _distill_params(multiparams, params):
    """Given arguments from the calling form *multiparams, **params,
    return a list of bind parameter structures, usually a list of
    dictionaries.

    In the case of 'raw' execution which accepts positional parameters,
    it may be a list of tuples or lists.

    """

    if not multiparams:
        if params:
            return [params]
        else:
            return []
    elif len(multiparams) == 1:
        zero = multiparams[0]
        if isinstance(zero, (list, tuple)):
            if (
                not zero
                or hasattr(zero[0], "__iter__")
                and not hasattr(zero[0], "strip")
            ):
                # execute(stmt, [{}, {}, {}, ...])
                # execute(stmt, [(), (), (), ...])
                return zero
            else:
                # execute(stmt, ("value", "value"))
                return [zero]
        elif hasattr(zero, "keys"):
            # execute(stmt, {"key":"value"})
            return [zero]
        else:
            # execute(stmt, "value")
            return [[zero]]
    else:
        if hasattr(multiparams[0], "__iter__") and not hasattr(
            multiparams[0], "strip"
        ):
            return multiparams
        else:
            return [multiparams]
