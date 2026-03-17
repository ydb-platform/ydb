import asyncio
import collections
import functools
import sys
import time
from contextvars import ContextVar

from sqlalchemy.engine import Engine, Connection
from sqlalchemy.sql import schema

from .aiocontextvars import patch_asyncio
from .exceptions import MultipleResultsFound, NoResultFound
from .transaction import GinoTransaction

patch_asyncio()


class _BaseDBAPIConnection:
    _reset_agent = None
    gino_conn = None

    def __init__(self, cursor_cls):
        self._cursor_cls = cursor_cls
        self._closed = False

    def commit(self):
        pass

    def cursor(self):
        return self._cursor_cls(self)

    @property
    def raw_connection(self):
        raise NotImplementedError

    async def acquire(self, *, timeout=None, read_only=False):
        if self._closed:
            raise ValueError("This connection is already released permanently.")
        return await self._acquire(timeout, read_only)

    async def _acquire(self, timeout):
        raise NotImplementedError

    async def release(self, permanent):
        if permanent:
            self._closed = True
        return await self._release()

    async def _release(self):
        raise NotImplementedError


class _DBAPIConnection(_BaseDBAPIConnection):
    def __init__(self, cursor_cls, pool=None):
        super().__init__(cursor_cls)
        self._pool = pool
        self._conn = None
        self._lock = asyncio.Lock()

    @property
    def raw_connection(self):
        return self._conn

    async def _acquire(self, timeout, read_only):
        try:
            if timeout is None:
                await self._lock.acquire()
            else:
                before = time.monotonic()
                await asyncio.wait_for(self._lock.acquire(), timeout=timeout)
                after = time.monotonic()
                timeout -= after - before
            if self._conn is None:
                self._conn = await self._pool.acquire(timeout=timeout, read_only=read_only)
            return self._conn
        finally:
            self._lock.release()

    async def _release(self):
        conn, self._conn = self._conn, None
        if conn is None:
            return False
        await self._pool.release(conn)
        return True


class _ReusingDBAPIConnection(_BaseDBAPIConnection):
    def __init__(self, cursor_cls, root):
        super().__init__(cursor_cls)
        self._root = root

    @property
    def raw_connection(self):
        return self._root.raw_connection

    async def _acquire(self, timeout, read_only):
        return await self._root.acquire(timeout=timeout, read_only=read_only)

    async def _release(self):
        pass


# noinspection PyPep8Naming,PyMethodMayBeStatic
class _bypass_no_param:
    def keys(self):
        return []


_bypass_no_param = _bypass_no_param()


# noinspection PyAbstractClass
class _SAConnection(Connection):
    def _execute_context(self, dialect, constructor, statement, parameters, *args):
        if parameters == [_bypass_no_param]:
            constructor = getattr(
                self.dialect.execution_ctx_cls,
                constructor.__name__ + "_prepared",
                constructor,
            )
        return super()._execute_context(
            dialect, constructor, statement, parameters, *args
        )


# noinspection PyAbstractClass
class _SAEngine(Engine):
    _connection_cls = _SAConnection

    def __init__(self, dialect, **kwargs):
        super().__init__(None, dialect, None, **kwargs)


class _AcquireContext:
    __slots__ = ["_acquire", "_conn"]

    def __init__(self, acquire):
        self._acquire = acquire
        self._conn = None

    async def __aenter__(self):
        self._conn = await self._acquire()
        return self._conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        conn, self._conn = self._conn, None
        await conn.release()

    def __await__(self):
        return self._acquire().__await__()


class _TransactionContext:
    __slots__ = ["_conn_ctx", "_tx_ctx"]

    def __init__(self, conn_ctx, args):
        self._conn_ctx = conn_ctx
        self._tx_ctx = args

    async def __aenter__(self):
        conn = await self._conn_ctx.__aenter__()
        try:
            args, kwargs = self._tx_ctx
            self._tx_ctx = conn.transaction(*args, **kwargs)
            return await self._tx_ctx.__aenter__()
        except Exception:
            await self._conn_ctx.__aexit__(*sys.exc_info())
            raise

    async def __aexit__(self, *exc_info):
        try:
            tx, self._tx_ctx = self._tx_ctx, None
            return await tx.__aexit__(*exc_info)
        except Exception:
            exc_info = sys.exc_info()
            raise
        finally:
            await self._conn_ctx.__aexit__(*exc_info)


class GinoConnection:
    """
    Represents an actual database connection.

    This is the root of all query API like :meth:`all`, :meth:`first`,
    :meth:`one`, :meth:`one_or_none`, :meth:`scalar` or :meth:`status`,
    those on engine or query are simply wrappers of methods in this class.

    Usually instances of this class are created by :meth:`.GinoEngine.acquire`.

    .. note::

        :class:`.GinoConnection` may refer to zero or one underlying database
        connection - when a :class:`.GinoConnection` is acquired with
        ``lazy=True``, the underlying connection may still be in the pool,
        until a query API is called or :meth:`get_raw_connection` is called.

        Oppositely, one underlying database connection can be shared by many
        :class:`.GinoConnection` instances when they are acquired with
        ``reuse=True``. The actual database connection is only returned to the
        pool when the **root** :class:`.GinoConnection` is released. Read more
        in :meth:`GinoEngine.acquire` method.

    .. seealso::

        :doc:`/explanation/engine`

    """

    # noinspection PyProtectedMember
    schema_for_object = schema._schema_getter(None)
    """A SQLAlchemy compatibility attribute, don't use it for now, it bites."""

    def __init__(self, dialect, sa_conn, stack=None):
        self._dialect = dialect
        self._sa_conn = sa_conn
        self._stack = stack

    @property
    def _dbapi_conn(self):
        return self._sa_conn.connection

    @property
    def raw_connection(self):
        """
        The current underlying database connection instance, type depends on
        the dialect in use. May be ``None`` if self is a lazy connection.

        """
        return self._dbapi_conn.raw_connection

    async def get_raw_connection(self, *, timeout=None):
        """
        Get the underlying database connection, acquire one if none present.

        :param timeout: Seconds to wait for the underlying acquiring
        :return: Underlying database connection instance depending on the
                 dialect in use
        :raises: :class:`~asyncio.TimeoutError` if the acquiring timed out

        """
        return await self._dbapi_conn.acquire(timeout=timeout)

    async def release(self, *, permanent=True):
        """
        Returns the underlying database connection to its pool.

        If ``permanent=False``, this connection will be set in lazy mode with
        underlying database connection returned, the next query on this
        connection will cause a new database connection acquired. This is
        useful when this connection may still be useful again later, while some
        long-running I/O operations are about to take place, which should not
        take up one database connection or even transaction for that long time.

        Otherwise with ``permanent=True`` (default), this connection will be
        marked as closed after returning to pool, and be no longer usable
        again.

        If this connection is a reusing connection, then only this connection
        is closed (depending on ``permanent``), the reused underlying
        connection will **not** be returned back to the pool.

        Practically it is recommended to return connections in the reversed
        order as they are borrowed, but if this connection is a reused
        connection with still other opening connections reusing it, then on
        release the underlying connection **will be** returned to the pool,
        with all the reusing connections losing an available underlying
        connection. The availability of further operations on those reusing
        connections depends on the given ``permanent`` value.

        .. seealso::

            :meth:`.GinoEngine.acquire`

        """
        if permanent and self._stack is not None:
            dbapi_conn = self._stack.remove(lambda x: x.gino_conn is self)
            if dbapi_conn:
                await dbapi_conn.release(True)
            else:
                raise ValueError("This connection is already released.")
        else:
            await self._dbapi_conn.release(permanent)

    @property
    def dialect(self):
        """
        The :class:`~sqlalchemy.engine.interfaces.Dialect` in use, inherited
        from the engine created this connection.

        """
        return self._dialect

    def _execute(self, clause, multiparams, params):
        return self._sa_conn.execute(clause, *multiparams, **params)

    async def all(self, clause, *multiparams, **params):
        """
        Runs the given query in database, returns all results as a list.

        This method accepts the same parameters taken by SQLAlchemy
        :meth:`~sqlalchemy.engine.Connectable.execute`. You can pass in a raw
        SQL string, or *any* SQLAlchemy query clauses.

        If the given query clause is built by CRUD models, then the returning
        rows will be turned into relevant model objects (Only one type of model
        per query is supported for now, no relationship support yet). See
        :meth:`execution_options` for more information.

        If the given parameters are parsed as "executemany" - bulk inserting
        multiple rows in one call for example, the returning result from
        database will be discarded and this method will return ``None``.

        """
        result = self._execute(clause, multiparams, params)
        return await result.execute()

    async def first(self, clause, *multiparams, **params):
        """
        Runs the given query in database, returns the first result.

        If the query returns no result, this method will return ``None``.

        See :meth:`all` for common query comments.

        """
        result = self._execute(clause, multiparams, params)
        return await result.execute(one=True)

    async def one_or_none(self, clause, *multiparams, **params):
        """
        Runs the given query in database, returns at most one result.

        If the query returns no result, this method will return ``None``.
        If the query returns multiple results, this method will raise
        :class:`~gino.exceptions.MultipleResultsFound`.

        See :meth:`all` for common query comments.

        """
        result = self._execute(clause, multiparams, params)
        ret = await result.execute()

        if ret is None or len(ret) == 0:
            return None

        if len(ret) == 1:
            return ret[0]

        raise MultipleResultsFound("Multiple rows found for one_or_none()")

    async def one(self, clause, *multiparams, **params):
        """
        Runs the given query in database, returns exactly one result.

        If the query returns no result, this method will raise
        :class:`~gino.exceptions.NoResultFound`.
        If the query returns multiple results, this method will raise
        :class:`~gino.exceptions.MultipleResultsFound`.

        See :meth:`all` for common query comments.

        """
        try:
            ret = await self.one_or_none(clause, *multiparams, **params)
        except MultipleResultsFound:
            raise MultipleResultsFound("Multiple rows found for one()")

        if ret is None:
            raise NoResultFound("No row was found for one()")

        return ret

    async def scalar(self, clause, *multiparams, **params):
        """
        Runs the given query in database, returns the first result.

        If the query returns no result, this method will return ``None``.

        See :meth:`all` for common query comments.

        """
        result = self._execute(clause, multiparams, params)
        rv = await result.execute(one=True, return_model=False)
        if rv:
            return rv[0]
        else:
            return None

    async def status(self, clause, *multiparams, **params):
        """
        Runs the given query in database, returns the query status.

        The returning query status depends on underlying database and the
        dialect in use. For asyncpg it is a string, you can parse it like this:
        https://git.io/v7oze

        """
        result = self._execute(clause, multiparams, params)
        return await result.execute(status=True)

    def transaction(self, *args, **kwargs):
        """
        Starts a database transaction.

        There are two ways using this method: **managed** as an asynchronous
        context manager::

            async with conn.transaction() as tx:
                # run query in transaction

        or **manually** awaited::

            tx = await conn.transaction()
            try:
                # run query in transaction
                await tx.commit()
            except Exception:
                await tx.rollback()
                raise

        Where the ``tx`` is an instance of the
        :class:`~gino.transaction.GinoTransaction` class, feel free to read
        more about it.

        In the first managed mode, the transaction is automatically committed
        on exiting the context block, or rolled back if an exception was raised
        which led to the exit of the context. In the second manual mode, you'll
        need to manually call the
        :meth:`~gino.transaction.GinoTransaction.commit` or
        :meth:`~gino.transaction.GinoTransaction.rollback` methods on need.

        If this is a lazy connection, entering a transaction will cause a new
        database connection acquired if none was present.

        Transactions may support nesting depending on the dialect in use. For
        example in asyncpg, starting a second transaction on the same
        connection will create a save point in the database.

        For now, the parameters are directly passed to underlying database
        driver, read :meth:`asyncpg.connection.Connection.transaction` for
        asyncpg.

        """
        return GinoTransaction(self, args, kwargs)

    def iterate(self, clause, *multiparams, **params):
        """
        Creates a server-side cursor in database for large query results.

        Cursors must work within transactions::

            async with conn.transaction():
                async for user in conn.iterate(User.query):
                    # handle each user without loading all users into memory

        Alternatively, you can manually control how the cursor works::

            async with conn.transaction():
                cursor = await conn.iterate(User.query)
                user = await cursor.next()
                users = await cursor.many(10)

        Read more about how :class:`~gino.dialects.base.Cursor` works.

        Similarly, this method takes the same parameters as :meth:`all`.

        """
        result = self._execute(clause, multiparams, params)
        return result.iterate()

    def execution_options(self, **opt):
        """
        Set non-SQL options for the connection which take effect during
        execution.

        This method returns a copy of this :class:`.GinoConnection` which
        references the same underlying database connection, but with the given
        execution options set on the copy. Therefore, it is a good practice to
        discard the copy immediately after use, for example::

            row = await conn.execution_options(model=None).first(User.query)

        This is very much the same as SQLAlchemy
        :meth:`~sqlalchemy.engine.base.Connection.execution_options`, it
        actually does pass the execution options to the underlying SQLAlchemy
        :class:`~sqlalchemy.engine.base.Connection`. Furthermore, GINO added a
        few execution options:

        :param return_model: Boolean to control whether the returning results
          should be loaded into model instances, where the model class is
          defined in another execution option ``model``. Default is ``True``.

        :param model: Specifies the type of model instance to create on return.
          This has no effect if ``return_model`` is set to ``False``. Usually
          in queries built by CRUD models, this execution option is
          automatically set. For now, GINO only supports loading each row into
          one type of model object, relationships are not supported. Please use
          multiple queries for that. ``None`` for no postprocessing (default).

        :param timeout: Seconds to wait for the query to finish. ``None`` for
          no time out (default).

        :param loader: A loader expression to load the database rows into
          specified objective structure. It can be either:

          * A model class, so that the query will yield model instances of this
            class. It is your responsibility to make sure all the columns of
            this model is selected in the query.
          * A :class:`~sqlalchemy.schema.Column` instance, so that each result
            will be only a single value of this column. Please note, if you
            want to achieve fetching the very first value, you should use
            :meth:`~gino.engine.GinoConnection.first` instead of
            :meth:`~gino.engine.GinoConnection.scalar`. However, using directly
            :meth:`~gino.engine.GinoConnection.scalar` is a more direct way.
          * A tuple nesting more loader expressions recursively.
          * A :func:`callable` function that will be called for each row to
            fully customize the result. Two positional arguments will be passed
            to the function: the first is the :class:`row
            <sqlalchemy.engine.RowProxy>` instance, the second is a context
            object which is only present if nested else ``None``.
          * A :class:`~gino.loader.Loader` instance directly.
          * Anything else will be treated as literal values thus returned as
            whatever they are.

        """
        return type(self)(self._dialect, self._sa_conn.execution_options(**opt))

    async def _run_visitor(self, visitorcallable, element, **kwargs):
        await visitorcallable(self.dialect, self, **kwargs).traverse_single(element)

    async def prepare(self, clause):
        return await self._execute(clause, (_bypass_no_param,), {}).prepare(clause)


class _ContextualStack:
    __slots__ = ("_ctx", "_stack")

    def __init__(self, ctx):
        self._ctx = ctx
        self._stack = ctx.get()
        if self._stack is None:
            self._stack = collections.deque()
            ctx.set(self._stack)

    def __bool__(self):
        return bool(self._stack)

    @property
    def top(self):
        return self._stack[-1]

    def push(self, value):
        self._stack.append(value)

    def remove(self, checker):
        for i in range(len(self._stack)):
            if checker(self._stack[-1]):
                rv = self._stack.pop()
                if self._stack:
                    self._stack.rotate(-i)
                else:
                    self._ctx.set(None)
                return rv
            else:
                self._stack.rotate(1)


class GinoEngine:
    """
    Connects a :class:`~.dialects.base.Pool` and
    :class:`~sqlalchemy.engine.interfaces.Dialect` together to provide a source
    of database connectivity and behavior.

    A :class:`.GinoEngine` object is instantiated publicly using the
    :func:`gino.create_engine` function or
    :func:`db.set_bind() <gino.api.Gino.set_bind>` method.

    .. seealso::

        :doc:`/explanation/engine`

    """

    connection_cls = GinoConnection
    """Customizes the connection class to use, default is
    :class:`.GinoConnection`."""

    def __init__(
        self, dialect, pool, loop, logging_name=None, echo=None, execution_options=None
    ):
        self._sa_engine = _SAEngine(
            dialect,
            logging_name=logging_name,
            echo=echo,
            execution_options=execution_options,
        )
        self._dialect = dialect
        self._pool = pool
        self._loop = loop
        self._ctx = ContextVar("gino", default=None)

    @property
    def dialect(self):
        """
        Read-only property for the
        :class:`~sqlalchemy.engine.interfaces.Dialect` of this engine.

        """
        return self._dialect

    @property
    def raw_pool(self):
        """
        Read-only access to the underlying database connection pool instance.
        This depends on the actual dialect in use, :class:`~asyncpg.pool.Pool`
        of asyncpg for example.

        """
        return self._pool.raw_pool

    def acquire(self, *, timeout=None, reuse=False, lazy=False, reusable=True, read_only=False):
        """
        Acquire a connection from the pool.

        There are two ways using this method - as an asynchronous context
        manager::

            async with engine.acquire() as conn:
                # play with the connection

        which will guarantee the connection is returned to the pool when
        leaving the ``async with`` block; or as a coroutine::

            conn = await engine.acquire()
            try:
                # play with the connection
            finally:
                await conn.release()

        where the connection should be manually returned to the pool with
        :meth:`conn.release() <.GinoConnection.release>`.

        Within the same context (usually the same :class:`~asyncio.Task`, see
        also :doc:`/how-to/transaction`), a nesting acquire by default re

        :param timeout: Block up to ``timeout`` seconds until there is one free
          connection in the pool. Default is ``None`` - block forever until
          succeeded. This has no effect when ``lazy=True``, and depends on the
          actual situation when ``reuse=True``.

        :param reuse: Reuse the latest reusable acquired connection (before
          it's returned to the pool) in current context if there is one, or
          borrow a new one if none present. Default is ``False`` for always
          borrow a new one. This is useful when you are in a nested method call
          series, wishing to use the same connection without passing it around
          as parameters. See also: :doc:`/how-to/transaction`. A reusing
          connection is not reusable even if ``reusable=True``. If the reused
          connection happened to be a lazy one, then the reusing connection is
          lazy too.

        :param lazy: Don't acquire the actual underlying connection yet - do it
          only when needed. Default is ``False`` for always do it immediately.
          This is useful before entering a code block which may or may not make
          use of a given connection object. Feeding in a lazy connection will
          save the borrow-return job if the connection is never used. If
          setting ``reuse=True`` at the same time, then the reused connection -
          if any - applies the same laziness. For example, reusing a lazy
          connection with ``lazy=False`` will cause the reused connection to
          acquire an underlying connection immediately.

        :param reusable: Mark this connection as reusable or otherwise. This
          has no effect if it is a reusing connection. All reusable connections
          are placed in a stack, any reusing acquire operation will always
          reuse the top (latest) reusable connection. One reusable connection
          may be reused by several reusing connections - they all share one
          same underlying connection. Acquiring a connection with
          ``reusable=False`` and ``reusing=False`` makes it a cleanly isolated
          connection which is only referenced once here.

        :return: A :class:`.GinoConnection` object.

        """
        return _AcquireContext(
            functools.partial(self._acquire, timeout, reuse, lazy, reusable, read_only)
        )

    async def _acquire(self, timeout, reuse, lazy, reusable, read_only):
        stack = _ContextualStack(self._ctx)
        if reuse and stack:
            dbapi_conn = _ReusingDBAPIConnection(self._dialect.cursor_cls, stack.top)
            reusable = False
        else:
            dbapi_conn = _DBAPIConnection(self._dialect.cursor_cls, self._pool)
        rv = self.connection_cls(
            self._dialect,
            _SAConnection(self._sa_engine, dbapi_conn),
            stack if reusable else None,
        )
        dbapi_conn.gino_conn = rv
        if not lazy:
            await dbapi_conn.acquire(timeout=timeout, read_only=read_only)
        if reusable:
            stack.push(dbapi_conn)
        return rv

    @property
    def current_connection(self):
        """
        Gets the most recently acquired reusable connection in the context.
        ``None`` if there is no such connection.

        :return: :class:`.GinoConnection`

        """
        stack = self._ctx.get()
        if stack:
            return stack[-1].gino_conn

    async def close(self):
        """
        Close the engine, by closing the underlying pool.

        """
        await self._pool.close()

    async def all(self, clause, *multiparams, **params):
        """
        Acquires a connection with ``reuse=True`` and runs
        :meth:`~.GinoConnection.all` on it. ``reuse=True`` means you can safely
        do this without borrowing more than one underlying connection::

            async with engine.acquire():
                await engine.all('SELECT ...')

        The same applies for other query methods.

        """
        read_only = params.pop('read_only', True)
        reuse = params.pop('reuse', True)
        async with self.acquire(reuse=reuse, read_only=read_only) as conn:
            return await conn.all(clause, *multiparams, **params)

    async def first(self, clause, *multiparams, **params):
        """
        Runs :meth:`~.GinoConnection.first`, See :meth:`.all`.

        """
        read_only = params.pop('read_only', True)
        reuse = params.pop('reuse', True)
        async with self.acquire(reuse=reuse, read_only=read_only) as conn:
            return await conn.first(clause, *multiparams, **params)

    async def one_or_none(self, clause, *multiparams, **params):
        """
        Runs :meth:`~.GinoConnection.one_or_none`, See :meth:`.all`.

        """
        read_only = params.pop('read_only', True)
        reuse = params.pop('reuse', True)
        async with self.acquire(reuse=reuse, read_only=read_only) as conn:
            return await conn.one_or_none(clause, *multiparams, **params)

    async def one(self, clause, *multiparams, **params):
        """
        Runs :meth:`~.GinoConnection.one`, See :meth:`.all`.

        """
        read_only = params.pop('read_only', True)
        reuse = params.pop('reuse', True)
        async with self.acquire(reuse=reuse, read_only=read_only) as conn:
            return await conn.one(clause, *multiparams, **params)

    async def scalar(self, clause, *multiparams, **params):
        """
        Runs :meth:`~.GinoConnection.scalar`, See :meth:`.all`.

        """
        read_only = params.pop('read_only', True)
        reuse = params.pop('reuse', True)
        async with self.acquire(reuse=reuse, read_only=read_only) as conn:
            return await conn.scalar(clause, *multiparams, **params)

    async def status(self, clause, *multiparams, **params):
        """
        Runs :meth:`~.GinoConnection.status`. See also :meth:`.all`.

        """
        read_only = params.pop('read_only', True)
        reuse = params.pop('reuse', True)
        async with self.acquire(reuse=reuse, read_only=read_only) as conn:
            return await conn.status(clause, *multiparams, **params)

    def compile(self, clause, *multiparams, **params):
        """
        A shortcut for :meth:`~gino.dialects.base.AsyncDialectMixin.compile` on
        the dialect, returns raw SQL string and parameters according to the
        rules of the dialect.

        """
        return self._dialect.compile(clause, *multiparams, **params)

    def transaction(self, *args, timeout=None, reuse=True, reusable=True, **kwargs):
        """
        Borrows a new connection and starts a transaction with it.

        Different to :meth:`.GinoConnection.transaction`, transaction on engine
        level supports only managed usage::

            async with engine.transaction() as tx:
                # play with transaction here

        Where the implicitly acquired connection is available as
        :attr:`tx.connection <gino.transaction.GinoTransaction.connection>`.

        By default, :meth:`.transaction` acquires connection with
        ``reuse=True`` and ``reusable=True``, that means it by default tries to
        create a nested transaction instead of a new transaction on a new
        connection. You can change the default behavior by setting these two
        arguments.

        The other arguments are the same as
        :meth:`~.GinoConnection.transaction` on connection.

        .. seealso::

            :meth:`.GinoEngine.acquire`

            :meth:`.GinoConnection.transaction`

            :class:`~gino.transaction.GinoTransaction`

        :return: A asynchronous context manager that yields a
          :class:`~gino.transaction.GinoTransaction`

        """
        read_only = kwargs.pop('read_only', False)
        return _TransactionContext(
            self.acquire(timeout=timeout, reuse=reuse, reusable=reusable, read_only=read_only),
            (args, kwargs),
        )

    def iterate(self, clause, *multiparams, **params):
        """
        Creates a server-side cursor in database for large query results.

        This requires that there is a reusable connection in the current
        context, and an active transaction is present. Then its
        :meth:`.GinoConnection.iterate` is executed and returned.

        """
        connection = self.current_connection
        if connection is None:
            raise ValueError("No Connection in context, please provide one")
        return connection.iterate(clause, *multiparams, **params)

    def update_execution_options(self, **opt):
        """Update the default execution_options dictionary
        of this :class:`.GinoEngine`.

        .. seealso::

            :meth:`sqlalchemy.engine.Engine.update_execution_options`

            :meth:`.GinoConnection.execution_options`

        """
        self._sa_engine.update_execution_options(**opt)

    async def _run_visitor(self, *args, **kwargs):
        async with self.acquire(reuse=True) as conn:
            await getattr(conn, "_run_visitor")(*args, **kwargs)

    def repr(self, color=False):
        return self._pool.repr(color)

    def __repr__(self):
        return self.repr()
