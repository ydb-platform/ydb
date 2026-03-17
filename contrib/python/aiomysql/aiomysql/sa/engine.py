# ported from:
# https://github.com/aio-libs/aiopg/blob/master/aiopg/sa/engine.py
import asyncio

import aiomysql
from .connection import SAConnection
from .exc import InvalidRequestError, ArgumentError
from ..utils import _PoolContextManager, _PoolAcquireContextManager
from ..cursors import (
    Cursor, DeserializationCursor, DictCursor, SSCursor, SSDictCursor)


try:
    from sqlalchemy.dialects.mysql.pymysql import MySQLDialect_pymysql
    from sqlalchemy.dialects.mysql.mysqldb import MySQLCompiler_mysqldb
except ImportError:  # pragma: no cover
    raise ImportError('aiomysql.sa requires sqlalchemy')


class MySQLCompiler_pymysql(MySQLCompiler_mysqldb):
    def construct_params(self, params=None, _group_number=None, _check=True):
        pd = super().construct_params(params, _group_number, _check)

        for column in self.prefetch:
            pd[column.key] = self._exec_default(column.default)

        return pd

    def _exec_default(self, default):
        if default.is_callable:
            return default.arg(self.dialect)
        else:
            return default.arg


_dialect = MySQLDialect_pymysql(paramstyle='pyformat')
_dialect.statement_compiler = MySQLCompiler_pymysql
_dialect.default_paramstyle = 'pyformat'


def create_engine(minsize=1, maxsize=10, loop=None,
                  dialect=_dialect, pool_recycle=-1,  compiled_cache=None,
                  **kwargs):
    """A coroutine for Engine creation.

    Returns Engine instance with embedded connection pool.

    The pool has *minsize* opened connections to MySQL server.
    """
    deprecated_cursor_classes = [
        DeserializationCursor, DictCursor, SSCursor, SSDictCursor,
    ]

    cursorclass = kwargs.get('cursorclass', Cursor)
    if not issubclass(cursorclass, Cursor) or any(
        issubclass(cursorclass, cursor_class)
        for cursor_class in deprecated_cursor_classes
    ):
        raise ArgumentError('SQLAlchemy engine does not support '
                            'this cursor class')

    coro = _create_engine(minsize=minsize, maxsize=maxsize, loop=loop,
                          dialect=dialect, pool_recycle=pool_recycle,
                          compiled_cache=compiled_cache, **kwargs)
    return _EngineContextManager(coro)


async def _create_engine(minsize=1, maxsize=10, loop=None,
                         dialect=_dialect, pool_recycle=-1,
                         compiled_cache=None, **kwargs):

    if loop is None:
        loop = asyncio.get_event_loop()
    pool = await aiomysql.create_pool(minsize=minsize, maxsize=maxsize,
                                      loop=loop,
                                      pool_recycle=pool_recycle, **kwargs)
    conn = await pool.acquire()
    try:
        return Engine(dialect, pool, compiled_cache=compiled_cache, **kwargs)
    finally:
        pool.release(conn)


class Engine:
    """Connects a aiomysql.Pool and
    sqlalchemy.engine.interfaces.Dialect together to provide a
    source of database connectivity and behavior.

    An Engine object is instantiated publicly using the
    create_engine coroutine.
    """

    def __init__(self, dialect, pool, compiled_cache=None, **kwargs):
        self._dialect = dialect
        self._pool = pool
        self._compiled_cache = compiled_cache
        self._conn_kw = kwargs

    @property
    def dialect(self):
        """An dialect for engine."""
        return self._dialect

    @property
    def name(self):
        """A name of the dialect."""
        return self._dialect.name

    @property
    def driver(self):
        """A driver of the dialect."""
        return self._dialect.driver

    @property
    def minsize(self):
        return self._pool.minsize

    @property
    def maxsize(self):
        return self._pool.maxsize

    @property
    def size(self):
        return self._pool.size

    @property
    def freesize(self):
        return self._pool.freesize

    def close(self):
        """Close engine.

        Mark all engine connections to be closed on getting back to pool.
        Closed engine doesn't allow to acquire new connections.
        """
        self._pool.close()

    def terminate(self):
        """Terminate engine.

        Terminate engine pool with instantly closing all acquired
        connections also.
        """
        self._pool.terminate()

    async def wait_closed(self):
        """Wait for closing all engine's connections."""
        await self._pool.wait_closed()

    def acquire(self):
        """Get a connection from pool."""
        coro = self._acquire()
        return _EngineAcquireContextManager(coro, self)

    async def _acquire(self):
        raw = await self._pool.acquire()
        conn = SAConnection(raw, self, compiled_cache=self._compiled_cache)
        return conn

    def release(self, conn):
        """Revert back connection to pool."""
        if conn.in_transaction:
            raise InvalidRequestError("Cannot release a connection with "
                                      "not finished transaction")
        raw = conn.connection
        return self._pool.release(raw)

    def __enter__(self):
        raise RuntimeError(
            '"yield from" should be used as context manager expression')

    def __exit__(self, *args):
        # This must exist because __enter__ exists, even though that
        # always raises; that's how the with-statement works.
        pass  # pragma: nocover

    def __iter__(self):
        # This is not a coroutine.  It is meant to enable the idiom:
        #
        #     with (yield from engine) as conn:
        #         <block>
        #
        # as an alternative to:
        #
        #     conn = yield from engine.acquire()
        #     try:
        #         <block>
        #     finally:
        #         engine.release(conn)
        conn = yield from self.acquire()
        return _ConnectionContextManager(self, conn)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
        await self.wait_closed()


_EngineContextManager = _PoolContextManager
_EngineAcquireContextManager = _PoolAcquireContextManager


class _ConnectionContextManager:
    """Context manager.

    This enables the following idiom for acquiring and releasing a
    connection around a block:

        with (yield from engine) as conn:
            cur = yield from conn.cursor()

    while failing loudly when accidentally using:

        with engine:
            <block>
    """

    __slots__ = ('_engine', '_conn')

    def __init__(self, engine, conn):
        self._engine = engine
        self._conn = conn

    def __enter__(self):
        assert self._conn is not None
        return self._conn

    def __exit__(self, *args):
        try:
            self._engine.release(self._conn)
        finally:
            self._engine = None
            self._conn = None
