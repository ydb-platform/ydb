import asyncio
import json

import aiopg

from ..connection import TIMEOUT
from ..utils import _ContextManager, get_running_loop
from .connection import SAConnection

try:
    from sqlalchemy.dialects.postgresql.psycopg2 import (
        PGCompiler_psycopg2,
        PGDialect_psycopg2,
    )
except ImportError:  # pragma: no cover
    raise ImportError("aiopg.sa requires sqlalchemy")


class APGCompiler_psycopg2(PGCompiler_psycopg2):
    def construct_params(self, *args, **kwargs):
        pd = super().construct_params(*args, **kwargs)

        for column in self.prefetch:
            pd[column.key] = self._exec_default(column.default)

        return pd

    def _exec_default(self, default):
        if default.is_callable:
            return default.arg(self.dialect)
        else:
            return default.arg


def get_dialect(json_serializer=json.dumps, json_deserializer=lambda x: x):
    dialect = PGDialect_psycopg2(
        json_serializer=json_serializer, json_deserializer=json_deserializer
    )

    dialect.statement_compiler = APGCompiler_psycopg2
    dialect.implicit_returning = True
    dialect.supports_native_enum = True
    dialect.supports_smallserial = True  # 9.2+
    dialect._backslash_escapes = False
    dialect.supports_sane_multi_rowcount = True  # psycopg 2.0.9+
    dialect._has_native_hstore = True

    return dialect


_dialect = get_dialect()


def create_engine(
    dsn=None,
    *,
    minsize=1,
    maxsize=10,
    dialect=_dialect,
    timeout=TIMEOUT,
    pool_recycle=-1,
    **kwargs
):
    """A coroutine for Engine creation.

    Returns Engine instance with embedded connection pool.

    The pool has *minsize* opened connections to PostgreSQL server.
    """

    coro = _create_engine(
        dsn=dsn,
        minsize=minsize,
        maxsize=maxsize,
        dialect=dialect,
        timeout=timeout,
        pool_recycle=pool_recycle,
        **kwargs
    )
    return _ContextManager(coro, _close_engine)


async def _create_engine(
    dsn=None,
    *,
    minsize=1,
    maxsize=10,
    dialect=_dialect,
    timeout=TIMEOUT,
    pool_recycle=-1,
    **kwargs
):

    pool = await aiopg.create_pool(
        dsn,
        minsize=minsize,
        maxsize=maxsize,
        timeout=timeout,
        pool_recycle=pool_recycle,
        **kwargs
    )
    conn = await pool.acquire()
    try:
        real_dsn = conn.dsn
        return Engine(dialect, pool, real_dsn)
    finally:
        await pool.release(conn)


async def _close_engine(engine: "Engine") -> None:
    engine.close()
    await engine.wait_closed()


async def _close_connection(c: SAConnection) -> None:
    await c.close()


class Engine:
    """Connects a aiopg.Pool and
    sqlalchemy.engine.interfaces.Dialect together to provide a
    source of database connectivity and behavior.

    An Engine object is instantiated publicly using the
    create_engine coroutine.
    """

    __slots__ = ("_dialect", "_pool", "_dsn", "_loop")

    def __init__(self, dialect, pool, dsn):
        self._dialect = dialect
        self._pool = pool
        self._dsn = dsn
        self._loop = get_running_loop()

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
    def dsn(self):
        """DSN connection info"""
        return self._dsn

    @property
    def timeout(self):
        return self._pool.timeout

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

    @property
    def closed(self):
        return self._pool.closed

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
        return _ContextManager[SAConnection](coro, _close_connection)

    async def _acquire(self):
        raw = await self._pool.acquire()
        return SAConnection(raw, self)

    def release(self, conn):
        return self._pool.release(conn.connection)

    def __enter__(self):
        raise RuntimeError(
            '"await" should be used as context manager expression'
        )

    def __exit__(self, *args):
        # This must exist because __enter__ exists, even though that
        # always raises; that's how the with-statement works.
        pass  # pragma: nocover

    def __await__(self):
        # This is not a coroutine.  It is meant to enable the idiom:
        #
        #     with (await engine) as conn:
        #         <block>
        #
        # as an alternative to:
        #
        #     conn = await engine.acquire()
        #     try:
        #         <block>
        #     finally:
        #         engine.release(conn)
        conn = yield from self._acquire().__await__()
        return _ConnectionContextManager(conn, self._loop)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
        await self.wait_closed()


class _ConnectionContextManager:
    """Context manager.

    This enables the following idiom for acquiring and releasing a
    connection around a block:

        async with engine as conn:
            cur = await conn.cursor()

    while failing loudly when accidentally using:

        with engine:
            <block>
    """

    __slots__ = ("_conn", "_loop")

    def __init__(self, conn: SAConnection, loop: asyncio.AbstractEventLoop):
        self._conn = conn
        self._loop = loop

    def __enter__(self):
        return self._conn

    def __exit__(self, *args):
        asyncio.ensure_future(self._conn.close(), loop=self._loop)
        self._conn = None
