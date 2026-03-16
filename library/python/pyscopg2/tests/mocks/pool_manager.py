import asyncio

import mock

from pyscopg2.base import BasePoolManager
from pyscopg2.utils import Dsn


class TestConnection:
    def __init__(self, pool: "TestPool"):
        self._pool = pool
        self._is_closed = False
        self.close = mock.AsyncMock(side_effect=self.close)
        self.terminate = mock.Mock(side_effect=self.terminate)

    @property
    def is_closed(self):
        return self._is_closed

    async def is_master(self):
        if not self._pool.is_running:
            raise ConnectionRefusedError
        if self._pool.is_behind_firewall:
            await asyncio.sleep(100)
        return self._pool.is_master

    async def close(self):
        self._is_closed = True

    def terminate(self):
        self._is_closed = True


class PoolAcquireContext:
    def __init__(self, pool):
        self.pool = pool
        self.connection = None

    async def acquire_connection(self):
        self.connection = await self.pool.free.get()
        self.pool.used.add(self.connection)
        return self.connection

    async def __aenter__(self):
        return await self.acquire_connection()

    async def __aexit__(self, *exc):
        self.pool.used.remove(self.connection)
        self.pool.free.put_nowait(self.connection)

    def __await__(self):
        return self.acquire_connection().__await__()


class TestPool:
    def __init__(self, dsn: str, maxsize: int = 10):
        self.dsn = dsn
        self.is_master = dsn == "postgresql://test:test@master:5432/test"
        self.is_running = True
        self.is_behind_firewall = False
        self.used = set()
        self.free = asyncio.LifoQueue()
        self.connections = [TestConnection(self) for _ in range(maxsize)]
        for conn in self.connections:
            self.free.put_nowait(conn)

    @property
    def freesize(self):
        return self.free.qsize()

    def set_master(self, is_master: bool):
        self.is_master = is_master

    def behind_firewall(self, is_behind_firewall: bool):
        self.is_behind_firewall = is_behind_firewall

    def shutdown(self):
        self.is_running = False

    def startup(self):
        self.is_master = False
        self.is_running = True

    def acquire(self, **kwargs):
        return PoolAcquireContext(self)

    async def release(self, conn: TestConnection, **kwargs):
        self.used.remove(conn)
        await self.free.put(conn)

    async def close(self):
        for conn in self.connections:
            await conn.close()

    def terminate(self):
        for conn in self.connections:
            conn.terminate()


class TestPoolManager(BasePoolManager):
    def get_pool_freesize(self, pool: TestPool):
        return pool.freesize

    def acquire_from_pool(self, pool: TestPool, **kwargs):
        return pool.acquire(**kwargs)

    async def release_to_pool(
        self, connection: TestConnection, pool: TestPool, **kwargs
    ):
        await pool.release(connection, **kwargs)

    async def _is_master(self, connection: TestConnection):
        return await connection.is_master()

    async def _pool_factory(self, dsn: Dsn):
        return TestPool(str(dsn))

    async def _close(self, pool: TestPool):
        await pool.close()

    def _terminate(self, pool: TestPool):
        pool.terminate()

    def is_connection_closed(self, connection: TestConnection):
        return connection.is_closed


__all__ = ["TestPoolManager"]
