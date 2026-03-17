from typing import Optional, Sequence

from psycopg import AsyncConnection, errors
from psycopg.conninfo import conninfo_to_dict
from psycopg_pool import AsyncConnectionPool

from .base import BasePoolManager
from .metrics import DriverMetrics
from .utils import Dsn


class PoolAcquireContext:
    __slots__ = ("timeout", "connection", "done", "pool")

    def __init__(
        self,
        pool: AsyncConnectionPool,
        timeout: Optional[float] = None,
    ):
        self.pool = pool
        self.timeout = timeout
        self.connection = None
        self.done = False

    async def __aenter__(self):
        if self.connection is not None or self.done:
            raise errors.InterfaceError("a connection is already acquired")
        self.connection = await self.pool.getconn(self.timeout)
        return self.connection

    async def __aexit__(self, *exc):
        self.done = True
        con = self.connection
        self.connection = None
        await self.pool.putconn(con)

    def __await__(self):
        return self.pool.getconn(self.timeout).__await__()


class PoolManager(BasePoolManager):
    pools: Sequence[AsyncConnectionPool]

    def __init__(self, dsn: str, **kwargs):
        pool_factory_kwargs = kwargs.pop("pool_factory_kwargs", {})
        pool_factory_kwargs["max_waiting"] = -1
        super().__init__(
            dsn,
            pool_factory_kwargs=pool_factory_kwargs, **kwargs
        )

    def get_pool_freesize(self, pool: AsyncConnectionPool):
        return pool.get_stats()["pool_available"]

    def acquire_from_pool(self, pool: AsyncConnectionPool, **kwargs):
        return PoolAcquireContext(pool, **kwargs)

    async def release_to_pool(
        self,
        connection: AsyncConnection,
        pool: AsyncConnectionPool,
        **kwargs
    ):
        return await pool.putconn(connection)

    async def _is_master(self, connection: AsyncConnection):
        async with connection.cursor() as cur:
            await cur.execute("SHOW transaction_read_only")
            return (await cur.fetchone())[0] == "off"       # type: ignore

    async def _pool_factory(self, dsn: Dsn) -> AsyncConnectionPool:
        pool = AsyncConnectionPool(
            str(dsn), **self.pool_factory_kwargs
        )
        await pool.wait()
        return pool

    def _prepare_pool_factory_kwargs(self, kwargs: dict) -> dict:
        kwargs["min_size"] = kwargs.get("min_size", 1) + 1
        kwargs["max_size"] = kwargs.get("max_size", 10) + 1
        return kwargs

    async def _close(self, pool: AsyncConnectionPool):
        await pool.close()

    async def _terminate(self, pool: AsyncConnectionPool):
        pass

    def is_connection_closed(self, connection):
        return connection.closed

    def host(self, pool: AsyncConnectionPool):
        return conninfo_to_dict(pool.conninfo)["host"]

    def _driver_metrics(self) -> Sequence[DriverMetrics]:
        stats = [
            {
                **p.get_stats(),
                "host": self.host(p)
            }
            for p in self.pools
            if p
        ]
        return [
            DriverMetrics(
                min=stat["pool_min"],
                max=stat["pool_max"],
                idle=stat["pool_available"],
                used=stat["pool_size"],
                host=stat["host"],
            ) for stat in stats
        ]


__all__ = ("PoolManager",)
