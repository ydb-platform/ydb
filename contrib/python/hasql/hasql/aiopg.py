import asyncio
from typing import Sequence

import aiopg

from hasql.base import BasePoolManager
from hasql.metrics import DriverMetrics
from hasql.utils import Dsn


class PoolManager(BasePoolManager):
    pools: Sequence[aiopg.Pool]

    def get_pool_freesize(self, pool):
        return pool.freesize

    def acquire_from_pool(self, pool, **kwargs):
        return pool.acquire(**kwargs)

    async def release_to_pool(self, connection, pool, **kwargs):
        return await pool.release(connection, **kwargs)

    async def _is_master(self, connection):
        cursor = await connection.cursor()
        async with cursor:
            await cursor.execute("SHOW transaction_read_only")
            read_only = await cursor.fetchone()
            return read_only[0] == "off"

    async def _pool_factory(self, dsn: Dsn):
        return await aiopg.create_pool(str(dsn), **self.pool_factory_kwargs)

    def _prepare_pool_factory_kwargs(self, kwargs: dict) -> dict:
        kwargs["minsize"] = kwargs.get("minsize", 1) + 1
        kwargs["maxsize"] = kwargs.get("maxsize", 10) + 1
        return kwargs

    async def _close(self, pool):
        pool.close()
        await pool.wait_closed()

    async def _terminate(self, pool):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, pool.terminate)

    def is_connection_closed(self, connection):
        return connection.closed

    def host(self, pool: aiopg.Pool):
        return Dsn.parse(str(pool._dsn)).netloc

    def _driver_metrics(self) -> Sequence[DriverMetrics]:
        return [
            DriverMetrics(
                max=p.maxsize or 0,
                min=p.minsize,
                idle=p.freesize,
                used=p.size - p.freesize,
                host=Dsn.parse(str(p._dsn)).netloc,
            )
            for p in self.pools
            if p
        ]


__all__ = ("PoolManager",)
