import asyncio
from typing import ClassVar, Dict, Sequence

import asyncpg  # type: ignore
from packaging.version import parse as parse_version

from hasql.base import BasePoolManager
from hasql.metrics import DriverMetrics
from hasql.utils import Dsn


class PoolManager(BasePoolManager):
    pools: Sequence[asyncpg.Pool]
    cached_hosts: ClassVar[Dict[int, str]] = {}

    def get_pool_freesize(self, pool):
        return pool._queue.qsize()

    def acquire_from_pool(self, pool, **kwargs):
        return pool.acquire(**kwargs)

    async def release_to_pool(self, connection, pool, **kwargs):
        await pool.release(connection, **kwargs)

    async def _is_master(self, connection):
        read_only = await connection.fetchrow("SHOW transaction_read_only")
        return read_only[0] == "off"

    async def _pool_factory(self, dsn: Dsn):
        return await asyncpg.create_pool(str(dsn), **self.pool_factory_kwargs)

    def _prepare_pool_factory_kwargs(self, kwargs: dict) -> dict:
        kwargs["min_size"] = kwargs.get("min_size", 1) + 1
        kwargs["max_size"] = kwargs.get("max_size", 10) + 1
        return kwargs

    async def _close(self, pool):
        await pool.close()

    async def _terminate(self, pool):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, pool.terminate)

    def is_connection_closed(self, connection):
        return connection.is_closed()

    if parse_version(asyncpg.__version__) >= parse_version("0.29.0"):
        # We try to reproduce the same behaviour of the _working_addr
        # attribute prior the 0.29.0 version for getting the host
        # linked to a pool.
        def host(self, pool: asyncpg.Pool):
            conn = next(
                (holder._con for holder in pool._holders if holder._con),
                None
            )
            if conn is not None:
                addr, _ = conn._addr
                PoolManager.cached_hosts[id(pool)] = addr
            return PoolManager.cached_hosts[id(pool)]
    else:
        def host(self, pool: asyncpg.Pool):
            addr, _ = pool._working_addr
            return addr

    def _driver_metrics(self) -> Sequence[DriverMetrics]:
        return [
            DriverMetrics(
                max=p._maxsize,
                min=p._minsize,
                idle=self.get_pool_freesize(p),
                used=p._maxsize - self.get_pool_freesize(p),
                host=self.host(p),
            )
            for p in self.pools
            if p
        ]


__all__ = ("PoolManager",)
