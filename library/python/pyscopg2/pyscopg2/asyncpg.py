import asyncpg

from pyscopg2.base import BasePoolManager
from pyscopg2.utils import Dsn


class PoolManager(BasePoolManager):
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

    def _terminate(self, pool):
        pool.terminate()

    def is_connection_closed(self, connection):
        return connection.is_closed()


__all__ = ["PoolManager"]
