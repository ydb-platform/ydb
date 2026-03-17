import aiopg

from pyscopg2.base import BasePoolManager
from pyscopg2.utils import Dsn


class PoolManager(BasePoolManager):
    def get_pool_freesize(self, pool):
        return pool.freesize

    def acquire_from_pool(self, pool, **kwargs):
        return pool.acquire(**kwargs)

    async def release_to_pool(self, connection, pool, **kwargs):
        return await pool.release(connection, **kwargs)

    async def _is_master(self, connection):
        cursor = await connection.cursor()
        try:
            await cursor.execute("SHOW transaction_read_only")
            read_only = await cursor.fetchone()
            return read_only[0] == "off"
        finally:
            cursor.close()

    async def _pool_factory(self, dsn: Dsn):
        return await aiopg.create_pool(str(dsn), **self.pool_factory_kwargs)

    def _prepare_pool_factory_kwargs(self, kwargs: dict) -> dict:
        kwargs["minsize"] = kwargs.get("minsize", 1) + 1
        kwargs["maxsize"] = kwargs.get("maxsize", 10) + 1
        return kwargs

    async def _close(self, pool):
        pool.close()
        await pool.wait_closed()

    def _terminate(self, pool):
        pool.terminate()

    def is_connection_closed(self, connection):
        return connection.closed


__all__ = ["PoolManager"]
