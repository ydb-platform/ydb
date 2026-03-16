import asyncpgsa  # type: ignore
from hasql.asyncpg import PoolManager as AsyncPgPoolManager
from hasql.utils import Dsn


class PoolManager(AsyncPgPoolManager):
    async def _pool_factory(self, dsn: Dsn):
        return await asyncpgsa.create_pool(
            str(dsn), **self.pool_factory_kwargs,
        )


__all__ = ("PoolManager",)
