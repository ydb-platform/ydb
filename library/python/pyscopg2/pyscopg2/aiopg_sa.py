from pyscopg2.utils import sa_patch
sa_patch()

import aiopg.sa  # noqa: E402

from pyscopg2.aiopg import PoolManager as AioPgPoolManager  # noqa: E402
from pyscopg2.utils import Dsn  # noqa: E402


class PoolManager(AioPgPoolManager):
    async def _is_master(self, connection):
        read_only = await connection.scalar("SHOW transaction_read_only")
        return read_only == "off"

    async def _pool_factory(self, dsn: Dsn):
        return await aiopg.sa.create_engine(
            str(dsn),
            **self.pool_factory_kwargs,
        )


__all__ = ["PoolManager"]
