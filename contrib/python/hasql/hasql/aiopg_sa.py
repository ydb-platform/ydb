from typing import Sequence

import aiopg.sa
from psycopg2.extensions import parse_dsn

from hasql.aiopg import PoolManager as AioPgPoolManager
from hasql.metrics import DriverMetrics
from hasql.utils import Dsn


class PoolManager(AioPgPoolManager):
    pools: Sequence[aiopg.sa.Engine]  # type: ignore[assignment]

    async def _is_master(self, connection):
        read_only = await connection.scalar("SHOW transaction_read_only")
        return read_only == "off"

    async def _pool_factory(self, dsn: Dsn) -> aiopg.sa.Engine:
        return await aiopg.sa.create_engine(
            str(dsn),
            **self.pool_factory_kwargs,
        )

    def host(self, pool: aiopg.sa.Engine) -> str:  # type: ignore[override]
        return parse_dsn(pool.dsn).get("host", "")

    def _driver_metrics(self) -> Sequence[DriverMetrics]:
        return [
            DriverMetrics(
                max=p.maxsize,
                min=p.minsize,
                idle=p.freesize,
                used=p.size - p.freesize,
                host=parse_dsn(p.dsn).get("host", ""),
            )
            for p in self.pools
            if p
        ]


__all__ = ("PoolManager",)
