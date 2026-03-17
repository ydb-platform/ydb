from collections import defaultdict
from types import MappingProxyType
from typing import NamedTuple, Optional

from hasql.balancer_policy.base import BaseBalancerPolicy


class PoolOptions(NamedTuple):
    read_only: bool
    choose_master_as_replica: bool


class RoundRobinBalancerPolicy(BaseBalancerPolicy):
    def __init__(self, pool_manager):
        super().__init__(pool_manager)
        self._indexes = defaultdict(lambda: 0)
        self._choose_predicates = MappingProxyType({
            PoolOptions(True, False): self._replica_predicate,
            PoolOptions(True, True): self._master_as_replica_predicate,
            PoolOptions(False, False): self._master_predicate,
        })

    async def _get_pool(
            self,
            read_only: bool,
            fallback_master: Optional[bool] = None,
            choose_master_as_replica: bool = False,
    ):
        if read_only:
            if self._pool_manager.replica_pool_count == 0:
                if fallback_master:
                    read_only = False
                    choose_master_as_replica = False
                    if self._pool_manager.master_pool_count == 0:
                        await self._pool_manager.wait_masters_ready(1)
                else:
                    await self._pool_manager.wait_replicas_ready(1)
        else:
            if self._pool_manager.master_pool_count == 0:
                await self._pool_manager.wait_masters_ready(1)

        pool_options = PoolOptions(read_only, choose_master_as_replica)
        assert pool_options in self._choose_predicates

        predicate = self._choose_predicates[pool_options]
        start_index = self._indexes[pool_options]

        pools = self._pool_manager.pools
        for offset in range(len(pools)):
            index = (start_index + offset) % len(pools)
            current_pool = pools[index]
            if current_pool is not None and predicate(current_pool):
                self._indexes[pool_options] = (index + 1) % len(pools)
                return current_pool

    def _master_predicate(self, pool) -> bool:
        return self._pool_manager.pool_is_master(pool)

    def _replica_predicate(self, pool) -> bool:
        return self._pool_manager.pool_is_replica(pool)

    def _master_as_replica_predicate(self, pool) -> bool:
        return self._master_predicate(pool) or self._replica_predicate(pool)


__all__ = ("RoundRobinBalancerPolicy",)
