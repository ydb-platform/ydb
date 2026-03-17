import random

from hasql.balancer_policy.base import BaseBalancerPolicy


class GreedyBalancerPolicy(BaseBalancerPolicy):
    async def _get_pool(
        self,
        read_only: bool,
        fallback_master: bool = False,
        choose_master_as_replica: bool = False,
    ):
        candidates = []

        if read_only:
            candidates.extend(
                await self._pool_manager.get_replica_pools(
                    fallback_master=fallback_master,
                ),
            )

        if (
                not read_only or
                (
                    choose_master_as_replica and
                    self._pool_manager.master_pool_count > 0
                )
        ):
            candidates.extend(await self._pool_manager.get_master_pools())

        fat_pool = max(candidates, key=self._pool_manager.get_pool_freesize)
        max_freesize = self._pool_manager.get_pool_freesize(fat_pool)
        return random.choice([
            candidate
            for candidate in candidates
            if self._pool_manager.get_pool_freesize(candidate) == max_freesize
        ])


__all__ = ("GreedyBalancerPolicy",)
