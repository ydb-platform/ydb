import random
from typing import List, Optional

from pyscopg2.balancer_policy.base import BaseBalancerPolicy


MACHINE_EPSILON: float = 1e-16


class RandomWeightedBalancerPolicy(BaseBalancerPolicy):
    async def _get_pool(
            self,
            read_only: bool,
            fallback_master: Optional[bool] = None,
            choose_master_as_replica: bool = False
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

        response_times = self._get_response_times(candidates)
        self._reflect_times(response_times)
        self._normalize_times(response_times)
        choiced_index = self._weighted_choice(response_times)
        return candidates[choiced_index]

    def _get_response_times(self, pools: list) -> List[float]:
        return [
            self._pool_manager.get_last_response_time(pool)
            for pool in pools
        ]

    def _reflect_times(self, times: List[float]):
        sum_time = sum(times)
        for i in range(len(times)):
            times[i] = sum_time - times[i] + MACHINE_EPSILON

    def _normalize_times(self, times: List[float]):
        sum_time = sum(times)
        for i in range(len(times)):
            times[i] /= sum_time

    def _weighted_choice(self, probability_distribution: List[float]) -> int:
        rand = random.random()
        prefix_sum = 0
        for i, p in enumerate(probability_distribution):
            prefix_sum += p
            if rand <= prefix_sum:
                return i
        return len(probability_distribution) - 1


__all__ = ["RandomWeightedBalancerPolicy"]
