import random
from typing import Iterable, Optional

from hasql.balancer_policy.base import BaseBalancerPolicy


MACHINE_EPSILON: float = 1e-16


class RandomWeightedBalancerPolicy(BaseBalancerPolicy):
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

        choiced_index = self._weighted_choice(
            self._normalize_times(
                self._reflect_times(
                    self._get_response_times(candidates),
                ),
            ),
        )

        return candidates[choiced_index]

    def _get_response_times(self, pools: list) -> Iterable[Optional[float]]:
        for pool in pools:
            yield self._pool_manager.get_last_response_time(pool)

    @staticmethod
    def _reflect_times(
        times: Iterable[Optional[float]],
    ) -> Iterable[float]:
        list_times = [value or 0 for value in times]
        sum_time = sum(list_times)
        yield from map(lambda x: sum_time - x + MACHINE_EPSILON, list_times)

    @staticmethod
    def _normalize_times(times: Iterable[float]) -> Iterable[float]:
        list_times = list(times)
        sum_time = sum(list_times)
        yield from map(lambda x: sum_time / x, list_times)

    @staticmethod
    def _weighted_choice(probability_distribution: Iterable[float]) -> int:
        rand = random.random()
        prefix_sum: float = 0.0

        length = 0
        for i, p in enumerate(probability_distribution):
            length += 1
            prefix_sum += p
            if rand <= prefix_sum:
                return i
        return length - 1


__all__ = ["RandomWeightedBalancerPolicy"]
