"""Fallback orchestrator planner: each scheduled tick picks one random candidate for inject."""

from __future__ import annotations

import random

from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import DispatchCommand, dispatch
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.nemesis_planner_base import (
    NemesisPlannerBase,
    normalize_candidates,
)


class DefaultRandomHostPlanner(NemesisPlannerBase):
    """
    No cross-tick state: one random candidate per tick for inject.
    Disable-schedule extract is empty; manual inject/extract still dispatch to the chosen host.
    """

    PAYLOAD_INJECT: dict = {}
    PAYLOAD_EXTRACT: dict = {}

    def __init__(self, nemesis_type: str) -> None:
        super().__init__()
        self.nemesis_type = nemesis_type

    def scheduled_tick(self, candidates: list[ChaosTarget]) -> list[DispatchCommand]:
        targets = normalize_candidates(candidates)
        if not targets:
            return []
        target = random.choice(targets)
        return [dispatch(self.nemesis_type, target, "inject", self.PAYLOAD_INJECT)]

    def _drain_tracked_hosts(self) -> list[str]:
        return []

    def _register_inject(self, host: str) -> None:
        pass

    def _register_extract(self, host: str) -> None:
        pass
