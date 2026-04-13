"""Fallback orchestrator planner: each scheduled tick picks one random cluster host for inject."""

from __future__ import annotations

import random

from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import DispatchCommand, dispatch
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.nemesis_planner_base import NemesisPlannerBase


class DefaultRandomHostPlanner(NemesisPlannerBase):
    """
    No cross-tick state: one random host per tick for inject.
    Disable-schedule extract is empty; manual inject/extract still dispatch to the chosen host.
    """

    PAYLOAD_INJECT: dict = {}
    PAYLOAD_EXTRACT: dict = {}

    def __init__(self, nemesis_type: str) -> None:
        super().__init__()
        self.nemesis_type = nemesis_type

    def scheduled_tick(self, hosts: list[str]) -> list[DispatchCommand]:
        if not hosts:
            return []
        target = random.choice(hosts)
        return [dispatch(self.nemesis_type, target, "inject", self.PAYLOAD_INJECT)]

    def _drain_tracked_hosts(self) -> list[str]:
        return []

    def _register_inject(self, host: str) -> None:
        pass

    def _register_extract(self, host: str) -> None:
        pass
