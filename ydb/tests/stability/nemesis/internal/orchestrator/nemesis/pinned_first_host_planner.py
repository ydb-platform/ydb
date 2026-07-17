"""Planner that always runs inject/extract on the first candidate (inventory / YAML order).

tick to the same host matches single-actor harness behavior.
"""

from __future__ import annotations

from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import DispatchCommand, dispatch
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.nemesis_planner_base import (
    NemesisPlannerBase,
    normalize_candidates,
)


class PinnedFirstHostPlanner(NemesisPlannerBase):
    """``scheduled_tick`` → ``candidates[0]``; disable schedule → extract to that host."""

    PAYLOAD_INJECT: dict = {}
    PAYLOAD_EXTRACT: dict = {}

    def __init__(self, nemesis_type: str) -> None:
        super().__init__()
        self.nemesis_type = nemesis_type
        self._control_host: str | None = None
        self._control_target: ChaosTarget | None = None

    def scheduled_tick(self, candidates: list[ChaosTarget]) -> list[DispatchCommand]:
        targets = normalize_candidates(candidates)
        if not targets:
            return []
        t = targets[0]
        with self._lock:
            self._control_host = t.host
            self._control_target = t
        return [dispatch(self.nemesis_type, t, "inject", self.PAYLOAD_INJECT)]

    def _drain_tracked_hosts(self) -> list[str]:
        h = self._control_host
        self._control_host = None
        self._control_target = None
        return [h] if h else []

    def _register_inject(self, host: str) -> None:
        self._control_host = host
        self._control_target = ChaosTarget.for_host(host)

    def _register_extract(self, _host: str) -> None:
        # Manual extract uses the requested host from ``manual()``; pin stays for disable-schedule fanout.
        pass
