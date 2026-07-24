"""Serial-style inject: one tick dispatches to the owner host of a chosen node/slot."""

from __future__ import annotations

import random
from typing import ClassVar

from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import DispatchCommand, dispatch
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget, TargetKind
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.nemesis_planner_base import (
    NemesisPlannerBase,
    normalize_candidates,
)

# Lower bound of tools ``schedule_between_kills`` (30, 60) for node serial nemeses.
DEFAULT_SERIAL_STAGGER_SEC = 30.0


class SerialStaggeredInjectPlanner(NemesisPlannerBase):
    """
    Each ``scheduled_tick``: pick one node/slot from ``candidates`` and dispatch inject
    only to that entity's owner host (with optional sleep_before for stagger compatibility).

    Previously fanned out the same node_id to K random hosts; that is incorrect for
    ChaosTarget — only the owner agent can kill the daemon.
    """

    PAYLOAD_INJECT: ClassVar[dict] = {}
    PAYLOAD_EXTRACT: ClassVar[dict] = {}

    def __init__(
        self,
        nemesis_type_key: str,
        *,
        target_kind: str,
        stagger_sec: float = DEFAULT_SERIAL_STAGGER_SEC,
    ) -> None:
        super().__init__()
        self._nemesis_type_key = nemesis_type_key
        self._target_kind = target_kind
        self._stagger_sec = stagger_sec
        self._last_hosts: list[str] = []

    @property
    def nemesis_type(self) -> str:  # type: ignore[override]
        return self._nemesis_type_key

    def scheduled_tick(self, candidates: list[ChaosTarget]) -> list[DispatchCommand]:
        targets = normalize_candidates(candidates)
        if not targets:
            return []

        if self._target_kind == "node":
            pool = [t for t in targets if t.kind is TargetKind.NODE] or targets
        elif self._target_kind == "slot":
            pool = [t for t in targets if t.kind is TargetKind.SLOT] or targets
        else:
            return []

        chosen = random.choice(pool)
        payload: dict = {"sleep_before": 0.0}
        if chosen.node_id is not None:
            payload["node_id"] = chosen.node_id
        if chosen.slot_idx is not None:
            payload["slot_idx"] = chosen.slot_idx
        if chosen.ic_port is not None:
            payload["node_ic_port"] = chosen.ic_port

        with self._lock:
            self._last_hosts = [chosen.host]
        return [dispatch(self._nemesis_type_key, chosen, "inject", payload)]

    def _drain_tracked_hosts(self) -> list[str]:
        out = list(self._last_hosts)
        self._last_hosts = []
        return out

    def _register_inject(self, host: str) -> None:
        self._last_hosts = [host]

    def _register_extract(self, _host: str) -> None:
        pass
