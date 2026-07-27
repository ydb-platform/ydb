"""Serial-style inject: one tick kills K node/slot entities in sequence, ``stagger_sec`` apart."""

from __future__ import annotations

import random
import uuid
from typing import ClassVar

from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import DispatchCommand, dispatch
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget, TargetKind
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.nemesis_planner_base import (
    NemesisPlannerBase,
    normalize_candidates,
)

# Lower bound of tools ``schedule_between_kills`` (30, 60) for node serial nemeses.
DEFAULT_SERIAL_STAGGER_SEC = 30.0

# Upper bound on entities killed within one tick (matches the pre-ChaosTarget planner).
MAX_ENTITIES_PER_TICK = 4


class SerialStaggeredInjectPlanner(NemesisPlannerBase):
    """
    Each ``scheduled_tick``: sample ``K`` distinct node/slot entities (``K`` in
    1..:data:`MAX_ENTITIES_PER_TICK`, capped by the candidate count) and dispatch one inject per
    entity **to that entity's own owner host**, with ``payload["sleep_before"] = i * stagger_sec``
    so the agents kill their daemons one after another instead of all at once.

    The pre-ChaosTarget version fanned the *same* ``node_id`` out to K random hosts, which only
    worked while the agent resolved the target locally; only the owner agent can kill the daemon.
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

        k = min(random.randint(1, MAX_ENTITIES_PER_TICK), len(pool))
        chosen = random.sample(pool, k)
        scenario_id = str(uuid.uuid4())
        commands = [
            dispatch(
                self._nemesis_type_key,
                target,
                "inject",
                self._payload_for(target, sleep_before=float(i) * self._stagger_sec),
                scenario_id=scenario_id,
            )
            for i, target in enumerate(chosen)
        ]
        with self._lock:
            self._last_hosts = [t.host for t in chosen]
        return commands

    def _payload_for(self, target: ChaosTarget, *, sleep_before: float) -> dict:
        """Ids the agent needs to find the daemon, plus how long it waits before killing it."""
        payload: dict = {"sleep_before": sleep_before}
        if target.node_id is not None:
            payload["node_id"] = target.node_id
        if target.slot_idx is not None:
            payload["slot_idx"] = target.slot_idx
        if target.ic_port is not None:
            payload["node_ic_port"] = target.ic_port
        return payload

    def _drain_tracked_hosts(self) -> list[str]:
        out = list(self._last_hosts)
        self._last_hosts = []
        return out

    def _register_inject(self, host: str) -> None:
        self._last_hosts = [host]

    def _register_extract(self, _host: str) -> None:
        pass
