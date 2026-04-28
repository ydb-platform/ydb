"""Serial-style inject: one tick fans out to several agents with increasing ``sleep_before`` in payload."""

from __future__ import annotations

import random
import uuid
from typing import ClassVar

from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import DispatchCommand, dispatch
from ydb.tests.stability.nemesis.internal.nemesis.cluster_context import require_external_cluster
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.nemesis_planner_base import NemesisPlannerBase

# Lower bound of tools ``schedule_between_kills`` (30, 60) for node serial nemeses.
DEFAULT_SERIAL_STAGGER_SEC = 30.0


class SerialStaggeredInjectPlanner(NemesisPlannerBase):
    """
    Each ``scheduled_tick``: sample ``K`` distinct hosts (``K`` in 1..4, capped by ``len(hosts)``),
    dispatch inject to each with ``payload["sleep_before"] = i * stagger_sec`` and the same
    ``node_id`` / ``slot_idx`` so every agent kills the same daemon (staggered in time).
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

    def scheduled_tick(self, hosts: list[str]) -> list[DispatchCommand]:
        if not hosts:
            return []
        cluster = require_external_cluster()
        if cluster is None:
            return []
        if self._target_kind == "node":
            ids = list(cluster.nodes.keys())
            if not ids:
                return []
            target_key, target_val = "node_id", random.choice(ids)
        elif self._target_kind == "slot":
            ids = list(cluster.slots.keys())
            if not ids:
                return []
            target_key, target_val = "slot_idx", random.choice(ids)
        else:
            return []

        k = min(random.randint(1, 4), len(hosts))
        chosen = random.sample(hosts, k)
        with self._lock:
            self._last_hosts = list(chosen)
        scenario_id = str(uuid.uuid4())
        return [
            dispatch(
                self._nemesis_type_key,
                h,
                "inject",
                {
                    "sleep_before": float(i) * self._stagger_sec,
                    target_key: target_val,
                },
                scenario_id=scenario_id,
            )
            for i, h in enumerate(chosen)
        ]

    def _drain_tracked_hosts(self) -> list[str]:
        out = list(self._last_hosts)
        self._last_hosts = []
        return out

    def _register_inject(self, host: str) -> None:
        self._last_hosts = [host]

    def _register_extract(self, _host: str) -> None:
        pass
