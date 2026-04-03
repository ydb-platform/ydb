"""Master-side state and planning for NodeKiller (agent execution is stateless)."""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import DispatchCommand, dispatch
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.nemesis_planner_base import NemesisPlannerBase


PAYLOAD_INJECT = {"mode": "kill_one_ic_port_process", "signal": "SIGKILL"}
PAYLOAD_EXTRACT = {"mode": "noop"}


@dataclass
class _KillNodeState:
    affected_hosts: set[str] = field(default_factory=set)


class KillNodeNemesisPlanner(NemesisPlannerBase):
    """Tracks hosts targeted by scheduled injects (for extract fan-out on schedule disable)."""

    PAYLOAD_INJECT = PAYLOAD_INJECT
    PAYLOAD_EXTRACT = PAYLOAD_EXTRACT

    def __init__(self) -> None:
        super().__init__()
        self._state = _KillNodeState()
        self.nemesis_type = "KillNodeNemesis"

    def scheduled_tick(self, hosts: list[str]) -> list[DispatchCommand]:
        if not hosts:
            return []
        target = random.choice(hosts)
        with self._lock:
            self._state.affected_hosts.add(target)
        return [dispatch(self.nemesis_type, target, "inject", PAYLOAD_INJECT)]

    def _drain_tracked_hosts(self) -> list[str]:
        targets = list(self._state.affected_hosts)
        self._state.affected_hosts.clear()
        return targets

    def _register_inject(self, host: str) -> None:
        self._state.affected_hosts.add(host)

    def _register_extract(self, host: str) -> None:
        self._state.affected_hosts.discard(host)
