"""Master-side state and planning for NetworkNemesis (agent execution is stateless)."""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import DispatchCommand, dispatch, fanout
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.nemesis_planner_base import NemesisPlannerBase

NETWORK_NEMESIS = "NetworkNemesis"

# Payloads — keep in sync with NetworkNemesis actor (catalog)
PAYLOAD_INJECT = {"op": "isolate_node"}
PAYLOAD_EXTRACT = {"op": "clear_network_isolation"}


@dataclass
class _NetworkIsolationState:
    isolated_hosts: set[str] = field(default_factory=set)
    max_affected: int = 4


class NetworkNemesisPlanner(NemesisPlannerBase):
    """Holds isolation set on the orchestrator; produces DispatchCommand lists for agents."""

    nemesis_type = NETWORK_NEMESIS
    PAYLOAD_INJECT = PAYLOAD_INJECT
    PAYLOAD_EXTRACT = PAYLOAD_EXTRACT

    def __init__(self, max_affected: int = 4) -> None:
        super().__init__()
        self._state = _NetworkIsolationState(max_affected=max_affected)

    def scheduled_tick(self, hosts: list[str]) -> list[DispatchCommand]:
        if not hosts:
            return []
        inject_target: str | None = None
        extract_targets: list[str] | None = None
        with self._lock:
            if len(self._state.isolated_hosts) >= self._state.max_affected:
                extract_targets = list(self._state.isolated_hosts)
                self._state.isolated_hosts.clear()
            else:
                avail = [h for h in hosts if h not in self._state.isolated_hosts]
                if not avail:
                    return []
                inject_target = random.choice(avail)
                self._state.isolated_hosts.add(inject_target)
        if inject_target is not None:
            return [dispatch(NETWORK_NEMESIS, inject_target, "inject", PAYLOAD_INJECT)]
        if not extract_targets:
            return []
        return fanout(NETWORK_NEMESIS, extract_targets, "extract", PAYLOAD_EXTRACT)

    def _drain_tracked_hosts(self) -> list[str]:
        targets = list(self._state.isolated_hosts)
        self._state.isolated_hosts.clear()
        return targets

    def _register_inject(self, host: str) -> None:
        self._state.isolated_hosts.add(host)

    def _register_extract(self, host: str) -> None:
        self._state.isolated_hosts.discard(host)
