"""Planner that always runs inject/extract on the first cluster host (YAML order).

Stateful cluster nemeses keep Python state in one agent process; dispatching every
tick to the same host matches single-actor harness behavior.
"""

from __future__ import annotations

from typing import ClassVar

from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import DispatchCommand, dispatch
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.nemesis_planner_base import NemesisPlannerBase


class PinnedFirstHostPlanner(NemesisPlannerBase):
    """``scheduled_tick`` → ``hosts[0]``; disable schedule → extract to that host."""

    PAYLOAD_INJECT: ClassVar[dict] = {}
    PAYLOAD_EXTRACT: ClassVar[dict] = {}

    def __init__(self, nemesis_type_key: str) -> None:
        super().__init__()
        self._nemesis_type_key = nemesis_type_key
        self._control_host: str | None = None

    @property
    def nemesis_type(self) -> str:  # type: ignore[override]
        return self._nemesis_type_key

    def scheduled_tick(self, hosts: list[str]) -> list[DispatchCommand]:
        if not hosts:
            return []
        h = hosts[0]
        with self._lock:
            self._control_host = h
        return [dispatch(self._nemesis_type_key, h, "inject", self.PAYLOAD_INJECT)]

    def _drain_tracked_hosts(self) -> list[str]:
        h = self._control_host
        self._control_host = None
        return [h] if h else []

    def _register_inject(self, host: str) -> None:
        self._control_host = host

    def _register_extract(self, _host: str) -> None:
        # Manual extract uses the requested host from ``manual()``; pin stays for disable-schedule fanout.
        pass
