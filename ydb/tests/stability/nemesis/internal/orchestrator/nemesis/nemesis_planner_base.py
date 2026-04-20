"""Shared orchestrator-side planner logic: lock, manual inject/extract bookkeeping, drain + fanout."""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from typing import ClassVar

from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import DispatchCommand, dispatch, fanout


class NemesisPlannerBase(ABC):
    """
    Subclasses hold nemesis-specific state and implement scheduled_tick.
    Tracking sets (who is affected) follow the same manual / disable patterns.
    """

    nemesis_type: ClassVar[str]
    PAYLOAD_INJECT: ClassVar[dict]
    PAYLOAD_EXTRACT: ClassVar[dict]

    def __init__(self) -> None:
        self._lock = threading.Lock()

    @abstractmethod
    def scheduled_tick(self, hosts: list[str]) -> list[DispatchCommand]:
        """Next step for scheduled chaos (subclass-specific)."""

    def extract_all_on_disable(self) -> list[DispatchCommand]:
        with self._lock:
            targets = self._drain_tracked_hosts()
        return (
            fanout(self.nemesis_type, targets, "extract", self.PAYLOAD_EXTRACT)
            if targets
            else []
        )

    @abstractmethod
    def _drain_tracked_hosts(self) -> list[str]:
        """Called under lock: return all tracked hosts and clear tracking."""

    def manual(self, host: str, action: str) -> list[DispatchCommand] | None:
        if action == "inject":
            with self._lock:
                self._register_inject(host)
            return [dispatch(self.nemesis_type, host, "inject", self.PAYLOAD_INJECT)]
        if action == "extract":
            with self._lock:
                self._register_extract(host)
            return [dispatch(self.nemesis_type, host, "extract", self.PAYLOAD_EXTRACT)]
        return None

    @abstractmethod
    def _register_inject(self, host: str) -> None:
        """Called under lock when UI schedules inject on host."""

    @abstractmethod
    def _register_extract(self, host: str) -> None:
        """Called under lock when UI schedules extract on host."""
