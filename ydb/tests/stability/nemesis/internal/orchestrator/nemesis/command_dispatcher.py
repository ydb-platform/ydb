"""Shared command delivery, history and telemetry for scheduler and manual requests.

Budget/recovery accounting belongs to the caller, not this delivery component.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime

from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import DispatchCommand
from ydb.tests.stability.nemesis.internal.nemesis.command_transport import AgentCommandTransport
from ydb.tests.stability.nemesis.internal.nemesis.catalog import guard_mode_for
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import GuardMode

logger = logging.getLogger(__name__)


class CommandDispatcher:
    def __init__(self, transport: AgentCommandTransport, *, metrics=None, history_limit: int = 50) -> None:
        self._transport = transport
        self._metrics = metrics
        self._history_limit = history_limit
        self._lock = threading.Lock()
        self._executions_history: list[dict] = []

    def append_executions_history(self, entry: dict) -> None:
        with self._lock:
            self._executions_history.append(entry)
            if len(self._executions_history) > self._history_limit:
                self._executions_history.pop(0)

    def recent_history(self, n: int = 15) -> list:
        with self._lock:
            return self._executions_history[-n:][::-1]

    def dispatch(self, cmd: DispatchCommand, *, track_history: bool) -> bool:
        """Report command acceptance using the existing history/metric formats."""
        metrics = self._metrics
        mode = guard_mode_for(cmd.nemesis_type)
        guard_mode_val = mode.value if mode is not None else None
        try:
            self._transport.send(cmd)

            if track_history:
                self.append_executions_history(
                    {
                        "type": cmd.nemesis_type,
                        "action": cmd.action,
                        "host": cmd.host,
                        "target": cmd.target.to_dict(),
                        "execution_id": cmd.execution_id,
                        "scenario_id": cmd.scenario_id,
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                    }
                )
            if metrics is not None:
                lease_id = cmd.lease_id or cmd.execution_id
                if cmd.action == "inject" and mode is not GuardMode.BYPASS:
                    metrics.fault_started(
                        target=cmd.target,
                        nemesis_type=cmd.nemesis_type,
                        execution_id=cmd.execution_id,
                        scenario_id=cmd.scenario_id,
                        lease_id=lease_id,
                        source="dispatch",
                        guard_mode=guard_mode_val,
                    )
                elif cmd.action == "extract":
                    metrics.fault_extract_dispatched(
                        target=cmd.target,
                        nemesis_type=cmd.nemesis_type,
                        execution_id=cmd.execution_id,
                        scenario_id=cmd.scenario_id,
                        lease_id=lease_id,
                        source="dispatch",
                    )
            return True
        except Exception as e:
            logger.error("Failed to dispatch %s to %s: %s", cmd.nemesis_type, cmd.host, e)
            if metrics is not None:
                metrics.fault_dispatch_failed(
                    target=cmd.target,
                    nemesis_type=cmd.nemesis_type,
                    action=cmd.action,
                    execution_id=cmd.execution_id,
                    scenario_id=cmd.scenario_id,
                    source="dispatch",
                    error=str(e),
                )
            return False
