"""Agent execution service shared by the HTTP adapter and in-process transport."""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping

from ydb.tests.stability.nemesis.internal.agent.nemesis.runner import NemesisManager

logger = logging.getLogger(__name__)


class AgentExecutionService:
    def __init__(self, manager: NemesisManager, definitions: Mapping[str, dict]) -> None:
        self._manager = manager
        self._definitions = definitions

    def start(self, process_type: str, action: str = "inject", payload=None) -> dict:
        if process_type not in self._definitions:
            return {"status": "error", "message": "Invalid process type"}
        self._manager.start_process(
            process_type, self._definitions[process_type]["runner"], action, payload=payload,
        )
        return {"status": "started"}

    def get_all(self) -> list[dict]:
        return self._manager.get_all()

    def wait_for_processes(self, timeout: float = 20.0, poll_interval: float = 0.2) -> int:
        """Block until locally-started actions finish; return how many are still running.

        Actions run in daemon threads, which the interpreter kills on shutdown — that would drop the
        teardown extracts for targets on this host.
        """
        deadline = time.monotonic() + float(timeout)

        def _running() -> int:
            return sum(1 for row in self._manager.get_all() if row.get("status") == "running")

        pending = _running()
        if not pending:
            return 0
        logger.info("waiting up to %.0fs for %d local nemesis action(s) to finish", timeout, pending)
        while time.monotonic() < deadline:
            pending = _running()
            if not pending:
                return 0
            time.sleep(poll_interval)
        pending = _running()
        if pending:
            logger.warning("%d local nemesis action(s) still running after %.0fs", pending, timeout)
        return pending
