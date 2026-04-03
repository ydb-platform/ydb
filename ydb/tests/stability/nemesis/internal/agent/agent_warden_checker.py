"""
AgentWardenChecker — runs safety checks in parallel on the background event loop
(blocking callables via run_in_executor). Each spec is invoked once per run in
collect_agent_safety_check_specs.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import threading
from datetime import datetime
from typing import Any, Dict, List

from ydb.tests.stability.nemesis.internal.agent.agent_warden_catalog import (
    AgentSafetyContext,
    collect_agent_safety_check_specs,
)
from ydb.tests.stability.nemesis.internal.event_loop import BackgroundEventLoop
from ydb.tests.stability.nemesis.internal.models import WardenCheckReport, WardenCheckResult
from ydb.tests.stability.nemesis.internal.safety_warden_execution import (
    SafetyWardenRun,
    build_safety_runs,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def _safety_checks_snapshot(
    batches: List[List[WardenCheckResult] | None],
    slot_names: List[str],
) -> List[WardenCheckResult]:
    """
    One API row per catalog slot, stable order. Finished slots use real warden rows;
    unfinished use catalog name + status pending so polls never look like overwrites.
    """
    n = len(slot_names)
    out: List[WardenCheckResult] = []
    for j in range(n):
        b = batches[j]
        if b is not None:
            out.extend(b)
        else:
            out.append(
                WardenCheckResult(
                    name=slot_names[j],
                    category="safety",
                    violations=[],
                    status="pending",
                )
            )
    return out


class AgentWardenChecker:
    """Agent-side safety checks only."""

    def __init__(self, log_directory: str = "/Berkanavt/kikimr/logs/"):
        self._last_report: WardenCheckReport = WardenCheckReport(status="idle")
        self._is_running: bool = False
        self._lock = threading.Lock()
        self._log_directory = log_directory
        self._hostname = socket.gethostname()
        self._event_loop = BackgroundEventLoop()

    def is_running(self) -> bool:
        with self._lock:
            return self._is_running

    def get_last_result(self) -> Dict[str, Any]:
        with self._lock:
            return self._last_report.to_dict()

    def start_checks(self) -> bool:
        with self._lock:
            if self._is_running:
                logger.debug("Safety checks already running, skipping")
                return False
            self._is_running = True
            started = datetime.utcnow().isoformat() + "Z"
            ctx = AgentSafetyContext(log_directory=self._log_directory, hostname=self._hostname)
            try:
                specs = collect_agent_safety_check_specs(ctx)
                slot_names, runs = build_safety_runs(specs, log_prefix=ctx.log_prefix)
            except Exception as e:
                logger.error("Failed to collect agent safety wardens: %s", e)
                self._is_running = False
                self._last_report = WardenCheckReport(
                    status="error",
                    started_at=started,
                    completed_at=datetime.utcnow().isoformat() + "Z",
                    liveness_checks=[],
                    safety_checks=[],
                    error_message=str(e),
                )
                return True

            initial_safety = [
                WardenCheckResult(
                    name=name,
                    category="safety",
                    violations=[],
                    status="pending",
                )
                for name in slot_names
            ]
            self._last_report = WardenCheckReport(
                status="running",
                started_at=started,
                safety_checks=initial_safety,
            )

        logger.info("Starting agent safety checks")
        self._event_loop.submit(self._run_checks_async(runs, ctx.log_prefix, slot_names))
        return True

    async def _run_checks_async(
        self,
        runs: List[SafetyWardenRun],
        log_prefix: str,
        slot_names: List[str],
    ):
        logger.info("Agent safety checks execution started")

        try:
            loop = asyncio.get_running_loop()
            n = len(runs)
            if n != len(slot_names):
                raise RuntimeError("slot_names length does not match runs")
            batches: List[List[WardenCheckResult] | None] = [None] * n

            async def _run_at_index(i: int, run: SafetyWardenRun) -> tuple[int, List[WardenCheckResult]]:
                batch = await loop.run_in_executor(None, run)
                return i, batch

            tasks = [_run_at_index(i, r) for i, r in enumerate(runs)]
            accumulated: List[WardenCheckResult] = []
            for finished in asyncio.as_completed(tasks):
                i, batch = await finished
                batches[i] = batch
                logger.debug("Agent safety slot %d/%d finished (%d row(s))", i + 1, n, len(batch))
                accumulated = _safety_checks_snapshot(batches, slot_names)
                with self._lock:
                    self._last_report = WardenCheckReport(
                        status="running",
                        started_at=self._last_report.started_at,
                        completed_at=None,
                        liveness_checks=[],
                        safety_checks=list(accumulated),
                    )

            with self._lock:
                self._last_report = WardenCheckReport(
                    status="completed",
                    started_at=self._last_report.started_at,
                    completed_at=datetime.utcnow().isoformat() + "Z",
                    liveness_checks=[],
                    safety_checks=list(accumulated),
                )
                self._is_running = False

            logger.info("Agent safety checks completed")

        except Exception as e:
            logger.error("Error running safety checks: %s", e)
            with self._lock:
                self._last_report = WardenCheckReport(
                    status="error",
                    started_at=self._last_report.started_at,
                    completed_at=datetime.utcnow().isoformat() + "Z",
                    liveness_checks=[],
                    safety_checks=[],
                    error_message=str(e),
                )
                self._is_running = False
