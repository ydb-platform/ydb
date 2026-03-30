"""
AgentWardenChecker — runs an ordered list of safety runnables (agent_safety_runs.build_agent_safety_runs).
Each run is sync Callable[[], list[WardenCheckResult]]; executed via asyncio_run_blocking one step at a time.
"""

from __future__ import annotations

import logging
import socket
import threading
from datetime import datetime
from typing import Any, Dict

from ydb.tests.stability.nemesis.internal.agent.agent_safety_runs import build_agent_safety_runs
from ydb.tests.stability.nemesis.internal.warden_catalog import AgentSafetyContext
from ydb.tests.stability.nemesis.internal.event_loop import BackgroundEventLoop, asyncio_run_blocking
from ydb.tests.stability.nemesis.internal.models import WardenCheckReport, WardenCheckResult

logger = logging.getLogger(__name__)


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
                logger.debug("[%s] Safety checks already running, skipping", self._hostname)
                return False
            self._is_running = True
            self._last_report = WardenCheckReport(
                status="running",
                started_at=datetime.utcnow().isoformat() + "Z",
            )

        logger.info("[%s] Starting agent safety checks", self._hostname)
        self._event_loop.submit(self._run_checks_async())
        return True

    async def _run_checks_async(self):
        start_time = datetime.utcnow()
        hostname = self._hostname
        logger.info("[%s] Agent safety checks execution started", hostname)

        try:
            ctx = AgentSafetyContext(log_directory=self._log_directory, hostname=hostname)
            runs = build_agent_safety_runs(ctx)
            accumulated: list[WardenCheckResult] = []

            for run in runs:
                batch = await asyncio_run_blocking(run)
                accumulated.extend(batch)
                with self._lock:
                    self._last_report = WardenCheckReport(
                        status="running",
                        started_at=self._last_report.started_at,
                        completed_at=None,
                        liveness_checks=[],
                        safety_checks=list(accumulated),
                    )

            ok_count = sum(1 for r in accumulated if r.status == "ok")
            violation_count = sum(1 for r in accumulated if r.status == "violation")
            error_count = sum(1 for r in accumulated if r.status == "error")

            with self._lock:
                self._last_report = WardenCheckReport(
                    status="completed",
                    started_at=self._last_report.started_at,
                    completed_at=datetime.utcnow().isoformat() + "Z",
                    liveness_checks=[],
                    safety_checks=accumulated,
                )
                self._is_running = False

            elapsed = (datetime.utcnow() - start_time).total_seconds()
            logger.info(
                "[%s] Agent safety checks completed in %.1fs: %d ok, %d violations, %d errors",
                hostname,
                elapsed,
                ok_count,
                violation_count,
                error_count,
            )

        except Exception as e:
            logger.error("[%s] Error running safety checks: %s", hostname, e)
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
