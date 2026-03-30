"""
AgentWardenChecker — runs safety checks from agent_safety_runs.build_agent_safety_runs in parallel
on the background event loop (blocking callables via run_in_executor).
"""

from __future__ import annotations

import asyncio
import logging
import socket
import threading
from datetime import datetime
from typing import Any, Dict, List

from ydb.tests.stability.nemesis.internal.agent.agent_safety_runs import AgentSafetyRun, build_agent_safety_runs
from ydb.tests.stability.nemesis.internal.warden_catalog import AgentSafetyContext
from ydb.tests.stability.nemesis.internal.event_loop import BackgroundEventLoop
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
                logger.debug("Safety checks already running, skipping")
                return False
            self._is_running = True
            self._last_report = WardenCheckReport(
                status="running",
                started_at=datetime.utcnow().isoformat() + "Z",
            )

        logger.info("Starting agent safety checks")
        self._event_loop.submit(self._run_checks_async())
        return True

    async def _run_checks_async(self):
        logger.info("Agent safety checks execution started")

        try:
            ctx = AgentSafetyContext(log_directory=self._log_directory, hostname=self._hostname)
            runs = build_agent_safety_runs(ctx)
            loop = asyncio.get_running_loop()
            n = len(runs)
            batches: List[List[WardenCheckResult] | None] = [None] * n

            async def _run_at_index(i: int, run: AgentSafetyRun) -> tuple[int, List[WardenCheckResult]]:
                batch = await loop.run_in_executor(None, run)
                return i, batch

            tasks = [_run_at_index(i, r) for i, r in enumerate(runs)]
            accumulated: List[WardenCheckResult] = []
            for finished in asyncio.as_completed(tasks):
                i, batch = await finished
                batches[i] = batch
                accumulated = []
                for j in range(n):
                    if batches[j] is not None:
                        accumulated.extend(batches[j])
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
