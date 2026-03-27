"""
AgentWardenChecker — safety checks on each agent host.

Each safety warden runs in its own asyncio step: await asyncio_run_blocking(...)
so the event loop yields between checks. Runs sequentially on the default executor
to avoid parallel log/IO on the same host (which can look like a hang).
"""

from __future__ import annotations

import logging
import socket
import threading
from datetime import datetime
from typing import Any, Callable, Dict, List, Tuple

from ydb.tests.library.nemesis.safety_warden import UnifiedAgentVerifyFailedSafetyWarden
from ydb.tests.library.wardens.logs import (
    kikimr_grep_dmesg_safety_warden_factory,
    kikimr_start_logs_safety_warden_factory,
)
from ydb.tests.stability.nemesis.internal.event_loop import BackgroundEventLoop, asyncio_run_blocking
from ydb.tests.stability.nemesis.internal.models import WardenCheckReport, WardenCheckResult
from ydb.tests.stability.nemesis.internal.warden_catalog import (
    CHECK_ID_AGENT_DMESG,
    CHECK_ID_AGENT_KIKIMR_START_PLAIN,
    CHECK_ID_AGENT_UNIFIED_VERIFY,
    check_id_for_agent_warden_instance,
)


logger = logging.getLogger(__name__)


class AgentWardenChecker:
    """
    Agent-side warden checker: SAFETY only.

    Args:
        log_directory: Path to kikimr logs directory (see AgentSettings.kikimr_logs_directory).
    """

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
                logger.debug(f"[{self._hostname}] Safety checks already running, skipping")
                return False
            self._is_running = True
            self._last_report = WardenCheckReport(
                status="running",
                started_at=datetime.utcnow().isoformat() + "Z",
            )

        logger.info(f"[{self._hostname}] Starting agent safety checks (one asyncio step per check)")
        self._event_loop.submit(self._run_checks_async())
        return True

    def _execute_one_warden(self, warden, check_id: str) -> WardenCheckResult:
        warden_name = str(warden)
        try:
            violations = warden.list_of_safety_violations()
            status = "violation" if violations else "ok"
            if violations:
                logger.info(f"[{self._hostname}] {warden_name}: {len(violations)} violation(s) found")
            return WardenCheckResult(
                name=warden_name,
                category="safety",
                violations=violations if violations else [],
                status=status,
                check_id=check_id,
            )
        except Exception as e:
            logger.error(f"[{self._hostname}] {warden_name}: error - {e}")
            return WardenCheckResult(
                name=warden_name,
                category="safety",
                violations=[],
                status="error",
                error_message=str(e),
                check_id=check_id,
            )

    def _execute_unified_verify(self) -> WardenCheckResult:
        warden_name = "UnifiedAgentVerifyFailedSafetyWarden"
        try:
            warden = UnifiedAgentVerifyFailedSafetyWarden(hours_back=24)
            actual_name = str(warden)
            violations = warden.list_of_safety_violations()
            status = "violation" if violations else "ok"
            if violations:
                logger.info(f"[{self._hostname}] {actual_name}: {len(violations)} violation(s) found")
            return WardenCheckResult(
                name=actual_name,
                category="safety",
                violations=violations if violations else [],
                status=status,
                check_id=CHECK_ID_AGENT_UNIFIED_VERIFY,
            )
        except Exception as e:
            logger.error(f"[{self._hostname}] {warden_name}: error - {e}")
            return WardenCheckResult(
                name=warden_name,
                category="safety",
                violations=[],
                status="error",
                error_message=str(e),
                check_id=CHECK_ID_AGENT_UNIFIED_VERIFY,
            )

    def _collect_agent_safety_jobs(
        self,
    ) -> List[Tuple[str, Callable[[], WardenCheckResult]]]:
        """Ordered (check_id, sync callable) — one job per warden / logical check."""
        local_hosts = [self._hostname]
        jobs: List[Tuple[str, Callable[[], WardenCheckResult]]] = []

        try:
            wardens = kikimr_start_logs_safety_warden_factory(
                local_hosts,
                ssh_username=None,
                deploy_path=self._log_directory,
                lines_after=5,
                cut=True,
                modification_days=1,
            )
        except Exception as e:
            logger.error(f"[{self._hostname}] kikimr_start_logs factory error - {e}")

            def factory_err() -> WardenCheckResult:
                return WardenCheckResult(
                    name="kikimr_start_logs_safety_warden_factory",
                    category="safety",
                    violations=[],
                    status="error",
                    error_message=str(e),
                    check_id=CHECK_ID_AGENT_KIKIMR_START_PLAIN,
                )

            jobs.append((CHECK_ID_AGENT_KIKIMR_START_PLAIN, factory_err))
        else:
            for w in wardens:
                cid = check_id_for_agent_warden_instance(w)

                def make_job(warden=w, check_id=cid):
                    return self._execute_one_warden(warden, check_id)

                jobs.append((cid, make_job))

        try:
            dmesg_wardens = kikimr_grep_dmesg_safety_warden_factory(
                local_hosts,
                ssh_username=None,
                lines_after=5,
            )
        except Exception as e:
            logger.error(f"[{self._hostname}] dmesg factory error - {e}")

            def dmesg_factory_err() -> WardenCheckResult:
                return WardenCheckResult(
                    name="kikimr_grep_dmesg_safety_warden_factory",
                    category="safety",
                    violations=[],
                    status="error",
                    error_message=str(e),
                    check_id=CHECK_ID_AGENT_DMESG,
                )

            jobs.append((CHECK_ID_AGENT_DMESG, dmesg_factory_err))
        else:
            for w in dmesg_wardens:
                cid = check_id_for_agent_warden_instance(w)

                def make_dmesg_job(warden=w, check_id=cid):
                    return self._execute_one_warden(warden, check_id)

                jobs.append((cid, make_dmesg_job))

        jobs.append((CHECK_ID_AGENT_UNIFIED_VERIFY, self._execute_unified_verify))
        return jobs

    async def _run_checks_async(self):
        start_time = datetime.utcnow()
        logger.info(f"[{self._hostname}] Agent safety checks execution started")

        try:
            jobs = self._collect_agent_safety_jobs()
            n = len(jobs)
            slots: List[WardenCheckResult | None] = [None] * n

            for i, (_, fn) in enumerate(jobs):
                res = await asyncio_run_blocking(fn)
                slots[i] = res
                with self._lock:
                    started = self._last_report.started_at
                    partial = [slots[j] for j in range(n) if slots[j] is not None]
                    self._last_report = WardenCheckReport(
                        status="running",
                        started_at=started,
                        completed_at=None,
                        liveness_checks=[],
                        safety_checks=partial,
                    )

            final = [slots[i] for i in range(n)]
            ok_count = sum(1 for r in final if r.status == "ok")
            violation_count = sum(1 for r in final if r.status == "violation")
            error_count = sum(1 for r in final if r.status == "error")

            with self._lock:
                self._last_report = WardenCheckReport(
                    status="completed",
                    started_at=self._last_report.started_at,
                    completed_at=datetime.utcnow().isoformat() + "Z",
                    liveness_checks=[],
                    safety_checks=final,
                )
                self._is_running = False

            elapsed = (datetime.utcnow() - start_time).total_seconds()
            logger.info(
                f"[{self._hostname}] Agent safety checks completed in {elapsed:.1f}s: "
                f"{ok_count} ok, {violation_count} violations, {error_count} errors"
            )

        except Exception as e:
            logger.error(f"[{self._hostname}] Error running safety checks: {e}")
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
