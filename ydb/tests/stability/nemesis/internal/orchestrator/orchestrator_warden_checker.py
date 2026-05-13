"""Orchestrator-side liveness and safety checks collector."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from datetime import datetime
from functools import partial
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from ydb.tests.library.harness.kikimr_cluster import ExternalKiKiMRCluster
from ydb.tests.stability.nemesis.internal.config import get_orchestrator_settings
from ydb.tests.stability.nemesis.internal.event_loop import BackgroundEventLoop
from ydb.tests.stability.nemesis.internal.models import WardenCheckReport, WardenCheckResult
from ydb.tests.stability.nemesis.internal.orchestrator.orchestrator_warden_execution import (
    run_orchestrator_aggregated_safety,
    run_orchestrator_liveness_subprocess_sync,
)
from ydb.tests.stability.nemesis.internal.orchestrator.orchestrator_warden_catalog import (
    ORCHESTRATOR_AGGREGATED_SAFETY_CHECKS,
    collect_orchestrator_cluster_safety_specs,
)
from ydb.tests.stability.nemesis.internal.safety_warden_execution import (
    SafetyWardenRun,
    build_safety_runs,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Agents run several heavy greps in parallel; PDisk scans the whole cluster (per-node timeout in warden).
_AGENT_SAFETY_WAIT_MAX_SECONDS = 900
_AGENT_SAFETY_POLL_INTERVAL = 2.0


class OrchestratorWardenChecker:
    """Orchestrator-side warden checker for liveness and safety checks."""

    def __init__(
        self,
        hosts: List[str] = None,
        mon_port: int = 8765,
        *,
        fetch_agent_warden_result: Optional[Callable[[str], Dict[str, Any]]] = None,
        get_monitored_hosts: Optional[Callable[[], List[str]]] = None,
    ):
        self._last_report: WardenCheckReport = WardenCheckReport(status="idle")
        self._is_running: bool = False
        self._lock = threading.Lock()
        self._hosts = hosts or []
        self._mon_port = mon_port
        self._cluster = None
        self._event_loop = BackgroundEventLoop()
        self._fetch_agent_warden = fetch_agent_warden_result
        self._get_monitored_hosts = get_monitored_hosts

    def _monitored_agent_hosts(self) -> List[str]:
        if self._get_monitored_hosts is not None:
            return list(self._get_monitored_hosts())
        return list(self._hosts)

    def _fetch_agent_report(self, host: str) -> Dict[str, Any]:
        if self._fetch_agent_warden is None:
            return {"status": "error", "error_message": "fetch_agent_warden_result not configured"}
        try:
            return self._fetch_agent_warden(host)
        except Exception as e:
            logger.error("Failed to get warden status from %s: %s", host, e)
            return {"status": "error", "error_message": str(e)}

    @staticmethod
    def _agent_safety_done(report: Dict[str, Any]) -> bool:
        return report.get("status", "idle") in ("completed", "error")

    async def _wait_for_agent_safety_completion_async(
        self,
        *,
        max_wait_seconds: int = _AGENT_SAFETY_WAIT_MAX_SECONDS,
        poll_interval_seconds: float = _AGENT_SAFETY_POLL_INTERVAL,
    ) -> Tuple[Dict[str, Dict[str, Any]], Set[str]]:
        """
        Poll agents until each has completed safety (or error), or timeout.
        Returns (last_json_per_host, hosts_still_pending_after_deadline).
        """
        monitored = self._monitored_agent_hosts()
        last_reports: Dict[str, Dict[str, Any]] = {}
        pending: Set[str] = set(monitored)

        if not monitored:
            return last_reports, pending

        start = time.time()
        logger.info("Waiting for %d agent(s) to complete safety checks...", len(monitored))

        while pending and (time.time() - start) < max_wait_seconds:
            still: Set[str] = set()
            for host in pending:
                report = self._fetch_agent_report(host)
                last_reports[host] = report
                if self._agent_safety_done(report):
                    logger.debug("Agent %s safety finished with status=%s", host, report.get("status"))
                else:
                    still.add(host)
            pending = still
            if pending:
                logger.debug("Still waiting for %d agent(s): %s", len(pending), pending)
                await asyncio.sleep(poll_interval_seconds)

        if pending:
            logger.warning(
                "Timeout waiting for agent safety after %ds; pending: %s",
                max_wait_seconds,
                pending,
            )
        else:
            logger.info("All %d agent(s) reported safety completed after %.1fs", len(monitored), time.time() - start)

        return last_reports, pending

    def set_hosts(self, hosts: List[str], mon_port: int = None):
        """Set hosts to monitor."""
        self._hosts = hosts
        if mon_port is not None:
            self._mon_port = mon_port
        self._cluster = None

    def is_running(self) -> bool:
        with self._lock:
            return self._is_running

    def get_last_result(self) -> Dict[str, Any]:
        with self._lock:
            return self._last_report.to_dict()

    def start_checks(self) -> bool:
        with self._lock:
            if self._is_running:
                logger.debug("Orchestrator checks already running, skipping")
                return False
            self._is_running = True
            self._last_report = WardenCheckReport(
                status="running",
                started_at=datetime.utcnow().isoformat() + "Z",
            )

        logger.info("Starting orchestrator warden checks in background event loop")
        self._event_loop.submit(self._run_checks_async())
        return True

    async def _run_checks_async(self):
        start_time = datetime.utcnow()
        logger.info("Orchestrator warden checks execution started")

        try:
            cluster = self._get_cluster()
            liveness_results: List[WardenCheckResult] = []
            safety_results: List[WardenCheckResult] = []

            if cluster is not None:
                loop = asyncio.get_running_loop()
                liveness_results = await loop.run_in_executor(
                    None,
                    partial(
                        run_orchestrator_liveness_subprocess_sync,
                        self._nemesis_agent_binary(),
                        get_orchestrator_settings().yaml_config_location,
                    ),
                )
                self._publish_running(liveness_results, safety_results)

                agg_specs = list(ORCHESTRATOR_AGGREGATED_SAFETY_CHECKS)

                # Use unified SafetyCheckSpec pipeline for cluster safety
                cluster_safety_specs = collect_orchestrator_cluster_safety_specs(cluster)
                _slot_names, local_runs = build_safety_runs(cluster_safety_specs, log_prefix="")

                async def _run_local_run(run: SafetyWardenRun) -> List[WardenCheckResult]:
                    return await loop.run_in_executor(None, run)

                async def _all_local() -> List[List[WardenCheckResult]]:
                    if not local_runs:
                        return []
                    return list(await asyncio.gather(*[_run_local_run(r) for r in local_runs]))

                # Do not use asyncio.gather for (local, wait): PDisk can take sum(per-node timeouts)
                # across the cluster while agent wait caps at _AGENT_SAFETY_WAIT_MAX_SECONDS. Until *both*
                # finish, gather would block and the API would show empty orchestrator safety_checks.
                wait_task = asyncio.create_task(
                    self._wait_for_agent_safety_completion_async(
                        max_wait_seconds=_AGENT_SAFETY_WAIT_MAX_SECONDS,
                        poll_interval_seconds=_AGENT_SAFETY_POLL_INTERVAL,
                    )
                )
                local_task = asyncio.create_task(_all_local())
                try:
                    local_batches = await local_task
                except Exception:
                    wait_task.cancel()
                    try:
                        await wait_task
                    except asyncio.CancelledError:
                        pass
                    raise

                safety_results: List[WardenCheckResult] = []
                for batch in local_batches:
                    safety_results.extend(batch)
                self._publish_running(liveness_results, safety_results)

                per_host_reports, pending_hosts = await wait_task

                for spec in agg_specs:
                    batch = run_orchestrator_aggregated_safety(
                        spec,
                        per_host_reports=per_host_reports,
                        pending_timeout_hosts=pending_hosts,
                        max_wait_seconds=_AGENT_SAFETY_WAIT_MAX_SECONDS,
                    )
                    safety_results.extend(batch)
                    self._publish_running(liveness_results, safety_results)

                logger.debug(
                    "Orchestrator warden checks done: liveness=%d, safety=%d row(s)",
                    len(liveness_results),
                    len(safety_results),
                )

            with self._lock:
                self._last_report = WardenCheckReport(
                    status="completed",
                    started_at=self._last_report.started_at,
                    completed_at=datetime.utcnow().isoformat() + "Z",
                    liveness_checks=liveness_results,
                    safety_checks=safety_results,
                )
                self._is_running = False

            elapsed = (datetime.utcnow() - start_time).total_seconds()
            logger.info("Orchestrator warden checks completed in %.1fs", elapsed)

        except Exception as e:
            logger.error("Error running orchestrator checks: %s", e)
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

    def _publish_running(
        self,
        liveness_results: List[WardenCheckResult],
        safety_results: List[WardenCheckResult],
    ) -> None:
        with self._lock:
            self._last_report = WardenCheckReport(
                status="running",
                started_at=self._last_report.started_at,
                completed_at=None,
                liveness_checks=list(liveness_results),
                safety_checks=list(safety_results),
            )

    def _get_cluster(self):
        if self._cluster is None and self._hosts:
            self._cluster = ExternalKiKiMRCluster(
                get_orchestrator_settings().yaml_config_location, None, None
            )
        return self._cluster

    def _nemesis_agent_binary(self) -> str:
        root = get_orchestrator_settings().install_root.rstrip("/")
        return f"{root}/bin/agent"
