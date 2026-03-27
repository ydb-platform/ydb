"""Orchestrator-side liveness and safety checks collector."""

import asyncio
import json
import logging
import subprocess
import threading
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from ydb.tests.library.harness.kikimr_cluster import ExternalKiKiMRCluster
from ydb.tests.library.wardens.disk import AllPDisksAreInValidStateSafetyWarden
from ydb.tests.stability.nemesis.internal.config import get_master_settings
from ydb.tests.stability.nemesis.internal.event_loop import BackgroundEventLoop, asyncio_run_blocking
from ydb.tests.stability.nemesis.internal.models import WardenCheckReport, WardenCheckResult
from ydb.tests.stability.nemesis.internal.warden_catalog import (
    CHECK_ID_AGENT_UNIFIED_VERIFY,
    CHECK_ID_MASTER_PDISK,
    CHECK_ID_MASTER_VERIFY_AGGREGATED,
    MASTER_SAFETY_CHECK_IDS_ORDER,
)


logger = logging.getLogger(__name__)


def deduplicate_verify_failed(violations: List[str]) -> List[str]:
    """
    Post-process VERIFY failed violations:
    - Deduplicate by lines 2 and 3 of each stack trace (the actual error location)
    - Count occurrences of each unique error
    - Return formatted output with count and one sample stack trace per unique error

    Args:
        violations: List of full stack traces (each is a multi-line string)

    Returns:
        List of formatted violations with counts
    """
    if not violations:
        return []

    # Group by uniqueness key (lines 2-3 of stack trace)
    unique_errors: Dict[str, List[str]] = defaultdict(list)

    for violation in violations:
        lines = violation.split('\n')
        # Get lines 2 and 3 (0-indexed: 1 and 2) as uniqueness key
        if len(lines) >= 3:
            key = f"{lines[1].strip()}|{lines[2].strip()}"
        elif len(lines) >= 2:
            key = lines[1].strip()
        else:
            key = lines[0].strip() if lines else "unknown"

        unique_errors[key].append(violation)

    # Format output
    result = []
    total_count = len(violations)
    unique_count = len(unique_errors)

    result.append(f"Found {total_count} VERIFY failed error(s), {unique_count} unique type(s)")

    for key, error_list in sorted(unique_errors.items(), key=lambda x: -len(x[1])):
        count = len(error_list)
        sample = error_list[0]
        result.append(f"[{count}x] {sample}")

    return result


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
        self._last_report: WardenCheckReport = WardenCheckReport(status='idle')
        self._is_running: bool = False
        self._lock = threading.Lock()
        self._hosts = hosts or []
        self._mon_port = mon_port
        self._cluster = None
        self._event_loop = BackgroundEventLoop()
        self._fetch_agent_warden = fetch_agent_warden_result
        self._get_monitored_hosts = get_monitored_hosts

    def set_hosts(self, hosts: List[str], mon_port: int = None):
        """Set hosts to monitor."""
        self._hosts = hosts
        if mon_port is not None:
            self._mon_port = mon_port
        # Invalidate cluster cache
        self._cluster = None

    def is_running(self) -> bool:
        """Return True if checks are running."""
        with self._lock:
            return self._is_running

    def get_last_result(self) -> Dict[str, Any]:
        """Return last check result."""
        with self._lock:
            return self._last_report.to_dict()

    def start_checks(self) -> bool:
        """Start liveness checks in the background event loop."""
        with self._lock:
            if self._is_running:
                logger.debug("Orchestrator checks already running, skipping")
                return False
            self._is_running = True
            self._last_report = WardenCheckReport(
                status='running',
                started_at=datetime.utcnow().isoformat() + 'Z'
            )

        logger.info("Starting orchestrator warden checks in background event loop")

        # Submit async checks to background event loop
        self._event_loop.submit(self._run_checks_async())
        return True

    async def _run_checks_async(self):
        """Run checks asynchronously in the background event loop."""
        start_time = datetime.utcnow()
        logger.info("Orchestrator warden checks execution started")

        try:
            cluster = self._get_cluster()
            liveness_results = []
            safety_results = []

            if cluster is not None:
                # Run liveness checks with progress updates
                logger.debug("Running liveness checks...")
                for result in self._run_liveness_checks_with_progress():
                    liveness_results.append(result)
                    # Update report with current progress
                    with self._lock:
                        self._last_report = WardenCheckReport(
                            status='running',
                            started_at=self._last_report.started_at,
                            completed_at=None,
                            liveness_checks=liveness_results.copy(),
                            safety_checks=safety_results.copy()
                        )
                logger.debug(f"Liveness checks completed: {len(liveness_results)} checks")

                logger.debug(
                    "Running master safety checks (parallel asyncio tasks: %s)",
                    ", ".join(MASTER_SAFETY_CHECK_IDS_ORDER),
                )

                async def _run_one_master_safety(check_id: str):
                    if check_id == CHECK_ID_MASTER_PDISK:
                        return await asyncio_run_blocking(
                            lambda: list(self._run_pdisk_check_sync(cluster))
                        )
                    if check_id == CHECK_ID_MASTER_VERIFY_AGGREGATED:
                        return [await self._run_aggregated_verify_failed_check_async()]
                    raise RuntimeError(f"Unknown master safety check_id: {check_id}")

                groups = await asyncio.gather(
                    *(_run_one_master_safety(cid) for cid in MASTER_SAFETY_CHECK_IDS_ORDER)
                )
                for part in groups:
                    safety_results.extend(part)
                with self._lock:
                    self._last_report = WardenCheckReport(
                        status='running',
                        started_at=self._last_report.started_at,
                        completed_at=None,
                        liveness_checks=liveness_results.copy(),
                        safety_checks=safety_results.copy(),
                    )
                logger.debug(
                    f"Master safety done: pdisk={len(pdisk_results)} row(s), "
                    f"aggregated status={aggregated_result.status}"
                )

            # Count results by status
            liveness_ok = sum(1 for r in liveness_results if r.status == 'ok')
            liveness_violation = sum(1 for r in liveness_results if r.status == 'violation')
            safety_ok = sum(1 for r in safety_results if r.status == 'ok')
            safety_violation = sum(1 for r in safety_results if r.status == 'violation')

            with self._lock:
                self._last_report = WardenCheckReport(
                    status='completed',
                    started_at=self._last_report.started_at,
                    completed_at=datetime.utcnow().isoformat() + 'Z',
                    liveness_checks=liveness_results,
                    safety_checks=safety_results
                )
                self._is_running = False

            elapsed = (datetime.utcnow() - start_time).total_seconds()
            logger.info(
                f"Orchestrator warden checks completed in {elapsed:.1f}s: "
                f"liveness({liveness_ok} ok, {liveness_violation} violations), "
                f"safety({safety_ok} ok, {safety_violation} violations)"
            )

        except Exception as e:
            logger.error(f"Error running orchestrator checks: {e}")
            with self._lock:
                self._last_report = WardenCheckReport(
                    status='error',
                    started_at=self._last_report.started_at,
                    completed_at=datetime.utcnow().isoformat() + 'Z',
                    liveness_checks=[],
                    safety_checks=[],
                    error_message=str(e)
                )
                self._is_running = False

    def _get_cluster(self):
        """Create cluster object for wardens."""
        if self._cluster is None and self._hosts:
            self._cluster = ExternalKiKiMRCluster(get_master_settings().yaml_config_location, None, None)
        return self._cluster

    def _nemesis_agent_binary(self) -> str:
        root = get_master_settings().install_root.rstrip("/")
        return f"{root}/bin/agent"

    def _run_liveness_checks_sync(self, timeout_seconds: int = 60) -> List[WardenCheckResult]:
        """Run liveness checks via subprocess with timeout."""
        try:
            yaml_config = get_master_settings().yaml_config_location
        except Exception as e:
            logger.error(f"Failed to get settings: {e}")
            return [WardenCheckResult(
                name='LivenessChecks',
                category='liveness',
                violations=[],
                status='error',
                error_message=f"Failed to get settings: {e}"
            )]

        logger.info(f"Running liveness checks via subprocess with {timeout_seconds}s timeout")

        # Build command to run liveness checks
        # Use the compiled nemesis binary directly
        cmd = [
            self._nemesis_agent_binary(),
            'liveness',
            '--yaml-config-location', yaml_config
        ]

        try:
            start_time = time.time()
            logger.info(f"Executing: {' '.join(cmd)}")

            # Run subprocess with timeout
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout_seconds
            )

            elapsed = time.time() - start_time
            logger.info(f"Subprocess completed in {elapsed:.1f}s with return code {result.returncode}")

            if result.stderr:
                logger.debug(f"Subprocess stderr: {result.stderr[:500]}")

            # Parse JSON output
            if result.stdout:
                try:
                    output = json.loads(result.stdout)
                    checks = output.get('checks', [])

                    # Convert to WardenCheckResult objects
                    results = []
                    for check in checks:
                        results.append(WardenCheckResult(
                            name=check.get('name', 'Unknown'),
                            category=check.get('category', 'liveness'),
                            violations=check.get('violations', []),
                            status=check.get('status', 'error'),
                            error_message=check.get('error_message')
                        ))

                    if output.get('status') == 'error' and output.get('error_message'):
                        logger.error(f"Liveness subprocess error: {output['error_message']}")

                    return results

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse liveness output: {e}")
                    logger.error(f"Raw output: {result.stdout[:500]}")
                    return [WardenCheckResult(
                        name='LivenessChecks',
                        category='liveness',
                        violations=[],
                        status='error',
                        error_message=f"Failed to parse output: {e}"
                    )]
            else:
                logger.error("No output from liveness subprocess")
                return [WardenCheckResult(
                    name='LivenessChecks',
                    category='liveness',
                    violations=[],
                    status='error',
                    error_message="No output from subprocess"
                )]

        except subprocess.TimeoutExpired:
            logger.warning(f"Liveness checks timed out after {timeout_seconds}s - killing subprocess")
            return [WardenCheckResult(
                name='LivenessChecks',
                category='liveness',
                violations=[],
                status='error',
                error_message=f"Timeout after {timeout_seconds}s - subprocess killed"
            )]

        except Exception as e:
            logger.error(f"Failed to run liveness subprocess: {e}")
            return [WardenCheckResult(
                name='LivenessChecks',
                category='liveness',
                violations=[],
                status='error',
                error_message=str(e)
            )]

    def _run_liveness_checks_with_progress(self, timeout_seconds: int = 60) -> List[WardenCheckResult]:
        """Run liveness checks via subprocess with timeout and yield results."""
        # Since subprocess runs as a single unit, we can't show progress during execution
        # But we can yield results as soon as they're available
        for result in self._run_liveness_checks_sync(timeout_seconds):
            yield result

    def _run_pdisk_check_sync(self, cluster) -> List[WardenCheckResult]:
        """Run PDisk state check."""
        results = []

        try:
            pdisk_warden = AllPDisksAreInValidStateSafetyWarden(
                cluster,
                timeout_seconds=30
            )

            violations = pdisk_warden.list_of_safety_violations()
            status = 'violation' if violations else 'ok'
            results.append(WardenCheckResult(
                name='AllPDisksAreInValidState',
                category='safety',
                violations=violations if violations else [],
                status=status,
                check_id=CHECK_ID_MASTER_PDISK,
            ))
            if violations:
                logger.info(f"AllPDisksAreInValidState: {len(violations)} violation(s) found")
        except Exception as e:
            logger.error(f"AllPDisksAreInValidState: error - {e}")
            results.append(WardenCheckResult(
                name='AllPDisksAreInValidState',
                category='safety',
                violations=[],
                status='error',
                error_message=str(e),
                check_id=CHECK_ID_MASTER_PDISK,
            ))

        return results

    async def _run_aggregated_verify_failed_check_async(self, max_wait_seconds: int = 120, poll_interval_seconds: float = 2.0) -> WardenCheckResult:
        """Aggregate VERIFY failed errors from all agents."""
        monitored = (
            self._get_monitored_hosts()
            if self._get_monitored_hosts is not None
            else list(self._hosts)
        )

        def get_agent_status(host: str) -> Dict[str, Any]:
            """Get the warden check status from an agent."""
            if self._fetch_agent_warden is None:
                return {"status": "error", "error_message": "fetch_agent_warden_result not configured"}
            try:
                return self._fetch_agent_warden(host)
            except Exception as e:
                logger.error(f"Failed to get status from {host}: {e}")
                return {"status": "error", "error_message": str(e)}

        def is_agent_completed(result: Dict[str, Any]) -> bool:
            """Check if an agent has completed its checks."""
            status = result.get("status", "idle")
            # Consider completed if status is 'completed' or 'error'
            # 'idle' means checks haven't started, 'running' means still in progress
            return status in ("completed", "error")

        # Wait for all agents to complete their checks
        start_time = time.time()
        all_completed = False
        pending_hosts = set(monitored)

        logger.info(f"Waiting for {len(monitored)} agents to complete safety checks...")

        while not all_completed and (time.time() - start_time) < max_wait_seconds:
            still_pending = set()

            for host in pending_hosts:
                result = get_agent_status(host)
                if not is_agent_completed(result):
                    still_pending.add(host)
                else:
                    logger.debug(f"Agent {host} completed with status: {result.get('status')}")

            pending_hosts = still_pending

            if pending_hosts:
                logger.debug(f"Still waiting for {len(pending_hosts)} agents: {pending_hosts}")
                await asyncio.sleep(poll_interval_seconds)
            else:
                all_completed = True

        if pending_hosts:
            logger.warning(
                f"Timeout waiting for agents to complete. "
                f"Still pending after {max_wait_seconds}s: {pending_hosts}"
            )

        elapsed_wait = time.time() - start_time
        logger.info(f"All agents completed (or timed out) after {elapsed_wait:.1f}s")

        # Now collect VERIFY failed violations from all agents
        verify_failed_violations = []
        verify_failed_hosts = []

        def get_verify_failed_from_host(host: str):
            """Extract VERIFY failed violations from an agent's result."""
            try:
                result = get_agent_status(host)

                # Extract VERIFY failed violations
                if result.get("safety_checks"):
                    for check in result["safety_checks"]:
                        if check.get("check_id") == CHECK_ID_AGENT_UNIFIED_VERIFY:
                            if check.get("violations"):
                                return host, check["violations"]
                        elif "UnifiedAgentVerifyFailed" in check.get("name", ""):
                            if check.get("violations"):
                                return host, check["violations"]
                return host, []
            except Exception as e:
                logger.error(f"Failed to get VERIFY failed from {host}: {e}")
                return host, []

        # Get results from all hosts sequentially
        logger.debug("Collecting VERIFY failed violations from all agents...")
        for host in monitored:
            host_result, violations = get_verify_failed_from_host(host)
            if violations:
                logger.debug(f"Agent {host}: {len(violations)} VERIFY failed violation(s)")
                verify_failed_violations.extend(violations)
                verify_failed_hosts.append(host_result)

        # Apply deduplication post-processor
        aggregated_violations = []
        if verify_failed_violations:
            logger.info(f"Aggregating {len(verify_failed_violations)} VERIFY failed violations from {len(verify_failed_hosts)} hosts")
            aggregated_violations = deduplicate_verify_failed(verify_failed_violations)
            logger.info(f"After deduplication: {len(aggregated_violations)} unique violation types")
        else:
            logger.debug("No VERIFY failed violations found across all agents")

        # Add warning if some agents didn't complete in time
        error_message = None
        if pending_hosts:
            aggregated_violations.append(f"Timeout: agents {list(pending_hosts)} did not complete in {max_wait_seconds}s")
            error_message = f"Timeout: agents {list(pending_hosts)} did not complete in {max_wait_seconds}s"

        return WardenCheckResult(
            name='UnifiedAgentVerifyFailedAggregated',
            category='safety',
            violations=aggregated_violations,
            status='violation' if aggregated_violations or error_message else 'ok',
            error_message=error_message,
            affected_hosts=verify_failed_hosts,
            check_id=CHECK_ID_MASTER_VERIFY_AGGREGATED,
        )
