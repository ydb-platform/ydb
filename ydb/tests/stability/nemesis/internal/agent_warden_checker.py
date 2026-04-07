"""
AgentWardenChecker - Agent-side asynchronous safety checks collector.

This module provides the AgentWardenChecker class that runs on each agent
and performs SAFETY checks only (log checks, dmesg OOM - requires local access).

Uses warden definitions from agent_safety_warden_factory().
"""

import logging
import socket
import threading
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field

from ydb.tests.stability.nemesis.internal.event_loop import BackgroundEventLoop


logger = logging.getLogger(__name__)

# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class WardenCheckResult:
    """Result of a single warden check."""
    name: str
    category: str  # 'liveness' or 'safety'
    violations: List[str]
    status: str  # 'ok', 'violation', 'error'
    error_message: Optional[str] = None
    affected_hosts: List[str] = field(default_factory=list)  # For aggregated checks


@dataclass
class WardenCheckReport:
    """Complete report of all warden checks."""
    status: str  # 'idle', 'running', 'completed', 'error'
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    liveness_checks: List[WardenCheckResult] = field(default_factory=list)
    safety_checks: List[WardenCheckResult] = field(default_factory=list)
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'status': self.status,
            'started_at': self.started_at,
            'completed_at': self.completed_at,
            'liveness_checks': [
                {
                    'name': c.name,
                    'category': c.category,
                    'violations': c.violations,
                    'status': c.status,
                    'error_message': c.error_message,
                    'affected_hosts': c.affected_hosts
                }
                for c in self.liveness_checks
            ],
            'safety_checks': [
                {
                    'name': c.name,
                    'category': c.category,
                    'violations': c.violations,
                    'status': c.status,
                    'error_message': c.error_message,
                    'affected_hosts': c.affected_hosts
                }
                for c in self.safety_checks
            ],
            'error_message': self.error_message
        }


# =============================================================================
# Agent Warden Checker
# =============================================================================

class AgentWardenChecker:
    """
    Agent-side warden checker that runs SAFETY checks only.

    Safety checks require local access (logs, dmesg) and run on each agent.
    Uses warden definitions from agent_safety_warden_factory().

    Supports operations:
    - start_checks(): Begin running checks asynchronously
    - get_last_result(): Return the last check result

    Args:
        log_directory: Path to kikimr logs directory
        ssh_username: SSH username for remote commands (usually None for local)
    """

    def __init__(self, log_directory: str = "/Berkanavt/kikimr/logs/", ssh_username: str = None):
        self._last_report: WardenCheckReport = WardenCheckReport(status='idle')
        self._is_running: bool = False
        self._lock = threading.Lock()
        self._log_directory = log_directory
        self._ssh_username = ssh_username
        self._hostname = socket.gethostname()
        self._event_loop = BackgroundEventLoop()

    def is_running(self) -> bool:
        """Check if checks are currently running."""
        with self._lock:
            return self._is_running

    def get_last_result(self) -> Dict[str, Any]:
        """Return the last check result as a dictionary."""
        with self._lock:
            return self._last_report.to_dict()

    def start_checks(self) -> bool:
        """
        Start running safety checks in the background event loop.

        Returns:
            True if checks were started, False if already running
        """
        with self._lock:
            if self._is_running:
                logger.debug(f"[{self._hostname}] Safety checks already running, skipping")
                return False
            self._is_running = True
            self._last_report = WardenCheckReport(
                status='running',
                started_at=datetime.utcnow().isoformat() + 'Z'
            )

        logger.info(f"[{self._hostname}] Starting agent safety checks in background event loop")

        # Submit async checks to background event loop
        self._event_loop.submit(self._run_checks_async())
        return True

    async def _run_checks_async(self):
        """Run safety checks asynchronously in the background event loop."""
        start_time = datetime.utcnow()
        logger.info(f"[{self._hostname}] Agent safety checks execution started")

        try:
            safety_results = []

            # Run checks with progress updates
            for result in self._run_safety_checks_with_progress():
                safety_results.append(result)

                # Update report with current progress
                with self._lock:
                    self._last_report = WardenCheckReport(
                        status='running',
                        started_at=self._last_report.started_at,
                        completed_at=None,
                        liveness_checks=[],  # Agent doesn't run liveness checks
                        safety_checks=safety_results.copy()
                    )

            # Count results by status
            ok_count = sum(1 for r in safety_results if r.status == 'ok')
            violation_count = sum(1 for r in safety_results if r.status == 'violation')
            error_count = sum(1 for r in safety_results if r.status == 'error')

            with self._lock:
                self._last_report = WardenCheckReport(
                    status='completed',
                    started_at=self._last_report.started_at,
                    completed_at=datetime.utcnow().isoformat() + 'Z',
                    liveness_checks=[],  # Agent doesn't run liveness checks
                    safety_checks=safety_results
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
                    status='error',
                    started_at=self._last_report.started_at,
                    completed_at=datetime.utcnow().isoformat() + 'Z',
                    liveness_checks=[],
                    safety_checks=[],
                    error_message=str(e)
                )
                self._is_running = False

    def _run_safety_checks_with_progress(self) -> List[WardenCheckResult]:
        """Run safety checks synchronously and yield results as they complete."""
        # Import existing wardens
        from ydb.tests.library.wardens.logs import (
            kikimr_start_logs_safety_warden_factory,
            kikimr_grep_dmesg_safety_warden_factory
        )
        from ydb.tests.library.nemesis.safety_warden import UnifiedAgentVerifyFailedSafetyWarden

        # Create wardens for local host
        local_hosts = [self._hostname]

        # 1. Check kikimr.start logs for errors (GrepLogFileForMarkersSafetyWarden)
        logger.debug(f"[{self._hostname}] Running GrepLogFileForMarkersSafetyWarden")
        for result in self._run_warden_factory_sync_with_progress(
            'GrepLogFileForMarkersSafetyWarden',
            lambda: kikimr_start_logs_safety_warden_factory(
                local_hosts,
                ssh_username=self._ssh_username,
                deploy_path=self._log_directory,
                lines_after=5,
                cut=True,
                modification_days=1
            )
        ):
            yield result
        logger.debug(f"[{self._hostname}] GrepLogFileForMarkersSafetyWarden completed")

        # 2. Check dmesg for OOM (GrepDMesgForPatternsSafetyWarden)
        logger.debug(f"[{self._hostname}] Running GrepDMesgForPatternsSafetyWarden")
        for result in self._run_warden_factory_sync_with_progress(
            'GrepDMesgForPatternsSafetyWarden',
            lambda: kikimr_grep_dmesg_safety_warden_factory(
                local_hosts,
                ssh_username=self._ssh_username,
                lines_after=5
            )
        ):
            yield result
        logger.debug(f"[{self._hostname}] GrepDMesgForPatternsSafetyWarden completed")

        # 3. Check unified_agent for VERIFY failed errors
        logger.debug(f"[{self._hostname}] Running UnifiedAgentVerifyFailedSafetyWarden")
        result = self._run_single_warden_sync(
            'UnifiedAgentVerifyFailedSafetyWarden',
            lambda: UnifiedAgentVerifyFailedSafetyWarden(hours_back=24)
        )
        yield result
        logger.debug(f"[{self._hostname}] UnifiedAgentVerifyFailedSafetyWarden: status={result.status}")

    def _run_safety_checks_sync(self) -> List[WardenCheckResult]:
        """Run safety checks synchronously (legacy method for compatibility)."""
        return list(self._run_safety_checks_with_progress())

    def _run_warden_factory_sync_with_progress(
        self,
        factory_name: str,
        factory_fn: Callable
    ) -> List[WardenCheckResult]:
        """Run a warden factory and yield results as each warden completes."""
        try:
            wardens = factory_fn()
            for warden in wardens:
                warden_name = str(warden)
                try:
                    violations = warden.list_of_safety_violations()
                    status = 'violation' if violations else 'ok'
                    result = WardenCheckResult(
                        name=warden_name,
                        category='safety',
                        violations=violations if violations else [],
                        status=status
                    )
                    yield result
                    if violations:
                        logger.info(f"[{self._hostname}] {warden_name}: {len(violations)} violation(s) found")
                except Exception as e:
                    logger.error(f"[{self._hostname}] {warden_name}: error - {e}")
                    result = WardenCheckResult(
                        name=warden_name,
                        category='safety',
                        violations=[],
                        status='error',
                        error_message=str(e)
                    )
                    yield result
        except Exception as e:
            logger.error(f"[{self._hostname}] {factory_name}: factory error - {e}")
            result = WardenCheckResult(
                name=factory_name,
                category='safety',
                violations=[],
                status='error',
                error_message=str(e)
            )
            yield result

    def _run_warden_factory_sync(
        self,
        factory_name: str,
        factory_fn: Callable
    ) -> List[WardenCheckResult]:
        """Run a warden factory and return results for all wardens it creates."""
        return list(self._run_warden_factory_sync_with_progress(factory_name, factory_fn))

    def _run_single_warden_sync(
        self,
        warden_name: str,
        warden_fn: Callable
    ) -> WardenCheckResult:
        """Run a single warden and return its result."""
        try:
            warden = warden_fn()
            actual_name = str(warden)
            violations = warden.list_of_safety_violations()
            status = 'violation' if violations else 'ok'
            if violations:
                logger.info(f"[{self._hostname}] {actual_name}: {len(violations)} violation(s) found")
            return WardenCheckResult(
                name=actual_name,
                category='safety',
                violations=violations if violations else [],
                status=status
            )
        except Exception as e:
            logger.error(f"[{self._hostname}] {warden_name}: error - {e}")
            return WardenCheckResult(
                name=warden_name,
                category='safety',
                violations=[],
                status='error',
                error_message=str(e)
            )


# Global instance for the agent (safety checks only)
warden_checker = AgentWardenChecker()
