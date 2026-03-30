"""Subprocess liveness + thin wrappers around orchestrator_warden_catalog specs."""

from __future__ import annotations

import json
import logging
import subprocess
import time
from typing import Any, Callable, Dict, List, Optional

from ydb.tests.library.harness.kikimr_cluster import ExternalKiKiMRCluster
from ydb.tests.stability.nemesis.internal.models import WardenCheckResult
from ydb.tests.stability.nemesis.internal.orchestrator.orchestrator_warden_catalog import (
    ORCHESTRATOR_LIVENESS_CHECKS,
    OrchestratorAggregatedSafetyCheck,
    OrchestratorClusterSafetyCheck,
    OrchestratorLivenessCheck,
)


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def liveness_check_result_dict(spec: OrchestratorLivenessCheck, cluster: ExternalKiKiMRCluster) -> Dict[str, Any]:
    try:
        warden = spec.build(cluster)
        violations = warden.list_of_liveness_violations
        status = "violation" if violations else "ok"
        return {
            "name": spec.name,
            "category": "liveness",
            "status": status,
            "violations": violations if violations else [],
        }
    except Exception as e:
        return {
            "name": spec.name,
            "category": "liveness",
            "status": "error",
            "violations": [],
            "error_message": str(e),
        }


def run_orchestrator_liveness_cli_batch(cluster: ExternalKiKiMRCluster) -> List[Dict[str, Any]]:
    return [liveness_check_result_dict(spec, cluster) for spec in ORCHESTRATOR_LIVENESS_CHECKS]


def orchestrator_cluster_safety_warden_to_result(warden: Any, spec: OrchestratorClusterSafetyCheck) -> WardenCheckResult:
    warden_name = str(warden)
    try:
        violations = warden.list_of_safety_violations()
        status = "violation" if violations else "ok"
        if violations:
            logger.info("%s: %d violation(s) found", warden_name, len(violations))
        return WardenCheckResult(
            name=warden_name,
            category="safety",
            violations=violations if violations else [],
            status=status,
        )
    except Exception as e:
        logger.error("%s: error - %s", warden_name, e)
        return WardenCheckResult(
            name=warden_name,
            category="safety",
            violations=[],
            status="error",
            error_message=str(e),
        )


def orchestrator_cluster_safety_build_error_result(
    spec: OrchestratorClusterSafetyCheck,
    exc: Exception,
) -> WardenCheckResult:
    logger.error("%s: build failed - %s", spec.name, exc)
    return WardenCheckResult(
        name=spec.name,
        category="safety",
        violations=[],
        status="error",
        error_message=str(exc),
    )


def run_orchestrator_cluster_safety_sync(
    cluster: ExternalKiKiMRCluster,
    spec: OrchestratorClusterSafetyCheck,
) -> List[WardenCheckResult]:
    try:
        warden = spec.build(cluster)
    except Exception as e:
        return [orchestrator_cluster_safety_build_error_result(spec, e)]
    return [orchestrator_cluster_safety_warden_to_result(warden, spec)]


def run_orchestrator_aggregated_safety(
    spec: OrchestratorAggregatedSafetyCheck,
    *,
    per_host_reports: Dict[str, Dict[str, Any]],
    pending_timeout_hosts: Optional[set[str]] = None,
    max_wait_seconds: int = 120,
) -> List[WardenCheckResult]:
    return spec.new_runner().aggregate(
        per_host_reports=per_host_reports,
        pending_timeout_hosts=pending_timeout_hosts or set(),
        max_wait_seconds=max_wait_seconds,
    )


def run_orchestrator_liveness_subprocess_sync(
    nemesis_binary: str,
    yaml_config: str,
    *,
    timeout_seconds: int = 60,
) -> List[WardenCheckResult]:
    logger.info("Running liveness checks via subprocess with %ds timeout", timeout_seconds)

    cmd = [nemesis_binary, "liveness", "--yaml-config-location", yaml_config]

    try:
        start_time = time.time()
        logger.info("Executing: %s", " ".join(cmd))

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )

        elapsed = time.time() - start_time
        logger.info("Subprocess completed in %.1fs with return code %s", elapsed, result.returncode)

        if result.stderr:
            logger.debug("Subprocess stderr: %s", result.stderr[:500])

        if result.stdout:
            try:
                output = json.loads(result.stdout)
                checks = output.get("checks", [])

                rows: List[WardenCheckResult] = []
                for check in checks:
                    rows.append(
                        WardenCheckResult(
                            name=check.get("name", "Unknown"),
                            category=check.get("category", "liveness"),
                            violations=check.get("violations", []),
                            status=check.get("status", "error"),
                            error_message=check.get("error_message"),
                        )
                    )

                if output.get("status") == "error" and output.get("error_message"):
                    logger.error("Liveness subprocess error: %s", output["error_message"])

                return rows

            except json.JSONDecodeError as e:
                logger.error("Failed to parse liveness output: %s", e)
                logger.error("Raw output: %s", result.stdout[:500])
                return [
                    WardenCheckResult(
                        name="LivenessChecks",
                        category="liveness",
                        violations=[],
                        status="error",
                        error_message=f"Failed to parse output: {e}",
                    )
                ]

        logger.error("No output from liveness subprocess")
        return [
            WardenCheckResult(
                name="LivenessChecks",
                category="liveness",
                violations=[],
                status="error",
                error_message="No output from subprocess",
            )
        ]

    except subprocess.TimeoutExpired:
        logger.warning("Liveness checks timed out after %ds", timeout_seconds)
        return [
            WardenCheckResult(
                name="LivenessChecks",
                category="liveness",
                violations=[],
                status="error",
                error_message=f"Timeout after {timeout_seconds}s",
            )
        ]

    except Exception as e:
        logger.error("Failed to run liveness subprocess: %s", e)
        return [
            WardenCheckResult(
                name="LivenessChecks",
                category="liveness",
                violations=[],
                status="error",
                error_message=str(e),
            )
        ]
