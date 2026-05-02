"""Aggregates agent safety JSON already collected by the orchestrator (no HTTP / no waiting)."""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Set

from ydb.tests.stability.nemesis.internal.models import WardenCheckResult

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def deduplicate_verify_failed(violations: List[str]) -> List[str]:
    if not violations:
        return []

    unique_errors: Dict[str, List[str]] = defaultdict(list)

    for violation in violations:
        lines = violation.split("\n")
        if len(lines) >= 3:
            key = f"{lines[1].strip()}|{lines[2].strip()}"
        elif len(lines) >= 2:
            key = lines[1].strip()
        else:
            key = lines[0].strip() if lines else "unknown"

        unique_errors[key].append(violation)

    result: List[str] = []
    total_count = len(violations)
    unique_count = len(unique_errors)

    result.append(f"Found {total_count} VERIFY failed error(s), {unique_count} unique type(s)")

    for _key, error_list in sorted(unique_errors.items(), key=lambda x: -len(x[1])):
        count = len(error_list)
        sample = error_list[0]
        result.append(f"[{count}x] {sample}")

    return result


class UnifiedAgentVerifyFailedAggregated:
    """Merges violations from agent rows whose name matches the source warden class name."""

    # Aggregates results from agent-side wardens that read unified_agent /
    # kikimr-start logs; neither exists in the local harness deployment.
    supports_local_mode = False

    def __init__(
        self,
        *,
        agent_source_class_name: str,
        result_name: str,
    ):
        self._agent_source_class_name = agent_source_class_name
        self._result_name = result_name

    @staticmethod
    def _row_matches_class(check: Dict[str, Any], class_name: str) -> bool:
        name = (check.get("name") or "").strip()
        if not name:
            return False
        if name == class_name:
            return True
        first = name.split()[0]
        if first == class_name:
            return True
        # Agent catalog: ``{factory}__{ClassName}_{index}`` or legacy ``{ClassName}_{index}``.
        if first.startswith(class_name + "_"):
            return True
        if "__" in first:
            _, rest = first.split("__", 1)
            if rest == class_name or rest.startswith(class_name + "_"):
                return True
        return False

    def aggregate(
        self,
        *,
        per_host_reports: Dict[str, Dict[str, Any]],
        pending_timeout_hosts: Optional[Iterable[str]] = None,
        max_wait_seconds: int = 120,
    ) -> List[WardenCheckResult]:
        pending: Set[str] = set(pending_timeout_hosts or ())
        cls_name = self._agent_source_class_name

        verify_failed_violations: List[str] = []
        verify_failed_hosts: List[str] = []

        for host, report in per_host_reports.items():
            violations = self._violations_for_host(report, host, cls_name)
            if violations:
                logger.debug("Host %s: %d violation(s) for class %s", host, len(violations), cls_name)
                verify_failed_violations.extend(violations)
                verify_failed_hosts.append(host)

        aggregated_violations: List[str] = []
        if verify_failed_violations:
            logger.info(
                "Aggregating %d violations from %d hosts (agent class name=%s)",
                len(verify_failed_violations),
                len(verify_failed_hosts),
                cls_name,
            )
            aggregated_violations = deduplicate_verify_failed(verify_failed_violations)
        else:
            logger.debug("No violations for agent class name=%s", cls_name)

        error_message: Optional[str] = None
        if pending:
            msg = f"Timeout: agents {sorted(pending)} did not complete in {max_wait_seconds}s"
            aggregated_violations.append(msg)
            error_message = msg

        return [
            WardenCheckResult(
                name=self._result_name,
                category="safety",
                violations=aggregated_violations,
                status="violation" if aggregated_violations or error_message else "ok",
                error_message=error_message,
                affected_hosts=verify_failed_hosts,
            )
        ]

    def _violations_for_host(
        self,
        report: Dict[str, Any],
        host: str,
        class_name: str,
    ) -> List[str]:
        try:
            if report.get("safety_checks"):
                for check in report["safety_checks"]:
                    if self._row_matches_class(check, class_name) and check.get("violations"):
                        return list(check["violations"])
            return []
        except Exception as e:
            logger.error("Failed to collect violations from %s: %s", host, e)
            return []
