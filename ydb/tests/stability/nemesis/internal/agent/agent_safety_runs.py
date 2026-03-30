"""
Run agent safety checks: one executor task per (slot_name, warden) from collect_agent_safety_warden_pairs.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, List, Tuple

from ydb.tests.stability.nemesis.internal.models import WardenCheckResult

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def safety_warden_to_result(
    warden: Any,
    slot_name: str,
    *,
    log_prefix: str = "",
) -> WardenCheckResult:
    """Run list_of_safety_violations() on a library safety warden and build WardenCheckResult."""
    warden_name = str(warden)
    try:
        violations = warden.list_of_safety_violations()
        status = "violation" if violations else "ok"
        if violations:
            logger.info("%s%s: %d violation(s) found", log_prefix, warden_name, len(violations))
        return WardenCheckResult(
            name=slot_name,
            category="safety",
            violations=violations if violations else [],
            status=status,
        )
    except Exception as e:
        logger.error("%s%s: error - %s", log_prefix, warden_name, e)
        return WardenCheckResult(
            name=slot_name,
            category="safety",
            violations=[],
            status="error",
            error_message=str(e),
        )


def safety_build_error_result(slot_name: str, exc: Exception) -> WardenCheckResult:
    """Single error row when building the batch fails before runs start."""
    logger.error("%s: build failed - %s", slot_name, exc)
    return WardenCheckResult(
        name=slot_name,
        category="safety",
        violations=[],
        status="error",
        error_message=str(exc),
    )


AgentSafetyRun = Callable[[], List[WardenCheckResult]]


def _make_warden_run(slot_name: str, warden: Any, log_prefix: str) -> AgentSafetyRun:
    def _run() -> List[WardenCheckResult]:
        return [safety_warden_to_result(warden, slot_name, log_prefix=log_prefix)]

    return _run


def build_agent_safety_runs_from_pairs(
    pairs: List[Tuple[str, Any]],
    log_prefix: str,
) -> List[AgentSafetyRun]:
    """One callable per warden; factories were already invoked in collect_agent_safety_warden_pairs."""
    return [_make_warden_run(name, w, log_prefix) for name, w in pairs]
