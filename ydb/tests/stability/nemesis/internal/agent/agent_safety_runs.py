"""
Run agent safety checks from warden_catalog.AGENT_SAFETY_CHECKS (same pattern as ORCHESTRATOR_LIVENESS_CHECKS).
Each spec yields Callable[[], list[WardenCheckResult]]; runs execute in parallel via run_in_executor.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, List

from ydb.tests.stability.nemesis.internal.models import WardenCheckResult
from ydb.tests.stability.nemesis.internal.warden_catalog import (
    AGENT_SAFETY_CHECKS,
    AgentSafetyCheck,
    AgentSafetyContext,
)

logger = logging.getLogger(__name__)


def safety_warden_to_result(
    warden: Any,
    spec: AgentSafetyCheck,
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
            name=warden_name,
            category="safety",
            violations=violations if violations else [],
            status=status,
            check_id=spec.check_id,
        )
    except Exception as e:
        logger.error("%s%s: error - %s", log_prefix, warden_name, e)
        return WardenCheckResult(
            name=warden_name,
            category="safety",
            violations=[],
            status="error",
            error_message=str(e),
            check_id=spec.check_id,
        )


def safety_build_error_result(spec: AgentSafetyCheck, exc: Exception) -> WardenCheckResult:
    """Single error row when build(ctx) fails before producing a warden."""
    logger.error("%s: build failed - %s", spec.name, exc)
    return WardenCheckResult(
        name=spec.name,
        category="safety",
        violations=[],
        status="error",
        error_message=str(exc),
        check_id=spec.check_id,
    )


AgentSafetyRun = Callable[[], List[WardenCheckResult]]


def build_agent_safety_runs(ctx: AgentSafetyContext) -> List[AgentSafetyRun]:
    """Ordered safety runs for one agent host (order = AGENT_SAFETY_CHECKS)."""
    runs: List[AgentSafetyRun] = []
    lp = ctx.log_prefix

    for spec in AGENT_SAFETY_CHECKS:

        def _run(
            s: AgentSafetyContext = ctx,
            specification: AgentSafetyCheck = spec,
            log_prefix: str = lp,
        ) -> List[WardenCheckResult]:
            try:
                warden = specification.build(s)
            except Exception as e:
                return [safety_build_error_result(specification, e)]
            return [safety_warden_to_result(warden, specification, log_prefix=log_prefix)]

        runs.append(_run)

    return runs
