"""
Unified safety warden execution: SafetyCheckSpec + run pipeline.

Both agent and orchestrator safety checks use the same ``SafetyCheckSpec`` for
registration and the same ``build_safety_runs`` / ``safety_warden_to_result``
pipeline for execution.

A ``SafetyCheckSpec`` wraps either:
- a **factory** that produces ``(slot_name, warden)`` pairs (agent log factories), or
- a **single warden builder** (orchestrator cluster checks).

The execution path is always:
  specs → build_safety_runs(specs) → SafetyWardenRun callables → run_in_executor
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Iterable, List, Optional, Tuple

from ydb.tests.stability.nemesis.internal.models import WardenCheckResult

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

SafetyWardenRun = Callable[[], List[WardenCheckResult]]


# ---------------------------------------------------------------------------
# SafetyCheckSpec — unified registration for agent and orchestrator safety
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SafetyCheckSpec:
    """
    Unified safety check specification.

    Provide exactly one of:
    - ``build_pairs``: a callable that returns ``List[(slot_name, warden)]``.
      Used for agent log factories that produce multiple wardens per factory.
    - ``build_warden``: a callable that returns a single warden instance.
      The ``name`` field is used as the slot name.

    Both kinds of wardens must implement ``list_of_safety_violations() -> list``.
    """

    name: str
    description: str = ""
    build_pairs: Optional[Callable[[], List[Tuple[str, Any]]]] = None
    build_warden: Optional[Callable[[], Any]] = None


def collect_safety_warden_pairs(
    specs: Iterable[SafetyCheckSpec],
) -> List[Tuple[str, Any]]:
    """
    Collect ``(slot_name, warden)`` pairs from all specs.

    Specs with ``build_pairs`` expand into multiple pairs.
    Specs with ``build_warden`` produce a single ``(name, warden)`` pair.
    If building fails, a ``_BuildFailedSafetyWarden`` placeholder is used.
    """
    pairs: List[Tuple[str, Any]] = []
    for spec in specs:
        try:
            if spec.build_pairs is not None:
                pairs.extend(spec.build_pairs())
            elif spec.build_warden is not None:
                warden = spec.build_warden()
                pairs.append((spec.name, warden))
            else:
                logger.warning("SafetyCheckSpec %r has neither build_pairs nor build_warden", spec.name)
        except Exception as exc:
            logger.error("Failed to build safety check %r: %s", spec.name, exc)
            pairs.append((spec.name, _BuildFailedSafetyWarden(exc)))
    return pairs


# ---------------------------------------------------------------------------
# Execution helpers
# ---------------------------------------------------------------------------


class _BuildFailedSafetyWarden:
    """Placeholder so a failed spec.build still goes through safety_warden_to_result (error row)."""

    __slots__ = ("_exc",)

    def __init__(self, exc: Exception):
        self._exc = exc

    def list_of_safety_violations(self):
        raise self._exc


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


def _make_warden_run(slot_name: str, warden: Any, log_prefix: str) -> SafetyWardenRun:
    def _run() -> List[WardenCheckResult]:
        return [safety_warden_to_result(warden, slot_name, log_prefix=log_prefix)]

    return _run


def build_safety_runs(
    specs: Iterable[SafetyCheckSpec],
    log_prefix: str = "",
) -> Tuple[List[str], List[SafetyWardenRun]]:
    """
    Collect pairs from specs, then build runs.

    Returns ``(slot_names, runs)`` — both in the same order.
    """
    pairs = collect_safety_warden_pairs(specs)
    slot_names = [name for name, _ in pairs]
    runs = [_make_warden_run(name, w, log_prefix) for name, w in pairs]
    return slot_names, runs
