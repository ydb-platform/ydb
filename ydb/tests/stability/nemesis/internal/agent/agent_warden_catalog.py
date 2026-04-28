"""Agent-side warden catalog: safety checks using unified SafetyCheckSpec."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Iterable, List, Tuple

from ydb.tests.library.nemesis.safety_warden import (
    LocalCommandExecutor,
    UnifiedAgentVerifyFailedSafetyWarden,
    GrepJournalctlKernelForPatternsSafetyWarden,
)
from ydb.tests.library.wardens.logs import (
    kikimr_start_logs_safety_warden_factory,
    kikimr_crit_and_alert_logs_safety_warden_factory,
)
from ydb.tests.stability.nemesis.internal.safety_warden_execution import SafetyCheckSpec

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AgentSafetyContext:
    """Context passed when collecting wardens for a host."""

    log_directory: str
    hostname: str

    @property
    def log_prefix(self) -> str:
        return f"[{self.hostname}] "

    @property
    def local_hosts(self) -> List[str]:
        return [self.hostname]


def _agent_safety_slot_name(factory_name: str, warden: Any, index: int) -> str:
    """Stable wire id: ``{factory}__{WardenClass}_{index}`` (double underscore separates factory from class)."""
    return f"{factory_name}__{type(warden).__name__}_{index}"


def _pairs_from_wardens(factory_name: str, wardens: Iterable[Any]) -> List[Tuple[str, Any]]:
    return [
        (_agent_safety_slot_name(factory_name, w, i), w)
        for i, w in enumerate(wardens)
    ]


def collect_agent_safety_check_specs(ctx: AgentSafetyContext) -> List[SafetyCheckSpec]:
    """
    Agent safety check specs.

    Each spec wraps a factory that produces ``(slot_name, warden)`` pairs.
    To add a new agent safety check, append a ``SafetyCheckSpec`` to this list.

    All command-based wardens use ``LocalCommandExecutor`` — commands run
    directly on the agent host, no SSH.
    """
    local_executor = LocalCommandExecutor()

    return [
        SafetyCheckSpec(
            name="kikimr_start_logs",
            description="Check Kikimr start logs for errors",
            build_pairs=lambda: _pairs_from_wardens(
                kikimr_start_logs_safety_warden_factory.__name__,
                kikimr_start_logs_safety_warden_factory(
                    executor=local_executor,
                    deploy_path=ctx.log_directory,
                    lines_after=5,
                    cut=True,
                    modification_days=1,
                ),
            ),
        ),
        SafetyCheckSpec(
            name="kikimr_grep_kernel_log",
            description="Grep kernel log (journalctl -k) for Kikimr-related errors (local, no SSH)",
            build_warden=lambda: GrepJournalctlKernelForPatternsSafetyWarden(
                executor=local_executor,
                list_of_markers=['Out of memory: Kill process'],
                lines_after=5,
                hours_back=24,
            ),
        ),
        SafetyCheckSpec(
            name="kikimr_crit_and_alert_logs",
            description="Check Kikimr logs for CRIT and ALERT entries",
            build_pairs=lambda: _pairs_from_wardens(
                kikimr_crit_and_alert_logs_safety_warden_factory.__name__,
                kikimr_crit_and_alert_logs_safety_warden_factory(
                    executor=local_executor,
                ),
            ),
        ),
        SafetyCheckSpec(
            name="unified_agent_verify_failed",
            description="Check for VERIFY failed errors in unified agent logs",
            build_warden=lambda: UnifiedAgentVerifyFailedSafetyWarden(hours_back=24),
        ),
    ]
