"""Agent-side warden catalog: safety checks using unified SafetyCheckSpec."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Iterable, List, Tuple

from ydb.tests.library.nemesis.safety_warden import (
    AggregateSafetyWarden,
    LocalCommandExecutor,
    UnifiedAgentVerifyFailedSafetyWarden,
    UnifiedAgentSanitizerSafetyWarden,
    GrepJournalctlKernelForPatternsSafetyWarden,
)
from ydb.tests.library.wardens.logs import (
    kikimr_start_logs_safety_warden_factory,
    kikimr_crit_and_alert_logs_safety_warden_factory,
    kikimr_grep_kernel_log_safety_warden_factory,
)
from ydb.tests.stability.nemesis.internal.nemesis.cluster_context import require_external_cluster
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


def safety_warden_factory(cluster, lines_after=20, cut=False, modification_days=3):
    """
    Local version of ``ydb.tests.library.wardens.factories.safety_warden_factory``.

    Uses ``LocalCommandExecutor`` instead of ``RemoteCommandExecutor`` because
    the agent runs directly on the host.  Iterates over ``cluster.nodes`` and
    ``cluster.slots`` to discover all log directories (including slot dirs like
    ``/Berkanavt/kikimr_N/logs``).

    Default parameters match ``stability/tool``'s ``perform_checks`` call.
    """
    executor = LocalCommandExecutor()
    wardens = []
    wardens.extend(kikimr_grep_kernel_log_safety_warden_factory(executor=executor))

    by_directory = {}
    for node in list(cluster.slots.values()) + list(cluster.nodes.values()):
        if node.logs_directory not in by_directory:
            by_directory[node.logs_directory] = []
        by_directory[node.logs_directory].append(node.host)

    for directory in by_directory:
        wardens.extend(
            kikimr_start_logs_safety_warden_factory(
                executor=executor,
                deploy_path=directory,
                lines_after=lines_after,
                cut=cut,
                modification_days=modification_days,
            )
        )

    return AggregateSafetyWarden(wardens)


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
            name="safety_warden_factory",
            description="Local safety_warden_factory (nodes + slots log dirs, lines_after=20, cut=False, modification_days=3)",
            build_warden=lambda: safety_warden_factory(require_external_cluster()),
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
        SafetyCheckSpec(
            name="unified_agent_sanitizer",
            description="Check for sanitizer errors (ASan/LSan/TSan/MSan/UBSan) in unified agent logs",
            build_warden=lambda: UnifiedAgentSanitizerSafetyWarden(hours_back=24),
        ),
    ]
