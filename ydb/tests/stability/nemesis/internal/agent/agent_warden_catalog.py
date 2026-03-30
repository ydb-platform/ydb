"""Agent-side warden catalog: safety checks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, List, Tuple

from ydb.tests.library.nemesis.safety_warden import UnifiedAgentVerifyFailedSafetyWarden
from ydb.tests.library.wardens.logs import (
    kikimr_grep_dmesg_safety_warden_factory,
    kikimr_start_logs_safety_warden_factory,
    kikimr_crit_and_alert_logs_safety_warden_factory,
)


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


def collect_agent_safety_warden_pairs(ctx: AgentSafetyContext) -> List[Tuple[str, Any]]:
    """One (slot_name, warden) per slot: log factories once each, then unified verify-failed warden."""
    pairs = _pairs_from_wardens(
        kikimr_start_logs_safety_warden_factory.__name__,
        kikimr_start_logs_safety_warden_factory(
            ctx.local_hosts,
            None,
            ctx.log_directory,
            lines_after=5,
            cut=True,
            modification_days=1,
        ),
    )
    pairs += _pairs_from_wardens(
        kikimr_grep_dmesg_safety_warden_factory.__name__,
        kikimr_grep_dmesg_safety_warden_factory(ctx.local_hosts, None, lines_after=5),
    )
    pairs += _pairs_from_wardens(
        kikimr_crit_and_alert_logs_safety_warden_factory.__name__,
        kikimr_crit_and_alert_logs_safety_warden_factory(ctx.local_hosts, None),
    )
    unified = UnifiedAgentVerifyFailedSafetyWarden(hours_back=24)
    pairs.append((_agent_safety_slot_name("unified_agent_verify_failed", unified, 0), unified))
    return pairs
