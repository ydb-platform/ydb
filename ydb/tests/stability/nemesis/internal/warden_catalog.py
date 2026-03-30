"""
Single source of truth for warden check definitions (liveness + safety).

- ORCHESTRATOR_LIVENESS_CHECKS: liveness on orchestrator (subprocess / ExternalKiKiMRCluster).
- AGENT_SAFETY_CHECKS: agent safety checks; build(ctx) returns one warden with list_of_safety_violations.
- ORCHESTRATOR_SAFETY_CHECKS: orchestrator-only safety metadata (wire ids + API rows; execution in OrchestratorWardenChecker).
- SAFETY_CHECK_ROWS: every safety check for the API (agent + orchestrator rows mirror the catalogs above).
- Orchestrator safety/liveness execution order: orchestrator_warden_checker.ORCHESTRATOR_WARDEN_STEPS.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Literal, Tuple

from ydb.tests.library.harness.kikimr_cluster import ExternalKiKiMRCluster
from ydb.tests.library.nemesis.safety_warden import (
    GrepDMesgForPatternsSafetyWarden,
    GrepGzippedLogFilesForMarkersSafetyWarden,
    GrepLogFileForMarkers,
    UnifiedAgentVerifyFailedSafetyWarden,
)
from ydb.tests.library.wardens.datashard import TxCompleteLagLivenessWarden
from ydb.tests.library.wardens.hive import AllTabletsAliveLivenessWarden, BootQueueSizeWarden
from ydb.tests.library.wardens.schemeshard import SchemeShardHasNoInFlightTransactions

# --- Stable wire ids (API + orchestrator aggregation; do not rename slugs casually) -------------
# Agent: AgentSafetyCheck.id -> AgentSafetyCheck.check_id ("safety.agent.<id>").
# Orchestrator: OrchestratorSafetyCheck.id -> check_id ("safety.orchestrator.<id>").
# Resolve at call sites with agent_safety_check_id(...) / orchestrator_safety_check_id(...).

# Markers for kikimr.start plain + gzip checks (same set as former kikimr_start_logs_safety_warden_factory).
_AGENT_KIKIMR_START_MARKERS: List[str] = [
    "VERIFY",
    "FAIL ",
    "signal 11",
    "signal 6",
    "signal 15",
    "uncaught exception",
    "ERROR: AddressSanitizer",
    "SIG",
]

_AGENT_DMESG_MARKERS: List[str] = ["Out of memory: Kill process"]


@dataclass(frozen=True)
class AgentSafetyContext:
    """Context passed to every agent safety check build (like ExternalKiKiMRCluster for liveness)."""

    log_directory: str
    hostname: str

    @property
    def log_prefix(self) -> str:
        return f"[{self.hostname}] "

    @property
    def local_hosts(self) -> List[str]:
        return [self.hostname]


@dataclass(frozen=True)
class AgentSafetyCheck:
    """One agent safety check; build(ctx) returns a warden with list_of_safety_violations."""

    id: str
    name: str
    description: str
    build: Callable[[AgentSafetyContext], Any]

    @property
    def check_id(self) -> str:
        """Stable id in JSON / for orchestrator↔agent matching (prefix + catalog slug)."""
        return f"safety.agent.{self.id}"


AGENT_SAFETY_CHECKS: Tuple[AgentSafetyCheck, ...] = (
    AgentSafetyCheck(
        "kikimr_start_plain",
        "GrepLogFileForMarkersSafetyWarden",
        "Check kikimr.start logs for error markers",
        lambda ctx: GrepLogFileForMarkers(
            ctx.local_hosts,
            log_file_name=os.path.join(ctx.log_directory, "kikimr.start"),
            list_of_markers=_AGENT_KIKIMR_START_MARKERS,
            username=None,
            lines_after=5,
            cut=True,
        ),
    ),
    AgentSafetyCheck(
        "kikimr_start_gzip",
        "GrepGzippedLogFilesForMarkersSafetyWarden",
        "Check gzipped kikimr.start logs for error markers",
        lambda ctx: GrepGzippedLogFilesForMarkersSafetyWarden(
            ctx.local_hosts,
            log_file_pattern=os.path.join(ctx.log_directory, "kikimr.start.*gz"),
            list_of_markers=_AGENT_KIKIMR_START_MARKERS,
            modification_days=1,
            username=None,
            lines_after=5,
            cut=True,
        ),
    ),
    AgentSafetyCheck(
        "dmesg",
        "GrepDMesgForPatternsSafetyWarden",
        "Check dmesg for OOM and other critical patterns",
        lambda ctx: GrepDMesgForPatternsSafetyWarden(
            ctx.local_hosts,
            list_of_markers=_AGENT_DMESG_MARKERS,
            username=None,
            lines_after=5,
        ),
    ),
    AgentSafetyCheck(
        "unified_verify_failed",
        "UnifiedAgentVerifyFailedSafetyWarden",
        "Check unified_agent logs for VERIFY failed errors",
        lambda _ctx: UnifiedAgentVerifyFailedSafetyWarden(hours_back=24),
    ),
)


def agent_safety_check_id(slug: str) -> str:
    """Return wire check_id for an agent check by its catalog slug (see AgentSafetyCheck.id)."""
    for spec in AGENT_SAFETY_CHECKS:
        if spec.id == slug:
            return spec.check_id
    raise KeyError(slug)


@dataclass(frozen=True)
class OrchestratorSafetyCheck:
    """Orchestrator-only safety check metadata (execution is in orchestrator_warden_checker)."""

    id: str
    name: str
    description: str

    @property
    def check_id(self) -> str:
        return f"safety.orchestrator.{self.id}"


ORCHESTRATOR_SAFETY_CHECKS: Tuple[OrchestratorSafetyCheck, ...] = (
    OrchestratorSafetyCheck(
        "pdisk",
        "AllPDisksAreInValidState",
        "Check all PDisks are in valid state",
    ),
    OrchestratorSafetyCheck(
        "verify_failed_aggregated",
        "UnifiedAgentVerifyFailedAggregated",
        "Aggregate and deduplicate VERIFY failed errors from all agents",
    ),
)


def orchestrator_safety_check_id(slug: str) -> str:
    """Return wire check_id for an orchestrator safety check by its catalog slug (see OrchestratorSafetyCheck.id)."""
    for spec in ORCHESTRATOR_SAFETY_CHECKS:
        if spec.id == slug:
            return spec.check_id
    raise KeyError(slug)


@dataclass(frozen=True)
class OrchestratorLivenessCheck:
    """One liveness check on the orchestrator; build(cluster) returns a warden with list_of_liveness_violations."""

    name: str
    description: str
    build: Callable[[ExternalKiKiMRCluster], Any]


ORCHESTRATOR_LIVENESS_CHECKS: Tuple[OrchestratorLivenessCheck, ...] = (
    OrchestratorLivenessCheck(
        "AllTabletsAlive",
        "Check that all tablets are alive",
        lambda c: AllTabletsAliveLivenessWarden(c),
    ),
    OrchestratorLivenessCheck(
        "BootQueueSize",
        "Check boot queue size is acceptable",
        lambda c: BootQueueSizeWarden(c),
    ),
    OrchestratorLivenessCheck(
        "SchemeShardNoInFlightTx",
        "Check SchemeShard has no stuck in-flight transactions",
        lambda c: SchemeShardHasNoInFlightTransactions(c),
    ),
    OrchestratorLivenessCheck(
        "TxCompleteLag",
        "Check transaction completion lag",
        lambda c: TxCompleteLagLivenessWarden(c),
    ),
)


@dataclass(frozen=True)
class SafetyCheckRow:
    """One documented safety check."""

    check_id: str
    name: str
    description: str
    location: Literal["agent", "orchestrator"]


SAFETY_CHECK_ROWS: Tuple[SafetyCheckRow, ...] = (
    *(
        SafetyCheckRow(s.check_id, s.name, s.description, "agent")
        for s in AGENT_SAFETY_CHECKS
    ),
    *(
        SafetyCheckRow(s.check_id, s.name, s.description, "orchestrator")
        for s in ORCHESTRATOR_SAFETY_CHECKS
    ),
)


def get_all_warden_definitions() -> List[Dict[str, Any]]:
    """Flat list for API: liveness (orchestrator) + safety (per location row)."""
    out: List[Dict[str, Any]] = []
    for c in ORCHESTRATOR_LIVENESS_CHECKS:
        out.append(
            {
                "name": c.name,
                "category": "liveness",
                "description": c.description,
                "location": "orchestrator",
            }
        )
    for r in SAFETY_CHECK_ROWS:
        out.append(
            {
                "check_id": r.check_id,
                "name": r.name,
                "category": "safety",
                "description": r.description,
                "location": r.location,
            }
        )
    return out
