"""
Single source of truth for warden check definitions (liveness + safety).

- MASTER_LIVENESS_CHECKS: liveness checks on the master (orchestrator / CLI subprocess).
- SAFETY_CHECK_ROWS: every safety check (stable check_id + API name + location).
- MASTER_SAFETY_CHECK_IDS_ORDER: execution order for master-side safety (parallel tasks allowed).
- check_id_for_agent_warden_instance: map library warden object -> check_id for aggregation / API.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Literal, Tuple

from ydb.tests.library.harness.kikimr_cluster import ExternalKiKiMRCluster
from ydb.tests.library.wardens.datashard import TxCompleteLagLivenessWarden
from ydb.tests.library.wardens.hive import AllTabletsAliveLivenessWarden, BootQueueSizeWarden
from ydb.tests.library.wardens.schemeshard import SchemeShardHasNoInFlightTransactions

# --- Stable ids (API + orchestrator aggregation; do not rename casually) -------------

CHECK_ID_AGENT_KIKIMR_START_PLAIN = "safety.agent.kikimr_start_plain"
CHECK_ID_AGENT_KIKIMR_START_GZIP = "safety.agent.kikimr_start_gzip"
CHECK_ID_AGENT_DMESG = "safety.agent.dmesg"
CHECK_ID_AGENT_UNIFIED_VERIFY = "safety.agent.unified_verify_failed"

CHECK_ID_MASTER_PDISK = "safety.master.pdisk"
CHECK_ID_MASTER_VERIFY_AGGREGATED = "safety.master.verify_failed_aggregated"

MASTER_SAFETY_CHECK_IDS_ORDER: Tuple[str, ...] = (
    CHECK_ID_MASTER_PDISK,
    CHECK_ID_MASTER_VERIFY_AGGREGATED,
)


def check_id_for_agent_warden_instance(warden: Any) -> str:
    """Map a library safety warden instance to a stable check_id."""
    cls = type(warden).__name__
    if cls == "GrepLogFileForMarkers":
        return CHECK_ID_AGENT_KIKIMR_START_PLAIN
    if cls == "GrepGzippedLogFilesForMarkersSafetyWarden":
        return CHECK_ID_AGENT_KIKIMR_START_GZIP
    if cls == "GrepDMesgForPatternsSafetyWarden":
        return CHECK_ID_AGENT_DMESG
    if cls == "UnifiedAgentVerifyFailedSafetyWarden":
        return CHECK_ID_AGENT_UNIFIED_VERIFY
    return f"safety.agent.{cls}"


@dataclass(frozen=True)
class MasterLivenessCheck:
    """One liveness check on the master; build(cluster) returns a warden with list_of_liveness_violations."""

    name: str
    description: str
    build: Callable[[ExternalKiKiMRCluster], Any]


MASTER_LIVENESS_CHECKS: Tuple[MasterLivenessCheck, ...] = (
    MasterLivenessCheck(
        "AllTabletsAlive",
        "Check that all tablets are alive",
        lambda c: AllTabletsAliveLivenessWarden(c),
    ),
    MasterLivenessCheck(
        "BootQueueSize",
        "Check boot queue size is acceptable",
        lambda c: BootQueueSizeWarden(c),
    ),
    MasterLivenessCheck(
        "SchemeShardNoInFlightTx",
        "Check SchemeShard has no stuck in-flight transactions",
        lambda c: SchemeShardHasNoInFlightTransactions(c),
    ),
    MasterLivenessCheck(
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
    location: Literal["agent", "master"]


SAFETY_CHECK_ROWS: Tuple[SafetyCheckRow, ...] = (
    SafetyCheckRow(
        CHECK_ID_AGENT_KIKIMR_START_PLAIN,
        "GrepLogFileForMarkersSafetyWarden",
        "Check kikimr.start logs for error markers",
        "agent",
    ),
    SafetyCheckRow(
        CHECK_ID_AGENT_KIKIMR_START_GZIP,
        "GrepGzippedLogFilesForMarkersSafetyWarden",
        "Check gzipped kikimr.start logs for error markers",
        "agent",
    ),
    SafetyCheckRow(
        CHECK_ID_AGENT_DMESG,
        "GrepDMesgForPatternsSafetyWarden",
        "Check dmesg for OOM and other critical patterns",
        "agent",
    ),
    SafetyCheckRow(
        CHECK_ID_AGENT_UNIFIED_VERIFY,
        "UnifiedAgentVerifyFailedSafetyWarden",
        "Check unified_agent logs for VERIFY failed errors",
        "agent",
    ),
    SafetyCheckRow(
        CHECK_ID_MASTER_PDISK,
        "AllPDisksAreInValidState",
        "Check all PDisks are in valid state",
        "master",
    ),
    SafetyCheckRow(
        CHECK_ID_MASTER_VERIFY_AGGREGATED,
        "UnifiedAgentVerifyFailedAggregated",
        "Aggregate and deduplicate VERIFY failed errors from all agents",
        "master",
    ),
)


def get_all_warden_definitions() -> List[Dict[str, Any]]:
    """Flat list for API: liveness (master) + safety (per location row)."""
    out: List[Dict[str, Any]] = []
    for c in MASTER_LIVENESS_CHECKS:
        out.append(
            {
                "name": c.name,
                "category": "liveness",
                "description": c.description,
                "location": "master",
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
