"""Orchestrator-side warden catalog: liveness, cluster safety, aggregated safety."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, List, Tuple

from ydb.tests.library.harness.kikimr_cluster import ExternalKiKiMRCluster
from ydb.tests.library.wardens.datashard import TxCompleteLagLivenessWarden
from ydb.tests.library.wardens.disk import AllPDisksAreInValidStateSafetyWarden
from ydb.tests.library.wardens.hive import AllTabletsAliveLivenessWarden, BootQueueSizeWarden
from ydb.tests.library.wardens.pq import PersQueueHasNoStuckTransactions
from ydb.tests.library.wardens.schemeshard import SchemeShardHasNoInFlightTransactions
from ydb.tests.stability.nemesis.internal.orchestrator.unified_agent_verify_failed_aggregated import (
    UnifiedAgentVerifyFailedAggregated,
)
from ydb.tests.stability.nemesis.internal.safety_warden_execution import SafetyCheckSpec


# ---------------------------------------------------------------------------
# Cluster safety checks (unified SafetyCheckSpec)
# ---------------------------------------------------------------------------


def collect_orchestrator_cluster_safety_specs(
    cluster: ExternalKiKiMRCluster,
) -> List[SafetyCheckSpec]:
    """
    Orchestrator cluster safety check specs.

    Each spec wraps a ``build_warden`` that takes no args (cluster is captured).
    To add a new orchestrator cluster safety check, append a ``SafetyCheckSpec``.
    """
    return [
        SafetyCheckSpec(
            name="AllPDisksAreInValidState",
            description="Check all PDisks are in valid state",
            build_warden=lambda: AllPDisksAreInValidStateSafetyWarden(cluster, timeout_seconds=30),
        ),
    ]


# ---------------------------------------------------------------------------
# Aggregated safety checks (cross-agent aggregation)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OrchestratorAggregatedSafetyCheck:
    """
    Aggregate over agent safety responses.
    agent_source_class_name — agent catalog slot name (e.g. "unified_agent_verify_failed"); matched against the agent-reported check["name"] field.
    See UnifiedAgentVerifyFailedAggregated._row_matches_class.
    """

    name: str
    description: str
    agent_source_class_name: str
    impl: type

    def new_runner(self) -> "UnifiedAgentVerifyFailedAggregated":
        return self.impl(
            agent_source_class_name=self.agent_source_class_name,
            result_name=self.name,
        )


ORCHESTRATOR_AGGREGATED_SAFETY_CHECKS: Tuple[OrchestratorAggregatedSafetyCheck, ...] = (
    OrchestratorAggregatedSafetyCheck(
        name="UnifiedAgentVerifyFailedAggregated",
        description="Aggregate and deduplicate VERIFY failed errors from all agents",
        agent_source_class_name='unified_agent_verify_failed',
        impl=UnifiedAgentVerifyFailedAggregated,
    ),
)


# ---------------------------------------------------------------------------
# Liveness checks (orchestrator only)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OrchestratorLivenessCheck:
    """build(cluster, counters=None) -> warden. No counters means the warden downloads itself."""

    name: str
    description: str
    build: Callable[..., Any]


ORCHESTRATOR_LIVENESS_CHECKS: Tuple[OrchestratorLivenessCheck, ...] = (
    OrchestratorLivenessCheck(
        name="AllTabletsAlive",
        description="Check that all tablets are alive",
        build=lambda c, counters=None: AllTabletsAliveLivenessWarden(c, counters=counters),
    ),
    OrchestratorLivenessCheck(
        name="BootQueueSize",
        description="Check boot queue size is acceptable",
        build=lambda c, counters=None: BootQueueSizeWarden(c, counters=counters),
    ),
    OrchestratorLivenessCheck(
        name="SchemeShardNoInFlightTx",
        description="Check SchemeShard has no stuck in-flight transactions",
        build=lambda c, counters=None: SchemeShardHasNoInFlightTransactions(c, counters=counters),
    ),
    OrchestratorLivenessCheck(
        name="TxCompleteLag",
        description="Check transaction completion lag",
        build=lambda c, counters=None: TxCompleteLagLivenessWarden(c, counters=counters),
    ),
    OrchestratorLivenessCheck(
        name="PersQueueNoStuckTx",
        description="Check PersQueue has no stuck transactions (PQ/TxCompleteLag, PQ/TxInFly)",
        build=lambda c, counters=None: PersQueueHasNoStuckTransactions(c, counters=counters),
    ),
)
