"""Orchestrator-side warden catalog: liveness, cluster safety, aggregated safety."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, List, Tuple, Union

from ydb.tests.library.harness.kikimr_cluster import ExternalKiKiMRCluster
from ydb.tests.library.nemesis.safety_warden import UnifiedAgentVerifyFailedSafetyWarden
from ydb.tests.library.wardens.datashard import TxCompleteLagLivenessWarden
from ydb.tests.library.wardens.disk import AllPDisksAreInValidStateSafetyWarden
from ydb.tests.library.wardens.hive import AllTabletsAliveLivenessWarden, BootQueueSizeWarden
from ydb.tests.library.wardens.schemeshard import SchemeShardHasNoInFlightTransactions
from ydb.tests.stability.nemesis.internal.orchestrator.unified_agent_verify_failed_aggregated import (
    UnifiedAgentVerifyFailedAggregated,
)


@dataclass(frozen=True)
class OrchestratorClusterSafetyCheck:
    """Orchestrator safety on cluster; build(cluster) -> warden with list_of_safety_violations()."""

    name: str
    description: str
    build: Callable[[ExternalKiKiMRCluster], Any]


ORCHESTRATOR_CLUSTER_SAFETY_CHECKS: Tuple[OrchestratorClusterSafetyCheck, ...] = (
    OrchestratorClusterSafetyCheck(
        name="AllPDisksAreInValidState",
        description="Check all PDisks are in valid state",
        build=lambda c: AllPDisksAreInValidStateSafetyWarden(c, timeout_seconds=30),
    ),
)


class _BuildFailedSafetyWarden:
    """Placeholder so a failed spec.build still goes through safety_warden_to_result (error row)."""

    __slots__ = ("_exc",)

    def __init__(self, exc: Exception):
        self._exc = exc

    def list_of_safety_violations(self):
        raise self._exc


def collect_orchestrator_cluster_safety_warden_pairs(
    cluster: ExternalKiKiMRCluster,
) -> List[Tuple[str, Any]]:
    """Same shape as agent: ``(slot_name, warden)`` for each catalog entry; ``spec.name`` is the slot name."""
    out: List[Tuple[str, Any]] = []
    for spec in ORCHESTRATOR_CLUSTER_SAFETY_CHECKS:
        try:
            warden = spec.build(cluster)
        except Exception as e:
            warden = _BuildFailedSafetyWarden(e)
        out.append((spec.name, warden))
    return out


@dataclass(frozen=True)
class OrchestratorAggregatedSafetyCheck:
    """
    Агрегат по safety-ответам агентов.
    agent_source_class_name — имя класса warden на агенте; совпадение см. UnifiedAgentVerifyFailedAggregated._row_matches_class.
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
        agent_source_class_name=UnifiedAgentVerifyFailedSafetyWarden.__name__,
        impl=UnifiedAgentVerifyFailedAggregated,
    ),
)


ORCHESTRATOR_SAFETY_CHECKS: Tuple[Union[OrchestratorClusterSafetyCheck, OrchestratorAggregatedSafetyCheck], ...] = (
    *ORCHESTRATOR_CLUSTER_SAFETY_CHECKS,
    *ORCHESTRATOR_AGGREGATED_SAFETY_CHECKS,
)


@dataclass(frozen=True)
class OrchestratorLivenessCheck:
    """Liveness на оркестраторе; build(cluster) -> warden с list_of_liveness_violations (property)."""

    name: str
    description: str
    build: Callable[[ExternalKiKiMRCluster], Any]


ORCHESTRATOR_LIVENESS_CHECKS: Tuple[OrchestratorLivenessCheck, ...] = (
    OrchestratorLivenessCheck(
        name="AllTabletsAlive",
        description="Check that all tablets are alive",
        build=lambda c: AllTabletsAliveLivenessWarden(c),
    ),
    OrchestratorLivenessCheck(
        name="BootQueueSize",
        description="Check boot queue size is acceptable",
        build=lambda c: BootQueueSizeWarden(c),
    ),
    OrchestratorLivenessCheck(
        name="SchemeShardNoInFlightTx",
        description="Check SchemeShard has no stuck in-flight transactions",
        build=lambda c: SchemeShardHasNoInFlightTransactions(c),
    ),
    OrchestratorLivenessCheck(
        name="TxCompleteLag",
        description="Check transaction completion lag",
        build=lambda c: TxCompleteLagLivenessWarden(c),
    ),
)
