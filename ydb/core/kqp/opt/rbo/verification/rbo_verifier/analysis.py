"""Validated, read-only plan facts shared by every invocation and stage task.

Analysis has no symbolic rows, caches, observers or solver state. An analyzed
plan belongs to one snapshot; consumers receive only the facts they need.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from itertools import combinations
from types import MappingProxyType
from typing import Mapping

from . import decimal
from .ir import (
    Aggregate, Column, Filter, Join, Limit, OuterBind, PlanNode, Project,
    ScalarSubplan, Snapshot, Sort, Subplan, UnionAll, _plan_descendants,
    plan_node_inputs, validate_snapshot,
)


class AnalysisError(ValueError):
    """A well-formed plan needs a relational behavior outside the model."""


@dataclass(frozen=True, slots=True)
class ValidatedPlan:
    """Schema/admission checks precede cross-plan schema comparison and analysis."""

    snapshot: Snapshot
    schemas: Mapping[str, Mapping[str, Column]] = field(init=False)

    def __post_init__(self) -> None:
        schemas = validate_snapshot(self.snapshot)
        object.__setattr__(self, "schemas", MappingProxyType({
            key: MappingProxyType(value) for key, value in schemas.items()
        }))

    @property
    def output_schema(self) -> tuple[Column, ...]:
        root = self.schemas[self.snapshot.plan.root]
        return tuple(root[name] for name in self.snapshot.plan.output)


@dataclass(frozen=True, slots=True)
class AnalyzedPlan:
    snapshot: Snapshot
    nodes: Mapping[str, PlanNode]
    schemas: Mapping[str, Mapping[str, Column]]
    parents: Mapping[str, frozenset[str]]
    decimal_sum_state_producers: frozenset[tuple[str, str]]
    decimal_sum_state_consumers: frozenset[tuple[str, str]]
    subplans_by_consumer: Mapping[str, tuple[Subplan, ...]]
    scalar_outer_binds: Mapping[str, OuterBind]


def analyze_snapshot(snapshot: Snapshot) -> AnalyzedPlan:
    """Validate once, then derive facts without permitting downstream mutation."""

    return analyze_validated(ValidatedPlan(snapshot))


def analyze_validated(validated: ValidatedPlan) -> AnalyzedPlan:
    """Derive invocation facts after schema compatibility has been established."""

    snapshot = validated.snapshot
    nodes = snapshot.plan.node_map()
    parents: dict[str, set[str]] = {node_id: set() for node_id in nodes}
    for parent in snapshot.plan.nodes:
        for child in plan_node_inputs(parent):
            parents[child].add(parent.id)
    frozen_parents = {node_id: frozenset(items) for node_id, items in parents.items()}
    _reject_correlated_limit_fanout(snapshot, nodes, frozen_parents)
    producers, consumers = _decimal_sum_state_lineages(snapshot, nodes, frozen_parents)

    by_consumer: dict[str, list[Subplan]] = {}
    outer_binds: dict[str, OuterBind] = {}
    for subplan in snapshot.plan.subplans:
        for consumer in subplan.consumers:
            by_consumer.setdefault(consumer, []).append(subplan)
        if isinstance(subplan, ScalarSubplan) and subplan.dependency is not None:
            descendants = _plan_descendants(nodes, subplan.root)
            outer_binds[subplan.binding] = next(
                node for node in snapshot.plan.nodes
                if isinstance(node, OuterBind) and node.id in descendants
            )
    return AnalyzedPlan(
        snapshot,
        MappingProxyType(nodes),
        validated.schemas,
        MappingProxyType(frozen_parents),
        producers,
        consumers,
        MappingProxyType({key: tuple(value) for key, value in by_consumer.items()}),
        MappingProxyType(outer_binds),
    )


def _decimal_sum_state_lineages(
    snapshot: Snapshot,
    nodes: Mapping[str, PlanNode],
    parents: Mapping[str, frozenset[str]],
) -> tuple[
    frozenset[tuple[str, str]],
    frozenset[tuple[str, str]],
]:
    """Return the exact private intermediate/final SUM certificate pairs."""

    exposed_roots = {
        snapshot.plan.root,
        *(subplan.root for subplan in snapshot.plan.subplans),
    }
    producers: set[tuple[str, str]] = set()
    consumers: set[tuple[str, str]] = set()
    for producer in snapshot.plan.nodes:
        if (
            not isinstance(producer, Aggregate)
            or producer.phase != "intermediate"
            or producer.distinct_all
            or producer.id in exposed_roots
            or len(parents[producer.id]) != 1
        ):
            continue
        consumer = nodes[next(iter(parents[producer.id]))]
        if (
            not isinstance(consumer, Aggregate)
            or consumer.phase != "final"
            or consumer.input != producer.id
            or consumer.keys != producer.keys
            or consumer.distinct_all
        ):
            continue

        for producer_trait in producer.aggregates:
            if (
                producer_trait.function != "sum"
                or producer_trait.distinct
                or producer_trait.unwrap
                or not decimal.is_type(producer_trait.output_type)
                or producer_trait.output in consumer.keys
            ):
                continue
            uses = tuple(
                trait
                for trait in consumer.aggregates
                if trait.input == producer_trait.output
            )
            if len(uses) != 1:
                continue
            consumer_trait = uses[0]
            if (
                consumer_trait.function != "sum"
                or consumer_trait.distinct
                or consumer_trait.unwrap
                or consumer_trait.output_type != producer_trait.output_type
                or not decimal.is_type(consumer_trait.output_type)
            ):
                continue
            producers.add((producer.id, producer_trait.output))
            consumers.add((consumer.id, consumer_trait.output))
    return frozenset(producers), frozenset(consumers)


def _reject_correlated_limit_fanout(
    snapshot: Snapshot,
    nodes: Mapping[str, PlanNode],
    parents: Mapping[str, frozenset[str]],
) -> None:
    """Fail closed when two Limit branches observe one latent stream order."""

    cache: dict[str, frozenset[str]] = {}
    ordered: dict[str, bool] = {}

    def has_sequence(node_id: str) -> bool:
        if node_id not in ordered:
            node = nodes[node_id]
            if isinstance(node, Sort):
                result = True
            elif isinstance(node, (Project, Filter, OuterBind, Limit)):
                result = has_sequence(node.input)
            else:
                result = False
            ordered[node_id] = result
        return ordered[node_id]

    def reachable_limits(node_id: str) -> frozenset[str]:
        if node_id not in cache:
            limits = {node_id} if isinstance(nodes[node_id], Limit) else set()
            if not isinstance(nodes[node_id], (Sort, Aggregate, Join, UnionAll)):
                for parent in parents[node_id]:
                    limits.update(reachable_limits(parent))
            cache[node_id] = frozenset(limits)
        return cache[node_id]

    for child, consumers in parents.items():
        if snapshot.stage_graph is None and has_sequence(child):
            continue
        for left, right in combinations(sorted(consumers), 2):
            left_limits = reachable_limits(left)
            right_limits = reachable_limits(right)
            if left_limits - right_limits and right_limits - left_limits:
                distinct = (left_limits - right_limits) | (right_limits - left_limits)
                raise AnalysisError(
                    f"shared stream {child!r} feeds independently ordered Limit "
                    f"branches {', '.join(sorted(distinct))}; correlated fan-out "
                    "is not modeled"
                )
