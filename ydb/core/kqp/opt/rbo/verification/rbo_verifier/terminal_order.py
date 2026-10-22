"""Private completeness certificate for one terminal Sort/local-Sort+Merge.

For a common visible total order K, the complete sorted language S_K(B)
determines and is determined by its bag B.  Independent complete local Sorts
followed by a producer-order-preserving Merge realize every sequence in
S_K(sum B_i): restrict a desired global sequence to each producer, choose those
local orders, and interleave them back.  This module admits only a choice-free,
error-free equijoin prefix and direct visible aliases; generic order metadata
alone is deliberately insufficient evidence for this theorem.
"""
from __future__ import annotations

from dataclasses import dataclass, replace

from .analysis import ValidatedPlan
from .ir import (
    Expr, Join, Project, Projection, Scan, Snapshot, SnapshotError, Sort,
    StageEdge, StageOutput,
)
from .types import DOUBLE, is_ordered_type


@dataclass(frozen=True, slots=True)
class _VisibleKey:
    output: str
    type: str
    nullable: bool
    ascending: bool
    nulls_first: bool


@dataclass(frozen=True, slots=True)
class _Terminal:
    sort: Sort
    projects: tuple[Project, ...]  # Root first.
    scans: tuple[Scan, ...]
    join: Join
    visible_order: tuple[_VisibleKey, ...]


@dataclass(frozen=True, slots=True)
class Certificate:
    """An internal paired model reduction, bound to the exact validated inputs."""

    original_before: Snapshot
    original_after: Snapshot
    before: ValidatedPlan
    after: ValidatedPlan
    visible_order: tuple[_VisibleKey, ...]


def _direct_project(node: Project) -> bool:
    return node.ordered and all(
        projection.expression.kind == "column"
        and projection.expression == Expr("column", column=projection.expression.column)
        and not projection.error_on_null
        and not projection.require_total
        and projection.require_at_most_one is None
        for projection in node.columns
    )


def _terminal(validated: ValidatedPlan) -> _Terminal | None:
    snapshot = validated.snapshot
    if snapshot.semantic_mode is not None or snapshot.plan.subplans:
        return None
    nodes = snapshot.plan.node_map()
    scans = tuple(node for node in nodes.values() if isinstance(node, Scan))
    joins = tuple(node for node in nodes.values() if isinstance(node, Join))
    sorts = tuple(node for node in nodes.values() if isinstance(node, Sort))
    if len(scans) != 2 or len(joins) != 1 or len(sorts) != 1:
        return None
    if any(not isinstance(node, (Scan, Join, Project, Sort)) for node in nodes.values()):
        return None
    if any(node.predicate is not None or node.pushed_limit is not None or node.predicate_columns
           for node in scans):
        return None
    if any(isinstance(node, Project) and not _direct_project(node) for node in nodes.values()):
        return None
    # No aggregate/Double carrier interpretation or comparison tags are erased.
    if any(column.type == DOUBLE or column.integral_avg_rank
           for schema in validated.schemas.values() for column in schema.values()):
        return None
    join = joins[0]
    if (join.kind not in {"inner", "left"} or not join.keys
            or join.predicate != Expr("literal", value=True, result_type="Bool", nullable=False)):
        return None
    if {join.left, join.right} != {node.id for node in scans}:
        return None
    sort = sorts[0]
    if sort.limit is not None or not sort.order:
        return None
    expected_phase = "undefined" if snapshot.stage_graph is None else "intermediate"
    if sort.phase != expected_phase:
        return None

    # Trace only direct ordered projections, retaining an actual visible alias
    # for every key.  A hidden key never becomes an assumed visible expression.
    aliases = {name: name for name in snapshot.plan.output}
    terminal_projects: list[Project] = []
    node = nodes[snapshot.plan.root]
    while isinstance(node, Project):
        terminal_projects.append(node)
        projected = {p.output: p.expression.column for p in node.columns}
        aliases = {output: projected[column] for output, column in aliases.items()}
        node = nodes[node.input]
    if node is not sort:
        return None
    visible_order = []
    for key in sort.order:
        # Decimal uses the existing total raw-code Sort comparator, including
        # -Inf/+Inf/NaN.  Ordinary SQL Decimal equality is not used here.
        column = validated.schemas[sort.input][key.column]
        if key.comparison is not None or not is_ordered_type(column.type):
            return None
        visible = next((output for output, source in aliases.items() if source == key.column), None)
        if visible is None:
            return None
        visible_order.append(_VisibleKey(
            visible, column.type, column.nullable, key.ascending, key.nulls_first,
        ))

    # The complete rootward chain is the only consumer of the Sort.  Reject
    # fan-out even when extra consumers eventually feed the same result root.
    expected_child = sort.id
    for parent in reversed(terminal_projects):
        actual_parents = tuple(n.id for n in nodes.values()
                               if isinstance(n, Project) and n.input == expected_child)
        if actual_parents != (parent.id,):
            return None
        expected_child = parent.id
    if expected_child != snapshot.plan.root:
        return None
    return _Terminal(sort, tuple(terminal_projects), scans, join, tuple(visible_order))


def _merge_edge(snapshot: Snapshot, terminal: _Terminal) -> StageEdge | None:
    graph = snapshot.stage_graph
    if graph is None or len(graph.stages) != 4 or len(graph.edges) != 3:
        return None
    stages = graph.stage_map()
    root = stages[graph.root_stage]
    # Only direct projections may follow the Merge, in its single consumer.
    if (not terminal.projects or root.source_storage is not None
            or root.nodes != tuple(p.id for p in reversed(terminal.projects))
            or root.inputs != (terminal.sort.id,)
            or root.outputs != (StageOutput(0, snapshot.plan.root),)):
        return None
    merged = tuple(edge for edge in graph.edges if edge.kind == "merge")
    if len(merged) != 1:
        return None
    edge = merged[0]
    if (edge.consumer != root.id or edge.consumer_input != 0 or edge.producer_output != 0
            or edge.order != terminal.sort.order):
        return None
    producer = stages[edge.producer]
    if (producer.source_storage is not None
            or producer.nodes != (terminal.join.id, terminal.sort.id)
            or producer.inputs != (terminal.join.left, terminal.join.right)
            or producer.outputs != (StageOutput(0, terminal.sort.id),)
            or terminal.sort.input != terminal.join.id):
        return None
    source_stages = {}
    for scan in terminal.scans:
        candidates = tuple(stage for stage in graph.stages if scan.id in stage.nodes)
        if len(candidates) != 1:
            return None
        stage = candidates[0]
        if (stage.nodes != (scan.id,) or stage.inputs
                or stage.outputs != (StageOutput(0, scan.id),)
                or stage.source_storage not in {"row", "column"}):
            return None
        source_stages[scan.id] = stage.id
    if len(set(source_stages.values()) | {producer.id, root.id}) != 4:
        return None
    for port, scan_id in enumerate((terminal.join.left, terminal.join.right)):
        inputs = tuple(e for e in graph.edges if e.consumer == producer.id and e.consumer_input == port)
        if len(inputs) != 1:
            return None
        incoming = inputs[0]
        if (incoming.kind != "hash_shuffle" or incoming.producer != source_stages[scan_id]
                or incoming.producer_output != 0):
            return None
        # Keep the actual keys/hash function, including an incorrect optimizer
        # choice.  The existing routed bag proof must detect that mismatch.
    return edge


def _bag_model(validated: ValidatedPlan, terminal: _Terminal, merge: StageEdge | None) -> ValidatedPlan:
    snapshot = validated.snapshot
    identity = Project(
        terminal.sort.id, terminal.sort.input,
        tuple(Projection(name, Expr("column", column=name))
              for name in validated.schemas[terminal.sort.input]),
        ordered=False,
    )
    graph = snapshot.stage_graph
    if merge is not None:
        assert graph is not None
        graph = replace(graph, edges=tuple(
            replace(edge, kind="union_all", parallel=False, order=()) if edge is merge else edge
            for edge in graph.edges
        ))
    return ValidatedPlan(replace(snapshot, stage_graph=graph, plan=replace(
        snapshot.plan,
        nodes=tuple(identity if node is terminal.sort else node for node in snapshot.plan.nodes),
    )))


def certify(before: ValidatedPlan, after: ValidatedPlan) -> Certificate | None:
    """Return an exact paired bag model, or retain the generic sequence path.

    Original admission is mandatory before this call.  The returned copied
    snapshots undergo ordinary validation too; no caller-provided certificate
    or new semantic wire flag is accepted.
    """
    if before.snapshot.stage_graph is not None or after.snapshot.stage_graph is None:
        return None
    if before.snapshot.plan.output != after.snapshot.plan.output:
        return None
    if before.output_schema != after.output_schema:
        return None
    left, right = _terminal(before), _terminal(after)
    if left is None or right is None or left.visible_order != right.visible_order:
        return None
    edge = _merge_edge(after.snapshot, right)
    if edge is None:
        return None
    try:
        left_bag = _bag_model(before, left, None)
        right_bag = _bag_model(after, right, edge)
    except SnapshotError:
        # A valid input that does not fit the replacement validator stays on
        # the existing exact semantics instead of failing or broadening scope.
        return None
    return Certificate(before.snapshot, after.snapshot, left_bag, right_bag, left.visible_order)
