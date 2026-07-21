"""Two-task execution semantics for the strict version-one StageGraph."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, TypeAlias

from . import smt
from .ir import (
    Scan,
    Snapshot,
    Stage,
    StageEdge,
    stage_input_slots,
    stage_task_counts,
    validate_snapshot,
)
from .relation import (
    Database,
    Evaluator as RelationEvaluator,
    NodeObserver,
    Relation,
    RelationFamily,
    Row,
    combine_families,
    limit_family,
    map_family,
    merge_family,
    single,
)
from .scalar import Encoder as ScalarEncoder, Value


TASKS = 2


class StageError(ValueError):
    """A valid snapshot uses StageGraph semantics not modeled by this evaluator."""


@dataclass(frozen=True, slots=True)
class Partitions:
    relations: tuple[RelationFamily, ...]


EdgeObserver: TypeAlias = Callable[[StageEdge, int, RelationFamily], None]


class Router:
    """Shared, deterministic symbolic partition choices for both snapshots."""

    def __init__(self, script: smt.Script) -> None:
        self.script = script
        self._sources: dict[tuple[str, int], smt.Term] = {}
        self._hashes: dict[tuple[str, tuple[str, ...]], smt.Function] = {}

    def source_task(self, table: str, slot: int) -> smt.Term:
        key = (table, slot)
        if key not in self._sources:
            self._sources[key] = self.script.fresh_constant(
                f"source_task:{table}:{slot}", smt.BOOL
            )
        return self._sources[key]

    def hash_task(self, edge: StageEdge, row: Row) -> smt.Term:
        values = tuple(row.values[key] for key in edge.keys)
        assert edge.hash_function is not None
        key = (edge.hash_function, tuple(value.type for value in values))
        function = self._hashes.get(key)
        arguments = tuple(
            term
            for value in values
            for term in (value.is_null, _canonical(value))
        )
        if function is None:
            function = self.script.fresh_function(
                f"{edge.hash_function}:task",
                tuple(term.sort for term in arguments),
                smt.BOOL,
            )
            self._hashes[key] = function
        return function(*arguments)


class Evaluator:
    def __init__(
        self,
        snapshot: Snapshot,
        database: Database,
        scalar: ScalarEncoder,
        router: Router,
        node_observer: NodeObserver | None = None,
        edge_observer: EdgeObserver | None = None,
    ) -> None:
        if snapshot.stage_graph is None:
            raise StageError("snapshot has no StageGraph")
        self.snapshot = snapshot
        self.graph = snapshot.stage_graph
        self.database = database
        self.scalar = scalar
        self.router = router
        self.node_observer = node_observer
        self.edge_observer = edge_observer
        self.schemas = validate_snapshot(snapshot)
        self.task_counts = stage_task_counts(snapshot)
        self.stages = self.graph.stage_map()
        self.nodes = snapshot.plan.node_map()
        self.incoming = {
            stage.id: tuple(sorted(
                (edge for edge in self.graph.edges if edge.consumer == stage.id),
                key=lambda edge: edge.consumer_input,
            ))
            for stage in self.graph.stages
        }
        self.outputs: dict[tuple[str, int], Partitions] = {}
        self.complete: set[str] = set()

    def root(self) -> RelationFamily:
        self._evaluate_stage(self.graph.root_stage)
        family = _gather(self.outputs[(self.graph.root_stage, 0)].relations)
        columns = {column.name: column for column in family.columns}
        output = self.snapshot.plan.output
        return map_family(
            family,
            lambda relation: Relation(
                tuple(columns[name] for name in output),
                tuple(
                    Row(row.present, {name: row.values[name] for name in output})
                    for row in relation.rows
                ),
                sequence=relation.sequence,
                order=(
                    relation.order
                    if relation.order is not None
                    and all(item.column in output for item in relation.order)
                    else None
                ),
            ),
        )

    def _evaluate_stage(self, stage_id: str) -> None:
        if stage_id in self.complete:
            return
        stage = self.stages[stage_id]
        edges = self.incoming[stage_id]
        for edge in edges:
            self._evaluate_stage(edge.producer)

        if not edges:
            evaluated = self._source(stage)
        else:
            task_count = self.task_counts[stage_id]
            inputs: list[Partitions] = []
            parallel_offset = 0
            for edge in edges:
                source = self.outputs[(edge.producer, edge.producer_output)]
                connected = self._connect(edge, source, task_count, parallel_offset)
                inputs.append(connected)
                if self.edge_observer is not None:
                    for task, family in enumerate(connected.relations):
                        self.edge_observer(edge, task, family)
                if edge.kind == "union_all" and edge.parallel:
                    parallel_offset = (parallel_offset + len(source.relations)) % task_count
            slots = stage_input_slots(self.snapshot.plan, stage)
            evaluators = [
                RelationEvaluator(
                    self.snapshot,
                    self.database,
                    self.scalar,
                    {
                        (parent, child): inputs[ordinal].relations[task]
                        for ordinal, (parent, child, _) in enumerate(slots)
                    },
                    choice_scope=f"stage:{stage.id}:task:{task}",
                    node_observer=self.node_observer,
                )
                for task in range(task_count)
            ]
            evaluated = {
                output.index: Partitions(tuple(evaluator.node(output.node) for evaluator in evaluators))
                for output in stage.outputs
            }

        for index, partitions in evaluated.items():
            self.outputs[(stage.id, index)] = partitions
        self.complete.add(stage_id)

    def _source(self, stage: Stage) -> dict[int, Partitions]:
        evaluator = RelationEvaluator(
            self.snapshot,
            self.database,
            self.scalar,
            choice_scope=f"stage:{stage.id}:task:0",
            defer_pushed_limits=True,
            node_observer=self.node_observer if stage.source_storage is None else None,
        )
        if stage.source_storage is None:
            return {
                output.index: Partitions((evaluator.node(output.node),))
                for output in stage.outputs
            }

        scans = [self.nodes[node_id] for node_id in stage.nodes if isinstance(self.nodes[node_id], Scan)]
        if len(scans) != 1:
            raise StageError(f"source stage {stage.id!r} does not contain exactly one scan")
        scan = scans[0]
        raw_scan = evaluator.node(scan.id)

        def source_partition(relation: Relation, task_index: int) -> Relation:
            rows = []
            for slot, row in enumerate(relation.rows):
                task = self.router.source_task(scan.table, slot)
                belongs = smt.not_(task) if task_index == 0 else task
                rows.append(Row(smt.and_(row.present, belongs), row.values))
            return Relation(
                relation.columns,
                tuple(rows),
                sequence=relation.sequence,
                order=relation.order,
            )

        scan_partitions = tuple(
            map_family(
                raw_scan,
                lambda relation, task=task: source_partition(relation, task),
            )
            for task in range(TASKS)
        )
        if scan.pushed_limit is not None:
            scan_partitions = tuple(
                limit_family(
                    partition,
                    scan.pushed_limit,
                    None,
                    f"stage:{stage.id}:task:{task}:scan:{scan.id}:pushed_limit",
                )
                for task, partition in enumerate(scan_partitions)
            )
        evaluators = tuple(
            RelationEvaluator(
                self.snapshot,
                self.database,
                self.scalar,
                node_overrides={scan.id: partition},
                choice_scope=f"stage:{stage.id}:task:{task}",
                defer_pushed_limits=True,
                node_observer=self.node_observer,
            )
            for task, partition in enumerate(scan_partitions)
        )
        return {
            output.index: Partitions(
                tuple(task_evaluator.node(output.node) for task_evaluator in evaluators)
            )
            for output in stage.outputs
        }

    def _connect(
        self,
        edge: StageEdge,
        source: Partitions,
        tasks: int,
        parallel_offset: int,
    ) -> Partitions:
        if edge.kind == "map":
            if len(source.relations) != tasks:
                raise StageError("map connection cannot change task count")
            return source
        if edge.kind == "broadcast":
            family = _gather(source.relations)
            return Partitions(tuple(family for _ in range(tasks)))
        if edge.kind == "hash_shuffle":
            family = _gather(source.relations)
            if tasks == 1:
                return Partitions((family,))
            if tasks != TASKS:
                raise StageError("hash shuffle exceeds the two-task bound")

            def hash_partition(relation: Relation, task_index: int) -> Relation:
                rows = []
                for row in relation.rows:
                    task = self.router.hash_task(edge, row)
                    belongs = smt.not_(task) if task_index == 0 else task
                    rows.append(Row(smt.and_(row.present, belongs), row.values))
                return Relation(
                    relation.columns,
                    tuple(rows),
                    sequence=relation.sequence,
                    order=relation.order,
                )

            return Partitions(
                tuple(
                    map_family(
                        family,
                        lambda relation, task=task: hash_partition(relation, task),
                    )
                    for task in range(tasks)
                )
            )
        if edge.kind == "union_all" and not edge.parallel:
            if tasks != 1:
                raise StageError("serial union-all requires one consumer task")
            return Partitions((_gather(source.relations),))
        if edge.kind == "union_all" and edge.parallel:
            if tasks not in {1, TASKS}:
                raise StageError("parallel union-all exceeds the two-task bound")
            columns = source.relations[0].columns
            partitions: list[RelationFamily | None] = [None] * tasks
            for producer_task, family in enumerate(source.relations):
                if family.columns != columns:
                    raise StageError("connection input schemas differ")
                target = (parallel_offset + producer_task) % tasks
                current = partitions[target]
                partitions[target] = (
                    family if current is None else _gather((current, family))
                )
            return Partitions(tuple(
                family if family is not None else single(Relation(columns, ()))
                for family in partitions
            ))
        if edge.kind == "merge":
            if tasks != 1:
                raise StageError("merge connection requires one consumer task")
            columns = source.relations[0].columns

            def merge_inputs(relations: tuple[Relation, ...]) -> Relation:
                for relation in relations:
                    _require_merge_order(relation, edge)
                return Relation(
                    columns,
                    tuple(row for relation in relations for row in relation.rows),
                )

            groups = []
            next_index = 0
            for family in source.relations:
                size = len(family.outcomes[0].relation.rows)
                groups.append(tuple(range(next_index, next_index + size)))
                next_index += size
            gathered = combine_families(source.relations, merge_inputs)
            return Partitions((
                merge_family(
                    gathered,
                    edge.order,
                    tuple(groups),
                    f"merge:{edge.id}",
                ),
            ))
        raise AssertionError(f"unknown connection kind {edge.kind!r}")


def _canonical(value: Value) -> smt.Term:
    default = smt.FALSE if value.value.sort == smt.BOOL else smt.ZERO
    return smt.ite(value.is_null, default, value.value)


def _gather(families: tuple[RelationFamily, ...]) -> RelationFamily:
    if not families:
        raise StageError("connection has no producer tasks")
    columns = families[0].columns
    if any(family.columns != columns for family in families[1:]):
        raise StageError("connection input schemas differ")
    return combine_families(
        families,
        lambda relations: Relation(
            columns,
            tuple(row for relation in relations for row in relation.rows),
            sequence=len(relations) == 1 and relations[0].sequence,
            order=relations[0].order if len(relations) == 1 else None,
        ),
    )


def _require_merge_order(relation: Relation, edge: StageEdge) -> None:
    if not relation.sequence or relation.order is None:
        raise StageError(f"merge edge {edge.id!r} input is not ordered")
    if len(relation.order) != len(edge.order):
        raise StageError(f"merge edge {edge.id!r} input order arity differs")
    for actual, expected in zip(relation.order, edge.order):
        if (
            actual.ascending != expected.ascending
            or actual.nulls_first != expected.nulls_first
        ):
            raise StageError(f"merge edge {edge.id!r} input order differs")
        if actual.column == expected.column:
            continue
        if any(
            actual.column not in row.values or expected.column not in row.values
            for row in relation.rows
        ):
            raise StageError(f"merge edge {edge.id!r} input order columns differ")
        if any(
            row.values[actual.column] != row.values[expected.column]
            for row in relation.rows
        ):
            raise StageError(f"merge edge {edge.id!r} input order columns differ")
