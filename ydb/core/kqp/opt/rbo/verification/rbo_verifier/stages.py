"""Two-task execution semantics for the strict version-one StageGraph."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, TypeAlias

from . import smt
from .ir import (
    Column,
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
    Occurrence,
    PartitionFact,
    Relation,
    RelationFamily,
    Row,
    combine_families,
    limit_family,
    map_family,
    merge_family,
    single,
)
from .scalar import DecimalAverageState, Encoder as ScalarEncoder, Value
from .types import family


TASKS = 2
MAX_EXPLICIT_TASK_COPY_ROWS = 8


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
        # String and Utf8 have the same raw-byte hash in MiniKQL.  Sharing the
        # symbolic function is required when comparison-compatible cross-type
        # keys carry the same bytes through independently typed plans.
        key_types = tuple(
            "String/Utf8" if family(value.type) == "string" else value.type
            for value in values
        )
        key = (edge.hash_function, key_types)
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
                    Row(
                        row.present,
                        {name: row.values[name] for name in output},
                        row.occurrence,
                        row.partition_facts,
                    )
                    for row in relation.rows
                ),
                sequence=relation.sequence,
                order=(
                    relation.order
                    if relation.order is not None
                    and all(item.column in output for item in relation.order)
                    else None
                ),
                ordinals=relation.ordinals,
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
                rows.append(
                    Row(
                        smt.and_(row.present, belongs),
                        row.values,
                        row.occurrence,
                        row.partition_facts
                        | frozenset((PartitionFact(task, task_index == 1),)),
                    )
                )
            return Relation(
                relation.columns,
                tuple(rows),
                sequence=relation.sequence,
                order=relation.order,
                ordinals=relation.ordinals,
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
                    rows.append(
                        Row(
                            smt.and_(row.present, belongs),
                            row.values,
                            row.occurrence,
                            row.partition_facts
                            | frozenset((PartitionFact(task, task_index == 1),)),
                        )
                    )
                return Relation(
                    relation.columns,
                    tuple(rows),
                    sequence=relation.sequence,
                    order=relation.order,
                    ordinals=relation.ordinals,
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
                    ordinals=tuple(
                        ordinal
                        for relation in relations
                        for ordinal in _sequence_ordinals(relation)
                    ),
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
                    self.scalar.script,
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

    def gather(relations: tuple[Relation, ...]) -> Relation:
        if len(relations) == 1:
            relation = relations[0]
            return Relation(
                columns,
                relation.rows,
                sequence=relation.sequence,
                order=relation.order,
                ordinals=relation.ordinals,
            )
        rows = tuple(row for relation in relations for row in relation.rows)
        return Relation(
            columns,
            _compact_exclusive_rows(
                rows,
                columns,
                merge_conditional_values=(
                    len(rows) > MAX_EXPLICIT_TASK_COPY_ROWS
                ),
            ),
        )

    return combine_families(families, gather)


def _compact_exclusive_rows(
    rows: tuple[Row, ...],
    columns: tuple[Column, ...],
    *,
    merge_conditional_values: bool = True,
) -> tuple[Row, ...]:
    """Coalesce task copies only when routing proves pairwise exclusivity.

    Broadcast copies have no contradictory partition fact and therefore retain
    their SQL bag multiplicity.  Source and hash partitions carry opposite facts,
    so gathering them can recover one logical occurrence without duplicating a
    guarded slot for every task.  Equal values always compact.  Small differing
    task-local states may remain explicit because their simpler aggregate terms
    are substantially easier for the quantifier-free solver; both shapes denote
    the same exact bag.
    """

    groups: list[list[Row]] = []
    by_occurrence: dict[Occurrence, list[list[Row]]] = {}
    for row in rows:
        if row.occurrence is None:
            groups.append([row])
            continue
        candidates = by_occurrence.setdefault(row.occurrence, [])
        for group in candidates:
            if all(_partition_exclusive(row, other) for other in group) and (
                merge_conditional_values
                or all(_same_values(row, other, columns) for other in group)
            ):
                group.append(row)
                break
        else:
            group = [row]
            candidates.append(group)
            groups.append(group)

    return tuple(_merge_exclusive_rows(group, columns) for group in groups)


def _partition_exclusive(left: Row, right: Row) -> bool:
    return any(
        left_fact.term == right_fact.term
        and left_fact.value != right_fact.value
        for left_fact in left.partition_facts
        for right_fact in right.partition_facts
    )


def _same_values(
    left: Row,
    right: Row,
    columns: tuple[Column, ...],
) -> bool:
    return all(
        left.values[column.name].type == right.values[column.name].type
        and left.values[column.name].is_null == right.values[column.name].is_null
        and left.values[column.name].value == right.values[column.name].value
        and left.values[column.name].decimal_average_state
        == right.values[column.name].decimal_average_state
        for column in columns
    )


def _merge_exclusive_rows(rows: list[Row], columns: tuple[Column, ...]) -> Row:
    if len(rows) == 1:
        return rows[0]
    occurrence = rows[0].occurrence
    if occurrence is None or any(row.occurrence != occurrence for row in rows):
        raise StageError("exclusive row compaction mixed logical occurrences")
    if any(
        not _partition_exclusive(left, right)
        for index, left in enumerate(rows)
        for right in rows[index + 1 :]
    ):
        raise StageError("exclusive row compaction received overlapping task copies")

    values = {}
    for column in columns:
        alternatives = [row.values[column.name] for row in rows]
        if any(value.type != alternatives[0].type for value in alternatives[1:]):
            raise StageError("exclusive row compaction received different value types")
        bounds = [value.decimal_finite_abs_bound for value in alternatives]
        bound = (
            None
            if any(item is None for item in bounds)
            else max(item for item in bounds if item is not None)
        )
        is_null = alternatives[-1].is_null
        value = alternatives[-1].value
        for row, alternative in reversed(list(zip(rows[:-1], alternatives[:-1]))):
            is_null = smt.ite(row.present, alternative.is_null, is_null)
            value = smt.ite(row.present, alternative.value, value)
        average_states = [
            alternative.decimal_average_state
            for alternative in alternatives
        ]
        average_state = None
        if any(state is not None for state in average_states):
            if any(state is None for state in average_states):
                raise StageError(
                    "exclusive row compaction mixed Decimal avg state and scalar values"
                )
            states = [state for state in average_states if state is not None]
            if any(state.sum_type != states[0].sum_type for state in states[1:]):
                raise StageError(
                    "exclusive row compaction received different Decimal avg state types"
                )
            state_sum = states[-1].sum
            state_count = states[-1].count
            for row, state in reversed(list(zip(rows[:-1], states[:-1]))):
                state_sum = smt.ite(row.present, state.sum, state_sum)
                state_count = smt.ite(row.present, state.count, state_count)
            average_state = DecimalAverageState(
                sum_type=states[0].sum_type,
                sum=state_sum,
                count=state_count,
                finite_abs_bound=max(state.finite_abs_bound for state in states),
                count_bound=max(state.count_bound for state in states),
            )
        values[column.name] = Value(
            alternatives[0].type,
            is_null,
            value,
            bound,
            average_state,
        )

    common_facts = set(rows[0].partition_facts)
    for row in rows[1:]:
        common_facts.intersection_update(row.partition_facts)
    return Row(
        smt.or_(*(row.present for row in rows)),
        values,
        occurrence,
        frozenset(common_facts),
    )


def _sequence_ordinals(relation: Relation) -> tuple[smt.Term, ...]:
    if not relation.sequence:
        raise StageError("merge producer is not a sequence")
    if relation.ordinals is not None:
        return relation.ordinals
    return tuple(smt.int_value(index) for index in range(len(relation.rows)))


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
