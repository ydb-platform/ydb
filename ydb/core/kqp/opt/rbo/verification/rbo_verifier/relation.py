"""Bounded bag semantics for the version-one relational operators."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations, permutations
from math import factorial
from typing import Callable, Iterator, Mapping, TypeAlias

from . import decimal, smt
from .ir import (
    Aggregate,
    AggregateTrait,
    Column,
    EmptySource,
    Expr,
    ExistsSubplan,
    Filter,
    Join,
    Limit,
    PlanNode,
    Project,
    Scan,
    ScalarSubplan,
    Snapshot,
    Sort,
    SortOrder,
    Subplan,
    UnionAll,
    plan_node_inputs,
    validate_snapshot,
)
from .scalar import DecimalAverageState, Encoder as ScalarEncoder
from .scalar import Value, date_domain, integer_domain, smt_sort
from .types import DATE, family, is_decimal_type, is_ordered_type


@dataclass(frozen=True, slots=True)
class Occurrence:
    """One logical bag occurrence, independent of task routing copies."""

    operation: str
    node: str
    ordinal: int | None = None
    inputs: tuple["Occurrence", ...] = ()


@dataclass(frozen=True, slots=True)
class PartitionFact:
    """A routing choice whose value is implied whenever a row is present."""

    term: smt.Term
    value: bool

    def __post_init__(self) -> None:
        if self.term.sort != smt.BOOL:
            raise ValueError("partition fact must use an SMT Boolean")


@dataclass(frozen=True, slots=True)
class Row:
    present: smt.Term
    values: Mapping[str, Value]
    occurrence: Occurrence | None = None
    partition_facts: frozenset[PartitionFact] = frozenset()


@dataclass(frozen=True, slots=True)
class Relation:
    columns: tuple[Column, ...]
    rows: tuple[Row, ...]
    sequence: bool = False
    order: tuple[SortOrder, ...] | None = None
    ordinals: tuple[smt.Term, ...] | None = None

    def __post_init__(self) -> None:
        _require_relation_rows(len(self.rows), "relation")
        if self.ordinals is not None:
            if len(self.ordinals) != len(self.rows):
                raise ValueError("relation ordinals must align with rows")
            if any(ordinal.sort != smt.INT for ordinal in self.ordinals):
                raise ValueError("relation ordinals must be SMT integers")


@dataclass(frozen=True, slots=True)
class BoundedChoice:
    """One globally inspectable, locally quantified relational choice."""

    term: smt.Term
    bound: int

    def __post_init__(self) -> None:
        if self.term.operation != "symbol" or self.term.sort != smt.INT:
            raise ValueError("bounded choice must be a named SMT integer")
        if type(self.bound) is not int or self.bound <= 0:
            raise ValueError("bounded choice bound must be positive")


@dataclass(frozen=True, slots=True)
class Outcome:
    """One enabled query observation and its correlated relational choices."""

    enabled: smt.Term
    relation: Relation
    error: smt.Term
    decisions: tuple[tuple[str, int], ...] = ()
    choices: tuple[BoundedChoice, ...] = ()

    def __post_init__(self) -> None:
        if self.enabled.sort != smt.BOOL:
            raise ValueError("outcome enabled condition must be Boolean")
        if self.error.sort != smt.BOOL:
            raise ValueError("outcome error condition must be Boolean")


@dataclass(frozen=True, slots=True)
class RelationFamily:
    """All bags a plan may produce for one symbolic database."""

    outcomes: tuple[Outcome, ...]

    @property
    def columns(self) -> tuple[Column, ...]:
        if not self.outcomes:
            raise RelationError("relation family has no outcomes")
        columns = self.outcomes[0].relation.columns
        if any(outcome.relation.columns != columns for outcome in self.outcomes[1:]):
            raise RelationError("relation-family outcome schemas differ")
        return columns

    @property
    def sequence(self) -> bool:
        if not self.outcomes:
            raise RelationError("relation family has no outcomes")
        sequence = self.outcomes[0].relation.sequence
        if any(outcome.relation.sequence != sequence for outcome in self.outcomes[1:]):
            raise RelationError("relation-family outcomes disagree on sequence semantics")
        return sequence

    def certain(self) -> Relation:
        """Return the sole unconditional relation, primarily for diagnostics/tests."""

        if (
            len(self.outcomes) != 1
            or self.outcomes[0].enabled != smt.TRUE
            or self.outcomes[0].error != smt.FALSE
            or self.outcomes[0].decisions
            or self.outcomes[0].choices
        ):
            raise RelationError("relation has multiple, conditional, or error outcomes")
        return self.outcomes[0].relation


@dataclass(frozen=True, slots=True)
class SubplanOutcome:
    """One subplan relation plus any local cardinality error."""

    outcome: Outcome
    cardinality_error: smt.Term

    def __post_init__(self) -> None:
        if self.cardinality_error.sort != smt.BOOL:
            raise ValueError("subplan cardinality error condition must be Boolean")


@dataclass(frozen=True, slots=True)
class SubplanFamily:
    """Subplan outcomes whose local cardinality error is demand-gated later."""

    outcomes: tuple[SubplanOutcome, ...]


@dataclass(frozen=True, slots=True)
class _SubplanPartial:
    """One compatible source/subplan outcome product under construction."""

    enabled: smt.Term
    relations: tuple[Relation, ...]
    inherited_errors: tuple[smt.Term, ...]
    cardinality_errors: tuple[smt.Term, ...]
    decisions: tuple[tuple[str, int], ...]
    choices: tuple[BoundedChoice, ...]


NodeObserver: TypeAlias = Callable[[str, str, RelationFamily], None]


@dataclass(frozen=True, slots=True)
class MismatchBranch:
    """One exact, independently solvable part of the mismatch predicate."""

    name: str
    predicate: smt.Term


@dataclass(frozen=True, slots=True)
class FamilyMismatch:
    """Canonical mismatch formula and its exact distributive decomposition."""

    counterexample: smt.Term
    branches: tuple[MismatchBranch, ...]


@dataclass(frozen=True, slots=True)
class FamilyComparison:
    """The exact normalized outcome pairs used by family equivalence."""

    left: RelationFamily
    right: RelationFamily
    ordered: bool
    pair_equal: tuple[tuple[smt.Term, ...], ...]
    mismatch: FamilyMismatch


@dataclass(frozen=True, slots=True)
class WitnessCell:
    type: str
    is_null: smt.Term
    value: smt.Term


@dataclass(frozen=True, slots=True)
class WitnessRow:
    present: smt.Term
    cells: Mapping[str, WitnessCell]


class RelationError(ValueError):
    """A valid snapshot uses relational semantics not modeled by this evaluator."""


MAX_OUTCOME_ALTERNATIVES = 256
MAX_OUTCOME_COMPARISONS = 4096
MAX_ENUMERATED_SEQUENCE_ROWS = 3
MAX_RELATION_ROWS = 4096
MAX_RELATION_ROW_PAIRS = 16384


def _require_relation_rows(count: int, operation: str) -> None:
    if count > MAX_RELATION_ROWS:
        raise RelationError(
            f"{operation} requires {count} candidate rows, exceeding "
            f"the {MAX_RELATION_ROWS} row construction audit bound"
        )


def _require_relation_row_pairs(count: int, operation: str) -> None:
    if count > MAX_RELATION_ROW_PAIRS:
        raise RelationError(
            f"{operation} requires {count} candidate-row pairs, exceeding "
            f"the {MAX_RELATION_ROW_PAIRS} pair construction audit bound"
        )


def _unordered_row_pairs(count: int) -> int:
    return count * (count - 1) // 2


def _live_row_indices(rows: tuple[Row, ...]) -> tuple[int, ...]:
    """Return slots whose guards are not syntactically false."""

    return tuple(
        index
        for index, row in enumerate(rows)
        if row.present != smt.FALSE
    )


def _live_row_count(relation: Relation) -> int:
    return len(_live_row_indices(relation.rows))


class Database:
    """One shared symbolic catalog; self-joins reuse the same table rows."""

    def __init__(self, snapshot: Snapshot, row_bound: int, script: smt.Script) -> None:
        if row_bound < 0:
            raise ValueError("row bound must not be negative")
        if snapshot.tables:
            _require_relation_rows(row_bound, "database table")
        self.relations: dict[str, Relation] = {}
        self.witness: dict[str, tuple[WitnessRow, ...]] = {}
        for table in snapshot.tables:
            rows: list[Row] = []
            witness_rows: list[WitnessRow] = []
            for slot in range(row_bound):
                present = script.fresh_constant(f"{table.name}_{slot}_present", smt.BOOL)
                values: dict[str, Value] = {}
                cells: dict[str, WitnessCell] = {}
                for column in table.columns:
                    is_null = (
                        script.fresh_constant(f"{table.name}_{slot}_{column.name}_null", smt.BOOL)
                        if column.nullable
                        else smt.FALSE
                    )
                    value = script.fresh_constant(
                        f"{table.name}_{slot}_{column.name}_value",
                        smt_sort(column.type),
                    )
                    if family(column.type) == "string":
                        script.register_string_term(value)
                    elif column.type == DATE:
                        script.assert_global(date_domain(value))
                    elif family(column.type) == "int":
                        script.assert_global(integer_domain(value, column.type))
                    elif decimal.is_type(column.type):
                        script.assert_global(decimal.domain(value, column.type))
                    values[column.name] = Value(column.type, is_null, value)
                    cells[column.name] = WitnessCell(column.type, is_null, value)
                rows.append(
                    Row(
                        present,
                        values,
                        Occurrence("table", table.name, slot),
                    )
                )
                witness_rows.append(WitnessRow(present, cells))
            self.relations[table.name] = Relation(table.columns, tuple(rows))
            self.witness[table.name] = tuple(witness_rows)
            for key in table.unique_keys:
                for left_index, left_row in enumerate(rows):
                    for right_row in rows[left_index + 1 :]:
                        equal_columns: list[smt.Term] = []
                        for column_name in key.columns:
                            left = left_row.values[column_name]
                            right = right_row.values[column_name]
                            if key.nulls_distinct:
                                equal_columns.append(
                                    smt.and_(
                                        smt.not_(left.is_null),
                                        smt.not_(right.is_null),
                                        smt.eq(left.value, right.value),
                                    )
                                )
                            else:
                                equal_columns.append(
                                    smt.or_(
                                        smt.and_(left.is_null, right.is_null),
                                        smt.and_(
                                            smt.not_(left.is_null),
                                            smt.not_(right.is_null),
                                            smt.eq(left.value, right.value),
                                        ),
                                    )
                                )
                        script.assert_global(
                            smt.not_(
                                smt.and_(left_row.present, right_row.present, *equal_columns)
                            )
                        )


class Evaluator:
    def __init__(
        self,
        snapshot: Snapshot,
        database: Database,
        scalar: ScalarEncoder,
        edge_inputs: Mapping[tuple[str, int], RelationFamily] | None = None,
        node_overrides: Mapping[str, RelationFamily] | None = None,
        choice_scope: str = "logical",
        defer_pushed_limits: bool = False,
        node_observer: NodeObserver | None = None,
    ) -> None:
        self.snapshot = snapshot
        self.database = database
        self.scalar = scalar
        self.nodes = snapshot.plan.node_map()
        self.schemas = validate_snapshot(snapshot)
        _reject_correlated_limit_fanout(snapshot)
        self.cache: dict[str, RelationFamily] = dict(node_overrides or {})
        self.edge_inputs = edge_inputs or {}
        self.choice_scope = choice_scope
        self.defer_pushed_limits = defer_pushed_limits
        self.node_observer = node_observer
        self.observed_nodes: set[str] = set()
        self.subplans_by_consumer = {
            node_id: tuple(
                subplan
                for subplan in snapshot.plan.subplans
                if node_id in subplan.consumers
            )
            for node_id in {
                consumer
                for subplan in snapshot.plan.subplans
                for consumer in subplan.consumers
            }
        }
        self.subplan_families: dict[str, SubplanFamily] = {}

    def root(self) -> RelationFamily:
        family = self.node(self.snapshot.plan.root)
        columns_by_name = {column.name: column for column in family.columns}
        output = tuple(self.snapshot.plan.output)
        return map_family(
            family,
            lambda relation: Relation(
                columns=tuple(columns_by_name[name] for name in output),
                rows=tuple(
                    Row(
                        row.present,
                        {name: row.values[name] for name in output},
                        row.occurrence,
                        row.partition_facts,
                    )
                    for row in relation.rows
                ),
                sequence=relation.sequence,
                order=_retained_order(relation.order, output),
                ordinals=relation.ordinals,
            ),
        )

    def node(self, node_id: str) -> RelationFamily:
        if node_id not in self.cache:
            node = self.nodes[node_id]
            self.cache[node_id] = self._evaluate(node)
        family = self.cache[node_id]
        if self.node_observer is not None and node_id not in self.observed_nodes:
            self.node_observer(self.choice_scope, node_id, family)
            self.observed_nodes.add(node_id)
        return family

    def _evaluate(self, node: PlanNode) -> RelationFamily:
        if isinstance(node, EmptySource):
            return single(
                Relation(
                    (),
                    (Row(smt.TRUE, {}, Occurrence("empty", node.id)),),
                )
            )

        if isinstance(node, Scan):
            source = self.database.relations[node.table]
            source_columns = {column.name: column for column in source.columns}
            columns = tuple(
                Column(mapping.output, source_columns[mapping.source].type, source_columns[mapping.source].nullable)
                for mapping in node.columns
            )
            rows = []
            for row in source.rows:
                values = {
                    mapping.output: row.values[mapping.source]
                    for mapping in node.columns
                }
                present = row.present
                if node.predicate is not None:
                    present = smt.and_(
                        present,
                        self.scalar.is_true(
                            self.scalar.evaluate(node.predicate, values)
                        ),
                    )
                rows.append(
                    Row(
                        present,
                        values,
                        row.occurrence,
                        row.partition_facts,
                    )
                )
            family = single(Relation(columns, tuple(rows)))
            if node.pushed_limit is not None and not self.defer_pushed_limits:
                raise RelationError(
                    "pushed scan limits must be evaluated per column-source task"
                )
            return family

        if isinstance(node, Project):
            source = self._input(node.id, 0, node.input)
            columns = self._columns(node.id)
            return self._with_consumer_subplans(
                node.id,
                source,
                lambda relation, bindings: Relation(
                    columns,
                    tuple(
                        Row(
                            row.present,
                            {
                                projection.output: self.scalar.evaluate(
                                    projection.expression,
                                    dict(row.values) | bindings(row),
                                )
                                for projection in node.columns
                            },
                            row.occurrence,
                            row.partition_facts,
                        )
                        for row in relation.rows
                    ),
                    sequence=relation.sequence,
                    order=_projected_order(relation.order, node),
                    ordinals=relation.ordinals,
                ),
            )

        if isinstance(node, Filter):
            source = self._input(node.id, 0, node.input)
            return self._with_consumer_subplans(
                node.id,
                source,
                lambda relation, bindings: Relation(
                    relation.columns,
                    tuple(
                        Row(
                            smt.and_(
                                row.present,
                                self.scalar.is_true(
                                    self.scalar.evaluate(
                                        node.predicate,
                                        dict(row.values) | bindings(row),
                                    )
                                ),
                            ),
                            row.values,
                            row.occurrence,
                            row.partition_facts,
                        )
                        for row in relation.rows
                    ),
                    sequence=relation.sequence,
                    order=relation.order,
                    ordinals=relation.ordinals,
                ),
            )

        if isinstance(node, Limit):
            return limit_family(
                self._input(node.id, 0, node.input),
                node.count,
                node.offset,
                self.scalar.script,
                f"{self.choice_scope}:limit:{node.id}",
                ensure_at_most_one=node.ensure_at_most_one,
            )

        if isinstance(node, Sort):
            family = sort_family(
                self._input(node.id, 0, node.input),
                node.order,
                self.scalar.script,
                f"{self.choice_scope}:sort:{node.id}",
            )
            if node.limit is not None:
                family = limit_family(
                    family,
                    node.limit,
                    None,
                    self.scalar.script,
                    f"{self.choice_scope}:topsort:{node.id}",
                )
            return family

        if isinstance(node, Aggregate):
            return map_family(
                self._input(node.id, 0, node.input),
                lambda relation: self._aggregate(node, relation),
            )

        if isinstance(node, Join):
            return combine_families(
                (
                    self._input(node.id, 0, node.left),
                    self._input(node.id, 1, node.right),
                ),
                lambda relations: self._join(node, relations[0], relations[1]),
            )

        if isinstance(node, UnionAll):
            sources = tuple(
                self._input(node.id, index, item.node)
                for index, item in enumerate(node.inputs)
            )
            if node.ordered:
                sources = tuple(
                    source
                    if source.sequence
                    else _as_sequence_family(
                        source,
                        self.scalar.script,
                        f"{self.choice_scope}:union:{node.id}:input:{index}",
                    )
                    for index, source in enumerate(sources)
                )

            def union(relations: tuple[Relation, ...]) -> Relation:
                _require_relation_rows(
                    sum(len(source.rows) for source in relations),
                    "union-all",
                )
                rows: list[Row] = []
                for input_ordinal, (item, source) in enumerate(
                    zip(node.inputs, relations)
                ):
                    for row in source.rows:
                        rows.append(
                            Row(
                                row.present,
                                {
                                    output: row.values[input_name]
                                    for output, input_name in zip(
                                        node.output, item.columns
                                    )
                                },
                                _derived_occurrence(
                                    "union_all",
                                    node.id,
                                    row.occurrence,
                                    ordinal=input_ordinal,
                                ),
                                row.partition_facts,
                            )
                        )
                ordinals: tuple[smt.Term, ...] | None = None
                if node.ordered and any(
                    source.ordinals is not None for source in relations
                ):
                    ordinal_items: list[smt.Term] = []
                    prior_rows: list[Row] = []
                    for source in relations:
                        offset = smt.add(
                            *(
                                smt.ite(row.present, smt.ONE, smt.ZERO)
                                for row in prior_rows
                            )
                        )
                        ordinal_items.extend(
                            smt.add(offset, _compressed_rank(source, index))
                            for index in range(len(source.rows))
                        )
                        prior_rows.extend(source.rows)
                    ordinals = tuple(ordinal_items)
                return Relation(
                    self._columns(node.id),
                    tuple(rows),
                    sequence=node.ordered,
                    ordinals=ordinals,
                )

            return combine_families(sources, union)

        raise AssertionError(f"unknown plan node {type(node).__name__}")

    def _aggregate(self, node: Aggregate, source: Relation) -> Relation:
        if node.distinct_all:
            raise RelationError("DistinctAll aggregate semantics are not modeled")
        if any(trait.distinct for trait in node.aggregates):
            raise RelationError("distinct aggregate semantics are not modeled")
        if any(trait.unwrap for trait in node.aggregates):
            raise RelationError("unwrapped aggregate semantics are not modeled")
        unsupported = sorted(
            {trait.function for trait in node.aggregates}
            - {"avg", "count", "max", "sum"}
        )
        if unsupported:
            raise RelationError(
                f"aggregate functions are not modeled: {', '.join(unsupported)}"
            )

        rows: list[Row] = []
        if not node.keys:
            matches = tuple(row.present for row in source.rows)
            present = smt.or_(*matches) if node.phase == "intermediate" else smt.TRUE
            rows.append(
                Row(
                    present,
                    self._aggregate_values(node, source, matches, None),
                    Occurrence("aggregate", node.id),
                    (
                        _common_partition_facts(source.rows)
                        if node.phase == "intermediate"
                        else frozenset()
                    ),
                )
            )
        else:
            rows.extend(self._grouped_aggregate_rows(node, source))
        return Relation(self._columns(node.id), tuple(rows))

    def _grouped_aggregate_rows(
        self,
        node: Aggregate,
        source: Relation,
    ) -> tuple[Row, ...]:
        row_count = len(source.rows)
        directional_pair_count = row_count * row_count
        classes = _aggregate_key_classes(node.keys, source.rows)
        class_count = len(classes)
        class_memberships = class_count * row_count
        class_comparisons = class_count * (class_count + 1) // 2
        class_pair_count = class_memberships + class_comparisons
        classes_fit = (
            class_memberships <= MAX_RELATION_ROW_PAIRS
            and class_comparisons <= MAX_RELATION_ROW_PAIRS
        )
        classes_cheaper = class_pair_count < directional_pair_count
        # Above the directional cap, classes are an exact way to stay within
        # the audit bound. Below it, change representation only when they
        # strictly reduce the audited pair construction.
        use_classes = (
            class_count < row_count
            and classes_fit
            and (
                directional_pair_count > MAX_RELATION_ROW_PAIRS
                or classes_cheaper
            )
        )
        if use_classes:
            _require_relation_row_pairs(
                class_memberships,
                "grouped aggregate class membership",
            )
            _require_relation_row_pairs(
                class_comparisons,
                "grouped aggregate class comparison",
            )
            return self._shared_grouped_aggregate_rows(node, source, classes)

        if directional_pair_count <= MAX_RELATION_ROW_PAIRS:
            _require_relation_row_pairs(
                directional_pair_count,
                "grouped aggregate",
            )
            rows: list[Row] = []
            for index, candidate in enumerate(source.rows):
                matches = tuple(
                    smt.and_(row.present, self._same_group(node, candidate, row))
                    for row in source.rows
                )
                earlier = tuple(
                    smt.and_(row.present, self._same_group(node, candidate, row))
                    for row in source.rows[:index]
                )
                rows.append(
                    Row(
                        smt.and_(
                            candidate.present,
                            smt.not_(smt.or_(*earlier)),
                        ),
                        self._aggregate_values(node, source, matches, candidate),
                        _derived_occurrence(
                            "aggregate",
                            node.id,
                            candidate.occurrence,
                        ),
                        candidate.partition_facts,
                    )
                )
            return tuple(rows)

        _require_relation_row_pairs(
            row_count * (row_count + 1) // 2,
            "grouped aggregate",
        )
        return self._shared_grouped_aggregate_rows(
            node,
            source,
            tuple((index,) for index in range(row_count)),
        )

    def _shared_grouped_aggregate_rows(
        self,
        node: Aggregate,
        source: Relation,
        classes: tuple[tuple[int, ...], ...],
    ) -> tuple[Row, ...]:
        representatives = tuple(
            source.rows[members[0]]
            for members in classes
        )
        # The classes partition all source indices.  Aggregate membership still
        # ranges over the original rows so duplicate bag occurrences survive.
        source_classes = [0] * len(source.rows)
        for class_index, members in enumerate(classes):
            for source_index in members:
                source_classes[source_index] = class_index

        # Null-safe equality is symmetric, but row presence is not. Share one
        # composite group-key comparison per unordered candidate-class pair.
        same_groups = {
            (left_index, right_index): self._same_group(
                node,
                representatives[left_index],
                representatives[right_index],
            )
            for left_index in range(len(classes))
            for right_index in range(left_index, len(classes))
        }

        def same_group(left_index: int, right_index: int) -> smt.Term:
            pair = (
                (left_index, right_index)
                if left_index <= right_index
                else (right_index, left_index)
            )
            return same_groups[pair]

        class_presence = tuple(
            smt.or_(*(source.rows[index].present for index in members))
            for members in classes
        )
        rows: list[Row] = []
        for class_index, members in enumerate(classes):
            candidate = representatives[class_index]
            matches = tuple(
                smt.and_(
                    row.present,
                    same_group(class_index, source_classes[source_index]),
                )
                for source_index, row in enumerate(source.rows)
            )
            earlier = tuple(
                (
                    matches[earlier_members[0]]
                    if len(earlier_members) == 1
                    else smt.and_(
                        class_presence[earlier_index],
                        same_group(class_index, earlier_index),
                    )
                )
                for earlier_index, earlier_members in enumerate(
                    classes[:class_index]
                )
            )
            member_rows = tuple(source.rows[index] for index in members)
            rows.append(
                Row(
                    smt.and_(
                        class_presence[class_index],
                        smt.not_(smt.or_(*earlier)),
                    ),
                    self._aggregate_values(node, source, matches, candidate),
                    (
                        _derived_occurrence(
                            "aggregate",
                            node.id,
                            candidate.occurrence,
                        )
                        if len(members) == 1
                        else None
                    ),
                    _common_partition_facts(member_rows),
                )
            )
        return tuple(rows)

    def _aggregate_values(
        self,
        node: Aggregate,
        source: Relation,
        matches: tuple[smt.Term, ...],
        candidate: Row | None,
    ) -> dict[str, Value]:
        values = (
            {}
            if candidate is None
            else {key: candidate.values[key] for key in node.keys}
        )
        for trait in node.aggregates:
            values[trait.output] = self._aggregate_value(
                node,
                trait,
                source,
                matches,
            )
        return values

    def _aggregate_value(
        self,
        node: Aggregate,
        trait: AggregateTrait,
        source: Relation,
        matches: tuple[smt.Term, ...],
    ) -> Value:
        non_null = tuple(
            smt.and_(matches[index], smt.not_(row.values[trait.input].is_null))
            for index, row in enumerate(source.rows)
        )
        if trait.function == "count":
            return Value(
                trait.output_type,
                smt.FALSE,
                smt.add(*(smt.ite(guard, smt.ONE, smt.ZERO) for guard in non_null)),
            )
        if trait.function == "max":
            guarded_values = tuple(
                (guard, row.values[trait.input])
                for guard, row in zip(non_null, source.rows)
                if guard != smt.FALSE
            )
            return Value(
                trait.output_type,
                smt.not_(smt.or_(*non_null)) if trait.output_nullable else smt.FALSE,
                decimal.aggregate_max(
                    tuple((guard, value.value) for guard, value in guarded_values)
                ),
                decimal_finite_abs_bound=max(
                    (_decimal_finite_abs_bound(value) for _, value in guarded_values),
                    default=0,
                ),
            )
        if trait.function == "sum":
            if decimal.is_type(trait.output_type):
                guarded_values = tuple(
                    (guard, row.values[trait.input])
                    for guard, row in zip(non_null, source.rows)
                    if guard != smt.FALSE
                )
                finite_abs_bound = sum(
                    _decimal_finite_abs_bound(value)
                    for _, value in guarded_values
                )
                result_type = decimal.parse_type(trait.output_type)
                assert result_type is not None
                if finite_abs_bound >= 10**result_type.precision:
                    raise RelationError(
                        f"Decimal sum may overflow its {trait.output_type} accumulator "
                        "within the current bound; non-associative overflow is not modeled"
                    )
                return Value(
                    trait.output_type,
                    smt.not_(smt.or_(*non_null))
                    if trait.output_nullable
                    else smt.FALSE,
                    decimal.sum_with_headroom(
                        tuple(
                            (guard, value.value)
                            for guard, value in guarded_values
                        ),
                        trait.output_type,
                        finite_abs_bound,
                    ),
                    decimal_finite_abs_bound=finite_abs_bound,
                )
            total = smt.add(
                *(
                    smt.ite(
                        guard,
                        _unwrap_sum(row.values[trait.input]),
                        smt.ZERO,
                    )
                    for guard, row in zip(non_null, source.rows)
                )
            )
            return Value(
                trait.output_type,
                smt.not_(smt.or_(*non_null)) if trait.output_nullable else smt.FALSE,
                _wrap_sum(total, trait.output_type),
            )
        if trait.function == "avg":
            assert trait.state is not None
            guarded_sums: list[tuple[smt.Term, smt.Term]] = []
            count_terms: list[smt.Term] = []
            finite_abs_bound = 0
            count_bound = 0
            for guard, row in zip(non_null, source.rows):
                if guard == smt.FALSE:
                    continue
                value = row.values[trait.input]
                if node.phase == "final":
                    state = value.decimal_average_state
                    if state is None or state.sum_type != trait.state.sum_type:
                        raise RelationError(
                            "final avg input does not carry its validated "
                            "intermediate Decimal state"
                        )
                    guarded_sums.append((guard, state.sum))
                    finite_abs_bound += state.finite_abs_bound
                    count_bound += state.count_bound
                    count_terms.append(smt.ite(guard, state.count, smt.ZERO))
                else:
                    guarded_sums.append((guard, value.value))
                    finite_abs_bound += _decimal_finite_abs_bound(value)
                    count_bound += 1
                    count_terms.append(smt.ite(guard, smt.ONE, smt.ZERO))

            state_type = decimal.parse_type(trait.state.sum_type)
            assert state_type is not None
            if finite_abs_bound >= 10**state_type.precision:
                raise RelationError(
                    f"Decimal avg sum may overflow its {trait.state.sum_type} "
                    "accumulator within the current bound; non-associative "
                    "overflow is not modeled"
                )
            if count_bound >= 1 << 64:
                raise RelationError(
                    "Decimal avg count may wrap its Uint64 accumulator "
                    "within the current bound"
                )
            total = decimal.sum_with_headroom(
                tuple(guarded_sums),
                trait.state.sum_type,
                finite_abs_bound,
            )
            count = smt.add(*count_terms)
            average = decimal.narrow_same_scale(
                decimal.divide(
                    total,
                    count,
                    trait.state.sum_type,
                    trait.state.count_type,
                ),
                trait.state.sum_type,
                trait.output_type,
            )
            output_type = decimal.parse_type(trait.output_type)
            assert output_type is not None
            return Value(
                trait.output_type,
                (
                    smt.not_(smt.or_(*non_null))
                    if trait.output_nullable
                    else smt.FALSE
                ),
                average,
                decimal_finite_abs_bound=min(
                    finite_abs_bound,
                    10**output_type.precision - 1,
                ),
                decimal_average_state=(
                    DecimalAverageState(
                        sum_type=trait.state.sum_type,
                        sum=total,
                        count=count,
                        finite_abs_bound=finite_abs_bound,
                        count_bound=count_bound,
                    )
                    if node.phase == "intermediate"
                    else None
                ),
            )
        raise AssertionError(f"unsupported aggregate function {trait.function!r}")

    def _same_group(self, node: Aggregate, left: Row, right: Row) -> smt.Term:
        return smt.and_(
            *(
                self.scalar.not_distinct(left.values[key], right.values[key])
                for key in node.keys
            )
        )

    def _input(self, parent: str, ordinal: int, child: str) -> RelationFamily:
        key = (parent, ordinal)
        return self.edge_inputs[key] if key in self.edge_inputs else self.node(child)

    def _with_consumer_subplans(
        self,
        node_id: str,
        source: RelationFamily,
        transform: Callable[
            [Relation, Callable[[Row], Mapping[str, Value]]],
            Relation,
        ],
    ) -> RelationFamily:
        subplans = self.subplans_by_consumer.get(node_id, ())
        if not subplans:
            return map_family(
                source,
                lambda relation: transform(relation, lambda _row: {}),
            )

        binding_families: list[SubplanFamily] = []
        for subplan in subplans:
            family = self.subplan_families.get(subplan.binding)
            if family is None:
                family = self._evaluate_subplan(subplan)
                self.subplan_families[subplan.binding] = family
            binding_families.append(family)

        partials = [
            _SubplanPartial(
                enabled=outcome.enabled,
                relations=(outcome.relation,),
                inherited_errors=(outcome.error,),
                cardinality_errors=(),
                decisions=outcome.decisions,
                choices=outcome.choices,
            )
            for outcome in source.outcomes
        ]
        for binding_family in binding_families:
            expanded: list[_SubplanPartial] = []
            for partial in partials:
                for subplan_outcome in binding_family.outcomes:
                    outcome = subplan_outcome.outcome
                    merged = _merge_decisions(
                        partial.decisions,
                        outcome.decisions,
                    )
                    if merged is None:
                        continue
                    expanded.append(
                        _SubplanPartial(
                            enabled=smt.and_(
                                partial.enabled,
                                outcome.enabled,
                            ),
                            relations=partial.relations
                            + (outcome.relation,),
                            inherited_errors=partial.inherited_errors
                            + (outcome.error,),
                            cardinality_errors=partial.cardinality_errors
                            + (subplan_outcome.cardinality_error,),
                            decisions=merged,
                            choices=_merge_choices(
                                partial.choices,
                                outcome.choices,
                            ),
                        )
                    )
                    if len(expanded) > MAX_OUTCOME_ALTERNATIVES:
                        raise RelationError(
                            "subplan outcome product exceeds "
                            f"the {MAX_OUTCOME_ALTERNATIVES} alternative audit bound"
                        )
            partials = expanded
        if not partials:
            raise RelationError("subplan binding family has no compatible outcomes")

        outcomes: list[Outcome] = []
        for partial in partials:
            exists_pairs = sum(
                len(partial.relations[0].rows)
                * len(partial.relations[index].rows)
                for index, subplan in enumerate(subplans, start=1)
                if isinstance(subplan, ExistsSubplan)
            )
            _require_relation_row_pairs(exists_pairs, "EXISTS evaluation")

            def bindings(row: Row) -> Mapping[str, Value]:
                return {
                    subplan.binding: self._subplan_value(
                        subplan,
                        row,
                        partial.relations[index],
                    )
                    for index, subplan in enumerate(subplans, start=1)
                }

            # A consumer row demands this binding's own cardinality check,
            # including through a dead scalar-expression branch. Errors already
            # produced inside the subplan remain observable without that row.
            demanded = smt.or_(
                *(row.present for row in partial.relations[0].rows)
            )
            outcomes.append(
                Outcome(
                    partial.enabled,
                    transform(partial.relations[0], bindings),
                    smt.or_(
                        *partial.inherited_errors,
                        *(
                            smt.and_(demanded, error)
                            for error in partial.cardinality_errors
                        ),
                    ),
                    partial.decisions,
                    partial.choices,
                )
            )
        return RelationFamily(
            tuple(outcomes),
        )

    def _evaluate_subplan(
        self,
        subplan: Subplan,
    ) -> SubplanFamily:
        if isinstance(subplan, ScalarSubplan):
            return self._evaluate_scalar_subplan(subplan)
        assert isinstance(subplan, ExistsSubplan)
        return SubplanFamily(
            tuple(
                SubplanOutcome(outcome, smt.FALSE)
                for outcome in self.node(subplan.root).outcomes
            )
        )

    def _subplan_value(
        self,
        subplan: Subplan,
        outer_row: Row,
        relation: Relation,
    ) -> Value:
        if isinstance(subplan, ScalarSubplan):
            return relation.rows[0].values[subplan.binding]
        assert isinstance(subplan, ExistsSubplan)
        matches = []
        for inner_row in relation.rows:
            match = inner_row.present
            if subplan.predicate is not None:
                assert subplan.dependency is not None
                dependency = subplan.dependency
                match = smt.and_(
                    match,
                    self.scalar.is_true(
                        self.scalar.evaluate(
                            subplan.predicate,
                            {dependency: outer_row.values[dependency]}
                            | dict(inner_row.values),
                        )
                    ),
                )
            matches.append(match)
        return Value("Bool", smt.FALSE, smt.or_(*matches))

    def _evaluate_scalar_subplan(
        self,
        subplan: ScalarSubplan,
    ) -> SubplanFamily:
        binding = subplan.binding
        family = self.node(subplan.root)
        column = Column(binding, subplan.output.type, True)
        outcomes: list[SubplanOutcome] = []
        for outcome in family.outcomes:
            selected = self.scalar.null(subplan.output.type)
            for row in outcome.relation.rows:
                candidate = row.values[subplan.output.column]
                if candidate.decimal_average_state is not None:
                    raise RelationError(
                        f"scalar subplan {binding!r} exposes an intermediate AVG state"
                    )
                selected = self._select_value(row.present, candidate, selected)
            present_count = smt.add(
                *(
                    smt.ite(row.present, smt.ONE, smt.ZERO)
                    for row in outcome.relation.rows
                )
            )
            outcomes.append(
                SubplanOutcome(
                    Outcome(
                        outcome.enabled,
                        Relation(
                            (column,),
                            (
                                Row(
                                    smt.TRUE,
                                    {binding: selected},
                                    Occurrence("scalar_subplan", binding),
                                ),
                            ),
                        ),
                        outcome.error,
                        outcome.decisions,
                        outcome.choices,
                    ),
                    smt.lt(smt.ONE, present_count),
                )
            )
        return SubplanFamily(tuple(outcomes))

    @staticmethod
    def _select_value(
        condition: smt.Term,
        selected: Value,
        fallback: Value,
    ) -> Value:
        if selected.type != fallback.type:
            raise RelationError(
                "scalar subplan candidate types disagree: "
                f"{selected.type!r} and {fallback.type!r}"
            )
        finite_abs_bound = (
            max(
                selected.decimal_finite_abs_bound,
                fallback.decimal_finite_abs_bound,
            )
            if (
                selected.decimal_finite_abs_bound is not None
                and fallback.decimal_finite_abs_bound is not None
            )
            else None
        )
        return Value(
            selected.type,
            smt.ite(condition, selected.is_null, fallback.is_null),
            smt.ite(condition, selected.value, fallback.value),
            finite_abs_bound,
        )

    def _join(self, node: Join, left: Relation, right: Relation) -> Relation:
        matching_rows = len(left.rows) * len(right.rows)
        _require_relation_row_pairs(matching_rows, "join matching")
        emit_matches = node.kind not in {
            "left_semi",
            "right_semi",
            "left_anti",
            "right_anti",
            "exclusion",
        }
        emit_left = node.kind in {
            "left",
            "full",
            "left_anti",
            "left_semi",
            "exclusion",
        }
        emit_right = node.kind in {
            "right",
            "full",
            "right_anti",
            "right_semi",
            "exclusion",
        }
        output_rows = matching_rows if emit_matches else 0
        output_rows += len(left.rows) if emit_left else 0
        output_rows += len(right.rows) if emit_right else 0
        _require_relation_rows(output_rows, "join output")

        matches: list[list[smt.Term]] = []
        for left_row in left.rows:
            match_row: list[smt.Term] = []
            for right_row in right.rows:
                values = dict(left_row.values) | dict(right_row.values)
                match_row.append(
                    smt.and_(
                        left_row.present,
                        right_row.present,
                        self.scalar.is_true(self.scalar.evaluate(node.predicate, values)),
                    )
                )
            matches.append(match_row)

        rows: list[Row] = []
        if emit_matches:
            for left_index, left_row in enumerate(left.rows):
                for right_index, right_row in enumerate(right.rows):
                    rows.append(
                        Row(
                            matches[left_index][right_index],
                            dict(left_row.values) | dict(right_row.values),
                            _derived_occurrence(
                                "join_match",
                                node.id,
                                left_row.occurrence,
                                right_row.occurrence,
                            ),
                            left_row.partition_facts | right_row.partition_facts,
                        )
                    )

        if emit_left:
            right_nulls = {
                column.name: self.scalar.null(column.type)
                for column in right.columns
            }
            for index, left_row in enumerate(left.rows):
                matched = smt.or_(*matches[index])
                if node.kind == "left_semi":
                    present = smt.and_(left_row.present, matched)
                    values = left_row.values
                elif node.kind == "left_anti":
                    present = smt.and_(left_row.present, smt.not_(matched))
                    values = left_row.values
                else:
                    present = smt.and_(left_row.present, smt.not_(matched))
                    values = dict(left_row.values) | right_nulls
                rows.append(
                    Row(
                        present,
                        values,
                        _derived_occurrence(
                            f"join_{node.kind}_left",
                            node.id,
                            left_row.occurrence,
                        ),
                        left_row.partition_facts,
                    )
                )

        if emit_right:
            left_nulls = {
                column.name: self.scalar.null(column.type)
                for column in left.columns
            }
            for right_index, right_row in enumerate(right.rows):
                matched = smt.or_(*(matches[left_index][right_index] for left_index in range(len(left.rows))))
                if node.kind == "right_semi":
                    present = smt.and_(right_row.present, matched)
                    values = right_row.values
                elif node.kind == "right_anti":
                    present = smt.and_(right_row.present, smt.not_(matched))
                    values = right_row.values
                else:
                    present = smt.and_(right_row.present, smt.not_(matched))
                    values = left_nulls | dict(right_row.values)
                rows.append(
                    Row(
                        present,
                        values,
                        _derived_occurrence(
                            f"join_{node.kind}_right",
                            node.id,
                            right_row.occurrence,
                        ),
                        right_row.partition_facts,
                    )
                )

        # Inner/cross joins with an empty side simply have no candidate rows.
        return Relation(self._columns(node.id), tuple(rows))

    def _columns(self, node_id: str) -> tuple[Column, ...]:
        return tuple(self.schemas[node_id].values())


def _wrap_sum(value: smt.Term, scalar_type: str) -> smt.Term:
    modulus = 1 << 64
    if scalar_type == "Uint64":
        return smt.mod(value, modulus)
    if scalar_type == "Int64":
        sign = 1 << 63
        return smt.add(
            smt.mod(smt.add(value, smt.int_value(sign)), modulus),
            smt.int_value(-sign),
        )
    raise RelationError(f"sum output type {scalar_type!r} is not modeled")


def _decimal_finite_abs_bound(value: Value) -> int:
    if value.decimal_finite_abs_bound is not None:
        return value.decimal_finite_abs_bound
    decimal_type = decimal.parse_type(value.type)
    if decimal_type is None:
        raise RelationError(f"Decimal sum input type {value.type!r} is not modeled")
    return 10**decimal_type.precision - 1


def _unwrap_sum(value: Value) -> smt.Term:
    """Canonicalize nested partial sums before applying the same final wrap."""

    modulus = smt.int_value(1 << 64)
    term = value.value
    if (
        value.type == "Uint64"
        and term.operation == "mod"
        and term.arguments[1] == modulus
    ):
        return term.arguments[0]
    if value.type != "Int64" or term.operation != "+" or len(term.arguments) != 2:
        return term

    sign = smt.int_value(1 << 63)
    wrapped, offset = term.arguments
    if (
        offset != smt.int_value(-(1 << 63))
        or wrapped.operation != "mod"
        or wrapped.arguments[1] != modulus
    ):
        return term
    shifted = wrapped.arguments[0]
    if shifted.operation != "+" or len(shifted.arguments) != 2:
        return term
    raw, shift = shifted.arguments
    return raw if shift == sign else term


def _derived_occurrence(
    operation: str,
    node: str,
    *inputs: Occurrence | None,
    ordinal: int | None = None,
) -> Occurrence | None:
    """Retain provenance only when every input occurrence is known exactly."""

    if any(item is None for item in inputs):
        return None
    return Occurrence(
        operation,
        node,
        ordinal,
        tuple(item for item in inputs if item is not None),
    )


def _common_partition_facts(rows: tuple[Row, ...]) -> frozenset[PartitionFact]:
    """Facts implied by the disjunction of all candidate-row guards."""

    if not rows:
        return frozenset()
    common = set(rows[0].partition_facts)
    for row in rows[1:]:
        common.intersection_update(row.partition_facts)
    return frozenset(common)


def _aggregate_key_classes(
    keys: tuple[str, ...],
    rows: tuple[Row, ...],
) -> tuple[tuple[int, ...], ...]:
    """Partition candidates whose complete group-key terms are identical."""

    key_values = tuple(
        tuple(row.values[key] for key in keys)
        for row in rows
    )
    term_ids = iter(smt.structural_ids(
        tuple(
            term
            for values in key_values
            for value in values
            for term in (value.is_null, value.value)
        )
    ))
    class_by_signature: dict[tuple[tuple[str, int, int], ...], int] = {}
    classes: list[list[int]] = []
    for row_index, values in enumerate(key_values):
        signature = tuple(
            (value.type, next(term_ids), next(term_ids))
            for value in values
        )
        class_index = class_by_signature.get(signature)
        if class_index is None:
            class_index = len(classes)
            class_by_signature[signature] = class_index
            classes.append([])
        classes[class_index].append(row_index)
    return tuple(tuple(members) for members in classes)


def _retained_order(
    order: tuple[SortOrder, ...] | None,
    output: tuple[str, ...],
) -> tuple[SortOrder, ...] | None:
    if order is None:
        return None
    available = set(output)
    return order if all(item.column in available for item in order) else None


def _projected_order(
    order: tuple[SortOrder, ...] | None,
    project: Project,
) -> tuple[SortOrder, ...] | None:
    if order is None:
        return None
    aliases: dict[str, list[str]] = {}
    for projection in project.columns:
        expression = projection.expression
        if expression.kind == "column" and expression.column is not None:
            aliases.setdefault(expression.column, []).append(projection.output)

    result: list[SortOrder] = []
    for item in order:
        outputs = aliases.get(item.column, ())
        if not outputs:
            return None
        column = item.column if item.column in outputs else outputs[0]
        result.append(SortOrder(column, item.ascending, item.nulls_first))
    return tuple(result)


def single(relation: Relation) -> RelationFamily:
    return RelationFamily((Outcome(smt.TRUE, relation, smt.FALSE),))


def _reject_correlated_limit_fanout(snapshot: Snapshot) -> None:
    """Fail closed when two Limit branches observe one latent stream order."""

    nodes = snapshot.plan.node_map()
    parents: dict[str, set[str]] = {node_id: set() for node_id in nodes}
    for parent in snapshot.plan.nodes:
        for child in plan_node_inputs(parent):
            parents[child].add(parent.id)

    cache: dict[str, frozenset[str]] = {}
    ordered: dict[str, bool] = {}

    def has_sequence(node_id: str) -> bool:
        if node_id not in ordered:
            node = nodes[node_id]
            if isinstance(node, Sort):
                result = True
            elif isinstance(node, (Project, Filter, Limit)):
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
                raise RelationError(
                    f"shared stream {child!r} feeds independently ordered Limit "
                    f"branches {', '.join(sorted(distinct))}; correlated fan-out "
                    "is not modeled"
                )


def map_family(
    family: RelationFamily,
    transform: Callable[[Relation], Relation],
) -> RelationFamily:
    return RelationFamily(
        tuple(
            Outcome(
                outcome.enabled,
                transform(outcome.relation),
                outcome.error,
                outcome.decisions,
                outcome.choices,
            )
            for outcome in family.outcomes
        )
    )


def combine_families(
    families: tuple[RelationFamily, ...],
    combine: Callable[[tuple[Relation, ...]], Relation],
    combine_errors: Callable[
        [tuple[Relation, ...], tuple[smt.Term, ...]],
        smt.Term,
    ] | None = None,
) -> RelationFamily:
    """Take a compatible product, preserving choices and observable errors."""

    partials: list[
        tuple[
            smt.Term,
            tuple[Relation, ...],
            tuple[smt.Term, ...],
            tuple[tuple[str, int], ...],
            tuple[BoundedChoice, ...],
        ]
    ] = [
        (smt.TRUE, (), (), (), ())
    ]
    for relation_family in families:
        expanded: list[
            tuple[
                smt.Term,
                tuple[Relation, ...],
                tuple[smt.Term, ...],
                tuple[tuple[str, int], ...],
                tuple[BoundedChoice, ...],
            ]
        ] = []
        for enabled, relations, errors, decisions, choices in partials:
            for outcome in relation_family.outcomes:
                merged = _merge_decisions(decisions, outcome.decisions)
                if merged is None:
                    continue
                expanded.append(
                    (
                        smt.and_(enabled, outcome.enabled),
                        relations + (outcome.relation,),
                        errors + (outcome.error,),
                        merged,
                        _merge_choices(choices, outcome.choices),
                    )
                )
                if len(expanded) > MAX_OUTCOME_ALTERNATIVES:
                    raise RelationError(
                        "outcome product exceeds "
                        f"the {MAX_OUTCOME_ALTERNATIVES} alternative audit bound"
                    )
        partials = expanded
    if not partials:
        raise RelationError("relation family has no compatible outcomes")
    return RelationFamily(
        tuple(
            Outcome(
                enabled,
                combine(relations),
                (
                    smt.or_(*errors)
                    if combine_errors is None
                    else combine_errors(relations, errors)
                ),
                decisions,
                choices,
            )
            for enabled, relations, errors, decisions, choices in partials
        )
    )


def sort_family(
    source: RelationFamily,
    order: tuple[SortOrder, ...],
    script: smt.Script,
    decision: str,
) -> RelationFamily:
    """Represent every tie-respecting Sort sequence exactly.

    Families with at most three candidate rows stay quantifier-free for solver
    performance.  Larger inputs, or small families whose combined permutations
    exceed the outcome audit cap, use bounded ordinal choices for the same
    sequence language with quadratic construction.
    """

    if not order:
        raise RelationError("sort order must not be empty")
    if all(_live_row_count(outcome.relation) <= 1 for outcome in source.outcomes):
        outcomes: list[Outcome] = []
        for source_outcome in source.outcomes:
            relation = source_outcome.relation
            columns = {column.name for column in relation.columns}
            missing = [item.column for item in order if item.column not in columns]
            if missing:
                raise RelationError(
                    f"sort columns are absent: {', '.join(missing)}"
                )
            outcomes.append(
                Outcome(
                    source_outcome.enabled,
                    Relation(
                        relation.columns,
                        relation.rows,
                        sequence=True,
                        order=order,
                    ),
                    source_outcome.error,
                    source_outcome.decisions,
                    source_outcome.choices,
                )
            )
        return RelationFamily(tuple(outcomes))
    _require_relation_row_pairs(
        sum(
            _unordered_row_pairs(_live_row_count(outcome.relation))
            for outcome in source.outcomes
        ),
        "sort construction",
    )
    if _use_enumerated_sequences(source):
        return _enumerated_sort_family(source, order, decision)
    outcomes: list[Outcome] = []
    for source_outcome in source.outcomes:
        relation = source_outcome.relation
        columns = {column.name for column in relation.columns}
        missing = [item.column for item in order if item.column not in columns]
        if missing:
            raise RelationError(f"sort columns are absent: {', '.join(missing)}")
        ordinals, choices = _fresh_ordinals(
            script,
            f"{decision}:ordinal",
            relation.rows,
        )
        outcomes.append(
            Outcome(
                smt.and_(
                    source_outcome.enabled,
                    _ordinal_constraints(relation.rows, ordinals, order),
                ),
                Relation(
                    relation.columns,
                    relation.rows,
                    sequence=True,
                    order=order,
                    ordinals=ordinals,
                ),
                source_outcome.error,
                source_outcome.decisions,
                _merge_choices(source_outcome.choices, choices),
            )
        )
    if not outcomes:
        raise RelationError("sort produced no outcomes")
    return RelationFamily(tuple(outcomes))


def _use_enumerated_sequences(family: RelationFamily) -> bool:
    """Keep only tiny sequence languages quantifier-free."""

    if any(
        len(outcome.relation.rows) > MAX_ENUMERATED_SEQUENCE_ROWS
        for outcome in family.outcomes
    ):
        return False
    return sum(
        factorial(len(outcome.relation.rows))
        for outcome in family.outcomes
    ) <= MAX_OUTCOME_ALTERNATIVES


def _enumerated_sort_family(
    source: RelationFamily,
    order: tuple[SortOrder, ...],
    decision: str,
) -> RelationFamily:
    outcomes: list[Outcome] = []
    for source_outcome in source.outcomes:
        relation = source_outcome.relation
        columns = {column.name for column in relation.columns}
        missing = [item.column for item in order if item.column not in columns]
        if missing:
            raise RelationError(f"sort columns are absent: {', '.join(missing)}")
        if decision in dict(source_outcome.decisions):
            raise RelationError(f"duplicate sort decision {decision!r}")
        for choice, permutation in enumerate(permutations(range(len(relation.rows)))):
            rows = tuple(relation.rows[index] for index in permutation)
            outcomes.append(
                Outcome(
                    smt.and_(source_outcome.enabled, _rows_sorted(rows, order)),
                    Relation(
                        relation.columns,
                        rows,
                        sequence=True,
                        order=order,
                    ),
                    source_outcome.error,
                    tuple(sorted(
                        source_outcome.decisions + ((decision, choice),)
                    )),
                    source_outcome.choices,
                )
            )
    if not outcomes:
        raise RelationError("sort produced no outcomes")
    return RelationFamily(tuple(outcomes))


def merge_family(
    source: RelationFamily,
    order: tuple[SortOrder, ...],
    groups: tuple[tuple[int, ...], ...],
    script: smt.Script,
    decision: str,
) -> RelationFamily:
    """Represent every sorted, producer-order-preserving interleaving."""

    indices = tuple(index for group in groups for index in group)
    row_count = len(source.outcomes[0].relation.rows) if source.outcomes else 0
    if sorted(indices) != list(range(row_count)):
        raise RelationError("merge producer groups do not partition the input rows")
    _require_relation_row_pairs(
        max(
            (
                _unordered_row_pairs(_live_row_count(outcome.relation))
                for outcome in source.outcomes
            ),
            default=0,
        ),
        "merge construction",
    )
    interleavings = factorial(row_count)
    for group in groups:
        interleavings //= factorial(len(group))
    fixed_producer_orders = all(
        outcome.relation.ordinals is None
        or all(
            ordinal.operation == "int"
            for ordinal in outcome.relation.ordinals
        )
        for outcome in source.outcomes
    )
    if (
        fixed_producer_orders
        and interleavings * len(source.outcomes) <= MAX_OUTCOME_ALTERNATIVES
    ):
        return _enumerated_merge_family(source, order, groups, decision)

    _require_relation_row_pairs(
        sum(
            _unordered_row_pairs(_live_row_count(outcome.relation))
            + sum(
                (live := sum(
                    outcome.relation.rows[index].present != smt.FALSE
                    for index in group
                ))
                * (live - 1)
                for group in groups
            )
            for outcome in source.outcomes
        ),
        "merge ordinal construction",
    )

    outcomes: list[Outcome] = []
    for source_outcome in source.outcomes:
        relation = source_outcome.relation
        if len(relation.rows) != row_count:
            raise RelationError("merge outcomes have different row shapes")
        columns = {column.name for column in relation.columns}
        missing = [item.column for item in order if item.column not in columns]
        if missing:
            raise RelationError(f"merge columns are absent: {', '.join(missing)}")

        ordinals, choices = _fresh_ordinals(
            script,
            f"{decision}:ordinal",
            relation.rows,
        )
        constraints = [
            _ordinal_constraints(relation.rows, ordinals, order)
        ]
        input_ordinals = relation.ordinals
        for group in groups:
            live_group = tuple(
                (position, index)
                for position, index in enumerate(group)
                if relation.rows[index].present != smt.FALSE
            )
            for left_position, left_index in live_group:
                for right_position, right_index in live_group:
                    if left_index == right_index:
                        continue
                    left_input = (
                        input_ordinals[left_index]
                        if input_ordinals is not None
                        else smt.int_value(left_position)
                    )
                    right_input = (
                        input_ordinals[right_index]
                        if input_ordinals is not None
                        else smt.int_value(right_position)
                    )
                    constraints.append(
                        smt.or_(
                            smt.not_(
                                smt.and_(
                                    relation.rows[left_index].present,
                                    relation.rows[right_index].present,
                                    smt.lt(left_input, right_input),
                                )
                            ),
                            smt.lt(ordinals[left_index], ordinals[right_index]),
                        )
                    )
        outcomes.append(
            Outcome(
                smt.and_(source_outcome.enabled, *constraints),
                Relation(
                    relation.columns,
                    relation.rows,
                    sequence=True,
                    order=order,
                    ordinals=ordinals,
                ),
                source_outcome.error,
                source_outcome.decisions,
                _merge_choices(source_outcome.choices, choices),
            )
        )
    if not outcomes:
        raise RelationError("merge produced no outcomes")
    return RelationFamily(tuple(outcomes))


def _enumerated_merge_family(
    source: RelationFamily,
    order: tuple[SortOrder, ...],
    groups: tuple[tuple[int, ...], ...],
    decision: str,
) -> RelationFamily:
    outcomes: list[Outcome] = []
    for source_outcome in source.outcomes:
        relation = source_outcome.relation
        if decision in dict(source_outcome.decisions):
            raise RelationError(f"duplicate merge decision {decision!r}")
        producer_groups = groups
        if relation.ordinals is not None:
            if any(ordinal.operation != "int" for ordinal in relation.ordinals):
                raise RelationError("enumerated merge requires concrete input ordinals")
            producer_groups = tuple(
                tuple(
                    sorted(
                        group,
                        key=lambda index: relation.ordinals[index].atom,
                    )
                )
                for group in groups
            )
        for choice, permutation in enumerate(_interleavings(producer_groups)):
            rows = tuple(relation.rows[index] for index in permutation)
            outcomes.append(
                Outcome(
                    smt.and_(source_outcome.enabled, _rows_sorted(rows, order)),
                    Relation(
                        relation.columns,
                        rows,
                        sequence=True,
                        order=order,
                    ),
                    source_outcome.error,
                    tuple(sorted(
                        source_outcome.decisions + ((decision, choice),)
                    )),
                    source_outcome.choices,
                )
            )
    if not outcomes:
        raise RelationError("merge produced no outcomes")
    return RelationFamily(tuple(outcomes))


def _interleavings(
    groups: tuple[tuple[int, ...], ...],
) -> Iterator[tuple[int, ...]]:
    def visit(
        prefix: tuple[int, ...],
        cursors: tuple[int, ...],
    ) -> Iterator[tuple[int, ...]]:
        if all(cursor == len(group) for cursor, group in zip(cursors, groups)):
            yield prefix
            return
        for index, group in enumerate(groups):
            cursor = cursors[index]
            if cursor == len(group):
                continue
            next_cursors = list(cursors)
            next_cursors[index] += 1
            yield from visit(prefix + (group[cursor],), tuple(next_cursors))

    yield from visit((), tuple(0 for _ in groups))


def _fresh_ordinals(
    script: smt.Script,
    hint: str,
    rows: tuple[Row, ...],
) -> tuple[tuple[smt.Term, ...], tuple[BoundedChoice, ...]]:
    live_indices = _live_row_indices(rows)
    live_count = len(live_indices)
    ordinals = [smt.ZERO] * len(rows)
    choices: list[BoundedChoice] = []
    for index in live_indices:
        ordinal = script.fresh_constant(f"{hint}:{index}", smt.INT)
        script.register_quantified_choice(ordinal, live_count)
        ordinals[index] = ordinal
        choices.append(BoundedChoice(ordinal, live_count))
    return tuple(ordinals), tuple(choices)


def _ordinal_constraints(
    rows: tuple[Row, ...],
    ordinals: tuple[smt.Term, ...],
    order: tuple[SortOrder, ...] | None,
) -> smt.Term:
    """Constrain a bounded permutation without enumerating its sequences."""

    if len(rows) != len(ordinals):
        raise RelationError("sequence ordinals do not align with rows")
    live_indices = _live_row_indices(rows)
    bound = smt.int_value(len(live_indices))
    constraints: list[smt.Term] = []
    for row, ordinal in zip(rows, ordinals):
        in_range = smt.and_(
            smt.not_(smt.lt(ordinal, smt.ZERO)),
            smt.lt(ordinal, bound),
        )
        constraints.append(
            smt.ite(row.present, in_range, smt.eq(ordinal, smt.ZERO))
        )
    for position, left_index in enumerate(live_indices):
        left = rows[left_index]
        for right_index in live_indices[position + 1 :]:
            right = rows[right_index]
            both = smt.and_(left.present, right.present)
            constraints.append(
                smt.or_(
                    smt.not_(both),
                    smt.not_(smt.eq(ordinals[left_index], ordinals[right_index])),
                )
            )
            if order is None:
                continue
            constraints.extend((
                smt.or_(
                    smt.not_(smt.and_(both, _row_less(left, right, order))),
                    smt.lt(ordinals[left_index], ordinals[right_index]),
                ),
                smt.or_(
                    smt.not_(smt.and_(both, _row_less(right, left, order))),
                    smt.lt(ordinals[right_index], ordinals[left_index]),
                ),
            ))
    return smt.and_(*constraints)


def _rows_sorted(rows: tuple[Row, ...], order: tuple[SortOrder, ...]) -> smt.Term:
    live_rows = tuple(row for row in rows if row.present != smt.FALSE)
    return smt.and_(
        *(
            smt.or_(
                smt.not_(smt.and_(left.present, right.present)),
                smt.not_(_row_less(right, left, order)),
            )
            for index, left in enumerate(live_rows)
            for right in live_rows[index + 1 :]
        )
    )


def _row_less(left: Row, right: Row, order: tuple[SortOrder, ...]) -> smt.Term:
    prefix_equal = smt.TRUE
    less = smt.FALSE
    for item in order:
        left_value = left.values[item.column]
        right_value = right.values[item.column]
        less = smt.or_(
            less,
            smt.and_(
                prefix_equal,
                _ordered_value_less(left_value, right_value, item),
            ),
        )
        prefix_equal = smt.and_(
            prefix_equal,
            ScalarEncoder.not_distinct(left_value, right_value),
        )
    return less


def _ordered_value_less(left: Value, right: Value, order: SortOrder) -> smt.Term:
    if left.type != right.type:
        raise RelationError("sort comparison type mismatch")
    if not is_ordered_type(left.type):
        raise RelationError(
            "sort comparison requires integer, String/Utf8, Date, or Decimal values"
        )
    null_before = (
        smt.and_(left.is_null, smt.not_(right.is_null))
        if order.nulls_first
        else smt.and_(smt.not_(left.is_null), right.is_null)
    )
    if left.value.sort == smt.BOOL:
        ascending = smt.and_(smt.not_(left.value), right.value)
        descending = smt.and_(left.value, smt.not_(right.value))
    elif is_decimal_type(left.type):
        ascending = decimal.sort_less(left.value, right.value)
        descending = decimal.sort_less(right.value, left.value)
    else:
        ascending = smt.lt(left.value, right.value)
        descending = smt.lt(right.value, left.value)
    value_before = ascending if order.ascending else descending
    return smt.or_(
        null_before,
        smt.and_(
            smt.not_(left.is_null),
            smt.not_(right.is_null),
            value_before,
        ),
    )


def limit_family(
    source: RelationFamily,
    count_expression: Expr,
    offset_expression: Expr | None,
    script: smt.Script,
    decision: str,
    *,
    ensure_at_most_one: bool = False,
) -> RelationFamily:
    count = _uint64_literal(count_expression, "limit count")
    offset = (
        0
        if offset_expression is None
        else _uint64_literal(offset_expression, "limit offset")
    )
    if ensure_at_most_one and count > 1 and offset == 0:
        # A successful checked prefix contains at most one input row, so Take
        # cannot change it.  Every other prefix is the same observable error.
        # Retaining the source family also retains all upstream correlations.
        return _ensure_at_most_one(source)
    at_most_one = all(
        _live_row_count(outcome.relation) <= 1
        for outcome in source.outcomes
    )
    if count == 0 or (
        offset > 0
        and at_most_one
    ):
        result = map_family(
            source,
            lambda relation: Relation(
                relation.columns,
                (),
                sequence=relation.sequence,
            ),
        )
    elif (
        offset == 0
        and at_most_one
    ):
        result = source
    elif source.sequence:
        result = _ordered_limit_family(source, count_expression, offset_expression)
    else:
        result = _unordered_limit_family(
            source,
            count_expression,
            offset_expression,
            script,
            decision,
            ensure_at_most_one=ensure_at_most_one,
        )
    return _ensure_at_most_one(result) if ensure_at_most_one else result


def _ensure_at_most_one(source: RelationFamily) -> RelationFamily:
    """Observe the exact post-Skip/post-Take scalar-cardinality failure."""

    return RelationFamily(
        tuple(
            Outcome(
                outcome.enabled,
                outcome.relation,
                smt.or_(
                    outcome.error,
                    smt.lt(
                        smt.ONE,
                        smt.add(
                            *(
                                smt.ite(row.present, smt.ONE, smt.ZERO)
                                for row in outcome.relation.rows
                            )
                        ),
                    ),
                ),
                outcome.decisions,
                outcome.choices,
            )
            for outcome in source.outcomes
        )
    )


def _ordered_limit_family(
    source: RelationFamily,
    count_expression: Expr,
    offset_expression: Expr | None,
) -> RelationFamily:
    """Take an exact prefix slice while ignoring false-guarded row slots."""

    count = _uint64_literal(count_expression, "limit count")
    offset = (
        0
        if offset_expression is None
        else _uint64_literal(offset_expression, "limit offset")
    )

    def take(relation: Relation) -> Relation:
        rows: list[Row] = []
        for index, row in enumerate(relation.rows):
            if count == 0 or offset >= len(relation.rows):
                selected = smt.FALSE
            else:
                prefix = _compressed_rank(relation, index)
                lower = smt.TRUE if offset == 0 else smt.not_(
                    smt.lt(prefix, smt.int_value(offset))
                )
                upper_bound = offset + count
                upper = (
                    smt.TRUE
                    if upper_bound >= len(relation.rows)
                    else smt.lt(prefix, smt.int_value(upper_bound))
                )
                selected = smt.and_(row.present, lower, upper)
            rows.append(
                Row(
                    selected,
                    row.values,
                    row.occurrence,
                    row.partition_facts,
                )
            )
        return Relation(
            relation.columns,
            tuple(rows),
            sequence=True,
            order=relation.order,
            ordinals=relation.ordinals,
        )

    return map_family(source, take)


def _unordered_limit_family(
    source: RelationFamily,
    count_expression: Expr,
    offset_expression: Expr | None,
    script: smt.Script,
    decision: str,
    *,
    ensure_at_most_one: bool = False,
) -> RelationFamily:
    """Enumerate every legal unordered Take(Skip(input)) output bag.

    Take(1) uses one bounded selector and one conditional output row.  Larger
    outputs use a mask enabled exactly when all selected live slots are present
    and its size equals ``min(count, max(input_size - offset, 0))``.  Keeping
    false-guarded unselected slots preserves a stable relation shape for
    downstream nodes.
    When cardinality is checked, all outputs larger than one are the same
    observable error.  They therefore share one canonical error decision while
    zero- and one-row successes retain their exact masks.
    """

    count = _uint64_literal(count_expression, "limit count")
    offset = (
        0
        if offset_expression is None
        else _uint64_literal(offset_expression, "limit offset")
    )
    if count == 1:
        return _symbolic_singleton_limit_family(
            source,
            offset,
            script,
            decision,
        )

    alternatives = 0
    outcomes: list[Outcome] = []
    for source_outcome in source.outcomes:
        rows = source_outcome.relation.rows
        live_indices = _live_row_indices(rows)
        live_count = len(live_indices)
        if decision in dict(source_outcome.decisions):
            raise RelationError(f"duplicate unordered-limit decision {decision!r}")

        totals_by_size: dict[int, list[int]] = {}
        for total in range(live_count + 1):
            size = min(count, max(total - offset, 0))
            totals_by_size.setdefault(size, []).append(total)
        present_count = smt.add(
            *(
                smt.ite(rows[index].present, smt.ONE, smt.ZERO)
                for index in live_indices
            )
        )

        error_totals = tuple(
            total
            for size, totals in totals_by_size.items()
            if ensure_at_most_one and size > 1
            for total in totals
        )
        if error_totals:
            alternatives += 1
            if alternatives > MAX_OUTCOME_ALTERNATIVES:
                raise RelationError(
                    "unordered limit exceeds "
                    f"the {MAX_OUTCOME_ALTERNATIVES} alternative audit bound "
                    f"(decision={decision!r}, count={count}, offset={offset}, "
                    f"checked={ensure_at_most_one}, "
                    f"source_outcomes={len(source.outcomes)}, "
                    f"live_rows={live_count}, shaped_rows={len(rows)})"
                )
            outcomes.append(
                Outcome(
                    smt.and_(
                        source_outcome.enabled,
                        smt.or_(
                            *(
                                smt.eq(present_count, smt.int_value(total))
                                for total in error_totals
                            )
                        ),
                    ),
                    Relation(
                        source_outcome.relation.columns,
                        tuple(
                            Row(
                                smt.FALSE,
                                row.values,
                                row.occurrence,
                                row.partition_facts,
                            )
                            for row in rows
                        ),
                    ),
                    smt.TRUE,
                    tuple(
                        sorted(source_outcome.decisions + ((decision, 0),))
                    ),
                    source_outcome.choices,
                )
            )

        for size, valid_totals in totals_by_size.items():
            if ensure_at_most_one and size > 1:
                continue
            for indices in combinations(live_indices, size):
                alternatives += 1
                if alternatives > MAX_OUTCOME_ALTERNATIVES:
                    raise RelationError(
                        "unordered limit exceeds "
                        f"the {MAX_OUTCOME_ALTERNATIVES} alternative audit bound "
                        f"(decision={decision!r}, count={count}, offset={offset}, "
                        f"checked={ensure_at_most_one}, "
                        f"source_outcomes={len(source.outcomes)}, "
                        f"live_rows={live_count}, shaped_rows={len(rows)})"
                    )
                mask = sum(1 << index for index in indices)
                selected = tuple(rows[index].present for index in indices)
                enabled = smt.and_(
                    source_outcome.enabled,
                    *selected,
                    smt.or_(
                        *(
                            smt.eq(present_count, smt.int_value(total))
                            for total in valid_totals
                        )
                    ),
                )
                output_rows = tuple(
                    Row(
                        row.present if mask & (1 << index) else smt.FALSE,
                        row.values,
                        row.occurrence,
                        row.partition_facts,
                    )
                    for index, row in enumerate(rows)
                )
                decisions = tuple(
                    sorted(source_outcome.decisions + ((decision, mask),))
                )
                outcomes.append(
                    Outcome(
                        enabled,
                        Relation(source_outcome.relation.columns, output_rows),
                        source_outcome.error,
                        decisions,
                        source_outcome.choices,
                    )
                )
    if not outcomes:
        raise RelationError("unordered limit produced no outcomes")
    return RelationFamily(tuple(outcomes))


def _symbolic_singleton_limit_family(
    source: RelationFamily,
    offset: int,
    script: smt.Script,
    decision: str,
) -> RelationFamily:
    """Represent every unordered singleton with one bounded row selector.

    When more rows are present than ``offset``, any present row can be retained:
    an unordered Skip may discard other rows first.  Otherwise the output is
    empty.  One conditional row therefore denotes the exact Take(1) bag family
    without enumerating one outcome per candidate slot.
    """

    outcomes: list[Outcome] = []
    for outcome_index, source_outcome in enumerate(source.outcomes):
        relation = source_outcome.relation
        rows = relation.rows
        live_rows = tuple(rows[index] for index in _live_row_indices(rows))
        if decision in dict(source_outcome.decisions):
            raise RelationError(f"duplicate unordered-limit decision {decision!r}")

        if not live_rows or offset >= len(live_rows):
            outcomes.append(
                Outcome(
                    source_outcome.enabled,
                    Relation(
                        relation.columns,
                        (_absent_limit_row(relation),),
                    ),
                    source_outcome.error,
                    source_outcome.decisions,
                    source_outcome.choices,
                )
            )
            continue

        if len(live_rows) == 1:
            row = live_rows[0]
            outcomes.append(
                Outcome(
                    source_outcome.enabled,
                    Relation(
                        relation.columns,
                        (
                            Row(
                                row.present,
                                row.values,
                                row.occurrence,
                                row.partition_facts,
                            ),
                        ),
                    ),
                    source_outcome.error,
                    source_outcome.decisions,
                    source_outcome.choices,
                )
            )
            continue

        choice = script.fresh_constant(
            f"{decision}:selection:{outcome_index}",
            smt.INT,
        )
        script.register_quantified_choice(choice, len(live_rows))
        retained = (
            smt.or_(*(row.present for row in live_rows))
            if offset == 0
            else smt.lt(
                smt.int_value(offset),
                smt.add(
                    *(
                        smt.ite(row.present, smt.ONE, smt.ZERO)
                        for row in live_rows
                    )
                ),
            )
        )
        selected_present = smt.or_(
            *(
                smt.and_(
                    smt.eq(choice, smt.int_value(index)),
                    row.present,
                )
                for index, row in enumerate(live_rows)
            )
        )
        enabled = smt.and_(
            source_outcome.enabled,
            smt.not_(smt.lt(choice, smt.ZERO)),
            smt.lt(choice, smt.int_value(len(live_rows))),
            smt.or_(smt.not_(retained), selected_present),
        )
        values = {
            column.name: _select_limit_value(
                choice,
                tuple(row.values[column.name] for row in live_rows),
            )
            for column in relation.columns
        }
        outcomes.append(
            Outcome(
                enabled,
                Relation(
                    relation.columns,
                    (
                        Row(
                            retained,
                            values,
                            None,
                            _common_partition_facts(live_rows),
                        ),
                    ),
                ),
                source_outcome.error,
                source_outcome.decisions,
                _merge_choices(
                    source_outcome.choices,
                    (BoundedChoice(choice, len(live_rows)),),
                ),
            )
        )
    if not outcomes:
        raise RelationError("unordered singleton limit produced no outcomes")
    return RelationFamily(tuple(outcomes))


def _absent_limit_row(relation: Relation) -> Row:
    """Return one typed padding row for an always-empty singleton outcome."""

    if relation.rows:
        return Row(smt.FALSE, relation.rows[0].values)
    return Row(
        smt.FALSE,
        {
            column.name: Value(
                column.type,
                smt.TRUE if column.nullable else smt.FALSE,
                smt.FALSE if smt_sort(column.type) == smt.BOOL else smt.ZERO,
                0 if is_decimal_type(column.type) else None,
            )
            for column in relation.columns
        },
    )


def _select_limit_value(
    choice: smt.Term,
    alternatives: tuple[Value, ...],
) -> Value:
    """Conditionally select one typed value and its hidden Decimal metadata."""

    if not alternatives:
        raise RelationError("singleton limit has no value alternatives")
    first = alternatives[0]
    if any(value.type != first.type for value in alternatives[1:]):
        raise RelationError("singleton limit value alternatives have different types")

    is_null = first.is_null
    value = first.value
    for index, alternative in enumerate(alternatives[1:], start=1):
        selected = smt.eq(choice, smt.int_value(index))
        is_null = smt.ite(selected, alternative.is_null, is_null)
        value = smt.ite(selected, alternative.value, value)

    finite_bounds = tuple(
        alternative.decimal_finite_abs_bound
        for alternative in alternatives
    )
    finite_bound = (
        None
        if any(bound is None for bound in finite_bounds)
        else max(bound for bound in finite_bounds if bound is not None)
    )

    states = tuple(
        alternative.decimal_average_state
        for alternative in alternatives
    )
    average_state = None
    if any(state is not None for state in states):
        if any(state is None for state in states):
            raise RelationError(
                "singleton limit mixed Decimal avg state and scalar values"
            )
        present_states = tuple(state for state in states if state is not None)
        first_state = present_states[0]
        if any(
            state.sum_type != first_state.sum_type
            for state in present_states[1:]
        ):
            raise RelationError(
                "singleton limit received different Decimal avg state types"
            )
        state_sum = first_state.sum
        state_count = first_state.count
        for index, state in enumerate(present_states[1:], start=1):
            selected = smt.eq(choice, smt.int_value(index))
            state_sum = smt.ite(selected, state.sum, state_sum)
            state_count = smt.ite(selected, state.count, state_count)
        average_state = DecimalAverageState(
            sum_type=first_state.sum_type,
            sum=state_sum,
            count=state_count,
            finite_abs_bound=max(
                state.finite_abs_bound for state in present_states
            ),
            count_bound=max(state.count_bound for state in present_states),
        )

    return Value(
        first.type,
        is_null,
        value,
        finite_bound,
        average_state,
    )


def _uint64_literal(expression: Expr, description: str) -> int:
    if (
        expression.kind != "literal"
        or expression.result_type != "Uint64"
        or type(expression.value) is not int
        or not 0 <= expression.value < 1 << 64
    ):
        raise RelationError(f"{description} is not a non-null Uint64 literal")
    return expression.value


def _merge_decisions(
    left: tuple[tuple[str, int], ...],
    right: tuple[tuple[str, int], ...],
) -> tuple[tuple[str, int], ...] | None:
    merged = dict(left)
    for key, value in right:
        previous = merged.get(key)
        if previous is not None and previous != value:
            return None
        merged[key] = value
    return tuple(sorted(merged.items()))


def _merge_choices(
    left: tuple[BoundedChoice, ...],
    right: tuple[BoundedChoice, ...],
) -> tuple[BoundedChoice, ...]:
    merged = {choice.term: choice for choice in left}
    for choice in right:
        previous = merged.get(choice.term)
        if previous is not None and previous.bound != choice.bound:
            raise RelationError("shared bounded choice has inconsistent bounds")
        merged[choice.term] = choice
    return tuple(merged.values())


def bag_equal(left: Relation, right: Relation, scalar: ScalarEncoder) -> smt.Term:
    if len(left.columns) != len(right.columns):
        return smt.FALSE
    if any(a.type != b.type for a, b in zip(left.columns, right.columns)):
        return smt.FALSE

    left_names = tuple(column.name for column in left.columns)
    right_names = tuple(column.name for column in right.columns)

    def row_equal(
        first: Row,
        first_names: tuple[str, ...],
        second: Row,
        second_names: tuple[str, ...],
    ) -> smt.Term:
        return smt.and_(
            *(
                scalar.not_distinct(first.values[first_name], second.values[second_name])
                for first_name, second_name in zip(first_names, second_names)
            )
        )

    def multiplicity(
        relation: Relation,
        names: tuple[str, ...],
        candidate: Row,
        candidate_names: tuple[str, ...],
    ) -> smt.Term:
        return smt.add(
            *(
                smt.ite(
                    smt.and_(row.present, row_equal(row, names, candidate, candidate_names)),
                    smt.ONE,
                    smt.ZERO,
                )
                for row in relation.rows
            )
        )

    equalities: list[smt.Term] = []
    for candidate, candidate_names in (
        *((row, left_names) for row in left.rows),
        *((row, right_names) for row in right.rows),
    ):
        counts_equal = smt.eq(
            multiplicity(left, left_names, candidate, candidate_names),
            multiplicity(right, right_names, candidate, candidate_names),
        )
        equalities.append(smt.or_(smt.not_(candidate.present), counts_equal))
    return smt.and_(*equalities)


def sequence_equal(left: Relation, right: Relation, scalar: ScalarEncoder) -> smt.Term:
    """Compare compressed present-row sequences, ignoring guarded-out slots."""

    if not left.sequence or not right.sequence:
        return smt.FALSE
    if len(left.columns) != len(right.columns):
        return smt.FALSE
    if any(a.type != b.type for a, b in zip(left.columns, right.columns)):
        return smt.FALSE

    left_names = tuple(column.name for column in left.columns)
    right_names = tuple(column.name for column in right.columns)

    def values_equal(left_row: Row, right_row: Row) -> smt.Term:
        return smt.and_(
            *(
                scalar.not_distinct(
                    left_row.values[left_name],
                    right_row.values[right_name],
                )
                for left_name, right_name in zip(left_names, right_names)
            )
        )

    left_ranks = tuple(
        _compressed_rank(left, index) for index in range(len(left.rows))
    )
    right_ranks = tuple(
        _compressed_rank(right, index) for index in range(len(right.rows))
    )
    left_count = smt.add(*(smt.ite(row.present, smt.ONE, smt.ZERO) for row in left.rows))
    right_count = smt.add(*(smt.ite(row.present, smt.ONE, smt.ZERO) for row in right.rows))
    return smt.and_(
        smt.eq(left_count, right_count),
        *(
            smt.or_(
                smt.not_(
                    smt.and_(
                        left_row.present,
                        right_row.present,
                        smt.eq(left_ranks[left_index], right_ranks[right_index]),
                    )
                ),
                values_equal(left_row, right_row),
            )
            for left_index, left_row in enumerate(left.rows)
            for right_index, right_row in enumerate(right.rows)
        ),
    )


def _compressed_rank(relation: Relation, index: int) -> smt.Term:
    """Zero-based position among present rows in the represented sequence."""

    if relation.ordinals is None:
        return smt.add(
            *(
                smt.ite(row.present, smt.ONE, smt.ZERO)
                for row in relation.rows[:index]
            )
        )
    ordinal = relation.ordinals[index]
    return smt.add(
        *(
            smt.ite(
                smt.and_(row.present, smt.lt(other, ordinal)),
                smt.ONE,
                smt.ZERO,
            )
            for row, other in zip(relation.rows, relation.ordinals)
        )
    )


def _as_sequence_family(
    family: RelationFamily,
    script: smt.Script,
    scope: str,
) -> RelationFamily:
    """Give an unordered bag every possible compressed sequence order."""

    _require_relation_row_pairs(
        sum(
            _unordered_row_pairs(_live_row_count(outcome.relation))
            for outcome in family.outcomes
        ),
        "latent sequence construction",
    )
    if _use_enumerated_sequences(family):
        return _enumerated_as_sequence_family(
            family,
            f"{scope}:latent_sequence",
        )
    outcomes: list[Outcome] = []
    for index, source_outcome in enumerate(family.outcomes):
        relation = source_outcome.relation
        ordinals, choices = _fresh_ordinals(
            script,
            f"{scope}:latent_sequence:{index}:ordinal",
            relation.rows,
        )
        outcomes.append(
            Outcome(
                smt.and_(
                    source_outcome.enabled,
                    _ordinal_constraints(relation.rows, ordinals, None),
                ),
                Relation(
                    relation.columns,
                    relation.rows,
                    sequence=True,
                    ordinals=ordinals,
                ),
                source_outcome.error,
                source_outcome.decisions,
                _merge_choices(source_outcome.choices, choices),
            )
        )
    if not outcomes:
        raise RelationError("latent unordered sequence family has no outcomes")
    return RelationFamily(tuple(outcomes))


def _enumerated_as_sequence_family(
    family: RelationFamily,
    decision: str,
) -> RelationFamily:
    outcomes: list[Outcome] = []
    alternative = 0
    for source_outcome in family.outcomes:
        relation = source_outcome.relation
        if decision in dict(source_outcome.decisions):
            raise RelationError(f"duplicate latent-sequence decision {decision!r}")
        for permutation in permutations(range(len(relation.rows))):
            outcomes.append(
                Outcome(
                    source_outcome.enabled,
                    Relation(
                        relation.columns,
                        tuple(relation.rows[index] for index in permutation),
                        sequence=True,
                    ),
                    source_outcome.error,
                    tuple(sorted(
                        source_outcome.decisions + ((decision, alternative),)
                    )),
                    source_outcome.choices,
                )
            )
            alternative += 1
    if not outcomes:
        raise RelationError("latent unordered sequence family has no outcomes")
    return RelationFamily(tuple(outcomes))


def _comparison_inputs(
    left: RelationFamily,
    right: RelationFamily,
    script: smt.Script,
    scope: str,
) -> tuple[RelationFamily, RelationFamily, bool]:
    ordered = left.sequence
    if ordered and not right.sequence:
        right = _as_sequence_family(right, script, f"{scope}:right")
    left_choices = {
        choice.term
        for outcome in left.outcomes
        for choice in outcome.choices
    }
    right_choices = {
        choice.term
        for outcome in right.outcomes
        for choice in outcome.choices
    }
    shared_choices = left_choices.intersection(right_choices)
    if shared_choices:
        names = ", ".join(
            repr(term.atom)
            for term in sorted(
                shared_choices,
                key=lambda term: str(term.atom),
            )
        )
        raise RelationError(
            "comparison sides share bounded choice symbol(s) "
            f"{names}; quantified choice scopes must be disjoint"
        )
    _register_family_choices(left, script)
    _register_family_choices(right, script)
    left = _bounded_choice_family(left, script, f"{scope}:left")
    right = _bounded_choice_family(right, script, f"{scope}:right")
    comparisons = len(left.outcomes) * len(right.outcomes)
    if comparisons > MAX_OUTCOME_COMPARISONS:
        raise RelationError(
            f"outcome comparison requires {comparisons} pairs, exceeding "
            f"the {MAX_OUTCOME_COMPARISONS} pair audit bound"
        )
    return left, right, ordered


def _register_family_choices(
    family: RelationFamily,
    script: smt.Script,
) -> None:
    """Make hand-built and evaluator-produced family choices equally safe."""

    for outcome in family.outcomes:
        for choice in outcome.choices:
            script.register_quantified_choice(choice.term, choice.bound)


def _bounded_choice_family(
    family: RelationFamily,
    script: smt.Script,
    scope: str,
) -> RelationFamily:
    """Audit choice flow and make every outcome's legal range explicit."""

    bounded: list[Outcome] = []
    for index, outcome in enumerate(family.outcomes):
        carried: dict[smt.Term, BoundedChoice] = {}
        ranges: list[smt.Term] = []
        for choice in outcome.choices:
            if choice.term in carried:
                raise RelationError(
                    f"{scope} outcome {index} carries a duplicate bounded choice"
                )
            registered_bound = script.quantified_choice_bound(choice.term)
            if registered_bound != choice.bound:
                raise RelationError(
                    f"{scope} outcome {index} carries bounded choice "
                    f"{choice.term.atom!r} with bound {choice.bound}, "
                    f"but the SMT script registered {registered_bound}"
                )
            carried[choice.term] = choice
            ranges.extend((
                smt.not_(smt.lt(choice.term, smt.ZERO)),
                smt.lt(choice.term, smt.int_value(choice.bound)),
            ))

        observable_terms = [outcome.enabled, outcome.error]
        relation = outcome.relation
        if relation.ordinals is not None:
            observable_terms.extend(relation.ordinals)
        for row in relation.rows:
            observable_terms.append(row.present)
            observable_terms.extend(
                fact.term
                for fact in row.partition_facts
            )
            for value in row.values.values():
                observable_terms.extend((value.is_null, value.value))
                state = value.decimal_average_state
                if state is not None:
                    observable_terms.extend((state.sum, state.count))
        dependencies = set(
            script.quantified_choice_dependencies(observable_terms)
        )
        missing = dependencies.difference(carried)
        if missing:
            names = ", ".join(
                repr(term.atom)
                for term in sorted(missing, key=lambda term: str(term.atom))
            )
            raise RelationError(
                f"{scope} outcome {index} uses registered bounded "
                f"choice(s) {names} without carrying them"
            )

        bounded.append(
            Outcome(
                smt.and_(outcome.enabled, *ranges),
                relation,
                outcome.error,
                outcome.decisions,
                outcome.choices,
            )
        )
    return RelationFamily(tuple(bounded))


def _relations_equal(
    left: Relation,
    right: Relation,
    scalar: ScalarEncoder,
    ordered: bool,
) -> smt.Term:
    return (
        sequence_equal(left, right, scalar)
        if ordered
        else bag_equal(left, right, scalar)
    )


def _outcomes_equal(
    left: Outcome,
    right: Outcome,
    scalar: ScalarEncoder,
    ordered: bool,
) -> smt.Term:
    """Compare the one observable status and, on success, the result relation."""

    return smt.or_(
        smt.and_(left.error, right.error),
        smt.and_(
            smt.not_(left.error),
            smt.not_(right.error),
            _relations_equal(left.relation, right.relation, scalar, ordered),
        ),
    )


def _outcome_equal_matrix(
    left: RelationFamily,
    right: RelationFamily,
    scalar: ScalarEncoder,
    ordered: bool,
) -> tuple[tuple[smt.Term, ...], ...]:
    return tuple(
        tuple(
            _outcomes_equal(left_outcome, right_outcome, scalar, ordered)
            for right_outcome in right.outcomes
        )
        for left_outcome in left.outcomes
    )


def _family_mismatch(
    left: RelationFamily,
    right: RelationFamily,
    scalar: ScalarEncoder,
    ordered: bool,
    left_to_right_equal: tuple[tuple[smt.Term, ...], ...],
) -> FamilyMismatch:
    def choice_terms(outcome: Outcome) -> tuple[smt.Term, ...]:
        return tuple(choice.term for choice in outcome.choices)

    def exists_enabled(family: RelationFamily) -> smt.Term:
        return smt.or_(
            *(
                smt.exists(choice_terms(outcome), outcome.enabled)
                for outcome in family.outcomes
            )
        )

    def target_contains(
        target: RelationFamily,
        equalities: tuple[smt.Term, ...],
    ) -> smt.Term:
        return smt.or_(
            *(
                smt.exists(
                    choice_terms(target_outcome),
                    smt.and_(
                        target_outcome.enabled,
                        equalities[index],
                    ),
                )
                for index, target_outcome in enumerate(target.outcomes)
            )
        )

    def unmatched(
        source: RelationFamily,
        target: RelationFamily,
        equality: tuple[tuple[smt.Term, ...], ...],
    ) -> tuple[smt.Term, ...]:
        # Source choices stay globally existential and inspectable.  Target
        # choices are shadowed by the existential membership test; negating it
        # therefore proves that no legal target sequence matches this source.
        return tuple(
            smt.and_(
                outcome.enabled,
                smt.not_(target_contains(target, equality[index])),
            )
            for index, outcome in enumerate(source.outcomes)
        )

    right_to_left_equal = _outcome_equal_matrix(
        right,
        left,
        scalar,
        ordered,
    )
    left_exists = exists_enabled(left)
    right_exists = exists_enabled(right)
    globally_enabled = smt.and_(
        smt.or_(*(outcome.enabled for outcome in left.outcomes)),
        smt.or_(*(outcome.enabled for outcome in right.outcomes)),
    )
    left_unmatched = unmatched(left, right, left_to_right_equal)
    right_unmatched = unmatched(right, left, right_to_left_equal)
    left_empty = smt.not_(left_exists)
    right_empty = smt.not_(right_exists)
    counterexample = smt.or_(
        left_empty,
        right_empty,
        smt.and_(
            globally_enabled,
            smt.or_(*left_unmatched, *right_unmatched),
        ),
    )
    branches = (
        MismatchBranch("left_language_empty", left_empty),
        MismatchBranch("right_language_empty", right_empty),
        *(
            MismatchBranch(
                f"left_outcome_{index}_unmatched",
                smt.and_(globally_enabled, predicate),
            )
            for index, predicate in enumerate(left_unmatched)
        ),
        *(
            MismatchBranch(
                f"right_outcome_{index}_unmatched",
                smt.and_(globally_enabled, predicate),
            )
            for index, predicate in enumerate(right_unmatched)
        ),
    )
    return FamilyMismatch(counterexample, branches)


def compare_families(
    left: RelationFamily,
    right: RelationFamily,
    scalar: ScalarEncoder,
) -> FamilyComparison:
    """Expose the exact normalized outcome pairs used by family equivalence."""

    left, right, ordered = _comparison_inputs(
        left,
        right,
        scalar.script,
        "compare_families",
    )
    pair_equal = _outcome_equal_matrix(
        left,
        right,
        scalar,
        ordered,
    )
    mismatch = _family_mismatch(
        left,
        right,
        scalar,
        ordered,
        pair_equal,
    )
    return FamilyComparison(
        left,
        right,
        ordered,
        pair_equal,
        mismatch,
    )


def family_mismatch(
    left: RelationFamily,
    right: RelationFamily,
    scalar: ScalarEncoder,
) -> FamilyMismatch:
    """Return the canonical mismatch and exact independently solvable branches."""

    left, right, ordered = _comparison_inputs(
        left,
        right,
        scalar.script,
        "family_mismatch",
    )
    left_to_right_equal = _outcome_equal_matrix(
        left,
        right,
        scalar,
        ordered,
    )
    return _family_mismatch(
        left,
        right,
        scalar,
        ordered,
        left_to_right_equal,
    )


def family_equal(
    left: RelationFamily,
    right: RelationFamily,
    scalar: ScalarEncoder,
) -> smt.Term:
    """Mutual inclusion of enabled bags or initial-query result sequences."""

    return smt.not_(family_mismatch(left, right, scalar).counterexample)
