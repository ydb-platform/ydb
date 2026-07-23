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
    Filter,
    Join,
    Limit,
    PlanNode,
    Project,
    Scan,
    Snapshot,
    Sort,
    SortOrder,
    UnionAll,
    plan_node_inputs,
    validate_snapshot,
)
from .scalar import Encoder as ScalarEncoder
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
class OrdinalChoice:
    """One globally inspectable, locally quantified bounded sequence choice."""

    term: smt.Term
    bound: int

    def __post_init__(self) -> None:
        if self.term.operation != "symbol" or self.term.sort != smt.INT:
            raise ValueError("ordinal choice must be a named SMT integer")
        if type(self.bound) is not int or self.bound <= 0:
            raise ValueError("ordinal choice bound must be positive")


@dataclass(frozen=True, slots=True)
class Outcome:
    """One enabled relation and its correlated unordered-limit decisions."""

    enabled: smt.Term
    relation: Relation
    decisions: tuple[tuple[str, int], ...] = ()
    choices: tuple[OrdinalChoice, ...] = ()


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
            or self.outcomes[0].decisions
            or self.outcomes[0].choices
        ):
            raise RelationError("relation has multiple conditional outcomes")
        return self.outcomes[0].relation


NodeObserver: TypeAlias = Callable[[str, str, RelationFamily], None]


@dataclass(frozen=True, slots=True)
class FamilyComparison:
    """The exact normalized outcome pairs used by family equivalence."""

    left: RelationFamily
    right: RelationFamily
    ordered: bool
    pair_equal: tuple[tuple[smt.Term, ...], ...]
    equivalent: smt.Term


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
            return map_family(
                source,
                lambda relation: Relation(
                    columns,
                    tuple(
                        Row(
                            row.present,
                            {
                                projection.output: self.scalar.evaluate(
                                    projection.expression, row.values
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
            return map_family(
                source,
                lambda relation: Relation(
                    relation.columns,
                    tuple(
                        Row(
                            smt.and_(
                                row.present,
                                self.scalar.is_true(
                                    self.scalar.evaluate(node.predicate, row.values)
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
                f"{self.choice_scope}:limit:{node.id}",
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
                return Relation(self._columns(node.id), tuple(rows))

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
            {trait.function for trait in node.aggregates} - {"count", "max", "sum"}
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
            row_count = len(source.rows)
            directional_pair_count = row_count * row_count
            share_group_comparisons = (
                directional_pair_count > MAX_RELATION_ROW_PAIRS
            )
            if share_group_comparisons:
                _require_relation_row_pairs(
                    row_count * (row_count + 1) // 2,
                    "grouped aggregate",
                )
                # Null-safe equality is symmetric, but row presence is not.
                # Share one composite group-key comparison per unordered pair
                # and retain the member row's directional presence guard below.
                # The triangular preflight also keeps those N^2 guards strictly
                # below twice the generic pair bound.
                same_groups = {
                    (left_index, right_index): self._same_group(
                        node,
                        source.rows[left_index],
                        source.rows[right_index],
                    )
                    for left_index in range(row_count)
                    for right_index in range(left_index, row_count)
                }

                def same_group(left_index: int, right_index: int) -> smt.Term:
                    pair = (
                        (left_index, right_index)
                        if left_index <= right_index
                        else (right_index, left_index)
                    )
                    return same_groups[pair]

            else:
                _require_relation_row_pairs(
                    directional_pair_count,
                    "grouped aggregate",
                )

                def same_group(left_index: int, right_index: int) -> smt.Term:
                    return self._same_group(
                        node,
                        source.rows[left_index],
                        source.rows[right_index],
                    )

            for index, candidate in enumerate(source.rows):
                matches = tuple(
                    smt.and_(row.present, same_group(index, row_index))
                    for row_index, row in enumerate(source.rows)
                )
                earlier = (
                    matches[:index]
                    if share_group_comparisons
                    else tuple(
                        smt.and_(row.present, same_group(index, row_index))
                        for row_index, row in enumerate(source.rows[:index])
                    )
                )
                present = smt.and_(
                    candidate.present,
                    smt.not_(smt.or_(*earlier)),
                )
                rows.append(
                    Row(
                        present,
                        self._aggregate_values(node, source, matches, candidate),
                        _derived_occurrence(
                            "aggregate",
                            node.id,
                            candidate.occurrence,
                        ),
                        candidate.partition_facts,
                    )
                )
        return Relation(self._columns(node.id), tuple(rows))

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
            values[trait.output] = self._aggregate_value(trait, source, matches)
        return values

    def _aggregate_value(
        self,
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
    return RelationFamily((Outcome(smt.TRUE, relation),))


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
                outcome.decisions,
                outcome.choices,
            )
            for outcome in family.outcomes
        )
    )


def combine_families(
    families: tuple[RelationFamily, ...],
    combine: Callable[[tuple[Relation, ...]], Relation],
) -> RelationFamily:
    """Take a compatible product, preserving shared-DAG limit choices."""

    partials: list[
        tuple[
            smt.Term,
            tuple[Relation, ...],
            tuple[tuple[str, int], ...],
            tuple[OrdinalChoice, ...],
        ]
    ] = [
        (smt.TRUE, (), (), ())
    ]
    for relation_family in families:
        expanded: list[
            tuple[
                smt.Term,
                tuple[Relation, ...],
                tuple[tuple[str, int], ...],
                tuple[OrdinalChoice, ...],
            ]
        ] = []
        for enabled, relations, decisions, choices in partials:
            for outcome in relation_family.outcomes:
                merged = _merge_decisions(decisions, outcome.decisions)
                if merged is None:
                    continue
                expanded.append(
                    (
                        smt.and_(enabled, outcome.enabled),
                        relations + (outcome.relation,),
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
            Outcome(enabled, combine(relations), decisions, choices)
            for enabled, relations, decisions, choices in partials
        )
    )


def sort_family(
    source: RelationFamily,
    order: tuple[SortOrder, ...],
    script: smt.Script,
    decision: str,
) -> RelationFamily:
    """Represent every tie-respecting Sort sequence exactly.

    Small families stay quantifier-free for solver performance.  Once explicit
    permutations would exceed the ordinary outcome audit cap, bounded ordinal
    choices provide the same sequence language with quadratic construction.
    """

    if not order:
        raise RelationError("sort order must not be empty")
    _require_relation_row_pairs(
        sum(
            _unordered_row_pairs(len(outcome.relation.rows))
            for outcome in source.outcomes
        ),
        "sort construction",
    )
    if sum(factorial(len(outcome.relation.rows)) for outcome in source.outcomes) <= (
        MAX_OUTCOME_ALTERNATIVES
    ):
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
            len(relation.rows),
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
                source_outcome.decisions,
                _merge_choices(source_outcome.choices, choices),
            )
        )
    if not outcomes:
        raise RelationError("sort produced no outcomes")
    return RelationFamily(tuple(outcomes))


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
        _unordered_row_pairs(row_count),
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
        len(source.outcomes)
        * (
            _unordered_row_pairs(row_count)
            + sum(len(group) * (len(group) - 1) for group in groups)
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
            row_count,
        )
        constraints = [
            _ordinal_constraints(relation.rows, ordinals, order)
        ]
        input_ordinals = relation.ordinals
        for group in groups:
            for left_position, left_index in enumerate(group):
                for right_position, right_index in enumerate(group):
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
    row_count: int,
) -> tuple[tuple[smt.Term, ...], tuple[OrdinalChoice, ...]]:
    ordinals = tuple(
        script.fresh_constant(f"{hint}:{index}", smt.INT)
        for index in range(row_count)
    )
    for ordinal in ordinals:
        script.register_quantified_choice(ordinal)
    return ordinals, tuple(
        OrdinalChoice(ordinal, row_count) for ordinal in ordinals
    )


def _ordinal_constraints(
    rows: tuple[Row, ...],
    ordinals: tuple[smt.Term, ...],
    order: tuple[SortOrder, ...] | None,
) -> smt.Term:
    """Constrain a bounded permutation without enumerating its sequences."""

    if len(rows) != len(ordinals):
        raise RelationError("sequence ordinals do not align with rows")
    bound = smt.int_value(len(rows))
    constraints: list[smt.Term] = []
    for row, ordinal in zip(rows, ordinals):
        in_range = smt.and_(
            smt.not_(smt.lt(ordinal, smt.ZERO)),
            smt.lt(ordinal, bound),
        )
        constraints.append(
            smt.ite(row.present, in_range, smt.eq(ordinal, smt.ZERO))
        )
    for left_index, left in enumerate(rows):
        for right_index in range(left_index + 1, len(rows)):
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
    return smt.and_(
        *(
            smt.or_(
                smt.not_(smt.and_(left.present, right.present)),
                smt.not_(_row_less(right, left, order)),
            )
            for index, left in enumerate(rows)
            for right in rows[index + 1 :]
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
    decision: str,
) -> RelationFamily:
    if source.sequence:
        return _ordered_limit_family(source, count_expression, offset_expression)
    return _unordered_limit_family(
        source,
        count_expression,
        offset_expression,
        decision,
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
    decision: str,
) -> RelationFamily:
    """Enumerate every legal unordered Take(Skip(input)) output bag.

    A mask is enabled exactly when all selected slots are present and its size
    equals ``min(count, max(input_size - offset, 0))``.  Keeping false-guarded
    unselected slots preserves a stable relation shape for downstream nodes.
    """

    count = _uint64_literal(count_expression, "limit count")
    offset = (
        0
        if offset_expression is None
        else _uint64_literal(offset_expression, "limit offset")
    )
    alternatives = 0
    outcomes: list[Outcome] = []
    for source_outcome in source.outcomes:
        rows = source_outcome.relation.rows
        if decision in dict(source_outcome.decisions):
            raise RelationError(f"duplicate unordered-limit decision {decision!r}")

        totals_by_size: dict[int, list[int]] = {}
        for total in range(len(rows) + 1):
            size = min(count, max(total - offset, 0))
            totals_by_size.setdefault(size, []).append(total)
        present_count = smt.add(
            *(smt.ite(row.present, smt.ONE, smt.ZERO) for row in rows)
        )

        for size, valid_totals in totals_by_size.items():
            for indices in combinations(range(len(rows)), size):
                alternatives += 1
                if alternatives > MAX_OUTCOME_ALTERNATIVES:
                    raise RelationError(
                        "unordered limit exceeds "
                        f"the {MAX_OUTCOME_ALTERNATIVES} alternative audit bound"
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
                        decisions,
                        source_outcome.choices,
                    )
                )
    if not outcomes:
        raise RelationError("unordered limit produced no outcomes")
    return RelationFamily(tuple(outcomes))


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
    left: tuple[OrdinalChoice, ...],
    right: tuple[OrdinalChoice, ...],
) -> tuple[OrdinalChoice, ...]:
    merged = {choice.term: choice for choice in left}
    for choice in right:
        previous = merged.get(choice.term)
        if previous is not None and previous.bound != choice.bound:
            raise RelationError("shared ordinal choice has inconsistent bounds")
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
) -> RelationFamily:
    """Give an unordered bag every possible compressed sequence order."""

    _require_relation_row_pairs(
        sum(
            _unordered_row_pairs(len(outcome.relation.rows))
            for outcome in family.outcomes
        ),
        "latent sequence construction",
    )
    if sum(factorial(len(outcome.relation.rows)) for outcome in family.outcomes) <= (
        MAX_OUTCOME_ALTERNATIVES
    ):
        return _enumerated_as_sequence_family(family)
    outcomes: list[Outcome] = []
    for index, source_outcome in enumerate(family.outcomes):
        relation = source_outcome.relation
        ordinals, choices = _fresh_ordinals(
            script,
            f"latent_sequence:{index}:ordinal",
            len(relation.rows),
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
                source_outcome.decisions,
                _merge_choices(source_outcome.choices, choices),
            )
        )
    if not outcomes:
        raise RelationError("latent unordered sequence family has no outcomes")
    return RelationFamily(tuple(outcomes))


def _enumerated_as_sequence_family(family: RelationFamily) -> RelationFamily:
    outcomes: list[Outcome] = []
    for source_outcome in family.outcomes:
        relation = source_outcome.relation
        for permutation in permutations(range(len(relation.rows))):
            outcomes.append(
                Outcome(
                    source_outcome.enabled,
                    Relation(
                        relation.columns,
                        tuple(relation.rows[index] for index in permutation),
                        sequence=True,
                    ),
                    source_outcome.decisions,
                    source_outcome.choices,
                )
            )
    if not outcomes:
        raise RelationError("latent unordered sequence family has no outcomes")
    return RelationFamily(tuple(outcomes))


def _comparison_inputs(
    left: RelationFamily,
    right: RelationFamily,
    script: smt.Script,
) -> tuple[RelationFamily, RelationFamily, bool]:
    ordered = left.sequence
    if ordered and not right.sequence:
        right = _as_sequence_family(right, script)
    comparisons = len(left.outcomes) * len(right.outcomes)
    if comparisons > MAX_OUTCOME_COMPARISONS:
        raise RelationError(
            f"outcome comparison requires {comparisons} pairs, exceeding "
            f"the {MAX_OUTCOME_COMPARISONS} pair audit bound"
        )
    return left, right, ordered


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


def _families_equivalent(
    left: RelationFamily,
    right: RelationFamily,
    scalar: ScalarEncoder,
    ordered: bool,
) -> smt.Term:
    def relations_equal(left_relation: Relation, right_relation: Relation) -> smt.Term:
        return _relations_equal(left_relation, right_relation, scalar, ordered)

    def choice_terms(outcome: Outcome) -> tuple[smt.Term, ...]:
        return tuple(choice.term for choice in outcome.choices)

    def exists_enabled(family: RelationFamily) -> smt.Term:
        return smt.or_(
            *(
                smt.exists(choice_terms(outcome), outcome.enabled)
                for outcome in family.outcomes
            )
        )

    def target_contains(source_outcome: Outcome, target: RelationFamily) -> smt.Term:
        return smt.or_(
            *(
                smt.exists(
                    choice_terms(target_outcome),
                    smt.and_(
                        target_outcome.enabled,
                        relations_equal(
                            source_outcome.relation,
                            target_outcome.relation,
                        ),
                    ),
                )
                for target_outcome in target.outcomes
            )
        )

    def unmatched(source: RelationFamily, target: RelationFamily) -> smt.Term:
        # Source choices stay globally existential and inspectable.  Target
        # choices are shadowed by the existential membership test; negating it
        # therefore proves that no legal target sequence matches this source.
        return smt.or_(
            *(
                smt.and_(
                    outcome.enabled,
                    smt.not_(target_contains(outcome, target)),
                )
                for outcome in source.outcomes
            )
        )

    left_exists = exists_enabled(left)
    right_exists = exists_enabled(right)
    globally_enabled = smt.and_(
        smt.or_(*(outcome.enabled for outcome in left.outcomes)),
        smt.or_(*(outcome.enabled for outcome in right.outcomes)),
    )
    different = smt.or_(
        smt.not_(left_exists),
        smt.not_(right_exists),
        smt.and_(
            globally_enabled,
            smt.or_(unmatched(left, right), unmatched(right, left)),
        ),
    )
    return smt.not_(different)


def compare_families(
    left: RelationFamily,
    right: RelationFamily,
    scalar: ScalarEncoder,
) -> FamilyComparison:
    """Expose the exact normalized outcome pairs used by family equivalence."""

    left, right, ordered = _comparison_inputs(left, right, scalar.script)
    pair_equal = tuple(
        tuple(
            _relations_equal(
                left_outcome.relation,
                right_outcome.relation,
                scalar,
                ordered,
            )
            for right_outcome in right.outcomes
        )
        for left_outcome in left.outcomes
    )
    equivalent = _families_equivalent(left, right, scalar, ordered)
    return FamilyComparison(left, right, ordered, pair_equal, equivalent)


def family_equal(
    left: RelationFamily,
    right: RelationFamily,
    scalar: ScalarEncoder,
) -> smt.Term:
    """Mutual inclusion of enabled bags or initial-query result sequences."""

    left, right, ordered = _comparison_inputs(left, right, scalar.script)
    return _families_equivalent(left, right, scalar, ordered)
