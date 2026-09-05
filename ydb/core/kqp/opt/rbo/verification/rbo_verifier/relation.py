"""Bounded bag semantics for the version-one relational operators."""

from __future__ import annotations

from dataclasses import dataclass, replace
from itertools import combinations, permutations
from math import factorial
from typing import Callable, Iterator, Literal, Mapping, TypeAlias

from . import aggregate as aggregate_kernel
from . import decimal, join as join_kernel, smt, sort_network, sort_strategy
from . import window as window_kernel
from .errors import RelationError
from .analysis import AnalysisError, AnalyzedPlan, analyze_snapshot
from .ir import (
    Aggregate,
    AggregateTrait,
    Column,
    EmptySource,
    Expr,
    ExistsSubplan,
    Filter,
    InSubplan,
    INTEGRAL_AVG_RANK_COMPARISON,
    Join,
    JoinKey,
    Limit,
    OuterBind,
    PlanNode,
    Project,
    Scan,
    ScalarSubplan,
    Snapshot,
    Sort,
    SortOrder,
    Subplan,
    UnionAll,
    WINDOW_ROWS_KINDS,
    expression_columns,
    plan_node_inputs,
)
from .scalar import (
    DecimalAverageState,
    DecimalSumState,
    Encoder as ScalarEncoder,
    IntegralAverageCertificate,
    IntegralAverageState,
    average_metadata_terms,
    decimal_sum_state_terms,
)
from .scalar import Value, date_domain, integer_domain, smt_sort
from .types import BOOL, DATE, DOUBLE, family, is_decimal_type, is_ordered_type
from .value_transport import (
    ValueTransportError,
    select_scalar,
    validated_decimal_sum_state as _validated_decimal_sum_state,
)


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
    present_prefix: bool = False
    # Private semantic certificates.  They are derived from the modeled plan
    # and deliberately never enter the snapshot wire format.
    null_safe_unique_key: frozenset[str] | None = None
    task_partition_key: frozenset[str] | None = None

    def __post_init__(self) -> None:
        _require_relation_rows(len(self.rows), "relation")
        column_names = frozenset(column.name for column in self.columns)
        for name, key in (
            ("null-safe unique key", self.null_safe_unique_key),
            ("task partition key", self.task_partition_key),
        ):
            if key is None:
                continue
            if (
                type(key) is not frozenset
                or not key
                or any(type(column) is not str or not column for column in key)
            ):
                raise ValueError(
                    f"relation {name} must be a non-empty frozenset of column names"
                )
            missing = key - column_names
            if missing:
                raise ValueError(
                    f"relation {name} columns are absent: "
                    f"{', '.join(sorted(missing))}"
                )
        if self.ordinals is not None:
            if len(self.ordinals) != len(self.rows):
                raise ValueError("relation ordinals must align with rows")
            if any(ordinal.sort != smt.INT for ordinal in self.ordinals):
                raise ValueError("relation ordinals must be SMT integers")
        if self.present_prefix and (
            not self.sequence or self.ordinals is not None
        ):
            raise ValueError(
                "present-prefix relations require a fixed sequence without ordinals"
            )


@dataclass(frozen=True, slots=True)
class _DirectUniqueRhs:
    """Admission tied to the exact join and provenance-checked RHS rows."""

    node: Join
    right: Relation


@dataclass(frozen=True, slots=True)
class _DelayedCrossPlan:
    """A private Cross-spine proposal; the complete Filter stays authoritative."""

    factors: tuple[str, ...]
    seed: str
    schedule: tuple[tuple[Join, tuple[JoinKey, ...]], ...]
    local_conjuncts: Mapping[str, tuple[Expr, ...]]
    columns: tuple[Column, ...]


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
    row_bindings: tuple[Mapping[str, Value], ...]
    correlated_errors: tuple[smt.Term, ...]
    decisions: tuple[tuple[str, int], ...]
    choices: tuple[BoundedChoice, ...]


NodeObserver: TypeAlias = Callable[[str, str, RelationFamily], None]


@dataclass(slots=True)
class _CorrelatedPairBudget:
    """One cumulative construction budget for every correlated invocation."""

    count: int = 0

    def charge(self, count: int) -> None:
        self.count += count
        _require_relation_row_pairs(
            self.count,
            "correlated scalar evaluation",
        )


@dataclass(slots=True)
class _BooleanSubplanPairBudget:
    """One cumulative construction budget for every Boolean subplan outcome."""

    count: int = 0

    def charge(self, count: int) -> None:
        self.count += count
        _require_relation_row_pairs(
            self.count,
            "Boolean subplan evaluation",
        )


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
    # Optional exact portfolio whose shape is preferable to the canonical
    # formula for the solver.  None deliberately means that ordinary
    # canonical-first scheduling remains in force.
    preferred_branches: tuple[MismatchBranch, ...] | None = None


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


MAX_OUTCOME_ALTERNATIVES = 256
MAX_OUTCOME_COMPARISONS = 4096
MAX_ENUMERATED_SEQUENCE_ROWS = 3
MAX_RELATION_ROWS = 4096
MAX_RELATION_ROW_PAIRS = 16384
MAX_SORT_NETWORK_COMPARATORS = 32768
# Stable logical-width budget for the packed representation: live input rows
# times scalar payload lanes. It is not a Python-memory or final-formula byte
# estimate; constructors, output selectors, and later relational comparison
# still materialize terms. Compare-exchange cells themselves move one whole
# row term independently of its width.
MAX_SORT_NETWORK_PAYLOAD_CELLS = 131072
# The exact lexicographic comparator is emitted once as a define-fun. Keeping
# its key width bounded makes that shared definition easy to audit.
MAX_SORT_NETWORK_KEY_COLUMNS = 64
# Preferred keyed comparison is a solver-shape optimization, so keep both its
# branch fan-out and the row-pair cell work independently auditable.  Falling
# back to the ordinary exact decomposition preserves semantics at either cap.
MAX_PREFERRED_KEYED_MISMATCH_BRANCHES = 64
MAX_PREFERRED_KEYED_MISMATCH_COMPARISONS = 256


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


def _require_grouped_distinct_capacity(
    node: Aggregate,
    row_count: int,
    group_candidate_count: int,
) -> None:
    if not any(trait.distinct for trait in node.aggregates):
        return
    equality_terms = (
        group_candidate_count * row_count * (row_count - 1) // 2
    )
    if equality_terms > MAX_RELATION_ROW_PAIRS:
        raise RelationError(
            "grouped distinct aggregate requires "
            f"{equality_terms} distinct-equality terms, exceeding "
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


def _live_join_input(relation: Relation) -> Relation:
    """Erase join input slots whose presence guard is literally false."""

    indices = _live_row_indices(relation.rows)
    return (
        relation
        if len(indices) == len(relation.rows)
        else Relation(
            relation.columns,
            tuple(relation.rows[index] for index in indices),
        )
    )


def _syntactically_implies(term: smt.Term, required: smt.Term) -> bool:
    """Recognize the small guard language used by scan/task routing."""

    if term == smt.FALSE or term == required:
        return True
    if term.operation == "and":
        return any(
            _syntactically_implies(argument, required)
            for argument in term.arguments
        )
    if term.operation == "or":
        return all(
            _syntactically_implies(argument, required)
            for argument in term.arguments
        )
    return False


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
        outer_bindings: Mapping[str, Value] | None = None,
        _context: AnalyzedPlan | None = None,
        _correlated_pair_budget: _CorrelatedPairBudget | None = None,
        _boolean_subplan_pair_budget: _BooleanSubplanPairBudget | None = None,
    ) -> None:
        self.snapshot = snapshot
        self.database = database
        self.scalar = scalar
        if _context is None:
            try:
                _context = analyze_snapshot(snapshot)
            except AnalysisError as error:
                raise RelationError(str(error)) from error
        elif _context.snapshot is not snapshot:
            raise RelationError(
                "an evaluator context may only be shared by one snapshot"
            )
        self._context = _context
        self.nodes = _context.nodes
        self.schemas = _context.schemas
        self.cache: dict[str, RelationFamily] = dict(node_overrides or {})
        self.node_overrides = frozenset((node_overrides or {}).keys())
        self.edge_inputs = edge_inputs or {}
        self.choice_scope = choice_scope
        self.defer_pushed_limits = defer_pushed_limits
        self.node_observer = node_observer
        self.outer_bindings = outer_bindings or {}
        self.observed_nodes: set[str] = set()
        self.subplans_by_consumer = _context.subplans_by_consumer
        self.subplan_families: dict[str, SubplanFamily] = {}
        self.scalar_outer_binds = _context.scalar_outer_binds
        self._correlated_pair_budget = (
            _correlated_pair_budget
            if _correlated_pair_budget is not None
            else _CorrelatedPairBudget()
        )
        self._boolean_subplan_pair_budget = (
            _boolean_subplan_pair_budget
            if _boolean_subplan_pair_budget is not None
            else _BooleanSubplanPairBudget()
        )

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
                present_prefix=relation.present_prefix,
                null_safe_unique_key=_retained_key(
                    relation.null_safe_unique_key,
                    output,
                ),
                task_partition_key=_retained_key(
                    relation.task_partition_key,
                    output,
                ),
            ),
        )

    def node(self, node_id: str) -> RelationFamily:
        if node_id not in self.cache:
            node = self.nodes[node_id]
            self.cache[node_id] = self._evaluate(node)
        family = self.cache[node_id]
        # Observation happens at the producer, before any parent can route,
        # compact, sort, limit, project, or discard its rows. This lifecycle
        # lets completed-AVG certificates remain node-local.
        if self.node_observer is not None and node_id not in self.observed_nodes:
            self.node_observer(self.choice_scope, node_id, family)
            self.observed_nodes.add(node_id)
        family = _strip_integral_average_certificates(family)
        self.cache[node_id] = family
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

        if isinstance(node, OuterBind):
            value = self.outer_bindings.get(node.id)
            if value is None:
                raise RelationError(
                    f"outer_bind {node.id!r} was evaluated outside a "
                    "correlated scalar invocation"
                )
            if value.type != node.type:
                raise RelationError(
                    f"outer_bind {node.id!r} expected {node.type!r}, "
                    f"got {value.type!r}"
                )
            source = self._input(node.id, 0, node.input)
            column = Column(node.dependency, node.type, node.nullable)
            return map_family(
                source,
                lambda relation: Relation(
                    relation.columns + (column,),
                    tuple(
                        Row(
                            row.present,
                            dict(row.values) | {node.dependency: value},
                            row.occurrence,
                            row.partition_facts,
                        )
                        for row in relation.rows
                    ),
                    sequence=relation.sequence,
                    order=relation.order,
                    ordinals=relation.ordinals,
                    present_prefix=relation.present_prefix,
                ),
            )

        if isinstance(node, Project):
            source = self._input(node.id, 0, node.input)
            columns = self._columns(node.id)
            windows = tuple(
                window
                for projection in node.columns
                if (
                    window := _whole_partition_decimal_window_expression(
                        projection.expression
                    )
                )
                is not None
            )
            assert len(windows) <= 1
            window = windows[0] if windows else None
            ranks = tuple(
                projection.expression
                for projection in node.columns
                if projection.expression.kind == "window_rank"
            )
            row_windows = tuple(
                projection.expression
                for projection in node.columns
                if projection.expression.kind in WINDOW_ROWS_KINDS
            )

            def project(
                relation: Relation,
                bindings: Callable[[int, Row], Mapping[str, Value]],
                supplied_relational_values: (
                    tuple[Mapping[Expr, Value], ...] | None
                ) = None,
            ) -> Relation:
                relational_values: tuple[Mapping[Expr, Value], ...]
                if supplied_relational_values is not None:
                    relational_values = supplied_relational_values
                elif window is None:
                    relational_values = tuple({} for _row in relation.rows)
                else:
                    _require_relation_row_pairs(
                        len(relation.rows) * len(relation.rows),
                        window.kind.replace("_", " "),
                    )
                    relational_values = tuple(
                        {
                            window: self._whole_partition_decimal_window_value(
                                window,
                                relation,
                                row,
                            )
                        }
                        for row in relation.rows
                    )
                rows = []
                for row_index, row in enumerate(relation.rows):
                    values = dict(row.values) | bindings(row_index, row)
                    projected = {}
                    for projection in node.columns:
                        value = self.scalar.evaluate(
                            projection.expression,
                            values,
                            relational_values[row_index],
                        )
                        projected[projection.output] = (
                            replace(value, is_null=smt.FALSE)
                            if projection.error_on_null
                            else value
                        )
                    rows.append(
                        Row(
                            row.present,
                            projected,
                            row.occurrence,
                            row.partition_facts,
                        )
                    )
                return Relation(
                    columns,
                    tuple(rows),
                    sequence=relation.sequence,
                    order=_projected_order(relation.order, node),
                    ordinals=relation.ordinals,
                    present_prefix=relation.present_prefix,
                    null_safe_unique_key=_projected_key(
                        relation.null_safe_unique_key,
                        node,
                    ),
                    task_partition_key=_projected_key(
                        relation.task_partition_key,
                        node,
                    ),
                )

            marked_sources = tuple(
                projection.expression.column
                for projection in node.columns
                if projection.error_on_null
            )
            checked_concats = tuple(
                projection.expression
                for projection in node.columns
                if projection.expression.kind == "checked_concat"
            )

            def project_error(relation: Relation) -> smt.Term:
                return smt.or_(
                    *(
                        smt.and_(
                            row.present,
                            row.values[source].is_null,
                        )
                        for row in relation.rows
                        for source in marked_sources
                    ),
                    *(
                        smt.and_(
                            row.present,
                            self.scalar.checked_concat_failure(
                                expression,
                                row.values,
                            ),
                        )
                        for row in relation.rows
                        for expression in checked_concats
                    ),
                )

            if ranks or row_windows:
                # Ordered-window validation excludes subplans, so each source
                # outcome can retain its unstable-sort choices directly.
                outcomes: list[Outcome] = []
                for outcome_index, source_outcome in enumerate(source.outcomes):
                    if ranks:
                        (
                            windowed,
                            relational_values,
                            window_enabled,
                            window_choices,
                        ) = self._window_rank_values(
                            node,
                            source_outcome.relation,
                            ranks,
                            outcome_index,
                        )
                    else:
                        (
                            windowed,
                            relational_values,
                            window_enabled,
                            window_choices,
                        ) = self._window_rows_values(
                            node,
                            source_outcome.relation,
                            row_windows,
                            outcome_index,
                        )
                    outcomes.append(
                        Outcome(
                            smt.and_(source_outcome.enabled, window_enabled),
                            project(
                                windowed,
                                lambda _index, _row: {},
                                relational_values,
                            ),
                            smt.or_(
                                source_outcome.error,
                                (
                                    smt.FALSE
                                    if not marked_sources and not checked_concats
                                    else project_error(windowed)
                                ),
                            ),
                            source_outcome.decisions,
                            _merge_choices(
                                source_outcome.choices,
                                window_choices,
                            ),
                        )
                    )
                return RelationFamily(tuple(outcomes))

            return self._with_consumer_subplans(
                node.id,
                source,
                project,
                local_error=(
                    None
                    if not marked_sources and not checked_concats
                    else project_error
                ),
            )

        if isinstance(node, Filter):
            return self._filter(node)

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
                compact_prefix=(
                    node.limit is not None
                    and node.phase == "intermediate"
                ),
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

    def _window_rank_values(
        self,
        node: Project,
        source: Relation,
        ranks: tuple[Expr, ...],
        outcome_index: int,
    ) -> tuple[
        Relation,
        tuple[Mapping[Expr, Value], ...],
        smt.Term,
        tuple[BoundedChoice, ...],
    ]:
        """Evaluate sequential global Rank definitions on one stage task.

        KQP physical-stage connection inputs are Streams.  Each source window
        definition therefore lowers its Sort to UnstableSort, including the
        second definition in a rebuilt CalcOverWindowGroup.  Equal-key orders
        are consequently fresh between definitions.  CalcOverWindow exports no
        sorted constraint, so these physical sort orders affect Rank values but
        do not become an observable sequence contract for downstream operators.
        """

        ordered_ranks = tuple(
            sorted(ranks, key=lambda rank: rank.execution_order)
        )
        _require_relation_row_pairs(
            len(source.rows) * len(source.rows) * len(ordered_ranks),
            "window rank",
        )
        relation = source
        enabled: list[smt.Term] = []
        choices: tuple[BoundedChoice, ...] = ()
        relational_values: list[dict[Expr, Value]] = [
            {} for _row in source.rows
        ]
        for rank in ordered_ranks:
            assert rank.execution_order is not None
            assert rank.order_by is not None and len(rank.order_by) == 1
            order = rank.order_by
            order_item = order[0]
            _require_order_columns(relation.columns, order, "window rank")
            ordinals, rank_choices = _fresh_ordinals(
                self.scalar.script,
                f"{self.choice_scope}:window_rank:{node.id}:"
                f"{outcome_index}:{rank.execution_order}:ordinal",
                relation.rows,
            )
            enabled.append(
                _ordinal_constraints(relation.rows, ordinals, order)
            )
            choices = _merge_choices(choices, rank_choices)
            values = window_kernel.rank_values(
                tuple(row.present for row in relation.rows),
                tuple(row.values[order_item.column] for row in relation.rows),
                ordinals,
                lambda left, right: _ordered_value_less(left, right, order_item),
            )
            for row_values, value in zip(relational_values, values):
                row_values[rank] = value
        return (
            replace(
                source,
                sequence=False,
                order=None,
                ordinals=None,
                present_prefix=False,
                null_safe_unique_key=source.null_safe_unique_key,
                task_partition_key=source.task_partition_key,
            ),
            tuple(relational_values),
            smt.and_(*enabled),
            choices,
        )

    def _window_rows_values(
        self,
        node: Project,
        source: Relation,
        windows: tuple[Expr, ...],
        outcome_index: int,
    ) -> tuple[
        Relation,
        tuple[Mapping[Expr, Value], ...],
        smt.Term,
        tuple[BoundedChoice, ...],
    ]:
        """Evaluate q51's task-local unstable ROWS-prefix windows.

        Every transported definition lowers an independent unstable sort.
        Equal-date peers may therefore use a different permutation in each
        leaf, including the two outer MAX calls.  The Project publishes no
        downstream sequence contract.
        """

        ordered_windows = tuple(
            sorted(windows, key=lambda window: window.execution_order)
        )
        row_count = len(source.rows)
        construction_cost = len(ordered_windows) * (
            row_count * row_count + row_count * (row_count - 1) // 2
        )
        _require_relation_row_pairs(construction_cost, "q51 ROWS window")
        enabled: list[smt.Term] = []
        choices: tuple[BoundedChoice, ...] = ()
        relational_values: list[dict[Expr, Value]] = [
            {} for _row in source.rows
        ]
        for window in ordered_windows:
            assert window.window_input is not None
            assert window.partition_by is not None and len(window.partition_by) == 1
            assert window.execution_order is not None
            assert window.order_by is not None and len(window.order_by) == 1
            assert window.result_type is not None
            _require_order_columns(source.columns, window.order_by, "q51 ROWS window")
            partition = window.partition_by[0]
            if partition not in {column.name for column in source.columns}:
                raise RelationError(
                    f"q51 ROWS window partition column {partition!r} is unavailable"
                )
            ordinals, window_choices = _fresh_ordinals(
                self.scalar.script,
                f"{self.choice_scope}:window_rows:{node.id}:"
                f"{outcome_index}:{window.execution_order}:ordinal",
                source.rows,
            )
            enabled.append(
                _window_rows_ordinal_constraints(
                    self.scalar,
                    source.rows,
                    ordinals,
                    partition,
                    window.order_by,
                )
            )
            choices = _merge_choices(choices, window_choices)
            values = window_kernel.rows_prefix_values(
                window.kind,
                window.result_type,
                tuple(row.values[window.window_input] for row in source.rows),
                tuple(row.present for row in source.rows),
                tuple(row.values[partition] for row in source.rows),
                ordinals,
                self.scalar.not_distinct,
            )
            for row_values, value in zip(relational_values, values):
                row_values[window] = value
        return (
            replace(
                source,
                sequence=False,
                order=None,
                ordinals=None,
                present_prefix=False,
                null_safe_unique_key=source.null_safe_unique_key,
                task_partition_key=source.task_partition_key,
            ),
            tuple(relational_values),
            smt.and_(*enabled),
            choices,
        )

    def _whole_partition_decimal_window_value(
        self,
        expression: Expr,
        source: Relation,
        candidate: Row,
    ) -> Value:
        assert expression.kind in {"window_sum", "window_avg"}
        assert expression.window_input is not None
        assert expression.partition_by is not None
        assert expression.result_type is not None
        return window_kernel.whole_partition_decimal(
            expression.kind,
            expression.result_type,
            tuple(row.values[expression.window_input] for row in source.rows),
            tuple(row.present for row in source.rows),
            tuple(
                tuple(row.values[key] for key in expression.partition_by)
                for row in source.rows
            ),
            tuple(candidate.values[key] for key in expression.partition_by),
            self.scalar.not_distinct,
        )

    def _aggregate(self, node: Aggregate, source: Relation) -> Relation:
        modeled_functions = (
            {"distinct"}
            if node.distinct_all
            else {"avg", "count", "max", "min", "sum"}
        )
        unsupported = sorted(
            {trait.function for trait in node.aggregates}
            - modeled_functions
        )
        if unsupported:
            raise RelationError(
                f"aggregate functions are not modeled: {', '.join(unsupported)}"
            )

        rows: list[Row] = []
        if node.distinct_all:
            if not node.keys or len(node.keys) != len(node.aggregates):
                raise RelationError(
                    "DistinctAll requires one distinct trait for each ordered key"
                )
            rows.extend(self._grouped_aggregate_rows(node, source))
        elif not node.keys:
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
        return Relation(
            self._columns(node.id),
            tuple(rows),
            null_safe_unique_key=_aggregate_unique_key(node),
            task_partition_key=_aggregate_partition_key(
                node,
                source.task_partition_key,
            ),
        )

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
            _require_grouped_distinct_capacity(
                node,
                row_count,
                class_count,
            )
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
            _require_grouped_distinct_capacity(
                node,
                row_count,
                row_count,
            )
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
        _require_grouped_distinct_capacity(
            node,
            row_count,
            row_count,
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
        if node.distinct_all:
            if candidate is None:
                raise RelationError("DistinctAll requires a group representative")
            return {
                trait.output: candidate.values[key]
                for key, trait in zip(node.keys, node.aggregates)
            }
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
        values = tuple(row.values[trait.input] for row in source.rows)
        if trait.distinct:
            _require_relation_row_pairs(
                len(source.rows) * (len(source.rows) - 1) // 2,
                "distinct aggregate",
            )
        non_null = aggregate_kernel.non_null_membership(
            values,
            matches,
            distinct=trait.distinct,
            equal=self.scalar.aggregate_equal,
        )
        if (
            trait.function == "sum"
            and decimal.is_type(trait.output_type)
            and (node.id, trait.output) in self._context.decimal_sum_state_consumers
        ):
            combined = self._combined_decimal_sum_value(trait, source, matches)
            if combined is not None:
                return combined
        return aggregate_kernel.reduce(
            trait,
            node.phase,
            values,
            non_null,
            integral_average=self.scalar.integral_int64_average,
            carry_sum_state=(
                (node.id, trait.output) in self._context.decimal_sum_state_producers
            ),
        )

    def _combined_decimal_sum_value(
        self,
        trait: AggregateTrait,
        source: Relation,
        matches: tuple[smt.Term, ...],
    ) -> Value | None:
        """Admit a complete matching set of private partial SUM states."""

        input_column = next(
            column for column in source.columns if column.name == trait.input
        )
        guarded_states: list[tuple[smt.Term, DecimalSumState]] = []
        for match, row in zip(matches, source.rows):
            if row.present == smt.FALSE:
                continue
            value = row.values[trait.input]
            state = _validated_decimal_sum_state(value, input_column.nullable)
            if state is None or state.sum_type != trait.output_type:
                return None
            non_null = smt.and_(match, smt.not_(value.is_null))
            if non_null != smt.FALSE:
                guarded_states.append((non_null, state))
        return aggregate_kernel.combine_decimal_sum(
            tuple(guarded_states), trait.output_type, trait.output_nullable,
        )

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

    def _filter(
        self,
        node: Filter,
        *,
        encoding: Literal["auto", "baseline", "factored"] = "auto",
    ) -> RelationFamily:
        """Apply the whole SQL predicate after selecting an exact input encoding."""

        if encoding not in {"auto", "baseline", "factored"}:
            raise RelationError(f"unknown Filter encoding {encoding!r}")
        source = None if encoding == "baseline" else self._factor_delayed_cross_filter(node)
        if source is None:
            if encoding == "factored":
                raise RelationError("factored Filter requires an admitted private Cross spine")
            source = self._input(node.id, 0, node.input)

        def retain(relation: Relation, bindings: Callable[[int, Row], Mapping[str, Value]]) -> Relation:
            # Filtering keeps payload, order, provenance, and key certificates.
            # It can introduce holes, so a compact present prefix is no longer known.
            return replace(relation, rows=tuple(
                replace(row, present=smt.and_(row.present, self.scalar.is_true(
                    self.scalar.evaluate(node.predicate, dict(row.values) | bindings(index, row)),
                )))
                for index, row in enumerate(relation.rows)
            ), present_prefix=False)

        return self._with_consumer_subplans(node.id, source, retain)

    def _plan_delayed_cross_filter(
        self,
        node: Filter,
    ) -> _DelayedCrossPlan | None:
        """Admit and schedule a private spine without evaluating or pruning rows."""

        if (
            self.snapshot.stage_graph is not None
            or node.id in self.subplans_by_consumer
            or (node.id, 0) in self.edge_inputs
        ):
            return None

        conjuncts = (
            node.predicate.args
            if node.predicate.kind == "and"
            else (node.predicate,)
        )
        equalities: list[tuple[str, str]] = []
        for conjunct in conjuncts:
            if (
                conjunct.kind != "eq"
                or conjunct.null_safe
                or len(conjunct.args) != 2
                or any(
                    argument.kind != "column"
                    or argument.column is None
                    or argument.depth is not None
                    for argument in conjunct.args
                )
            ):
                continue
            left, right = conjunct.args
            assert left.column is not None and right.column is not None
            equalities.append((left.column, right.column))
        spine: list[Join] = []
        current = node.input
        while True:
            candidate = self.nodes[current]
            if not (
                isinstance(candidate, Join)
                and candidate.kind == "cross"
                and not candidate.keys
                and candidate.predicate.kind == "literal"
                and candidate.predicate.result_type == BOOL
                and candidate.predicate.nullable is False
                and candidate.predicate.value is True
            ):
                break
            # A cached node may be an explicit caller override or an already
            # observed shared producer.  Its original Cross meaning must win.
            if (
                candidate.id in self.cache
                or (candidate.id, 0) in self.edge_inputs
                or (candidate.id, 1) in self.edge_inputs
            ):
                return None
            spine.append(candidate)
            current = candidate.left
        if not spine:
            return None
        spine.reverse()
        for index, join in enumerate(spine):
            expected_parent = (
                spine[index + 1].id
                if index + 1 < len(spine)
                else node.id
            )
            if self._context.parents[join.id] != frozenset({expected_parent}):
                return None
        transformed_nodes = {
            item
            for join in spine
            for item in (join.id, join.right)
        } | {current}
        if (
            transformed_nodes & self.node_overrides
            or any(
                subplan.root in transformed_nodes
                for subplan in self.snapshot.plan.subplans
            )
        ):
            return None

        scheduled_seed, schedule = self._schedule_delayed_unique_rhs(
            spine,
            current,
            equalities,
        )
        factors = (current,) + tuple(join.right for join in spine)
        local_conjuncts = self._factor_local_conjuncts(
            factors,
            conjuncts,
        )
        has_keys = any(keys for _join, keys in schedule)
        if not has_keys and not any(local_conjuncts.values()):
            return None
        return _DelayedCrossPlan(
            factors, scheduled_seed, schedule, local_conjuncts, self._columns(node.input),
        )

    def _factor_delayed_cross_filter(self, node: Filter) -> RelationFamily | None:
        """Execute an admitted proposal; unresolved predicates are never assumed.

        Pruning removes only syntactically rejected rows. A key promotion is
        used only after checking the evaluated RHS's exact scan provenance.
        Neither step replaces the complete Filter applied by `_filter`.
        """

        plan = self._plan_delayed_cross_filter(node)
        if plan is None:
            return None
        factors, local_conjuncts = plan.factors, plan.local_conjuncts

        factor_sources = {
            factor: self.node(factor)
            for factor in factors
        }
        pruning_work = sum(
            len(outcome.relation.rows) * len(local_conjuncts[factor])
            for factor, family in factor_sources.items()
            for outcome in family.outcomes
        )
        pruned = False
        if pruning_work <= MAX_RELATION_ROW_PAIRS:
            for factor in factors:
                filtered, factor_pruned = (
                    self._prune_statically_rejected_factor_rows(
                        factor_sources[factor],
                        local_conjuncts[factor],
                    )
                )
                factor_sources[factor] = filtered
                pruned |= factor_pruned
        if not any(keys for _join, keys in plan.schedule) and not pruned:
            return None

        source = factor_sources[plan.seed]
        for original, keys in plan.schedule:
            right = factor_sources[original.right]

            def join_relations(
                relations: tuple[Relation, ...],
                original: Join = original,
                keys: tuple[JoinKey, ...] = keys,
            ) -> Relation:
                return self._join_scheduled_cross(
                    original,
                    keys,
                    relations[0],
                    relations[1],
                )

            source = combine_families(
                (source, right),
                join_relations,
            )
        columns = plan.columns
        return map_family(
            source,
            lambda relation: replace(
                relation,
                columns=columns,
                rows=tuple(
                    replace(
                        row,
                        values={
                            column.name: row.values[column.name]
                            for column in columns
                        },
                    )
                    for row in relation.rows
                ),
                null_safe_unique_key=None,
                task_partition_key=None,
            ),
        )

    def _factor_local_conjuncts(
        self,
        factors: tuple[str, ...],
        conjuncts: tuple[Expr, ...],
    ) -> dict[str, tuple[Expr, ...]]:
        result: dict[str, list[Expr]] = {
            factor: []
            for factor in factors
        }
        factor_columns = {
            factor: frozenset(self.schemas[factor])
            for factor in factors
        }
        for conjunct in conjuncts:
            columns = expression_columns(conjunct)
            if not columns:
                continue
            owners = tuple(
                factor
                for factor in factors
                if columns <= factor_columns[factor]
            )
            if len(owners) == 1:
                result[owners[0]].append(conjunct)
        return {
            factor: tuple(expressions)
            for factor, expressions in result.items()
        }

    def _prune_statically_rejected_factor_rows(
        self,
        source: RelationFamily,
        conjuncts: tuple[Expr, ...],
    ) -> tuple[RelationFamily, bool]:
        if not conjuncts:
            return source, False

        changed = False

        def prune(relation: Relation) -> Relation:
            nonlocal changed
            indices = tuple(
                index
                for index, row in enumerate(relation.rows)
                if not any(
                    smt.and_(
                        row.present,
                        self.scalar.is_true(
                            self.scalar.evaluate(conjunct, row.values)
                        ),
                    )
                    == smt.FALSE
                    for conjunct in conjuncts
                )
            )
            if len(indices) == len(relation.rows):
                return relation
            changed = True
            # Filtering out impossible positions preserves sequence and order;
            # ordinal metadata remains aligned by taking the same indices.
            return replace(
                relation,
                rows=tuple(relation.rows[index] for index in indices),
                ordinals=(
                    None
                    if relation.ordinals is None
                    else tuple(relation.ordinals[index] for index in indices)
                ),
                null_safe_unique_key=relation.null_safe_unique_key,
                task_partition_key=relation.task_partition_key,
            )

        return map_family(source, prune), changed

    def _schedule_delayed_unique_rhs(
        self,
        spine: list[Join],
        seed: str,
        equalities: list[tuple[str, str]],
    ) -> tuple[str, tuple[tuple[Join, tuple[JoinKey, ...]], ...]]:
        available = dict(self.schemas[seed])
        pending = list(spine)
        schedule: list[tuple[Join, tuple[JoinKey, ...]]] = []
        first = pending[0]
        assert first.left == seed
        # Cross is bag-commutative.  Rebase only the innermost factor when
        # the fixed orientation has no certificate but the original seed is
        # itself a certified direct unique RHS.
        if not self._delayed_unique_rhs_keys(
            first,
            equalities,
            available,
        ):
            rebased = replace(
                first,
                left=first.right,
                right=seed,
            )
            rebased_keys = self._delayed_unique_rhs_keys(
                rebased,
                equalities,
                self.schemas[first.right],
            )
            if rebased_keys:
                seed = first.right
                available = dict(self.schemas[seed])
                pending.pop(0)
                schedule.append((rebased, rebased_keys))
                available.update(self.schemas[rebased.right])
        while pending:
            selected = 0
            keys: tuple[JoinKey, ...] = ()
            for index, candidate in enumerate(pending):
                candidate_keys = self._delayed_unique_rhs_keys(
                    candidate,
                    equalities,
                    available,
                )
                if candidate_keys:
                    selected = index
                    keys = candidate_keys
                    break
            factor = pending.pop(selected)
            schedule.append((factor, keys))
            available.update(self.schemas[factor.right])
        return seed, tuple(schedule)

    def _join_scheduled_cross(
        self,
        original: Join,
        keys: tuple[JoinKey, ...],
        left: Relation,
        right: Relation,
    ) -> Relation:
        columns = left.columns + right.columns
        normalized = replace(original, kind="inner", keys=keys)
        left_schema = {
            column.name: column
            for column in left.columns
        }
        if keys and self._admit_direct_unique_rhs(
            normalized,
            right,
            left_schema=left_schema,
        ) is not None:
            return self._join(
                normalized,
                left,
                right,
                output_columns=columns,
                compact_left_schema=left_schema,
            )
        return self._join(
            original,
            left,
            right,
            output_columns=columns,
        )

    def _delayed_unique_rhs_keys(
        self,
        node: Join,
        equalities: list[tuple[str, str]],
        left_schema: Mapping[str, Column],
    ) -> tuple[JoinKey, ...]:
        right_schema = self.schemas[node.right]
        keys: list[JoinKey] = []
        for first, second in equalities:
            if first in left_schema and second in right_schema:
                left, right = first, second
            elif second in left_schema and first in right_schema:
                left, right = second, first
            else:
                continue
            if left_schema[left].type != right_schema[right].type:
                continue
            key = JoinKey(left, right)
            if key not in keys:
                keys.append(key)
        if not keys:
            return ()

        promoted = replace(node, kind="inner", keys=tuple(keys))
        return (
            tuple(keys)
            if self._direct_unique_rhs_scan(
                promoted,
                left_schema=left_schema,
            )
            is not None
            else ()
        )

    def _with_consumer_subplans(
        self,
        node_id: str,
        source: RelationFamily,
        transform: Callable[
            [Relation, Callable[[int, Row], Mapping[str, Value]]],
            Relation,
        ],
        local_error: Callable[[Relation], smt.Term] | None = None,
    ) -> RelationFamily:
        subplans = self.subplans_by_consumer.get(node_id, ())
        if not subplans:
            if local_error is not None:
                return RelationFamily(
                    tuple(
                        Outcome(
                            outcome.enabled,
                            transform(
                                outcome.relation,
                                lambda _index, _row: {},
                            ),
                            smt.or_(
                                outcome.error,
                                local_error(outcome.relation),
                            ),
                            outcome.decisions,
                            outcome.choices,
                        )
                        for outcome in source.outcomes
                    )
                )
            return map_family(
                source,
                lambda relation: transform(
                    relation,
                    lambda _index, _row: {},
                ),
            )

        correlated_scalars = tuple(
            subplan
            for subplan in subplans
            if (
                isinstance(subplan, ScalarSubplan)
                and subplan.dependency is not None
            )
        )
        # These roots are closed and may be shared across outer rows.  An
        # EXISTS predicate can still correlate the shared root with each row.
        closed_subplans = tuple(
            subplan
            for subplan in subplans
            if subplan not in correlated_scalars
        )
        binding_families: list[SubplanFamily] = []
        for subplan in closed_subplans:
            family = self.subplan_families.get(subplan.binding)
            if family is None:
                family = self._evaluate_subplan(subplan)
                self.subplan_families[subplan.binding] = family
            binding_families.append(family)

        partials: list[_SubplanPartial] = []
        for outer_outcome_index, outcome in enumerate(source.outcomes):
            row_bindings: list[dict[str, Value]] = [
                {} for _row in outcome.relation.rows
            ]
            correlated_errors: list[smt.Term] = []
            for subplan in correlated_scalars:
                values, errors = self._evaluate_correlated_scalar_rows(
                    subplan,
                    outcome.relation,
                    outer_outcome_index,
                    outcome.enabled,
                )
                for row_index, value in enumerate(values):
                    row_bindings[row_index][subplan.binding] = value
                correlated_errors.extend(errors)
            partials.append(
                _SubplanPartial(
                    enabled=outcome.enabled,
                    relations=(outcome.relation,),
                    inherited_errors=(outcome.error,),
                    cardinality_errors=(),
                    row_bindings=tuple(row_bindings),
                    correlated_errors=tuple(correlated_errors),
                    decisions=outcome.decisions,
                    choices=outcome.choices,
                )
            )

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
                            row_bindings=partial.row_bindings,
                            correlated_errors=partial.correlated_errors,
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
            membership_pairs = sum(
                len(partial.relations[0].rows)
                * len(partial.relations[index].rows)
                for index, subplan in enumerate(
                    closed_subplans,
                    start=1,
                )
                if isinstance(subplan, (ExistsSubplan, InSubplan))
            )
            self._boolean_subplan_pair_budget.charge(
                membership_pairs,
            )

            def bindings(row_index: int, row: Row) -> Mapping[str, Value]:
                values = {
                    subplan.binding: self._subplan_value(
                        subplan,
                        row,
                        partial.relations[index],
                    )
                    for index, subplan in enumerate(
                        closed_subplans,
                        start=1,
                    )
                }
                values.update(partial.row_bindings[row_index])
                return values

            # An uncorrelated binding's local cardinality check is demanded by
            # any consumer row, including through a dead expression branch.
            # Its inherited errors remain eager. Correlated invocation errors
            # above are already gated by their particular outer row.
            demanded = smt.or_(
                *(row.present for row in partial.relations[0].rows)
            )
            outcomes.append(
                Outcome(
                    partial.enabled,
                    transform(partial.relations[0], bindings),
                    smt.or_(
                        *partial.inherited_errors,
                        *partial.correlated_errors,
                        *(
                            smt.and_(demanded, error)
                            for error in partial.cardinality_errors
                        ),
                        *(
                            (local_error(partial.relations[0]),)
                            if local_error is not None
                            else ()
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
            if subplan.dependency is not None:
                raise RelationError(
                    "a correlated scalar subplan must be evaluated per outer row"
                )
            return self._evaluate_scalar_subplan(subplan)
        assert isinstance(subplan, (ExistsSubplan, InSubplan))
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
        assert isinstance(subplan, (ExistsSubplan, InSubplan))
        matches = []
        for inner_row in relation.rows:
            match = inner_row.present
            if isinstance(subplan, InSubplan):
                outer_value = outer_row.values[subplan.lookup.column]
                inner_value = inner_row.values[subplan.output.column]
                # The nullable-column slice is accepted only as a direct
                # positive Filter conjunct.  A SQL IN predicate makes that
                # Filter true exactly when one present pair is non-NULL and
                # equal; FALSE and UNKNOWN both reject the outer row.
                match = smt.and_(
                    match,
                    smt.not_(outer_value.is_null),
                    smt.not_(inner_value.is_null),
                    smt.eq(outer_value.value, inner_value.value),
                )
            elif subplan.predicate is not None:
                assert subplan.dependencies
                outer_bindings = {
                    dependency: outer_row.values[dependency]
                    for dependency in subplan.dependencies
                }
                match = smt.and_(
                    match,
                    self.scalar.is_true(
                        self.scalar.evaluate(
                            subplan.predicate,
                            outer_bindings | dict(inner_row.values),
                        )
                    ),
                )
            matches.append(match)
        return Value("Bool", smt.FALSE, smt.or_(*matches))

    def _evaluate_correlated_scalar_rows(
        self,
        subplan: ScalarSubplan,
        outer: Relation,
        outer_outcome_index: int,
        outer_outcome_enabled: smt.Term,
    ) -> tuple[tuple[Value, ...], tuple[smt.Term, ...]]:
        outer_bind = self.scalar_outer_binds[subplan.binding]
        closed = self.node(outer_bind.input)
        closed_outcome = self._deterministic_correlated_outcome(
            subplan,
            closed.outcomes,
            "closed input",
        )
        self._correlated_pair_budget.charge(
            len(outer.rows) * len(closed_outcome.relation.rows),
        )

        values: list[Value] = []
        errors: list[smt.Term] = []
        for row_index, outer_row in enumerate(outer.rows):
            if outer_row.present == smt.FALSE:
                values.append(self.scalar.null(subplan.output.type))
                errors.append(smt.FALSE)
                continue
            assert subplan.dependency is not None
            child = Evaluator(
                self.snapshot,
                self.database,
                self.scalar,
                node_overrides={outer_bind.input: closed},
                choice_scope=(
                    f"{self.choice_scope}:correlated_scalar:"
                    f"{subplan.binding}:outcome:{outer_outcome_index}:"
                    f"row:{row_index}"
                ),
                outer_bindings={
                    outer_bind.id: outer_row.values[subplan.dependency],
                },
                node_observer=self._invocation_observer(
                    smt.and_(outer_outcome_enabled, outer_row.present)
                ),
                _context=self._context,
                _correlated_pair_budget=self._correlated_pair_budget,
                _boolean_subplan_pair_budget=self._boolean_subplan_pair_budget,
            )
            scalar_family = self._scalarize_subplan(
                subplan,
                child.node(subplan.root),
            )
            scalar_outcome = self._deterministic_correlated_outcome(
                subplan,
                tuple(item.outcome for item in scalar_family.outcomes),
                "result",
            )
            scalarized = scalar_family.outcomes[0]
            values.append(
                scalarized.outcome.relation.rows[0].values[subplan.binding]
            )
            errors.append(
                smt.and_(
                    outer_row.present,
                    smt.or_(
                        scalar_outcome.error,
                        scalarized.cardinality_error,
                    ),
                )
            )
        return tuple(values), tuple(errors)

    def _invocation_observer(
        self,
        invocation_enabled: smt.Term,
    ) -> NodeObserver | None:
        """Hide diagnostic node outcomes for invocations absent in a witness."""

        observer = self.node_observer
        if observer is None:
            return None

        def observe(
            scope: str,
            node: str,
            family: RelationFamily,
        ) -> None:
            observer(
                scope,
                node,
                RelationFamily(
                    tuple(
                        Outcome(
                            smt.and_(invocation_enabled, outcome.enabled),
                            outcome.relation,
                            outcome.error,
                            outcome.decisions,
                            outcome.choices,
                        )
                        for outcome in family.outcomes
                    )
                ),
            )

        return observe

    @staticmethod
    def _deterministic_correlated_outcome(
        subplan: ScalarSubplan,
        outcomes: tuple[Outcome, ...],
        description: str,
    ) -> Outcome:
        if (
            len(outcomes) != 1
            or outcomes[0].enabled != smt.TRUE
            or outcomes[0].decisions
            or outcomes[0].choices
        ):
            raise RelationError(
                f"correlated scalar subplan {subplan.binding!r} "
                f"{description} has per-invocation relational choices"
            )
        return outcomes[0]

    def _evaluate_scalar_subplan(
        self,
        subplan: ScalarSubplan,
    ) -> SubplanFamily:
        return self._scalarize_subplan(subplan, self.node(subplan.root))

    def _scalarize_subplan(
        self,
        subplan: ScalarSubplan,
        family: RelationFamily,
    ) -> SubplanFamily:
        binding = subplan.binding
        column = Column(binding, subplan.output.type, True)
        outcomes: list[SubplanOutcome] = []
        for outcome in family.outcomes:
            selected = self.scalar.null(subplan.output.type)
            for row in outcome.relation.rows:
                candidate = row.values[subplan.output.column]
                if isinstance(
                    candidate.average_metadata,
                    (DecimalAverageState, IntegralAverageState),
                ):
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
        try:
            return select_scalar(((condition, selected),), fallback)
        except ValueTransportError as error:
            raise RelationError(str(error)) from error

    def _join(
        self,
        node: Join,
        left: Relation,
        right: Relation,
        *,
        output_columns: tuple[Column, ...] | None = None,
        compact_left_schema: Mapping[str, Column] | None = None,
        encoding: Literal["auto", "baseline", "compact"] = "auto",
    ) -> Relation:
        """Select an exact encoding; baseline and forced compact share inputs.

        IR validation supplies compatible keys, a Boolean residual, unambiguous
        input names, and the correctly NULL-extended output schema. Family
        choices/errors are lifted by the caller, outside either row encoding.
        """

        if encoding not in {"auto", "baseline", "compact"}:
            raise RelationError(f"unknown join encoding {encoding!r}")
        left = _live_join_input(left)
        right = _live_join_input(right)
        matching_rows = len(left.rows) * len(right.rows)
        _require_relation_row_pairs(matching_rows, "join matching")
        admission = (
            None
            if encoding == "baseline"
            else self._admit_direct_unique_rhs(
                node, right, left_schema=compact_left_schema,
            )
        )
        if encoding == "compact" and admission is None:
            raise RelationError("compact join requires a provenance-checked unique RHS")
        columns = self._columns(node.id) if output_columns is None else output_columns
        if admission is not None:
            _require_relation_rows(len(left.rows), "join output")
            return self._compact_direct_unique_rhs_join(
                admission,
                left,
                self._join_conditions(node, left, right),
                columns,
            )
        layout = join_kernel.shape(node.kind)
        _require_relation_rows(
            layout.candidate_count(len(left.rows), len(right.rows)), "join output",
        )
        emissions = join_kernel.reference_rows(
            layout,
            tuple(row.present for row in left.rows),
            tuple(row.present for row in right.rows),
            self._join_conditions(node, left, right),
        )
        return Relation(columns, tuple(
            self._join_emission(node, left, right, columns, emission)
            for emission in emissions
        ))

    def _join_emission(
        self,
        node: Join,
        left: Relation,
        right: Relation,
        columns: tuple[Column, ...],
        emission: join_kernel.Emission,
    ) -> Row:
        """Attach payload and exact routing provenance to one kernel emission.

        A matched guard implies both input presences: union their facts.
        A one-sided guard implies only the retained input's presence: retain
        only its facts. Occurrence tags deliberately match those two cases.
        """

        inputs = tuple(
            source.rows[index]
            for source, index in ((left, emission.left), (right, emission.right))
            if index is not None
        )
        values = {name: value for row in inputs for name, value in row.values.items()}
        for column in columns:
            if column.name not in values:
                values[column.name] = self.scalar.null(column.type)
        role = (
            "join_match"
            if len(inputs) == 2
            else f"join_{node.kind}_{'left' if emission.left is not None else 'right'}"
        )
        return Row(
            emission.present,
            values,
            _derived_occurrence(role, node.id, *(row.occurrence for row in inputs)),
            frozenset().union(*(row.partition_facts for row in inputs)),
        )

    def _join_conditions(
        self,
        node: Join,
        left: Relation,
        right: Relation,
    ) -> tuple[tuple[smt.Term, ...], ...]:
        """SQL-TRUE keys and residual only; neither input presence is included."""

        conditions: list[tuple[smt.Term, ...]] = []
        for left_row in left.rows:
            pair_conditions: list[smt.Term] = []
            for right_row in right.rows:
                values = dict(left_row.values) | dict(right_row.values)
                key_matches = tuple(
                    self.scalar.is_true(
                        self.scalar.equal(
                            left_row.values[key.left],
                            right_row.values[key.right],
                        )
                    )
                    for key in node.keys
                )
                pair_conditions.append(
                    smt.and_(
                        *key_matches,
                        self.scalar.is_true(self.scalar.evaluate(node.predicate, values)),
                    )
                )
            conditions.append(tuple(pair_conditions))
        return tuple(conditions)

    def _admit_direct_unique_rhs(
        self,
        node: Join,
        right: Relation,
        *,
        left_schema: Mapping[str, Column] | None = None,
    ) -> _DirectUniqueRhs | None:
        right_node = self._direct_unique_rhs_scan(
            node,
            left_schema=left_schema,
        )
        if right_node is None:
            return None
        expected_columns = tuple(self.schemas[right_node.id].values())
        if right.columns != expected_columns:
            return None

        source = self.database.relations[right_node.table]
        expected_outputs = {column.name for column in expected_columns}
        seen_slots: set[int] = set()
        for row in right.rows:
            if row.present == smt.FALSE:
                continue
            if set(row.values) != expected_outputs:
                return None
            occurrence = row.occurrence
            if not (
                occurrence is not None
                and occurrence.operation == "table"
                and occurrence.node == right_node.table
                and occurrence.ordinal is not None
                and not occurrence.inputs
            ):
                return None
            slot = occurrence.ordinal
            if slot in seen_slots or not 0 <= slot < len(source.rows):
                return None
            seen_slots.add(slot)
            source_row = source.rows[slot]
            if not _syntactically_implies(row.present, source_row.present):
                return None
            for mapping in right_node.columns:
                if (
                    row.values.get(mapping.output)
                    != source_row.values[mapping.source]
                ):
                    return None
        return _DirectUniqueRhs(node, right)

    def _direct_unique_rhs_scan(
        self,
        node: Join,
        *,
        left_schema: Mapping[str, Column] | None = None,
    ) -> Scan | None:
        if node.kind not in {"inner", "left"}:
            return None
        right_node = self.nodes[node.right]
        if not isinstance(right_node, Scan):
            return None
        if right_node.predicate is not None or right_node.pushed_limit is not None:
            return None
        if not (
            node.predicate.kind == "literal"
            and node.predicate.result_type == BOOL
            and node.predicate.nullable is False
            and node.predicate.value is True
        ):
            return None

        table = self.snapshot.table_map()[right_node.table]
        table_columns = table.column_map()
        left_schema = (
            self.schemas[node.left]
            if left_schema is None
            else left_schema
        )
        right_schema = self.schemas[node.right]
        # Catalog uniqueness is stated on source values. Requiring the same
        # scalar type keeps comparison coercions from collapsing distinct keys.
        identity_compared_sources = {
            mapping.source
            for mapping in right_node.columns
            for join_key in node.keys
            if (
                mapping.output == join_key.right
                and left_schema[join_key.left].type
                == right_schema[join_key.right].type
            )
        }
        if not any(
            set(key.columns) <= identity_compared_sources
            and all(not table_columns[column].nullable for column in key.columns)
            for key in table.unique_keys
        ):
            return None
        return right_node

    def _compact_direct_unique_rhs_join(
        self,
        admission: _DirectUniqueRhs,
        left: Relation,
        conditions: tuple[tuple[smt.Term, ...], ...],
        columns: tuple[Column, ...],
    ) -> Relation:
        """One slot per LHS, valid only after uniqueness/provenance admission.

        RHS selectors intentionally omit LHS presence. An absent output's
        payload is unobservable; stable selectors let routed copies coalesce.
        """

        node, right = admission.node, admission.right
        rhs_selectors = tuple(
            tuple(smt.and_(row.present, condition) for row, condition in zip(right.rows, pair_conditions))
            for pair_conditions in conditions
        )
        rows: list[Row] = []
        for left_index, left_row in enumerate(left.rows):
            matched = smt.and_(
                left_row.present,
                smt.or_(*rhs_selectors[left_index]),
            )
            selected: dict[str, Value] = {}
            for column in right.columns:
                value = self.scalar.null(column.type)
                for right_index, right_row in enumerate(right.rows):
                    if rhs_selectors[left_index][right_index] == smt.FALSE:
                        continue
                    value = self._select_value(
                        rhs_selectors[left_index][right_index],
                        right_row.values[column.name],
                        value,
                    )
                selected[column.name] = value
            rows.append(
                Row(
                    matched if node.kind == "inner" else left_row.present,
                    dict(left_row.values) | selected,
                    _derived_occurrence(
                        f"join_{node.kind}_unique_rhs",
                        node.id,
                        left_row.occurrence,
                    ),
                    left_row.partition_facts,
                )
            )
        return Relation(columns, tuple(rows))

    def _columns(self, node_id: str) -> tuple[Column, ...]:
        return tuple(self.schemas[node_id].values())


def _whole_partition_decimal_window_expression(expression: Expr) -> Expr | None:
    """Return the one validated relation-dependent leaf below an expression."""

    if expression.kind in {"window_sum", "window_avg"}:
        return expression
    matches = tuple(
        match
        for argument in expression.args
        if (
            match := _whole_partition_decimal_window_expression(argument)
        )
        is not None
    )
    assert len(matches) <= 1
    return matches[0] if matches else None


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


def _retained_key(
    key: frozenset[str] | None,
    output: tuple[str, ...],
) -> frozenset[str] | None:
    """Retain a certificate only when its complete key remains observable."""

    if key is None:
        return None
    return key if key <= frozenset(output) else None


def _projected_key(
    key: frozenset[str] | None,
    project: Project,
) -> frozenset[str] | None:
    """Map a certificate through exact, null-preserving column aliases."""

    if key is None:
        return None
    aliases: dict[str, list[str]] = {}
    for projection in project.columns:
        expression = projection.expression
        if (
            expression.kind == "column"
            and expression.column is not None
            and not projection.error_on_null
        ):
            aliases.setdefault(expression.column, []).append(projection.output)

    result: list[str] = []
    for source in sorted(key):
        outputs = aliases.get(source, ())
        if not outputs:
            return None
        result.append(source if source in outputs else outputs[0])
    projected = frozenset(result)
    return projected if len(projected) == len(key) else None


def _aggregate_unique_key(node: Aggregate) -> frozenset[str] | None:
    """Mint the complete null-safe grouping key in the output schema."""

    if not node.keys:
        return None
    if node.distinct_all:
        return frozenset(trait.output for trait in node.aggregates)
    return frozenset(node.keys)


def _aggregate_partition_key(
    node: Aggregate,
    key: frozenset[str] | None,
) -> frozenset[str] | None:
    """Retain task disjointness only when grouping cannot combine its key."""

    if key is None or not key <= frozenset(node.keys):
        return None
    if not node.distinct_all:
        return key
    aliases = {
        source: trait.output
        for source, trait in zip(node.keys, node.aggregates)
    }
    return frozenset(aliases[source] for source in key)


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
        result.append(
            SortOrder(
                column,
                item.ascending,
                item.nulls_first,
                item.comparison,
            )
        )
    return tuple(result)


def _require_order_columns(
    columns: tuple[Column, ...],
    order: tuple[SortOrder, ...],
    operation: str,
) -> None:
    """Check the narrow provenance contract at the semantic-use boundary."""

    by_name = {column.name: column for column in columns}
    for item in order:
        column = by_name.get(item.column)
        if column is None:
            raise RelationError(
                f"{operation} column {item.column!r} is absent"
            )
        if column.type == DOUBLE:
            if (
                item.comparison != INTEGRAL_AVG_RANK_COMPARISON
                or not column.integral_avg_rank
            ):
                raise RelationError(
                    f"{operation} Double ordering requires comparison "
                    f"{INTEGRAL_AVG_RANK_COMPARISON!r} on a completed "
                    "integral AVG output"
                )
            continue
        if item.comparison is not None:
            raise RelationError(
                f"{operation} comparison tags may only be used with Double"
            )
        if not is_ordered_type(column.type):
            raise RelationError(
                f"{operation} comparison type {column.type!r} is unsupported"
            )


def single(relation: Relation) -> RelationFamily:
    return RelationFamily((Outcome(smt.TRUE, relation, smt.FALSE),))


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


def _strip_integral_average_certificates(
    family: RelationFamily,
) -> RelationFamily:
    if not any(
        isinstance(value.average_metadata, IntegralAverageCertificate)
        for outcome in family.outcomes
        for row in outcome.relation.rows
        for value in row.values.values()
    ):
        return family
    return map_family(
        family,
        lambda relation: replace(
            relation,
            rows=tuple(
                replace(
                    row,
                    values={
                        name: (
                            replace(value, average_metadata=None)
                            if isinstance(
                                value.average_metadata,
                                IntegralAverageCertificate,
                            )
                            else value
                        )
                        for name, value in row.values.items()
                    },
                )
                for row in relation.rows
            ),
            null_safe_unique_key=relation.null_safe_unique_key,
            task_partition_key=relation.task_partition_key,
        ),
    )


@dataclass(frozen=True, slots=True)
class _FamilyProduct:
    """One compatible partial product, before the operator combines its rows."""

    enabled: smt.Term = smt.TRUE
    relations: tuple[Relation, ...] = ()
    errors: tuple[smt.Term, ...] = ()
    decisions: tuple[tuple[str, int], ...] = ()
    choices: tuple[BoundedChoice, ...] = ()


def combine_families(
    families: tuple[RelationFamily, ...],
    combine: Callable[[tuple[Relation, ...]], Relation],
    combine_errors: Callable[
        [tuple[Relation, ...], tuple[smt.Term, ...]],
        smt.Term,
    ] | None = None,
) -> RelationFamily:
    """Take a compatible product, preserving choices and observable errors."""

    partials = [_FamilyProduct()]
    for relation_family in families:
        expanded: list[_FamilyProduct] = []
        for partial in partials:
            for outcome in relation_family.outcomes:
                merged = _merge_decisions(partial.decisions, outcome.decisions)
                if merged is None:
                    continue
                expanded.append(
                    _FamilyProduct(
                        smt.and_(partial.enabled, outcome.enabled),
                        partial.relations + (outcome.relation,),
                        partial.errors + (outcome.error,),
                        merged,
                        _merge_choices(partial.choices, outcome.choices),
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
                partial.enabled,
                combine(partial.relations),
                (
                    smt.or_(*partial.errors)
                    if combine_errors is None
                    else combine_errors(partial.relations, partial.errors)
                ),
                partial.decisions,
                partial.choices,
            )
            for partial in partials
        )
    )


def _choose_sort_encoding(
    source: RelationFamily,
    order: tuple[SortOrder, ...],
    *,
    compact_prefix: bool,
    requested: sort_strategy.Encoding,
) -> sort_strategy.Plan:
    """Measure the input without allocating symbols; delegate policy decisions."""

    if not order:
        raise RelationError("sort order must not be empty")
    _require_order_columns(source.columns, order, "sort")
    unique_order = all(
        _order_covers_unique_key(outcome.relation, order)
        for outcome in source.outcomes
    )
    live_counts = tuple(_live_row_count(outcome.relation) for outcome in source.outcomes)
    trivial = all(count <= 1 for count in live_counts)
    costs = sort_strategy.Costs(
        row_pairs=sum(_unordered_row_pairs(count) for count in live_counts),
        comparators=sum(_sorting_network_cost(count) for count in live_counts),
        payload_cells=sum(
            _sorting_network_payload_cells(outcome.relation)
            for outcome in source.outcomes
        ),
        key_columns=len(order),
        unique_order=unique_order,
        enumerated=len(source.outcomes) == 1 and _use_enumerated_sequences(source),
        trivial=trivial,
        # The modeled StageGraph has two producer tasks. This is a cost hint,
        # not a Sort semantic assumption: a compact prefix reduces Merge work.
        merge_pairs=(
            max((_unordered_row_pairs(2 * count) for count in live_counts), default=0)
            if compact_prefix else 0
        ),
    )
    limits = sort_strategy.Limits(
        MAX_RELATION_ROW_PAIRS, MAX_SORT_NETWORK_COMPARATORS,
        MAX_SORT_NETWORK_PAYLOAD_CELLS, MAX_SORT_NETWORK_KEY_COLUMNS,
    )
    try:
        return sort_strategy.choose(costs, limits, requested)
    except ValueError as error:
        raise RelationError(str(error)) from error


def sort_family(
    source: RelationFamily,
    order: tuple[SortOrder, ...],
    script: smt.Script,
    decision: str,
    *,
    compact_prefix: bool = False,
    encoding: sort_strategy.Encoding = "auto",
) -> RelationFamily:
    """All tie-respecting sequences, with an independently selected exact encoding.

    Enumerated permutations are the tiny-domain reference. Ordinal choices and
    compare-exchange networks represent the same sequence language; a complete
    unique-key certificate permits deterministic predecessor ranks instead.
    """

    plan = _choose_sort_encoding(source, order, compact_prefix=compact_prefix, requested=encoding)
    if plan.encoding == "trivial":
        return map_family(source, lambda relation: Relation(
            relation.columns, relation.rows, sequence=True, order=order,
            null_safe_unique_key=relation.null_safe_unique_key,
            task_partition_key=relation.task_partition_key,
        ))
    if plan.encoding == "unique":
        return _unique_order_family(source, order)
    if plan.encoding == "enumerated":
        return _enumerated_sort_family(source, order, decision)
    if plan.encoding == "network":
        return _sorting_network_family(source, order, script, decision, deterministic_ties=plan.unique_order)
    return _ordinal_sort_family(source, order, script, decision)


def _ordinal_sort_family(
    source: RelationFamily,
    order: tuple[SortOrder, ...],
    script: smt.Script,
    decision: str,
) -> RelationFamily:
    """Constrain each live row to one tie-respecting ordinal permutation."""

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
                    null_safe_unique_key=relation.null_safe_unique_key,
                    task_partition_key=relation.task_partition_key,
                ),
                source_outcome.error,
                source_outcome.decisions,
                _merge_choices(source_outcome.choices, choices),
            )
        )
    if not outcomes:
        raise RelationError("sort produced no outcomes")
    return RelationFamily(tuple(outcomes))


def _order_covers_unique_key(
    relation: Relation,
    order: tuple[SortOrder, ...],
) -> bool:
    key = relation.null_safe_unique_key
    return key is not None and key <= frozenset(item.column for item in order)


def _unique_order_family(
    source: RelationFamily,
    order: tuple[SortOrder, ...],
) -> RelationFamily:
    """Use exact predecessor counts for a certificate-backed total order."""

    outcomes: list[Outcome] = []
    for source_outcome in source.outcomes:
        relation = source_outcome.relation
        if not _order_covers_unique_key(relation, order):
            raise RelationError("unique order does not cover its certified key")
        live_indices = _live_row_indices(relation.rows)
        ordinals = [smt.ZERO] * len(relation.rows)
        for index in live_indices:
            row = relation.rows[index]
            predecessor_count = smt.add(
                *(
                    smt.ite(
                        smt.and_(
                            relation.rows[other].present,
                            _row_less(relation.rows[other], row, order),
                        ),
                        smt.ONE,
                        smt.ZERO,
                    )
                    for other in live_indices
                    if other != index
                )
            )
            ordinals[index] = smt.ite(
                row.present,
                predecessor_count,
                smt.ZERO,
            )
        outcomes.append(
            Outcome(
                source_outcome.enabled,
                Relation(
                    relation.columns,
                    relation.rows,
                    sequence=True,
                    order=order,
                    ordinals=tuple(ordinals),
                    null_safe_unique_key=relation.null_safe_unique_key,
                    task_partition_key=relation.task_partition_key,
                ),
                source_outcome.error,
                source_outcome.decisions,
                source_outcome.choices,
            )
        )
    if not outcomes:
        raise RelationError("unique sort produced no outcomes")
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
                        null_safe_unique_key=relation.null_safe_unique_key,
                        task_partition_key=relation.task_partition_key,
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


@dataclass(frozen=True, slots=True)
class _DecimalAverageStateLayout:
    sum_type: str
    sum_lane: int
    count_lane: int
    finite_abs_bound: int
    count_bound: int


@dataclass(frozen=True, slots=True)
class _IntegralAverageStateLayout:
    count_lane: int
    minimum_lane: int
    maximum_lane: int
    count_bound: int


_SortingAverageStateLayout: TypeAlias = (
    _DecimalAverageStateLayout | _IntegralAverageStateLayout
)


@dataclass(frozen=True, slots=True)
class _SortingNetworkColumnLayout:
    column: Column
    null_lane: int
    value_lane: int
    decimal_finite_abs_bound: int | None
    average_metadata: _SortingAverageStateLayout | None = None


@dataclass(frozen=True, slots=True)
class _SortingNetworkLayout:
    lane_sorts: tuple[str, ...]
    columns: tuple[_SortingNetworkColumnLayout, ...]
    occurrence: Occurrence | None
    partition_facts: frozenset[PartitionFact]


@dataclass(frozen=True, slots=True)
class _SortingNetworkRowCodec:
    """Pack one complete semantic row into one exact SMT datatype value."""

    layout: _SortingNetworkLayout
    product: smt.ProductSort

    @classmethod
    def create(
        cls,
        relation: Relation,
        script: smt.Script,
        hint: str,
    ) -> "_SortingNetworkRowCodec":
        layout = _sorting_network_layout(relation)
        return cls(
            layout,
            script.fresh_product_sort(hint, layout.lane_sorts),
        )

    def pack(self, row: Row) -> smt.Term:
        lanes: list[smt.Term] = [row.present]
        for column in self.layout.columns:
            value = row.values[column.column.name]
            lanes.extend((value.is_null, value.value))
            state_layout = column.average_metadata
            if isinstance(state_layout, _DecimalAverageStateLayout):
                state = value.average_metadata
                if not isinstance(state, DecimalAverageState):
                    raise RelationError(
                        "sorting network row lost its Decimal avg state"
                    )
                lanes.extend((state.sum, state.count))
            elif isinstance(state_layout, _IntegralAverageStateLayout):
                state = value.average_metadata
                if not isinstance(state, IntegralAverageState):
                    raise RelationError(
                        "sorting network row lost its integral avg state"
                    )
                lanes.extend((state.count, state.minimum, state.maximum))
        return self.product.pack(*lanes)

    def present(self, payload: smt.Term) -> smt.Term:
        return self.product.select(payload, 0)

    def value(
        self,
        payload: smt.Term,
        column: _SortingNetworkColumnLayout,
    ) -> Value:
        average_state = None
        state_layout = column.average_metadata
        if isinstance(state_layout, _DecimalAverageStateLayout):
            average_state = DecimalAverageState(
                sum_type=state_layout.sum_type,
                sum=self.product.select(payload, state_layout.sum_lane),
                count=self.product.select(payload, state_layout.count_lane),
                finite_abs_bound=state_layout.finite_abs_bound,
                count_bound=state_layout.count_bound,
            )
        elif isinstance(state_layout, _IntegralAverageStateLayout):
            average_state = IntegralAverageState(
                count=self.product.select(payload, state_layout.count_lane),
                minimum=self.product.select(payload, state_layout.minimum_lane),
                maximum=self.product.select(payload, state_layout.maximum_lane),
                count_bound=state_layout.count_bound,
            )
        return Value(
            column.column.type,
            self.product.select(payload, column.null_lane),
            self.product.select(payload, column.value_lane),
            column.decimal_finite_abs_bound,
            average_state,
        )

    def key_row(
        self,
        payload: smt.Term,
        order: tuple[SortOrder, ...],
    ) -> Row:
        by_name = {
            column.column.name: column
            for column in self.layout.columns
        }
        return Row(
            self.present(payload),
            {
                item.column: self.value(payload, by_name[item.column])
                for item in order
            },
        )

    def unpack(self, payload: smt.Term) -> Row:
        return Row(
            self.present(payload),
            {
                column.column.name: self.value(payload, column)
                for column in self.layout.columns
            },
            self.layout.occurrence,
            self.layout.partition_facts,
        )


@dataclass(frozen=True, slots=True)
class _SortingNetworkItem:
    payload: smt.Term
    tie_rank: smt.Term


def _sorting_network_cost(row_count: int) -> int:
    return (
        0
        if row_count <= 1
        else sort_network.comparator_count(row_count)
    )


def _sorting_network_payload_cells(relation: Relation) -> int:
    row_count = _live_row_count(relation)
    if row_count <= 1:
        return 0
    try:
        lane_count = len(_sorting_network_layout(relation).lane_sorts)
    except RelationError:
        # The ordinal encoding may still represent this relation exactly.
        return MAX_SORT_NETWORK_PAYLOAD_CELLS + 1
    return row_count * lane_count


def _sorting_network_layout(relation: Relation) -> _SortingNetworkLayout:
    rows = tuple(
        relation.rows[index]
        for index in _live_row_indices(relation.rows)
    )
    if not rows:
        raise RelationError("sorting network row layout requires a live row")

    lane_sorts: list[str] = [smt.BOOL]
    columns: list[_SortingNetworkColumnLayout] = []
    for column in relation.columns:
        try:
            values = tuple(row.values[column.name] for row in rows)
        except KeyError as error:
            raise RelationError(
                f"sorting network row is missing column {column.name!r}"
            ) from error
        if any(value.type != column.type for value in values):
            raise RelationError(
                f"sorting network mixed scalar types for column {column.name!r}"
            )
        value_sort = smt_sort(column.type)
        if any(
            value.is_null.sort != smt.BOOL
            or value.value.sort != value_sort
            for value in values
        ):
            raise RelationError(
                f"sorting network mixed SMT lane sorts for column {column.name!r}"
            )

        null_lane = len(lane_sorts)
        value_lane = null_lane + 1
        lane_sorts.extend((smt.BOOL, value_sort))
        finite_bounds = tuple(
            value.decimal_finite_abs_bound
            for value in values
        )
        finite_abs_bound = (
            None
            if any(bound is None for bound in finite_bounds)
            else max(
                bound
                for bound in finite_bounds
                if bound is not None
            )
        )

        states = tuple(value.average_metadata for value in values)
        present_states = tuple(state for state in states if state is not None)
        if present_states and len(present_states) != len(states):
            raise RelationError(
                "sorting network mixed AVG state and scalar values"
            )
        average_state_layout = None
        if present_states:
            first_state = present_states[0]
            if any(
                type(state) is not type(first_state)
                for state in present_states[1:]
            ):
                raise RelationError("sorting network mixed AVG state layouts")
            if isinstance(first_state, DecimalAverageState):
                decimal_states = tuple(
                    state
                    for state in present_states
                    if isinstance(state, DecimalAverageState)
                )
                if any(
                    state.sum_type != first_state.sum_type
                    or state.sum.sort != first_state.sum.sort
                    or state.count.sort != first_state.count.sort
                    for state in decimal_states[1:]
                ):
                    raise RelationError(
                        "sorting network mixed Decimal avg state layouts"
                    )
                if (
                    not is_decimal_type(first_state.sum_type)
                    or first_state.sum.sort != smt_sort(first_state.sum_type)
                    or first_state.count.sort != smt.INT
                    or any(
                        type(state.finite_abs_bound) is not int
                        or state.finite_abs_bound < 0
                        or type(state.count_bound) is not int
                        or state.count_bound < 0
                        for state in decimal_states
                    )
                ):
                    raise RelationError(
                        "sorting network Decimal avg state has an invalid layout"
                    )
                sum_lane = len(lane_sorts)
                count_lane = sum_lane + 1
                lane_sorts.extend((first_state.sum.sort, first_state.count.sort))
                average_state_layout = _DecimalAverageStateLayout(
                    first_state.sum_type,
                    sum_lane,
                    count_lane,
                    max(state.finite_abs_bound for state in decimal_states),
                    max(state.count_bound for state in decimal_states),
                )
            elif isinstance(first_state, IntegralAverageState):
                integral_states = tuple(
                    state
                    for state in present_states
                    if isinstance(state, IntegralAverageState)
                )
                if (
                    any(
                        state.count.sort != smt.INT
                        or state.minimum.sort != smt.INT
                        or state.maximum.sort != smt.INT
                        or type(state.count_bound) is not int
                        or state.count_bound < 0
                        for state in integral_states
                    )
                ):
                    raise RelationError(
                        "sorting network integral avg state has an invalid layout"
                    )
                count_lane = len(lane_sorts)
                minimum_lane = count_lane + 1
                maximum_lane = count_lane + 2
                lane_sorts.extend((smt.INT, smt.INT, smt.INT))
                average_state_layout = _IntegralAverageStateLayout(
                    count_lane,
                    minimum_lane,
                    maximum_lane,
                    max(state.count_bound for state in integral_states),
                )
            else:
                raise RelationError(
                    "sorting network received unsupported AVG metadata"
                )

        columns.append(
            _SortingNetworkColumnLayout(
                column,
                null_lane,
                value_lane,
                finite_abs_bound,
                average_state_layout,
            )
        )

    occurrence = (
        rows[0].occurrence
        if all(row.occurrence == rows[0].occurrence for row in rows[1:])
        else None
    )
    return _SortingNetworkLayout(
        tuple(lane_sorts),
        tuple(columns),
        occurrence,
        _common_partition_facts(rows),
    )


def _sorting_network_family(
    source: RelationFamily,
    order: tuple[SortOrder, ...],
    script: smt.Script,
    decision: str,
    producer_groups: tuple[tuple[int, ...], ...] | None = None,
    *,
    deterministic_ties: bool = False,
) -> RelationFamily:
    """Sort exactly with a compact, fixed-topology compare-exchange network.

    One finite permutation rank travels with each candidate row. SQL keys
    dominate that rank; the rank only chooses among exact ties. A certified
    complete unique key permits fixed concrete ranks because present rows
    cannot tie. Otherwise all and only tie-respecting sequences are represented.
    Present rows dominate absent rows, so the fixed output slots form a present
    prefix that ordered Limit can slice without constructing every row pair.

    Merge additionally orders the ranks along each producer's semantic
    sequence. Fixed producer orders use a chain; symbolic producer orders use
    one exact constraint per unordered pair. Both leave precisely the legal
    cross-producer interleavings.
    """

    _require_order_columns(source.columns, order, "sort")
    if deterministic_ties and any(
        not _order_covers_unique_key(outcome.relation, order)
        for outcome in source.outcomes
    ):
        raise RelationError(
            "deterministic sorting-network ties require a unique key"
        )
    outcomes: list[Outcome] = []
    for outcome_index, source_outcome in enumerate(source.outcomes):
        relation = source_outcome.relation
        columns = {column.name for column in relation.columns}
        missing = [item.column for item in order if item.column not in columns]
        if missing:
            raise RelationError(f"sort columns are absent: {', '.join(missing)}")

        live_indices = _live_row_indices(relation.rows)
        rows = tuple(relation.rows[index] for index in live_indices)
        if not rows:
            outcomes.append(
                Outcome(
                    source_outcome.enabled,
                    Relation(
                        relation.columns,
                        (),
                        sequence=True,
                        order=order,
                        present_prefix=True,
                        null_safe_unique_key=relation.null_safe_unique_key,
                        task_partition_key=relation.task_partition_key,
                    ),
                    source_outcome.error,
                    source_outcome.decisions,
                    source_outcome.choices,
                )
            )
            continue
        if len(rows) == 1:
            outcomes.append(
                Outcome(
                    source_outcome.enabled,
                    Relation(
                        relation.columns,
                        rows,
                        sequence=True,
                        order=order,
                        present_prefix=True,
                        null_safe_unique_key=relation.null_safe_unique_key,
                        task_partition_key=relation.task_partition_key,
                    ),
                    source_outcome.error,
                    source_outcome.decisions,
                    source_outcome.choices,
                )
            )
            continue

        compact_index = {
            source_index: index
            for index, source_index in enumerate(live_indices)
        }
        tie_ranks: list[smt.Term] = []
        tie_choices: list[BoundedChoice] = []
        if deterministic_ties:
            tie_ranks.extend(
                smt.int_value(row_index) for row_index in range(len(rows))
            )
        else:
            for row_index in range(len(rows)):
                rank = script.fresh_constant(
                    f"{decision}:network:{outcome_index}:tie:{row_index}",
                    smt.INT,
                )
                tie_ranks.append(rank)
                tie_choices.append(BoundedChoice(rank, len(rows)))
            script.register_quantified_choices(
                (rank, len(rows))
                for rank in tie_ranks
            )

        constraints = [] if deterministic_ties else [smt.distinct(*tie_ranks)]
        if producer_groups is not None and not deterministic_ties:
            input_ordinals = relation.ordinals
            for group in producer_groups:
                members = tuple(
                    index
                    for index in group
                    if index in compact_index
                )
                concrete = (
                    input_ordinals is None
                    or all(
                        input_ordinals[index].operation == "int"
                        for index in members
                    )
                )
                if concrete:
                    ordered = (
                        members
                        if input_ordinals is None
                        else tuple(sorted(
                            members,
                            key=lambda index: input_ordinals[index].atom,
                        ))
                    )
                    if input_ordinals is not None:
                        ordinal_values = tuple(
                            input_ordinals[index].atom
                            for index in ordered
                        )
                        if len(set(ordinal_values)) != len(ordinal_values):
                            raise RelationError(
                                "merge producer input ordinals must be distinct"
                            )
                    constraints.extend(
                        smt.lt(
                            tie_ranks[compact_index[left]],
                            tie_ranks[compact_index[right]],
                        )
                        for left, right in zip(ordered, ordered[1:])
                    )
                    continue

                assert input_ordinals is not None
                for position, left in enumerate(members):
                    for right in members[position + 1 :]:
                        input_left = input_ordinals[left]
                        input_right = input_ordinals[right]
                        # Match the producer's symbolic order whenever both
                        # rows exist. Equal input ordinals carry no order,
                        # exactly as in the ordinal Merge representation.
                        constraints.append(
                            smt.or_(
                                smt.not_(smt.and_(
                                    relation.rows[left].present,
                                    relation.rows[right].present,
                                )),
                                smt.eq(input_left, input_right),
                                smt.eq(
                                    smt.lt(input_left, input_right),
                                    smt.lt(
                                        tie_ranks[compact_index[left]],
                                        tie_ranks[compact_index[right]],
                                    ),
                                ),
                            )
                        )

        codec = _SortingNetworkRowCodec.create(
            relation,
            script,
            f"{decision}:network:{outcome_index}:row",
        )
        before = script.fresh_defined_function(
            f"{decision}:network:{outcome_index}:before",
            (
                codec.product,
                smt.INT,
                codec.product,
                smt.INT,
            ),
            smt.BOOL,
            lambda parameters: _sorting_network_before(
                codec.key_row(parameters[0], order),
                parameters[1],
                codec.key_row(parameters[2], order),
                parameters[3],
                order,
            ),
        )
        items = [
            _SortingNetworkItem(codec.pack(row), rank)
            for row, rank in zip(rows, tie_ranks)
        ]
        padded_size = sort_network.padded_size(len(items))
        padding = _SortingNetworkItem(
            codec.pack(Row(smt.FALSE, rows[0].values)),
            smt.ZERO,
        )
        items.extend(padding for _ in range(padded_size - len(items)))
        for left_index, right_index, ascending in sort_network.comparators(
            len(rows)
        ):
            left = items[left_index]
            right = items[right_index]
            swap = (
                before(
                    right.payload,
                    right.tie_rank,
                    left.payload,
                    left.tie_rank,
                )
                if ascending
                else before(
                    left.payload,
                    left.tie_rank,
                    right.payload,
                    right.tie_rank,
                )
            )
            items[left_index] = _select_sorting_network_item(
                swap,
                right,
                left,
            )
            items[right_index] = _select_sorting_network_item(
                swap,
                left,
                right,
            )

        outcomes.append(
            Outcome(
                smt.and_(source_outcome.enabled, *constraints),
                Relation(
                    relation.columns,
                    tuple(
                        codec.unpack(item.payload)
                        for item in items[: len(rows)]
                    ),
                    sequence=True,
                    order=order,
                    present_prefix=True,
                    null_safe_unique_key=relation.null_safe_unique_key,
                    task_partition_key=relation.task_partition_key,
                ),
                source_outcome.error,
                source_outcome.decisions,
                _merge_choices(
                    source_outcome.choices,
                    tuple(tie_choices),
                ),
            )
        )
    if not outcomes:
        raise RelationError("sorting network produced no outcomes")
    return RelationFamily(tuple(outcomes))


def _sorting_network_before(
    left: Row,
    left_tie_rank: smt.Term,
    right: Row,
    right_tie_rank: smt.Term,
    order: tuple[SortOrder, ...],
) -> smt.Term:
    """Whether one candidate precedes another in the network's total order."""

    both_present = smt.and_(left.present, right.present)
    return smt.or_(
        smt.and_(left.present, smt.not_(right.present)),
        smt.and_(
            both_present,
            smt.or_(
                _row_less(left, right, order),
                smt.and_(
                    _sort_keys_equal(left, right, order),
                    smt.lt(left_tie_rank, right_tie_rank),
                ),
            ),
        ),
    )


def _sort_keys_equal(
    left: Row,
    right: Row,
    order: tuple[SortOrder, ...],
) -> smt.Term:
    return smt.and_(
        *(
            ScalarEncoder.not_distinct(
                left.values[item.column],
                right.values[item.column],
            )
            for item in order
        )
    )


def _select_sorting_network_item(
    condition: smt.Term,
    when_true: _SortingNetworkItem,
    when_false: _SortingNetworkItem,
) -> _SortingNetworkItem:
    if condition == smt.TRUE:
        return when_true
    if condition == smt.FALSE:
        return when_false
    return _SortingNetworkItem(
        smt.ite(
            condition,
            when_true.payload,
            when_false.payload,
        ),
        smt.ite(condition, when_true.tie_rank, when_false.tie_rank),
    )


def _merge_network_producer_pairs(
    source: RelationFamily,
    groups: tuple[tuple[int, ...], ...],
) -> int:
    """Count symbolic producer-order pairs needed by a Merge network."""

    count = 0
    for outcome in source.outcomes:
        relation = outcome.relation
        if relation.ordinals is None:
            continue
        for group in groups:
            live = tuple(
                index
                for index in group
                if relation.rows[index].present != smt.FALSE
            )
            if any(
                relation.ordinals[index].operation != "int"
                for index in live
            ):
                count += _unordered_row_pairs(len(live))
    return count


def merge_family(
    source: RelationFamily,
    order: tuple[SortOrder, ...],
    groups: tuple[tuple[int, ...], ...],
    script: smt.Script,
    decision: str,
) -> RelationFamily:
    """Represent every sorted, producer-order-preserving interleaving."""

    _require_order_columns(source.columns, order, "merge")
    indices = tuple(index for group in groups for index in group)
    row_count = len(source.outcomes[0].relation.rows) if source.outcomes else 0
    if sorted(indices) != list(range(row_count)):
        raise RelationError("merge producer groups do not partition the input rows")
    if any(
        len(outcome.relation.rows) != row_count
        for outcome in source.outcomes
    ):
        raise RelationError("merge outcomes have different row shapes")
    unique_order = all(
        _order_covers_unique_key(outcome.relation, order)
        for outcome in source.outcomes
    )
    pair_count = max(
        (
            _unordered_row_pairs(_live_row_count(outcome.relation))
            for outcome in source.outcomes
        ),
        default=0,
    )
    unique_pair_count = sum(
        _unordered_row_pairs(_live_row_count(outcome.relation))
        for outcome in source.outcomes
    )
    network_count = sum(
        _sorting_network_cost(_live_row_count(outcome.relation))
        for outcome in source.outcomes
    )
    payload_cells = sum(
        _sorting_network_payload_cells(outcome.relation)
        for outcome in source.outcomes
    )
    producer_pair_count = _merge_network_producer_pairs(source, groups)
    network_fits = (
        network_count <= MAX_SORT_NETWORK_COMPARATORS
        and payload_cells <= MAX_SORT_NETWORK_PAYLOAD_CELLS
        and len(order) <= MAX_SORT_NETWORK_KEY_COLUMNS
        and (
            unique_order
            or producer_pair_count <= MAX_RELATION_ROW_PAIRS
        )
    )

    def use_network() -> RelationFamily:
        return _sorting_network_family(
            source,
            order,
            script,
            decision,
            groups,
            deterministic_ties=unique_order,
        )

    if unique_order:
        if unique_pair_count <= MAX_RELATION_ROW_PAIRS:
            return _unique_order_family(source, order)
        if network_fits:
            return use_network()
        _require_relation_row_pairs(
            unique_pair_count,
            "merge unique-order construction",
        )

    if pair_count > MAX_RELATION_ROW_PAIRS:
        if network_fits:
            return use_network()
        _require_relation_row_pairs(pair_count, "merge construction")

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

    ordinal_pair_count = sum(
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
    )
    if (
        ordinal_pair_count > MAX_RELATION_ROW_PAIRS
        and network_fits
    ):
        return use_network()
    _require_relation_row_pairs(
        ordinal_pair_count,
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
                    null_safe_unique_key=relation.null_safe_unique_key,
                    task_partition_key=relation.task_partition_key,
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
                        null_safe_unique_key=relation.null_safe_unique_key,
                        task_partition_key=relation.task_partition_key,
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


def _window_rows_ordinal_constraints(
    scalar: ScalarEncoder,
    rows: tuple[Row, ...],
    ordinals: tuple[smt.Term, ...],
    partition: str,
    order: tuple[SortOrder, ...],
) -> smt.Term:
    """Constrain one unstable sort independently inside each q51 partition."""

    if len(rows) != len(ordinals):
        raise RelationError("q51 ROWS window ordinals do not align with rows")
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
            same_partition = scalar.not_distinct(
                left.values[partition],
                right.values[partition],
            )
            comparable = smt.and_(
                left.present,
                right.present,
                same_partition,
            )
            constraints.extend((
                smt.or_(
                    smt.not_(comparable),
                    smt.not_(smt.eq(ordinals[left_index], ordinals[right_index])),
                ),
                smt.or_(
                    smt.not_(smt.and_(comparable, _row_less(left, right, order))),
                    smt.lt(ordinals[left_index], ordinals[right_index]),
                ),
                smt.or_(
                    smt.not_(smt.and_(comparable, _row_less(right, left, order))),
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
    if left.type == DOUBLE:
        if order.comparison != INTEGRAL_AVG_RANK_COMPARISON:
            raise RelationError(
                "Double sort comparison requires the integral AVG rank tag"
            )
    elif order.comparison is not None:
        raise RelationError("sort comparison tags may only be used with Double")
    elif not is_ordered_type(left.type):
        raise RelationError(
            "sort comparison requires integer, String/Utf8, Date, Decimal, "
            "or certified completed integral AVG Double values"
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
    within_limit = all(
        _live_row_count(outcome.relation) <= count
        for outcome in source.outcomes
    )
    compact_ordered_singleton = (
        source.sequence
        and count == 1
        and offset == 0
        and any(
            _can_compact_ordered_singleton(
                outcome.relation,
                count,
                offset,
            )
            for outcome in source.outcomes
        )
    )
    if compact_ordered_singleton:
        result = _ordered_limit_family(
            source,
            count_expression,
            offset_expression,
        )
    elif offset == 0 and within_limit:
        result = source
    elif count == 0 or (
        offset > 0
        and at_most_one
    ):
        result = map_family(
            source,
            lambda relation: Relation(
                relation.columns,
                (),
                sequence=relation.sequence,
                null_safe_unique_key=relation.null_safe_unique_key,
                task_partition_key=relation.task_partition_key,
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
        if relation.present_prefix:
            return Relation(
                relation.columns,
                relation.rows[offset : offset + count],
                sequence=True,
                order=relation.order,
                present_prefix=True,
                null_safe_unique_key=relation.null_safe_unique_key,
                task_partition_key=relation.task_partition_key,
            )

        if _can_compact_ordered_singleton(relation, count, offset):
            return _compact_ordered_singleton(relation)

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
            null_safe_unique_key=relation.null_safe_unique_key,
            task_partition_key=relation.task_partition_key,
        )

    return map_family(source, take)


def _can_compact_ordered_singleton(
    relation: Relation,
    count: int,
    offset: int,
) -> bool:
    """Recognize the small exact ordered Take(1) representation."""

    if count != 1 or offset != 0 or not relation.sequence:
        return False
    if relation.ordinals is not None:
        return False
    live_rows = tuple(
        relation.rows[index]
        for index in _live_row_indices(relation.rows)
    )
    return (
        len(relation.rows) > 1
        and 0 < len(live_rows) <= MAX_ENUMERATED_SEQUENCE_ROWS
        and all(
            value.average_metadata is None
            for row in live_rows
            for value in row.values.values()
        )
    )


def _compact_ordered_singleton(relation: Relation) -> Relation:
    """Select the first present fixed-sequence row into one conditional slot."""

    live_rows = tuple(
        relation.rows[index]
        for index in _live_row_indices(relation.rows)
    )
    values = {
        column.name: _select_ordered_singleton_value(
            tuple(
                (row.present, row.values[column.name])
                for row in live_rows
            ),
            _ordered_singleton_fallback(column),
        )
        for column in relation.columns
    }
    return Relation(
        relation.columns,
        (
            Row(
                smt.or_(*(row.present for row in live_rows)),
                values,
                None,
                _common_partition_facts(live_rows),
            ),
        ),
        sequence=True,
        order=relation.order,
        present_prefix=True,
        null_safe_unique_key=relation.null_safe_unique_key,
        task_partition_key=relation.task_partition_key,
    )


def _select_ordered_singleton_value(
    candidates: tuple[tuple[smt.Term, Value], ...],
    fallback: Value,
) -> Value:
    """Left-biased ITE-select over a canonical absent-slot payload."""

    if not candidates:
        raise RelationError("ordered singleton selection has no candidates")
    alternatives = tuple(value for _, value in candidates)
    first = alternatives[0]
    if any(value.type != first.type for value in alternatives[1:]):
        raise RelationError(
            "ordered singleton value alternatives have different types"
        )
    if fallback.type != first.type:
        raise RelationError(
            "ordered singleton fallback has a different type"
        )
    if any(value.average_metadata is not None for value in alternatives):
        raise RelationError(
            "ordered singleton cannot select hidden AVG metadata"
        )

    try:
        return select_scalar(candidates, fallback)
    except ValueTransportError as error:
        raise RelationError(str(error)) from error


def _ordered_singleton_fallback(column: Column) -> Value:
    """Build a task-stable typed payload for an absent compact slot."""

    return Value(
        column.type,
        smt.TRUE if column.nullable else smt.FALSE,
        smt.FALSE if smt_sort(column.type) == smt.BOOL else smt.ZERO,
        0 if is_decimal_type(column.type) else None,
    )


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
                        null_safe_unique_key=(
                            source_outcome.relation.null_safe_unique_key
                        ),
                        task_partition_key=(
                            source_outcome.relation.task_partition_key
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
                        Relation(
                            source_outcome.relation.columns,
                            output_rows,
                            null_safe_unique_key=(
                                source_outcome.relation.null_safe_unique_key
                            ),
                            task_partition_key=(
                                source_outcome.relation.task_partition_key
                            ),
                        ),
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
                        null_safe_unique_key=relation.null_safe_unique_key,
                        task_partition_key=relation.task_partition_key,
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
                        null_safe_unique_key=relation.null_safe_unique_key,
                        task_partition_key=relation.task_partition_key,
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
                    null_safe_unique_key=relation.null_safe_unique_key,
                    task_partition_key=relation.task_partition_key,
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
    """Conditionally select one typed scalar value."""

    if not alternatives:
        raise RelationError("singleton limit has no value alternatives")
    first = alternatives[0]
    if any(value.type != first.type for value in alternatives[1:]):
        raise RelationError("singleton limit value alternatives have different types")

    if any(value.average_metadata is not None for value in alternatives):
        raise RelationError("singleton limit cannot select hidden AVG metadata")
    # Match the existing right-biased selector order exactly; slot zero is the
    # fallback, while larger matching indices take precedence.
    candidates = tuple(
        (smt.eq(choice, smt.int_value(index)), value)
        for index, value in enumerate(alternatives[1:], start=1)
    )
    return select_scalar(tuple(reversed(candidates)), first)


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

    if left.present_prefix and right.present_prefix:
        aligned = min(len(left.rows), len(right.rows))
        return smt.and_(
            *(
                smt.and_(
                    smt.eq(
                        left.rows[index].present,
                        right.rows[index].present,
                    ),
                    smt.or_(
                        smt.not_(left.rows[index].present),
                        values_equal(
                            left.rows[index],
                            right.rows[index],
                        ),
                    ),
                )
                for index in range(aligned)
            ),
            *(
                smt.not_(row.present)
                for row in left.rows[aligned:]
            ),
            *(
                smt.not_(row.present)
                for row in right.rows[aligned:]
            ),
        )

    if left.present_prefix != right.present_prefix:
        prefix_on_left = left.present_prefix
        prefix = left if prefix_on_left else right
        sparse = right if prefix_on_left else left
        sparse_indices = _live_row_indices(sparse.rows)
        aligned = min(len(prefix.rows), len(sparse_indices))
        sparse_ranks = (
            tuple(
                (index, _compressed_rank(sparse, index))
                for index in sparse_indices
            )
            if aligned
            else ()
        )
        prefix_count = smt.add(
            *(smt.ite(row.present, smt.ONE, smt.ZERO) for row in prefix.rows)
        )
        sparse_count = smt.add(
            *(smt.ite(row.present, smt.ONE, smt.ZERO) for row in sparse.rows)
        )

        def mixed_values_equal(prefix_row: Row, sparse_row: Row) -> smt.Term:
            return (
                values_equal(prefix_row, sparse_row)
                if prefix_on_left
                else values_equal(sparse_row, prefix_row)
            )

        # A present-prefix slot's compressed rank is its slot index.  Sparse
        # slots that are syntactically absent need no rank, and no prefix slot
        # beyond the number of live sparse candidates can be present.
        return smt.and_(
            smt.eq(prefix_count, sparse_count),
            *(smt.not_(row.present) for row in prefix.rows[aligned:]),
            *(
                smt.or_(
                    smt.not_(
                        smt.and_(
                            sparse.rows[sparse_index].present,
                            prefix_row.present,
                            smt.eq(
                                sparse_rank,
                                smt.int_value(prefix_index),
                            ),
                        )
                    ),
                    mixed_values_equal(
                        prefix_row,
                        sparse.rows[sparse_index],
                    ),
                )
                for sparse_index, sparse_rank in sparse_ranks
                for prefix_index, prefix_row in enumerate(prefix.rows[:aligned])
            ),
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
                state = value.average_metadata
                if state is not None:
                    observable_terms.extend(average_metadata_terms(state))
                sum_state = value.decimal_sum_state
                if sum_state is not None:
                    observable_terms.extend(decimal_sum_state_terms(sum_state))
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


def successful_family_reachable(
    family: RelationFamily,
    script: smt.Script,
    scope: str,
    predicate: Callable[[Relation], smt.Term],
) -> smt.Term:
    """Existentially test one successful outcome under its exact choice bounds."""

    _register_family_choices(family, script)
    bounded = _bounded_choice_family(family, script, scope)
    return smt.or_(
        *(
            smt.exists(
                tuple(choice.term for choice in outcome.choices),
                smt.and_(
                    outcome.enabled,
                    smt.not_(outcome.error),
                    predicate(outcome.relation),
                ),
            )
            for outcome in bounded.outcomes
        )
    )


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


def _preferred_keyed_mismatch_branches(
    left: RelationFamily,
    right: RelationFamily,
    scalar: ScalarEncoder,
    ordered: bool,
) -> tuple[MismatchBranch, ...] | None:
    """Return an exact small portfolio for two certified total sequences.

    A shared positional NULL-safe key contained in an identical positional
    order makes each side's sequence tie-free.  Equality then reduces to
    bidirectional key inclusion and equality of the non-key payload at each
    matching key.  Every other shape deliberately retains the general family
    comparison.
    """

    if not ordered or len(left.outcomes) != 1 or len(right.outcomes) != 1:
        return None
    left_outcome = left.outcomes[0]
    right_outcome = right.outcomes[0]
    if (
        left_outcome.choices
        or right_outcome.choices
        or left_outcome.decisions
        or right_outcome.decisions
    ):
        return None
    left_relation = left_outcome.relation
    right_relation = right_outcome.relation
    if not left_relation.sequence or not right_relation.sequence:
        return None
    if len(left_relation.columns) != len(right_relation.columns):
        return None
    if any(
        (
            left_column.type,
            left_column.nullable,
            left_column.integral_avg_rank,
        )
        != (
            right_column.type,
            right_column.nullable,
            right_column.integral_avg_rank,
        )
        for left_column, right_column in zip(
            left_relation.columns,
            right_relation.columns,
        )
    ):
        return None

    def column_positions(relation: Relation) -> dict[str, int] | None:
        positions = {
            column.name: index
            for index, column in enumerate(relation.columns)
        }
        return positions if len(positions) == len(relation.columns) else None

    left_positions = column_positions(left_relation)
    right_positions = column_positions(right_relation)
    if left_positions is None or right_positions is None:
        return None
    left_key = left_relation.null_safe_unique_key
    right_key = right_relation.null_safe_unique_key
    if left_key is None or right_key is None:
        return None
    if any(name not in left_positions for name in left_key) or any(
        name not in right_positions for name in right_key
    ):
        return None
    left_key_positions = frozenset(left_positions[name] for name in left_key)
    right_key_positions = frozenset(right_positions[name] for name in right_key)
    if not left_key_positions or left_key_positions != right_key_positions:
        return None

    def order_signature(
        relation: Relation,
        positions: Mapping[str, int],
    ) -> tuple[tuple[int, bool, bool, str | None], ...] | None:
        if relation.order is None:
            return None
        if any(item.column not in positions for item in relation.order):
            return None
        return tuple(
            (
                positions[item.column],
                item.ascending,
                item.nulls_first,
                item.comparison,
            )
            for item in relation.order
        )

    left_order = order_signature(left_relation, left_positions)
    right_order = order_signature(right_relation, right_positions)
    if left_order is None or left_order != right_order:
        return None
    ordered_positions = frozenset(item[0] for item in left_order)
    if not left_key_positions <= ordered_positions:
        return None

    left_live = _live_row_indices(left_relation.rows)
    right_live = _live_row_indices(right_relation.rows)
    payload_positions = tuple(
        index
        for index in range(len(left_relation.columns))
        if index not in left_key_positions
    )
    smaller_count = min(len(left_live), len(right_live))
    branch_count = (
        4
        + len(left_live)
        + len(right_live)
        + smaller_count * len(payload_positions)
    )
    comparison_count = len(left_live) * len(right_live) * (
        2 * len(left_key_positions)
        + len(payload_positions) * (len(left_key_positions) + 1)
    )
    if (
        branch_count > MAX_PREFERRED_KEYED_MISMATCH_BRANCHES
        or comparison_count > MAX_PREFERRED_KEYED_MISMATCH_COMPARISONS
    ):
        return None

    key_positions = tuple(sorted(left_key_positions))

    def key_equal(left_index: int, right_index: int) -> smt.Term:
        left_row = left_relation.rows[left_index]
        right_row = right_relation.rows[right_index]
        return smt.and_(
            *(
                scalar.not_distinct(
                    left_row.values[left_relation.columns[position].name],
                    right_row.values[right_relation.columns[position].name],
                )
                for position in key_positions
            )
        )

    key_equalities = {
        (left_index, right_index): key_equal(left_index, right_index)
        for left_index in left_live
        for right_index in right_live
    }
    both_enabled = smt.and_(left_outcome.enabled, right_outcome.enabled)
    both_successful = smt.and_(
        both_enabled,
        smt.not_(left_outcome.error),
        smt.not_(right_outcome.error),
    )
    branches: list[MismatchBranch] = []

    def append(name: str, predicate: smt.Term) -> None:
        if predicate != smt.FALSE:
            branches.append(MismatchBranch(name, predicate))

    append(
        "preferred_left_language_empty",
        smt.not_(left_outcome.enabled),
    )
    append(
        "preferred_right_language_empty",
        smt.not_(right_outcome.enabled),
    )
    append(
        "preferred_left_error_only",
        smt.and_(
            both_enabled,
            left_outcome.error,
            smt.not_(right_outcome.error),
        ),
    )
    append(
        "preferred_right_error_only",
        smt.and_(
            both_enabled,
            smt.not_(left_outcome.error),
            right_outcome.error,
        ),
    )
    for left_index in left_live:
        left_row = left_relation.rows[left_index]
        append(
            f"preferred_left_row_{left_index}_key_missing",
            smt.and_(
                both_successful,
                left_row.present,
                smt.not_(smt.or_(
                    *(
                        smt.and_(
                            right_relation.rows[right_index].present,
                            key_equalities[left_index, right_index],
                        )
                        for right_index in right_live
                    )
                )),
            ),
        )
    for right_index in right_live:
        right_row = right_relation.rows[right_index]
        append(
            f"preferred_right_row_{right_index}_key_missing",
            smt.and_(
                both_successful,
                right_row.present,
                smt.not_(smt.or_(
                    *(
                        smt.and_(
                            left_relation.rows[left_index].present,
                            key_equalities[left_index, right_index],
                        )
                        for left_index in left_live
                    )
                )),
            ),
        )

    payload_on_left = len(left_live) <= len(right_live)
    source_relation = left_relation if payload_on_left else right_relation
    target_relation = right_relation if payload_on_left else left_relation
    source_live = left_live if payload_on_left else right_live
    target_live = right_live if payload_on_left else left_live
    side = "left" if payload_on_left else "right"
    for source_index in source_live:
        source_row = source_relation.rows[source_index]
        for position in payload_positions:
            source_name = source_relation.columns[position].name
            target_name = target_relation.columns[position].name

            def matching_payload_differs(target_index: int) -> smt.Term:
                pair = (
                    (source_index, target_index)
                    if payload_on_left
                    else (target_index, source_index)
                )
                return smt.and_(
                    target_relation.rows[target_index].present,
                    key_equalities[pair],
                    smt.not_(scalar.not_distinct(
                        source_row.values[source_name],
                        target_relation.rows[target_index].values[target_name],
                    )),
                )

            append(
                f"preferred_{side}_row_{source_index}_column_{position}_payload_mismatch",
                smt.and_(
                    both_successful,
                    source_row.present,
                    smt.or_(
                        *(matching_payload_differs(index) for index in target_live)
                    ),
                ),
            )
    return tuple(branches) or None


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
    return FamilyMismatch(
        counterexample,
        branches,
        _preferred_keyed_mismatch_branches(
            left,
            right,
            scalar,
            ordered,
        ),
    )


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
