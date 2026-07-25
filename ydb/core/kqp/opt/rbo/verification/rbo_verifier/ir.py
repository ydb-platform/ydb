"""Strict version-one semantic snapshot IR.

This module intentionally contains no optimizer or solver logic.  A snapshot is
accepted only when every field is understood and every expression is well typed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Mapping, Sequence, TypeAlias

from . import decimal
from .types import (
    BOOL,
    DATE,
    DOUBLE,
    MAX_DATE,
    VOID,
    equality_comparison_compatible,
    family,
    integer_bounds,
    is_ordered_type,
    is_scalar_type,
    ordering_comparison_compatible,
    static_in_comparison_compatible,
)


FORMAT = "ydb-rbo-semantic-snapshot"
VERSION = 1
MAX_STATIC_IN_ITEMS = 512
MAX_BOUND_DEPTH = 64
MAX_EXPR_NODES = 1024
MAX_EXPR_DEPTH = 128
OPAQUE_DOUBLE_FINGERPRINT_PREFIX = "format:21:yql-passive-double-v1;"
JOIN_KINDS = frozenset(
    {
        "cross",
        "inner",
        "left",
        "right",
        "full",
        "left_semi",
        "right_semi",
        "left_anti",
        "right_anti",
        "exclusion",
    }
)
STAGE_CONNECTION_KINDS = frozenset({"map", "broadcast", "hash_shuffle", "union_all", "merge"})
HASH_FUNCTIONS = frozenset({"HashV1", "HashV2"})
OPERATOR_PHASES = frozenset({"undefined", "intermediate", "final"})


class SnapshotError(ValueError):
    """The snapshot is malformed or uses unsupported version-one semantics."""


@dataclass(frozen=True, slots=True)
class ValueType:
    name: str
    nullable: bool


@dataclass(frozen=True, slots=True)
class Column:
    name: str
    type: str
    nullable: bool

    @property
    def value_type(self) -> ValueType:
        return ValueType(self.type, self.nullable)


@dataclass(frozen=True, slots=True)
class Table:
    name: str
    columns: tuple[Column, ...]
    unique_keys: tuple[UniqueKey, ...]

    def column_map(self) -> dict[str, Column]:
        return {column.name: column for column in self.columns}


@dataclass(frozen=True, slots=True)
class UniqueKey:
    columns: tuple[str, ...]
    nulls_distinct: bool


@dataclass(frozen=True, slots=True)
class Expr:
    kind: str
    args: tuple[Expr, ...] = ()
    column: str | None = None
    depth: int | None = None
    value: bool | int | str | decimal.Literal | None = None
    result_type: str | None = None
    nullable: bool | None = None
    fingerprint: str | None = None
    null_safe: bool = False
    source_type: str | None = None


@dataclass(frozen=True, slots=True)
class ScanColumn:
    source: str
    output: str


@dataclass(frozen=True, slots=True)
class Scan:
    id: str
    table: str
    columns: tuple[ScanColumn, ...]
    predicate: Expr | None
    pushed_limit: Expr | None


@dataclass(frozen=True, slots=True)
class EmptySource:
    id: str


@dataclass(frozen=True, slots=True)
class Projection:
    output: str
    expression: Expr


@dataclass(frozen=True, slots=True)
class Project:
    id: str
    input: str
    columns: tuple[Projection, ...]
    ordered: bool


@dataclass(frozen=True, slots=True)
class Filter:
    id: str
    input: str
    predicate: Expr


@dataclass(frozen=True, slots=True)
class OuterBind:
    """One typed outer value injected into a correlated subplan invocation."""

    id: str
    input: str
    dependency: str
    type: str
    nullable: bool


@dataclass(frozen=True, slots=True)
class Limit:
    id: str
    input: str
    count: Expr
    offset: Expr | None
    phase: str
    ensure_at_most_one: bool = False


@dataclass(frozen=True, slots=True)
class SortOrder:
    column: str
    ascending: bool
    nulls_first: bool


@dataclass(frozen=True, slots=True)
class Sort:
    id: str
    input: str
    order: tuple[SortOrder, ...]
    limit: Expr | None
    phase: str


@dataclass(frozen=True, slots=True)
class AverageStateType:
    """Physical Decimal AVG accumulator hidden by the logical RBO IU type."""

    sum_type: str
    count_type: str
    nullable: bool


@dataclass(frozen=True, slots=True)
class AggregateTrait:
    input: str
    function: str
    output: str
    output_type: str
    output_nullable: bool
    distinct: bool
    unwrap: bool
    state: AverageStateType | None = None


@dataclass(frozen=True, slots=True)
class Aggregate:
    id: str
    input: str
    keys: tuple[str, ...]
    aggregates: tuple[AggregateTrait, ...]
    phase: str
    distinct_all: bool


@dataclass(frozen=True, slots=True)
class JoinKey:
    left: str
    right: str


@dataclass(frozen=True, slots=True)
class Join:
    id: str
    left: str
    right: str
    kind: str
    keys: tuple[JoinKey, ...]
    predicate: Expr


@dataclass(frozen=True, slots=True)
class UnionInput:
    node: str
    columns: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class UnionAll:
    id: str
    inputs: tuple[UnionInput, ...]
    output: tuple[str, ...]
    ordered: bool


PlanNode: TypeAlias = (
    EmptySource
    | Scan
    | Project
    | Filter
    | OuterBind
    | Limit
    | Sort
    | Aggregate
    | Join
    | UnionAll
)


@dataclass(frozen=True, slots=True)
class SubplanOutput:
    column: str
    type: str
    nullable: bool


@dataclass(frozen=True, slots=True)
class ScalarSubplan:
    """One exact scalar binding captured outside ordinary relational edges."""

    binding: str
    root: str
    output: SubplanOutput
    consumers: tuple[str, ...]
    dependency: str | None = None


@dataclass(frozen=True, slots=True)
class ExistsSubplan:
    """One uncorrelated or deliberately narrow correlated EXISTS binding."""

    binding: str
    root: str
    predicate: Expr | None
    dependencies: tuple[str, ...]
    consumers: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class InSubplan:
    """One exact uncorrelated integral/String IN binding used by a Filter."""

    binding: str
    root: str
    lookup: SubplanOutput
    output: SubplanOutput
    consumers: tuple[str, ...]


Subplan: TypeAlias = ScalarSubplan | ExistsSubplan | InSubplan


@dataclass(frozen=True, slots=True)
class Plan:
    nodes: tuple[PlanNode, ...]
    root: str
    output: tuple[str, ...]
    subplans: tuple[Subplan, ...]

    def node_map(self) -> dict[str, PlanNode]:
        return {node.id: node for node in self.nodes}


@dataclass(frozen=True, slots=True)
class StageOutput:
    index: int
    node: str


@dataclass(frozen=True, slots=True)
class Stage:
    id: str
    nodes: tuple[str, ...]
    inputs: tuple[str, ...]
    outputs: tuple[StageOutput, ...]
    source_storage: str | None


@dataclass(frozen=True, slots=True)
class StageEdge:
    id: str
    producer: str
    consumer: str
    occurrence: int
    producer_output: int
    consumer_input: int
    kind: str
    keys: tuple[str, ...] = ()
    hash_function: str | None = None
    use_spilling: bool | None = None
    parallel: bool | None = None
    order: tuple[SortOrder, ...] = ()


@dataclass(frozen=True, slots=True)
class StageGraph:
    root_stage: str
    stages: tuple[Stage, ...]
    edges: tuple[StageEdge, ...]

    def stage_map(self) -> dict[str, Stage]:
        return {stage.id: stage for stage in self.stages}


@dataclass(frozen=True, slots=True)
class Snapshot:
    tables: tuple[Table, ...]
    plan: Plan
    stage_graph: StageGraph | None = None

    def table_map(self) -> dict[str, Table]:
        return {table.name: table for table in self.tables}

    def output_schema(self) -> tuple[Column, ...]:
        schemas = validate_snapshot(self)
        root_schema = schemas[self.plan.root]
        return tuple(root_schema[name] for name in self.plan.output)


def _fail(path: str, message: str) -> None:
    raise SnapshotError(f"{path}: {message}")


@dataclass(slots=True)
class _ExprBudget:
    """One expanded-node and structural-depth budget for a complete expression."""

    nodes: int = 0

    def charge(self, path: str, depth: int) -> None:
        if depth > MAX_EXPR_DEPTH:
            _fail(
                path,
                f"expression structural depth exceeds the audit limit of {MAX_EXPR_DEPTH}",
            )
        self.nodes += 1
        if self.nodes > MAX_EXPR_NODES:
            _fail(
                path,
                f"expanded expression node count exceeds the audit limit of {MAX_EXPR_NODES}",
            )


def _object(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        _fail(path, "expected an object")
    return value


def _array(value: Any, path: str) -> Sequence[Any]:
    if not isinstance(value, list):
        _fail(path, "expected an array")
    return value


def _string(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value:
        _fail(path, "expected a non-empty string")
    return value


def _bool(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        _fail(path, "expected a Boolean")
    return value


def _index(value: Any, path: str) -> int:
    if type(value) is not int or value < 0:
        _fail(path, "expected a non-negative integer")
    return value


def _keys(
    value: Mapping[str, Any],
    required: set[str],
    path: str,
    optional: set[str] | None = None,
) -> None:
    optional = optional or set()
    missing = required - value.keys()
    unknown = value.keys() - required - optional
    if missing:
        _fail(path, f"missing fields: {', '.join(sorted(missing))}")
    if unknown:
        _fail(path, f"unknown fields: {', '.join(sorted(unknown))}")


def _scalar_type(value: Any, path: str) -> str:
    result = _string(value, path)
    if not is_scalar_type(result):
        _fail(path, f"unsupported scalar type {result!r}")
    return result


def _literal(value: Any, scalar_type: str, path: str) -> bool | int | str | decimal.Literal:
    if decimal.is_type(scalar_type):
        return _decimal_literal(value, scalar_type, path)
    scalar_family = family(scalar_type)
    if scalar_family == "int":
        if not isinstance(value, int) or isinstance(value, bool):
            _fail(path, f"value does not have type {scalar_type!r}")
        bounds = integer_bounds(scalar_type)
        assert bounds is not None
        if bounds[0] <= value < bounds[1]:
            return value
        _fail(
            path,
            f"{scalar_type} literal is outside "
            f"[{bounds[0]}, {bounds[1] - 1}]",
        )
    valid = (
        (scalar_family == "bool" and isinstance(value, bool))
        or (
            scalar_family == "date"
            and type(value) is int
            and 0 <= value < MAX_DATE
        )
        or (scalar_family == "string" and isinstance(value, str))
    )
    if not valid:
        _fail(path, f"value does not have type {scalar_type!r}")
    if scalar_family == "string":
        assert isinstance(value, str)
        try:
            value.encode("utf-8", errors="strict")
        except UnicodeEncodeError:
            _fail(path, f"{scalar_type} literal is not valid Unicode")
    return value


def _decimal_literal(value: Any, scalar_type: str, path: str) -> decimal.Literal:
    obj = _object(value, path)
    kind = _string(obj.get("kind"), f"{path}.kind")
    if kind == decimal.FINITE:
        _keys(obj, {"kind", "scaled"}, path)
        scaled = _string(obj["scaled"], f"{path}.scaled")
        try:
            result = decimal.Literal(kind, decimal.parse_scaled_integer(scaled))
            decimal.literal_code(result, scalar_type)
        except ValueError as error:
            _fail(path, str(error))
        return result
    if kind in {decimal.POS_INF, decimal.NEG_INF, decimal.NAN_KIND}:
        _keys(obj, {"kind"}, path)
        return decimal.Literal(kind)
    _fail(f"{path}.kind", f"unsupported Decimal literal kind {kind!r}")


def _parse_expr(
    value: Any,
    path: str,
    bound_depth: int = 0,
    *,
    budget: _ExprBudget | None = None,
    structural_depth: int = 1,
) -> Expr:
    active_budget = _ExprBudget() if budget is None else budget
    active_budget.charge(path, structural_depth)

    def parse_child(
        child: Any,
        child_path: str,
        child_bound_depth: int = bound_depth,
    ) -> Expr:
        return _parse_expr(
            child,
            child_path,
            child_bound_depth,
            budget=active_budget,
            structural_depth=structural_depth + 1,
        )

    obj = _object(value, path)
    kind = _string(obj.get("kind"), f"{path}.kind")

    if kind == "column":
        _keys(obj, {"kind", "column"}, path)
        return Expr(kind=kind, column=_string(obj["column"], f"{path}.column"))

    if kind == "bound":
        _keys(obj, {"kind", "depth"}, path)
        depth = _index(obj["depth"], f"{path}.depth")
        if depth >= bound_depth:
            _fail(f"{path}.depth", "does not refer to an enclosing IfPresent handler")
        return Expr(kind=kind, depth=depth)

    if kind == "void":
        _keys(obj, {"kind"}, path)
        return Expr(kind=kind, result_type=VOID, nullable=False)

    if kind == "literal":
        _keys(obj, {"kind", "type", "value"}, path)
        result_type = _scalar_type(obj["type"], f"{path}.type")
        return Expr(
            kind=kind,
            result_type=result_type,
            nullable=False,
            value=_literal(obj["value"], result_type, f"{path}.value"),
        )

    if kind == "null":
        _keys(obj, {"kind", "type"}, path)
        return Expr(
            kind=kind,
            result_type=_scalar_type(obj["type"], f"{path}.type"),
            nullable=True,
        )

    if kind in {"and", "or"}:
        _keys(obj, {"kind", "args"}, path)
        raw_args = _array(obj["args"], f"{path}.args")
        if not raw_args:
            _fail(f"{path}.args", "must not be empty")
        return Expr(
            kind=kind,
            args=tuple(
                parse_child(arg, f"{path}.args[{index}]")
                for index, arg in enumerate(raw_args)
            ),
        )

    if kind == "not":
        _keys(obj, {"kind", "arg"}, path)
        return Expr(
            kind=kind,
            args=(parse_child(obj["arg"], f"{path}.arg"),),
        )

    if kind == "exists":
        _keys(obj, {"kind", "arg"}, path)
        return Expr(
            kind=kind,
            args=(parse_child(obj["arg"], f"{path}.arg"),),
        )

    if kind == "in":
        _keys(obj, {"kind", "lookup", "items"}, path)
        raw_items = _array(obj["items"], f"{path}.items")
        if not 1 <= len(raw_items) <= MAX_STATIC_IN_ITEMS:
            _fail(
                f"{path}.items",
                f"must contain between 1 and {MAX_STATIC_IN_ITEMS} expressions",
            )
        return Expr(
            kind=kind,
            args=(
                parse_child(obj["lookup"], f"{path}.lookup"),
                *(
                    parse_child(item, f"{path}.items[{index}]")
                    for index, item in enumerate(raw_items)
                ),
            ),
        )

    if kind in {"eq", "lt", "lte", "gt", "gte"}:
        _keys(obj, {"kind", "left", "right"}, path, {"null_safe"})
        if kind != "eq" and "null_safe" in obj:
            _fail(f"{path}.null_safe", "is valid only for equality")
        return Expr(
            kind=kind,
            args=(
                parse_child(obj["left"], f"{path}.left"),
                parse_child(obj["right"], f"{path}.right"),
            ),
            null_safe=_bool(obj.get("null_safe", False), f"{path}.null_safe"),
        )

    if kind in {"add", "sub", "mul", "div"}:
        _keys(obj, {"kind", "left", "right", "type", "nullable"}, path)
        return Expr(
            kind=kind,
            args=(
                parse_child(obj["left"], f"{path}.left"),
                parse_child(obj["right"], f"{path}.right"),
            ),
            result_type=_scalar_type(obj["type"], f"{path}.type"),
            nullable=_bool(obj["nullable"], f"{path}.nullable"),
        )

    if kind == "cast_decimal":
        _keys(obj, {"kind", "arg", "source_type", "type", "nullable"}, path)
        return Expr(
            kind=kind,
            args=(parse_child(obj["arg"], f"{path}.arg"),),
            source_type=_scalar_type(
                obj["source_type"],
                f"{path}.source_type",
            ),
            result_type=_scalar_type(obj["type"], f"{path}.type"),
            nullable=_bool(obj["nullable"], f"{path}.nullable"),
        )

    if kind == "cast_integral":
        _keys(obj, {"kind", "arg", "type", "nullable"}, path)
        return Expr(
            kind=kind,
            args=(parse_child(obj["arg"], f"{path}.arg"),),
            result_type=_scalar_type(obj["type"], f"{path}.type"),
            nullable=_bool(obj["nullable"], f"{path}.nullable"),
        )

    if kind == "if":
        _keys(
            obj,
            {"kind", "condition", "then", "else", "type", "nullable"},
            path,
        )
        return Expr(
            kind=kind,
            args=(
                parse_child(obj["condition"], f"{path}.condition"),
                parse_child(obj["then"], f"{path}.then"),
                parse_child(obj["else"], f"{path}.else"),
            ),
            result_type=_scalar_type(obj["type"], f"{path}.type"),
            nullable=_bool(obj["nullable"], f"{path}.nullable"),
        )

    if kind == "if_present":
        _keys(
            obj,
            {"kind", "optional", "present", "missing", "type", "nullable"},
            path,
        )
        if bound_depth >= MAX_BOUND_DEPTH:
            _fail(path, "IfPresent binding depth exceeds the audit limit")
        return Expr(
            kind=kind,
            args=(
                parse_child(obj["optional"], f"{path}.optional"),
                parse_child(obj["present"], f"{path}.present", bound_depth + 1),
                parse_child(obj["missing"], f"{path}.missing"),
            ),
            result_type=_scalar_type(obj["type"], f"{path}.type"),
            nullable=_bool(obj["nullable"], f"{path}.nullable"),
        )

    if kind in {"opaque", "opaque_double"}:
        _keys(obj, {"kind", "fingerprint", "type", "nullable", "args"}, path)
        raw_args = _array(obj["args"], f"{path}.args")
        result_type = _scalar_type(obj["type"], f"{path}.type")
        nullable = _bool(obj["nullable"], f"{path}.nullable")
        fingerprint = _string(obj["fingerprint"], f"{path}.fingerprint")
        if kind == "opaque_double":
            if result_type != DOUBLE or nullable is not True:
                _fail(path, "opaque_double result must be Optional<Double>")
            if len(raw_args) != 3:
                _fail(f"{path}.args", "opaque_double requires exactly three arguments")
            if not (
                fingerprint.startswith(OPAQUE_DOUBLE_FINGERPRINT_PREFIX)
                and len(fingerprint) > len(OPAQUE_DOUBLE_FINGERPRINT_PREFIX)
            ):
                _fail(
                    f"{path}.fingerprint",
                    "opaque_double requires the audited passive-Double fingerprint prefix "
                    "and a non-empty identity suffix",
                )
        return Expr(
            kind=kind,
            args=tuple(
                parse_child(arg, f"{path}.args[{index}]")
                for index, arg in enumerate(raw_args)
            ),
            result_type=result_type,
            nullable=nullable,
            fingerprint=fingerprint,
        )

    _fail(f"{path}.kind", f"unsupported expression kind {kind!r}")


def _parse_uint64_literal(value: Any, path: str) -> Expr:
    """Decode the deliberately narrow v1 limit-count expression subset."""

    expression = _parse_expr(value, path)
    if expression.kind != "literal" or expression.result_type != "Uint64":
        _fail(path, "expected a non-null Uint64 literal")
    assert type(expression.value) is int
    if not 0 <= expression.value < 1 << 64:
        _fail(path, "Uint64 literal is outside [0, 2^64 - 1]")
    return expression


def _parse_sort_order(value: Any, path: str) -> tuple[SortOrder, ...]:
    order: list[SortOrder] = []
    for index, raw_item in enumerate(_array(value, path)):
        item_path = f"{path}[{index}]"
        item = _object(raw_item, item_path)
        _keys(item, {"column", "ascending", "nulls_first"}, item_path)
        order.append(
            SortOrder(
                _string(item["column"], f"{item_path}.column"),
                _bool(item["ascending"], f"{item_path}.ascending"),
                _bool(item["nulls_first"], f"{item_path}.nulls_first"),
            )
        )
    if not order:
        _fail(path, "must not be empty")
    return tuple(order)


def _parse_table(value: Any, path: str) -> Table:
    obj = _object(value, path)
    _keys(obj, {"name", "columns", "unique_keys"}, path)
    raw_columns = _array(obj["columns"], f"{path}.columns")
    if not raw_columns:
        _fail(f"{path}.columns", "must not be empty")
    columns: list[Column] = []
    for index, raw_column in enumerate(raw_columns):
        column_path = f"{path}.columns[{index}]"
        column = _object(raw_column, column_path)
        _keys(column, {"name", "type", "nullable"}, column_path)
        scalar_type = _scalar_type(column["type"], f"{column_path}.type")
        if scalar_type == DOUBLE:
            _fail(
                f"{column_path}.type",
                "Double is modeled only as a derived passive carrier, not a base-table value",
            )
        columns.append(
            Column(
                name=_string(column["name"], f"{column_path}.name"),
                type=scalar_type,
                nullable=_bool(column["nullable"], f"{column_path}.nullable"),
            )
        )
    unique_keys: list[UniqueKey] = []
    for index, raw_key in enumerate(_array(obj["unique_keys"], f"{path}.unique_keys")):
        key_path = f"{path}.unique_keys[{index}]"
        key = _object(raw_key, key_path)
        _keys(key, {"columns", "nulls_distinct"}, key_path)
        key_columns = tuple(
            _string(column, f"{key_path}.columns[{column_index}]")
            for column_index, column in enumerate(_array(key["columns"], f"{key_path}.columns"))
        )
        if not key_columns:
            _fail(f"{key_path}.columns", "must not be empty")
        _unique(key_columns, f"{key_path}.columns")
        unique_keys.append(UniqueKey(key_columns, _bool(key["nulls_distinct"], f"{key_path}.nulls_distinct")))
    return Table(
        name=_string(obj["name"], f"{path}.name"),
        columns=tuple(columns),
        unique_keys=tuple(unique_keys),
    )


def _parse_node(value: Any, path: str) -> PlanNode:
    obj = _object(value, path)
    node_id = _string(obj.get("id"), f"{path}.id")
    operation = _string(obj.get("op"), f"{path}.op")

    if operation == "empty_source":
        _keys(obj, {"id", "op"}, path)
        return EmptySource(node_id)

    if operation == "scan":
        _keys(
            obj,
            {"id", "op", "table", "columns"},
            path,
            {"predicate", "pushed_limit"},
        )
        columns: list[ScanColumn] = []
        for index, raw_column in enumerate(_array(obj["columns"], f"{path}.columns")):
            column_path = f"{path}.columns[{index}]"
            column = _object(raw_column, column_path)
            _keys(column, {"source", "output"}, column_path)
            columns.append(
                ScanColumn(
                    source=_string(column["source"], f"{column_path}.source"),
                    output=_string(column["output"], f"{column_path}.output"),
                )
            )
        if not columns:
            _fail(f"{path}.columns", "must not be empty")
        return Scan(
            id=node_id,
            table=_string(obj["table"], f"{path}.table"),
            columns=tuple(columns),
            predicate=(
                None
                if obj.get("predicate") is None
                else _parse_expr(obj["predicate"], f"{path}.predicate")
            ),
            pushed_limit=(
                None
                if obj.get("pushed_limit") is None
                else _parse_uint64_literal(obj["pushed_limit"], f"{path}.pushed_limit")
            ),
        )

    if operation == "project":
        _keys(obj, {"id", "op", "input", "columns", "ordered"}, path)
        columns: list[Projection] = []
        for index, raw_column in enumerate(_array(obj["columns"], f"{path}.columns")):
            column_path = f"{path}.columns[{index}]"
            column = _object(raw_column, column_path)
            _keys(column, {"output", "expression"}, column_path)
            columns.append(
                Projection(
                    output=_string(column["output"], f"{column_path}.output"),
                    expression=_parse_expr(column["expression"], f"{column_path}.expression"),
                )
            )
        if not columns:
            _fail(f"{path}.columns", "must not be empty")
        return Project(
            id=node_id,
            input=_string(obj["input"], f"{path}.input"),
            columns=tuple(columns),
            ordered=_bool(obj["ordered"], f"{path}.ordered"),
        )

    if operation == "filter":
        _keys(obj, {"id", "op", "input", "predicate"}, path)
        return Filter(
            node_id,
            _string(obj["input"], f"{path}.input"),
            _parse_expr(obj["predicate"], f"{path}.predicate"),
        )

    if operation == "outer_bind":
        _keys(
            obj,
            {"id", "op", "input", "dependency", "type", "nullable"},
            path,
        )
        scalar_type = _scalar_type(obj["type"], f"{path}.type")
        if scalar_type == DOUBLE:
            _fail(f"{path}.type", "outer_bind may not transport Double")
        return OuterBind(
            id=node_id,
            input=_string(obj["input"], f"{path}.input"),
            dependency=_string(obj["dependency"], f"{path}.dependency"),
            type=scalar_type,
            nullable=_bool(obj["nullable"], f"{path}.nullable"),
        )

    if operation == "limit":
        _keys(
            obj,
            {"id", "op", "input", "count", "offset", "phase"},
            path,
            {"ensure_at_most_one"},
        )
        phase = _string(obj["phase"], f"{path}.phase")
        if phase not in OPERATOR_PHASES:
            _fail(f"{path}.phase", f"unsupported limit phase {phase!r}")
        return Limit(
            id=node_id,
            input=_string(obj["input"], f"{path}.input"),
            count=_parse_uint64_literal(obj["count"], f"{path}.count"),
            offset=(
                None
                if obj["offset"] is None
                else _parse_uint64_literal(obj["offset"], f"{path}.offset")
            ),
            phase=phase,
            ensure_at_most_one=_bool(
                obj.get("ensure_at_most_one", False),
                f"{path}.ensure_at_most_one",
            ),
        )

    if operation == "sort":
        _keys(obj, {"id", "op", "input", "order", "limit", "phase"}, path)
        phase = _string(obj["phase"], f"{path}.phase")
        if phase not in OPERATOR_PHASES:
            _fail(f"{path}.phase", f"unsupported sort phase {phase!r}")
        return Sort(
            id=node_id,
            input=_string(obj["input"], f"{path}.input"),
            order=_parse_sort_order(obj["order"], f"{path}.order"),
            limit=(
                None
                if obj["limit"] is None
                else _parse_uint64_literal(obj["limit"], f"{path}.limit")
            ),
            phase=phase,
        )

    if operation == "aggregate":
        _keys(
            obj,
            {"id", "op", "input", "keys", "aggregates", "phase", "distinct_all"},
            path,
        )
        keys = tuple(
            _string(key, f"{path}.keys[{index}]")
            for index, key in enumerate(_array(obj["keys"], f"{path}.keys"))
        )
        aggregates: list[AggregateTrait] = []
        for index, raw_trait in enumerate(_array(obj["aggregates"], f"{path}.aggregates")):
            trait_path = f"{path}.aggregates[{index}]"
            trait = _object(raw_trait, trait_path)
            fields = {
                "input", "function", "output", "type", "nullable", "distinct", "unwrap"
            }
            if trait.get("function") == "avg":
                fields.add("state")
            _keys(
                trait,
                fields,
                trait_path,
            )
            function = _string(trait["function"], f"{trait_path}.function")
            state = None
            if function == "avg":
                raw_state = _object(trait["state"], f"{trait_path}.state")
                _keys(
                    raw_state,
                    {"sum_type", "count_type", "nullable"},
                    f"{trait_path}.state",
                )
                state = AverageStateType(
                    sum_type=_scalar_type(
                        raw_state["sum_type"],
                        f"{trait_path}.state.sum_type",
                    ),
                    count_type=_scalar_type(
                        raw_state["count_type"],
                        f"{trait_path}.state.count_type",
                    ),
                    nullable=_bool(
                        raw_state["nullable"],
                        f"{trait_path}.state.nullable",
                    ),
                )
            aggregates.append(
                AggregateTrait(
                    input=_string(trait["input"], f"{trait_path}.input"),
                    function=function,
                    output=_string(trait["output"], f"{trait_path}.output"),
                    output_type=_scalar_type(trait["type"], f"{trait_path}.type"),
                    output_nullable=_bool(trait["nullable"], f"{trait_path}.nullable"),
                    distinct=_bool(trait["distinct"], f"{trait_path}.distinct"),
                    unwrap=_bool(trait["unwrap"], f"{trait_path}.unwrap"),
                    state=state,
                )
            )
        if not aggregates:
            _fail(f"{path}.aggregates", "must not be empty")
        phase = _string(obj["phase"], f"{path}.phase")
        if phase not in OPERATOR_PHASES:
            _fail(f"{path}.phase", f"unsupported aggregate phase {phase!r}")
        return Aggregate(
            id=node_id,
            input=_string(obj["input"], f"{path}.input"),
            keys=keys,
            aggregates=tuple(aggregates),
            phase=phase,
            distinct_all=_bool(obj["distinct_all"], f"{path}.distinct_all"),
        )

    if operation == "join":
        _keys(
            obj,
            {"id", "op", "left", "right", "kind", "predicate"},
            path,
            {"keys"},
        )
        kind = _string(obj["kind"], f"{path}.kind")
        if kind not in JOIN_KINDS:
            _fail(f"{path}.kind", f"unsupported join kind {kind!r}")
        keys: list[JoinKey] = []
        budget = _ExprBudget()
        for index, raw_key in enumerate(
            _array(obj.get("keys", []), f"{path}.keys")
        ):
            key_path = f"{path}.keys[{index}]"
            key = _object(raw_key, key_path)
            _keys(key, {"left", "right"}, key_path)
            # Preserve the old complete-predicate audit bound: one equality
            # and its two column leaves became this side-explicit descriptor.
            for _ in range(3):
                budget.charge(key_path, 1)
            keys.append(
                JoinKey(
                    _string(key["left"], f"{key_path}.left"),
                    _string(key["right"], f"{key_path}.right"),
                )
            )
        predicate_depth = 1
        if keys:
            # Matching is the conjunction of every side-explicit key and the
            # residual predicate, even though that outer AND is not serialized
            # as an ordinary scalar expression.
            budget.charge(path, 1)
            predicate_depth = 2
        return Join(
            node_id,
            _string(obj["left"], f"{path}.left"),
            _string(obj["right"], f"{path}.right"),
            kind,
            tuple(keys),
            _parse_expr(
                obj["predicate"],
                f"{path}.predicate",
                budget=budget,
                structural_depth=predicate_depth,
            ),
        )

    if operation == "union_all":
        _keys(obj, {"id", "op", "inputs", "output", "ordered"}, path)
        inputs: list[UnionInput] = []
        for index, raw_input in enumerate(_array(obj["inputs"], f"{path}.inputs")):
            input_path = f"{path}.inputs[{index}]"
            union_input = _object(raw_input, input_path)
            _keys(union_input, {"node", "columns"}, input_path)
            inputs.append(
                UnionInput(
                    node=_string(union_input["node"], f"{input_path}.node"),
                    columns=tuple(
                        _string(column, f"{input_path}.columns[{column_index}]")
                        for column_index, column in enumerate(
                            _array(union_input["columns"], f"{input_path}.columns")
                        )
                    ),
                )
            )
        if len(inputs) != 2:
            _fail(f"{path}.inputs", "requires exactly two inputs")
        output = tuple(
            _string(column, f"{path}.output[{index}]")
            for index, column in enumerate(_array(obj["output"], f"{path}.output"))
        )
        if not output:
            _fail(f"{path}.output", "must not be empty")
        return UnionAll(
            node_id,
            tuple(inputs),
            output,
            _bool(obj["ordered"], f"{path}.ordered"),
        )

    _fail(f"{path}.op", f"unsupported operator {operation!r}")


def _parse_stage(value: Any, path: str) -> Stage:
    obj = _object(value, path)
    _keys(obj, {"id", "nodes", "inputs", "outputs", "source_storage"}, path)
    nodes = tuple(
        _string(item, f"{path}.nodes[{index}]")
        for index, item in enumerate(_array(obj["nodes"], f"{path}.nodes"))
    )
    if not nodes:
        _fail(f"{path}.nodes", "must not be empty")
    inputs = tuple(
        _string(item, f"{path}.inputs[{index}]")
        for index, item in enumerate(_array(obj["inputs"], f"{path}.inputs"))
    )
    outputs: list[StageOutput] = []
    for index, raw_output in enumerate(_array(obj["outputs"], f"{path}.outputs")):
        output_path = f"{path}.outputs[{index}]"
        output = _object(raw_output, output_path)
        _keys(output, {"index", "node"}, output_path)
        outputs.append(
            StageOutput(
                _index(output["index"], f"{output_path}.index"),
                _string(output["node"], f"{output_path}.node"),
            )
        )
    if not outputs:
        _fail(f"{path}.outputs", "must not be empty")
    storage = obj["source_storage"]
    if storage is not None and storage not in {"row", "column"}:
        _fail(f"{path}.source_storage", "expected null, 'row', or 'column'")
    return Stage(
        _string(obj["id"], f"{path}.id"),
        nodes,
        inputs,
        tuple(outputs),
        storage,
    )


def _parse_stage_edge(value: Any, path: str) -> StageEdge:
    obj = _object(value, path)
    common = {
        "id", "producer", "consumer", "occurrence", "producer_output", "consumer_input", "kind"
    }
    kind = _string(obj.get("kind"), f"{path}.kind")
    if kind not in STAGE_CONNECTION_KINDS:
        _fail(f"{path}.kind", f"unsupported connection kind {kind!r}")
    extra = {
        "hash_shuffle": {"keys", "hash_function", "use_spilling"},
        "union_all": {"parallel"},
        "merge": {"order"},
    }.get(kind, set())
    _keys(obj, common | extra, path)
    fields = dict(
        id=_string(obj["id"], f"{path}.id"),
        producer=_string(obj["producer"], f"{path}.producer"),
        consumer=_string(obj["consumer"], f"{path}.consumer"),
        occurrence=_index(obj["occurrence"], f"{path}.occurrence"),
        producer_output=_index(obj["producer_output"], f"{path}.producer_output"),
        consumer_input=_index(obj["consumer_input"], f"{path}.consumer_input"),
        kind=kind,
    )
    if kind == "hash_shuffle":
        keys = tuple(
            _string(item, f"{path}.keys[{index}]")
            for index, item in enumerate(_array(obj["keys"], f"{path}.keys"))
        )
        if not keys:
            _fail(f"{path}.keys", "must not be empty")
        hash_function = _string(obj["hash_function"], f"{path}.hash_function")
        if hash_function not in HASH_FUNCTIONS:
            _fail(f"{path}.hash_function", f"unsupported hash function {hash_function!r}")
        return StageEdge(
            **fields,
            keys=keys,
            hash_function=hash_function,
            use_spilling=_bool(obj["use_spilling"], f"{path}.use_spilling"),
        )
    if kind == "union_all":
        return StageEdge(**fields, parallel=_bool(obj["parallel"], f"{path}.parallel"))
    if kind == "merge":
        return StageEdge(
            **fields,
            order=_parse_sort_order(obj["order"], f"{path}.order"),
        )
    return StageEdge(**fields)


def _parse_stage_graph(value: Any, path: str) -> StageGraph:
    obj = _object(value, path)
    _keys(obj, {"root_stage", "stages", "edges", "assumptions"}, path)
    assumptions = _array(obj["assumptions"], f"{path}.assumptions")
    if assumptions:
        _fail(f"{path}.assumptions", "version one does not model distribution assumptions")
    return StageGraph(
        root_stage=_string(obj["root_stage"], f"{path}.root_stage"),
        stages=tuple(
            _parse_stage(stage, f"{path}.stages[{index}]")
            for index, stage in enumerate(_array(obj["stages"], f"{path}.stages"))
        ),
        edges=tuple(
            _parse_stage_edge(edge, f"{path}.edges[{index}]")
            for index, edge in enumerate(_array(obj["edges"], f"{path}.edges"))
        ),
    )


def _parse_subplan_output(value: Any, path: str) -> SubplanOutput:
    obj = _object(value, path)
    _keys(obj, {"column", "type", "nullable"}, path)
    scalar_type = _scalar_type(obj["type"], f"{path}.type")
    if scalar_type == DOUBLE:
        _fail(f"{path}.type", "subplans may not transport Double")
    return SubplanOutput(
        column=_string(obj["column"], f"{path}.column"),
        type=scalar_type,
        nullable=_bool(obj["nullable"], f"{path}.nullable"),
    )


def _parse_subplan(value: Any, path: str) -> Subplan:
    obj = _object(value, path)
    kind = _string(obj.get("kind"), f"{path}.kind")
    common_keys = {
        "binding",
        "kind",
        "root",
        "type",
        "nullable",
        "dependencies",
        "consumers",
    }
    if kind not in {"scalar", "exists", "in"}:
        _fail(f"{path}.kind", f"unsupported subplan kind {kind!r}")
    variant_keys = {
        "scalar": {"output"},
        "exists": {"predicate"},
        "in": {"lookup", "output"},
    }[kind]
    _keys(obj, common_keys | variant_keys, path)
    binding = _string(obj["binding"], f"{path}.binding")
    root = _string(obj["root"], f"{path}.root")
    scalar_type = _scalar_type(obj["type"], f"{path}.type")
    if scalar_type == DOUBLE:
        _fail(f"{path}.type", "subplans may not transport Double")
    nullable = _bool(obj["nullable"], f"{path}.nullable")
    dependencies = tuple(
        _string(item, f"{path}.dependencies[{index}]")
        for index, item in enumerate(
            _array(obj["dependencies"], f"{path}.dependencies")
        )
    )
    consumers = tuple(
        _string(item, f"{path}.consumers[{index}]")
        for index, item in enumerate(
            _array(obj["consumers"], f"{path}.consumers")
        )
    )
    if kind == "scalar":
        output = _parse_subplan_output(obj["output"], f"{path}.output")
        if scalar_type != output.type:
            _fail(
                f"{path}.type",
                "must exactly match the scalar subplan output type",
            )
        if not nullable:
            _fail(
                f"{path}.nullable",
                "a scalar subplan binding must be nullable because zero rows yield NULL",
            )
        if len(dependencies) > 1:
            _fail(
                f"{path}.dependencies",
                "scalar subplans support at most one outer dependency",
            )
        return ScalarSubplan(
            binding=binding,
            root=root,
            output=output,
            dependency=dependencies[0] if dependencies else None,
            consumers=consumers,
        )
    if kind == "in":
        if scalar_type != BOOL:
            _fail(f"{path}.type", "an IN binding must have type 'Bool'")
        if nullable:
            _fail(f"{path}.nullable", "this IN binding must be non-nullable")
        if dependencies:
            _fail(
                f"{path}.dependencies",
                "this IN binding must be uncorrelated",
            )

        lookup = _parse_subplan_output(obj["lookup"], f"{path}.lookup")
        output = _parse_subplan_output(obj["output"], f"{path}.output")
        if lookup.type != output.type:
            _fail(
                path,
                "IN lookup and output types must match exactly",
            )
        if (
            (lookup.nullable or output.nullable)
            and integer_bounds(lookup.type) is None
            and lookup.type != DATE
        ):
            _fail(
                path,
                "nullable-column IN supports only fixed-width integral or Date columns",
            )
        if (
            integer_bounds(lookup.type) is None
            and lookup.type not in {DATE, "String"}
        ):
            _fail(
                path,
                "this IN slice supports only fixed-width integral, "
                "Date, or String columns",
            )
        return InSubplan(
            binding=binding,
            root=root,
            lookup=lookup,
            output=output,
            consumers=consumers,
        )
    if scalar_type != BOOL:
        _fail(f"{path}.type", "an EXISTS binding must have type 'Bool'")
    if nullable:
        _fail(f"{path}.nullable", "an EXISTS binding must be non-nullable")
    if len(dependencies) > 2:
        _fail(
            f"{path}.dependencies",
            "EXISTS supports at most two outer dependencies",
        )
    predicate = (
        None
        if obj["predicate"] is None
        else _parse_expr(obj["predicate"], f"{path}.predicate")
    )
    if bool(dependencies) != (predicate is not None):
        _fail(
            f"{path}.predicate",
            "must be present exactly when EXISTS is correlated",
        )
    return ExistsSubplan(
        binding=binding,
        root=root,
        predicate=predicate,
        dependencies=dependencies,
        consumers=consumers,
    )


def parse_snapshot(value: Any) -> Snapshot:
    obj = _object(value, "snapshot")
    _keys(obj, {"format", "version", "schema", "plan", "stage_graph"}, "snapshot")
    if obj["format"] != FORMAT:
        _fail("snapshot.format", f"expected {FORMAT!r}")
    if type(obj["version"]) is not int or obj["version"] != VERSION:
        _fail("snapshot.version", f"expected version {VERSION}")
    schema = _object(obj["schema"], "snapshot.schema")
    _keys(schema, {"tables"}, "snapshot.schema")
    tables = tuple(
        _parse_table(table, f"snapshot.schema.tables[{index}]")
        for index, table in enumerate(_array(schema["tables"], "snapshot.schema.tables"))
    )
    raw_plan = _object(obj["plan"], "snapshot.plan")
    _keys(
        raw_plan,
        {"nodes", "root", "output"},
        "snapshot.plan",
        {"subplans"},
    )
    nodes = tuple(
        _parse_node(node, f"snapshot.plan.nodes[{index}]")
        for index, node in enumerate(_array(raw_plan["nodes"], "snapshot.plan.nodes"))
    )
    output = tuple(
        _string(column, f"snapshot.plan.output[{index}]")
        for index, column in enumerate(_array(raw_plan["output"], "snapshot.plan.output"))
    )
    subplans = tuple(
        _parse_subplan(subplan, f"snapshot.plan.subplans[{index}]")
        for index, subplan in enumerate(
            _array(raw_plan.get("subplans", []), "snapshot.plan.subplans")
        )
    )
    snapshot = Snapshot(
        tables=tables,
        plan=Plan(
            nodes=nodes,
            root=_string(raw_plan["root"], "snapshot.plan.root"),
            output=output,
            subplans=subplans,
        ),
        stage_graph=(
            None
            if obj["stage_graph"] is None
            else _parse_stage_graph(obj["stage_graph"], "snapshot.stage_graph")
        ),
    )
    validate_snapshot(snapshot)
    return snapshot


def load_snapshot(path: str | Path) -> Snapshot:
    try:
        with Path(path).open("r", encoding="utf-8") as stream:
            value = json.load(stream)
    except json.JSONDecodeError as error:
        raise SnapshotError(f"{path}: invalid JSON: {error}") from error
    except RecursionError as error:
        raise SnapshotError(f"{path}: JSON nesting exceeds the decoder limit") from error
    return parse_snapshot(value)


def _unique(values: Sequence[str], path: str) -> None:
    seen: set[str] = set()
    for value in values:
        if value in seen:
            _fail(path, f"duplicate name {value!r}")
        seen.add(value)


def _infer_expr(
    expr: Expr,
    columns: Mapping[str, Column],
    path: str,
    bindings: tuple[ValueType, ...] = (),
) -> ValueType:
    def shallow_type(candidate: Expr) -> str | None:
        if candidate.kind == "column":
            column = columns.get(candidate.column or "")
            return None if column is None else column.type
        if candidate.kind == "bound":
            if candidate.depth is None or not 0 <= candidate.depth < len(bindings):
                return None
            return bindings[candidate.depth].name
        if candidate.kind == "void":
            return VOID
        if candidate.kind in {
            "and",
            "or",
            "not",
            "exists",
            "in",
            "eq",
            "lt",
            "lte",
            "gt",
            "gte",
        }:
            return BOOL
        return candidate.result_type

    # Double has no interpreted scalar semantics in v1.  It is only an SMT
    # identity token created by the one audited constructor and thereafter
    # transported by direct column references.
    if expr.kind not in {"column", "opaque_double"}:
        if shallow_type(expr) == DOUBLE:
            _fail(
                path,
                "Double may be returned only by opaque_double or a direct column",
            )
        if any(shallow_type(argument) == DOUBLE for argument in expr.args):
            _fail(path, f"{expr.kind} may not consume Double")

    if expr.kind == "column":
        if expr.column not in columns:
            _fail(path, f"column {expr.column!r} is not available")
        value_type = columns[expr.column].value_type
        if value_type.name == VOID:
            _fail(path, "void may only flow transparently to a canonical count aggregate")
        return value_type

    if expr.kind == "bound":
        if expr.depth is None or not 0 <= expr.depth < len(bindings):
            _fail(path, "bound expression does not refer to an enclosing IfPresent handler")
        return bindings[expr.depth]

    if expr.kind == "void":
        _fail(path, "void may only flow transparently to a canonical count aggregate")

    if expr.kind in {"literal", "null", "opaque"}:
        assert expr.result_type is not None and expr.nullable is not None
        if expr.kind == "opaque":
            for index, arg in enumerate(expr.args):
                _infer_expr(arg, columns, f"{path}.args[{index}]", bindings)
        return ValueType(expr.result_type, expr.nullable)

    if expr.kind == "opaque_double":
        if expr.result_type != DOUBLE or expr.nullable is not True:
            _fail(path, "opaque_double result must be Optional<Double>")
        fingerprint = expr.fingerprint
        if not (
            isinstance(fingerprint, str)
            and fingerprint.startswith(OPAQUE_DOUBLE_FINGERPRINT_PREFIX)
            and len(fingerprint) > len(OPAQUE_DOUBLE_FINGERPRINT_PREFIX)
        ):
            _fail(
                f"{path}.fingerprint",
                "opaque_double requires the audited passive-Double fingerprint prefix "
                "and a non-empty identity suffix",
            )
        if len(expr.args) != 3:
            _fail(path, "opaque_double requires exactly three arguments")
        if any(
            argument.kind != "column" or argument.column is None
            for argument in expr.args
        ):
            _fail(path, "opaque_double arguments must be direct column references")
        argument_columns = tuple(argument.column for argument in expr.args)
        if len(set(argument_columns)) != 3:
            _fail(path, "opaque_double arguments must reference three distinct columns")
        for index, arg in enumerate(expr.args):
            argument = _infer_expr(
                arg,
                columns,
                f"{path}.args[{index}]",
                bindings,
            )
            if argument != ValueType("Int64", True):
                _fail(
                    f"{path}.args[{index}]",
                    "opaque_double arguments must be Optional<Int64>",
                )
        return ValueType(DOUBLE, True)

    if expr.kind in {"and", "or", "not"}:
        argument_types = [
            _infer_expr(arg, columns, f"{path}.args[{index}]", bindings)
            for index, arg in enumerate(expr.args)
        ]
        if any(argument.name != BOOL for argument in argument_types):
            _fail(path, f"{expr.kind} requires Boolean arguments")
        return ValueType(BOOL, any(argument.nullable for argument in argument_types))

    if expr.kind == "exists":
        _infer_expr(expr.args[0], columns, f"{path}.arg", bindings)
        return ValueType(BOOL, False)

    if expr.kind == "in":
        lookup = _infer_expr(expr.args[0], columns, f"{path}.lookup", bindings)
        item_name = None
        for index, item_expr in enumerate(expr.args[1:]):
            item_path = f"{path}.items[{index}]"
            item = _infer_expr(item_expr, columns, item_path, bindings)
            if item.nullable:
                _fail(item_path, "IN items must be non-nullable")
            if item_name is None:
                item_name = item.name
            elif item.name != item_name:
                _fail(item_path, f"IN items must have one type, starting with {item_name!r}")
            if decimal.is_type(lookup.name) or decimal.is_type(item.name):
                _fail(item_path, "Decimal IN is not supported")
            if not static_in_comparison_compatible(lookup.name, item.name):
                _fail(
                    item_path,
                    "IN item is not equality-compatible with its lookup: "
                    f"{lookup.name!r} and {item.name!r}",
                )
        return ValueType(BOOL, lookup.nullable)

    if expr.kind in {"eq", "lt", "lte", "gt", "gte"}:
        left = _infer_expr(expr.args[0], columns, f"{path}.left", bindings)
        right = _infer_expr(expr.args[1], columns, f"{path}.right", bindings)
        if not equality_comparison_compatible(left.name, right.name):
            label = "equality" if expr.kind == "eq" else "comparison"
            _fail(path, f"{label} type mismatch: {left.name!r} and {right.name!r}")
        if (
            expr.null_safe
            and (decimal.is_type(left.name) or decimal.is_type(right.name))
            and left.name != right.name
        ):
            _fail(path, "null-safe Decimal equality requires exactly matching types")
        if expr.kind != "eq" and not ordering_comparison_compatible(left.name, right.name):
            _fail(path, f"{expr.kind} requires integer, String/Utf8, Date, or Decimal arguments")
        return ValueType(BOOL, False if expr.null_safe else left.nullable or right.nullable)

    if expr.kind in {"add", "sub", "mul", "div"}:
        assert expr.result_type is not None and expr.nullable is not None
        left = _infer_expr(expr.args[0], columns, f"{path}.left", bindings)
        right = _infer_expr(expr.args[1], columns, f"{path}.right", bindings)
        if decimal.is_type(expr.result_type):
            if left.name != expr.result_type:
                _fail(path, f"Decimal {expr.kind} left operand must exactly match its result type")
            right_may_be_integral = expr.kind in {"mul", "div"} and family(right.name) == "int"
            if right.name != expr.result_type and not right_may_be_integral:
                _fail(
                    path,
                    f"Decimal {expr.kind} right operand must exactly match its result type"
                    + (" or be integral" if expr.kind in {"mul", "div"} else ""),
                )
        else:
            if family(expr.result_type) != "int":
                _fail(
                    path,
                    (
                        "integral div requires a fixed-width integer result"
                        if expr.kind == "div"
                        else f"{expr.kind} requires an integer result"
                    ),
                )
            if left.name != expr.result_type or right.name != expr.result_type:
                _fail(
                    path,
                    f"{expr.kind} operands and result must have exactly the same type: "
                    f"{left.name!r}, {right.name!r}, and {expr.result_type!r}",
                )
        nullable = (
            True
            if expr.kind == "div" and not decimal.is_type(expr.result_type)
            else left.nullable or right.nullable
        )
        if expr.nullable != nullable:
            _fail(
                path,
                (
                    "integral div result must be nullable"
                    if expr.kind == "div" and not decimal.is_type(expr.result_type)
                    else f"{expr.kind} nullability must equal the OR of operand nullability"
                ),
            )
        return ValueType(expr.result_type, nullable)

    if expr.kind == "cast_decimal":
        assert expr.result_type is not None and expr.nullable is not None
        argument = _infer_expr(expr.args[0], columns, f"{path}.arg", bindings)
        if expr.source_type is None:
            _fail(path, "Decimal cast requires its audited source type")
        if expr.source_type != argument.name:
            _fail(
                path,
                "Decimal cast source type annotation does not match its "
                f"serialized argument: {expr.source_type!r} != {argument.name!r}",
            )
        result = decimal.parse_type(expr.result_type)
        if result is None:
            _fail(path, "Decimal cast result must be a canonical Decimal type")
        if result.integral_digits < 1:
            _fail(path, "Decimal cast result must have at least one integral digit")
        source = decimal.parse_type(argument.name)
        if family(argument.name) == "int":
            pass
        elif source is None:
            _fail(path, "Decimal cast source must be integral or Decimal")
        elif source.scale != result.scale:
            _fail(path, "Decimal widening must preserve scale")
        elif source.precision > result.precision:
            _fail(path, "Decimal widening must not decrease precision")
        if expr.nullable != argument.nullable:
            _fail(path, "Decimal cast result nullability must match its source")
        return ValueType(expr.result_type, expr.nullable)

    if expr.kind == "cast_integral":
        assert expr.result_type is not None and expr.nullable is not None
        argument = _infer_expr(expr.args[0], columns, f"{path}.arg", bindings)
        if family(argument.name) != "int":
            _fail(path, "integral SafeCast source must be an integer")
        if family(expr.result_type) != "int":
            _fail(path, "integral SafeCast result must be an integer")
        if not expr.nullable:
            _fail(path, "integral SafeCast result must be nullable")
        source_bounds = integer_bounds(argument.name)
        result_bounds = integer_bounds(expr.result_type)
        assert source_bounds is not None and result_bounds is not None
        source_lower, source_upper = source_bounds
        result_lower, result_upper = result_bounds
        if result_lower <= source_lower and source_upper <= result_upper:
            _fail(path, "integral SafeCast must be a partial conversion")
        return ValueType(expr.result_type, True)

    if expr.kind == "if":
        assert expr.result_type is not None and expr.nullable is not None
        condition = _infer_expr(
            expr.args[0],
            columns,
            f"{path}.condition",
            bindings,
        )
        if condition.name != BOOL:
            _fail(f"{path}.condition", "If condition must be Boolean")
        then = _infer_expr(expr.args[1], columns, f"{path}.then", bindings)
        otherwise = _infer_expr(expr.args[2], columns, f"{path}.else", bindings)
        if then.name != expr.result_type or otherwise.name != expr.result_type:
            _fail(path, "If branch types must exactly match its result type")
        expected_nullable = condition.nullable or then.nullable or otherwise.nullable
        if expr.nullable != expected_nullable:
            _fail(path, "If nullability must equal the OR of condition and branches")
        return ValueType(expr.result_type, expr.nullable)

    if expr.kind == "if_present":
        assert expr.result_type is not None and expr.nullable is not None
        optional = _infer_expr(expr.args[0], columns, f"{path}.optional", bindings)
        if not optional.nullable:
            _fail(f"{path}.optional", "IfPresent optional must be nullable")
        result = ValueType(expr.result_type, expr.nullable)
        present = _infer_expr(
            expr.args[1],
            columns,
            f"{path}.present",
            (ValueType(optional.name, False), *bindings),
        )
        missing = _infer_expr(expr.args[2], columns, f"{path}.missing", bindings)
        if present != result:
            _fail(
                f"{path}.present",
                "IfPresent handler type and nullability must exactly match its result",
            )
        if missing != result:
            _fail(
                f"{path}.missing",
                "IfPresent missing type and nullability must exactly match its result",
            )
        return result

    raise AssertionError(f"parser admitted unknown expression kind {expr.kind!r}")


def _nullable_columns(columns: Mapping[str, Column]) -> dict[str, Column]:
    return {name: replace(column, nullable=True) for name, column in columns.items()}


def _sum_type(input_type: str) -> str | None:
    if input_type in {"Int8", "Int16", "Int32", "Int64"}:
        return "Int64"
    if input_type in {"Uint8", "Uint16", "Uint32", "Uint64"}:
        return "Uint64"
    return decimal.sum_type(input_type)


def _is_final_count_sum(node: Aggregate, nodes: Mapping[str, PlanNode], input_name: str) -> bool:
    child = nodes.get(node.input)
    return (
        node.phase == "final"
        and isinstance(child, Aggregate)
        and any(
            trait.output == input_name and trait.function == "count"
            for trait in child.aggregates
        )
    )


def _is_exact_scalar_uint64_unwrap(
    node: Aggregate,
    trait: AggregateTrait,
    input_column: Column,
) -> bool:
    """Recognize the scalar physical Coalesce(..., Uint64(0)) contract.

    TPhysicalAggregationBuilder lowers a scalar trait with Unwrap set through
    NeedToWrapWithCoalesce.  Keep the admitted prephysical shape deliberately
    narrower than that implementation hook: this is the reviewed scalar
    count-to-sum coalesce shape, and every other Unwrap remains closed.
    """

    return (
        not node.keys
        and node.phase == "final"
        and not node.distinct_all
        and trait.function == "sum"
        and not trait.distinct
        and trait.unwrap
        and input_column.type == "Uint64"
        and input_column.nullable
        and trait.output_type == "Uint64"
        and trait.output_nullable
    )


def _is_exact_scalar_int64_count_distinct(
    node: Aggregate,
    trait: AggregateTrait,
    input_column: Column,
) -> bool:
    """Recognize the direct logical COUNT(DISTINCT Int64) boundary."""

    return (
        not node.keys
        and node.phase == "undefined"
        and not node.distinct_all
        and trait.function == "count"
        and trait.distinct
        and not trait.unwrap
        and input_column.type == "Int64"
        and not input_column.nullable
        and trait.output_type == "Uint64"
        and not trait.output_nullable
    )


def plan_node_inputs(node: PlanNode) -> tuple[str, ...]:
    if isinstance(node, (EmptySource, Scan)):
        return ()
    if isinstance(node, (Project, Filter, OuterBind, Limit, Sort, Aggregate)):
        return (node.input,)
    if isinstance(node, Join):
        return (node.left, node.right)
    if isinstance(node, UnionAll):
        return tuple(item.node for item in node.inputs)
    raise AssertionError(f"unknown node class {type(node).__name__}")


def _void_columns(columns: Mapping[str, Column]) -> set[str]:
    return {name for name, column in columns.items() if column.type == VOID}


def _validate_average_state_dataflow(snapshot: Snapshot) -> None:
    """Keep hidden AVG tuples on one exact intermediate-to-final edge.

    The RBO operator graph annotates an intermediate AVG IU with its logical
    Decimal result type even though physical lowering carries an optional
    ``(Decimal(35, scale) sum, Uint64 count)`` tuple.  Admitting that IU as an
    ordinary scalar would silently model an average of partial averages.
    """

    nodes = snapshot.plan.node_map()
    consumers: dict[str, list[PlanNode]] = {node.id: [] for node in snapshot.plan.nodes}
    for consumer in snapshot.plan.nodes:
        for producer in plan_node_inputs(consumer):
            consumers[producer].append(consumer)

    for node in snapshot.plan.nodes:
        if not isinstance(node, Aggregate):
            continue

        final_average_traits = tuple(
            trait for trait in node.aggregates if trait.function == "avg"
        )
        if node.phase == "final":
            child = nodes.get(node.input)
            if final_average_traits and (
                not isinstance(child, Aggregate)
                or child.phase != "intermediate"
                or child.keys != node.keys
            ):
                _fail(
                    f"node {node.id!r}.aggregates",
                    "final avg must directly consume an intermediate aggregate "
                    "with the same ordered keys",
                )
            if isinstance(child, Aggregate) and child.phase == "intermediate":
                child_traits = {trait.output: trait for trait in child.aggregates}
                for trait in final_average_traits:
                    source = child_traits.get(trait.input)
                    if (
                        source is None
                        or source.function != "avg"
                        or source.output_type != trait.output_type
                        or source.state != trait.state
                    ):
                        _fail(
                            f"node {node.id!r}.aggregates",
                            "final avg must consume the matching intermediate "
                            "avg state with identical Decimal metadata",
                        )

        if node.phase != "intermediate":
            continue
        intermediate_average_traits = tuple(
            trait for trait in node.aggregates if trait.function == "avg"
        )
        if not intermediate_average_traits:
            continue
        node_consumers = consumers[node.id]
        if (
            len(node_consumers) != 1
            or not isinstance(node_consumers[0], Aggregate)
            or node_consumers[0].phase != "final"
            or node_consumers[0].input != node.id
        ):
            _fail(
                f"node {node.id!r}.aggregates",
                "intermediate avg state must have one direct final aggregate consumer",
            )
        final = node_consumers[0]
        assert isinstance(final, Aggregate)
        if final.keys != node.keys:
            _fail(
                f"node {node.id!r}.keys",
                "intermediate and final avg must have the same ordered keys",
            )
        for trait in intermediate_average_traits:
            uses = tuple(
                candidate
                for candidate in final.aggregates
                if candidate.input == trait.output
            )
            if (
                trait.output in final.keys
                or len(uses) != 1
                or uses[0].function != "avg"
                or uses[0].output_type != trait.output_type
                or uses[0].state != trait.state
            ):
                _fail(
                    f"node {node.id!r}.aggregates",
                    "each intermediate avg state must be used only by one "
                    "matching final avg trait",
                )

        graph = snapshot.stage_graph
        if graph is not None:
            output_stages = {
                (stage.id, output.index): output.node
                for stage in graph.stages
                for output in stage.outputs
            }
            hidden_outputs = {
                trait.output for trait in intermediate_average_traits
            }
            for edge in graph.edges:
                if output_stages.get(
                    (edge.producer, edge.producer_output)
                ) != node.id:
                    continue
                routed = set(edge.keys) | {
                    item.column for item in edge.order
                }
                if hidden_outputs & routed:
                    _fail(
                        f"stage edge {edge.id!r}",
                        "intermediate avg state may only be transported as payload",
                    )


def _validate_void_dataflow(
    snapshot: Snapshot,
    schemas: Mapping[str, Mapping[str, Column]],
) -> None:
    """Keep the unit carrier on passive paths that terminate in COUNT(*)."""

    message = "void may only flow transparently to a canonical count aggregate"
    root_voids = _void_columns(schemas[snapshot.plan.root])
    if root_voids:
        _fail("snapshot.plan.root", message)

    for node in snapshot.plan.nodes:
        if isinstance(node, (EmptySource, Scan)):
            continue

        if isinstance(node, Project):
            input_voids = _void_columns(schemas[node.input])
            passed = {
                column.expression.column
                for column in node.columns
                if column.expression.kind == "column"
                and column.expression.column in input_voids
            }
            if passed != input_voids:
                _fail(f"node {node.id!r}", message)
            continue

        if isinstance(node, (Filter, OuterBind, Limit, Sort)):
            continue

        if isinstance(node, Aggregate):
            input_voids = _void_columns(schemas[node.input])
            for input_name in input_voids:
                traits = tuple(
                    trait for trait in node.aggregates if trait.input == input_name
                )
                if (
                    node.distinct_all
                    or input_name in node.keys
                    or not traits
                    or any(
                        trait.function != "count" or trait.distinct or trait.unwrap
                        for trait in traits
                    )
                ):
                    _fail(f"node {node.id!r}", message)
            continue

        if isinstance(node, Join):
            left_voids = _void_columns(schemas[node.left])
            right_voids = _void_columns(schemas[node.right])
            if any(
                key.left in left_voids or key.right in right_voids
                for key in node.keys
            ):
                _fail(f"node {node.id!r}", message)

            dropped = set()
            retained = set()
            if node.kind in {"left_semi", "left_anti"}:
                dropped = right_voids
                retained = left_voids
            elif node.kind in {"right_semi", "right_anti"}:
                dropped = left_voids
                retained = right_voids
            if not dropped.issubset(retained):
                _fail(f"node {node.id!r}", message)
            continue

        if isinstance(node, UnionAll):
            for item in node.inputs:
                input_voids = _void_columns(schemas[item.node])
                if not input_voids.issubset(item.columns):
                    _fail(f"node {node.id!r}", message)
            continue

        raise AssertionError(f"unknown node class {type(node).__name__}")


def stage_input_slots(plan: Plan, stage: Stage) -> tuple[tuple[str, int, str], ...]:
    """Return (consumer node, child ordinal, producer node) for stage arguments."""

    members = set(stage.nodes)
    nodes = plan.node_map()
    return tuple(
        (node_id, child_index, child_id)
        for node_id in stage.nodes
        for child_index, child_id in enumerate(plan_node_inputs(nodes[node_id]))
        if child_id not in members
    )


def _validate_stage_graph(
    snapshot: Snapshot,
    schemas: Mapping[str, Mapping[str, Column]],
) -> None:
    graph = snapshot.stage_graph
    if graph is None:
        if any(
            isinstance(node, Scan)
            and (node.predicate is not None or node.pushed_limit is not None)
            for node in snapshot.plan.nodes
        ):
            _fail(
                "snapshot.stage_graph",
                "a pushed scan predicate or limit requires a column-storage source stage",
            )
        return
    path = "snapshot.stage_graph"
    if not graph.stages:
        _fail(f"{path}.stages", "must not be empty")
    _unique([stage.id for stage in graph.stages], f"{path}.stages")
    _unique([edge.id for edge in graph.edges], f"{path}.edges")
    stages = graph.stage_map()
    if graph.root_stage not in stages:
        _fail(f"{path}.root_stage", f"unknown stage {graph.root_stage!r}")

    plan_nodes = snapshot.plan.node_map()
    owner: dict[str, str] = {}
    outputs: dict[tuple[str, int], str] = {}
    for stage_index, stage in enumerate(graph.stages):
        stage_path = f"{path}.stages[{stage_index}]"
        _unique(stage.nodes, f"{stage_path}.nodes")
        members = set(stage.nodes)
        seen: set[str] = set()
        same_stage_parents = {node_id: 0 for node_id in stage.nodes}
        for node_id in stage.nodes:
            if node_id not in plan_nodes:
                _fail(f"{stage_path}.nodes", f"unknown plan node {node_id!r}")
            if node_id in owner:
                _fail(f"{stage_path}.nodes", f"plan node {node_id!r} is also in stage {owner[node_id]!r}")
            local_inputs = [item for item in plan_node_inputs(plan_nodes[node_id]) if item in members]
            if isinstance(plan_nodes[node_id], (Join, UnionAll)) and local_inputs:
                _fail(
                    f"{stage_path}.nodes",
                    "Join/UnionAll inputs must cross stage boundaries",
                )
            if any(item not in seen for item in local_inputs):
                _fail(f"{stage_path}.nodes", "must be in local topological order")
            for item in local_inputs:
                same_stage_parents[item] += 1
            seen.add(node_id)
            owner[node_id] = stage.id

        sinks = [node_id for node_id, parents in same_stage_parents.items() if parents == 0]
        if len(sinks) != 1:
            _fail(stage_path, "must have exactly one local output sink")
        sink = sinks[0]

        expected_inputs = tuple(slot[2] for slot in stage_input_slots(snapshot.plan, stage))
        if stage.inputs != expected_inputs:
            _fail(
                f"{stage_path}.inputs",
                f"expected cross-stage child occurrences {expected_inputs!r}",
            )
        indices = [output.index for output in stage.outputs]
        if sorted(indices) != list(range(len(indices))):
            _fail(f"{stage_path}.outputs", "indices must be contiguous from zero")
        for output in stage.outputs:
            if output.node not in seen:
                _fail(f"{stage_path}.outputs", f"node {output.node!r} is not a stage member")
            if output.node != sink:
                _fail(f"{stage_path}.outputs", "every output must map the local stage sink")
            outputs[(stage.id, output.index)] = output.node

        scans = [plan_nodes[node_id] for node_id in stage.nodes if isinstance(plan_nodes[node_id], Scan)]
        if stage.source_storage is None and scans:
            _fail(f"{stage_path}.source_storage", "a scan stage must declare source storage")
        if stage.source_storage is not None:
            if len(scans) != 1 or stage.inputs:
                _fail(stage_path, "a source stage must contain one scan and have no inputs")
            if stage.source_storage == "row" and len(stage.nodes) != 1:
                _fail(stage_path, "a row-storage source stage must contain only its scan")
        if (
            scans
            and (scans[0].predicate is not None or scans[0].pushed_limit is not None)
            and stage.source_storage != "column"
        ):
            _fail(
                f"{stage_path}.source_storage",
                "a pushed scan predicate or limit requires column storage",
            )

    missing = plan_nodes.keys() - owner.keys()
    if missing:
        _fail(f"{path}.stages", f"plan nodes have no stage: {', '.join(sorted(missing))}")
    root = stages[graph.root_stage]
    if len(root.outputs) != 1:
        _fail(f"{path}.root_stage", "must have exactly the synthetic output zero")
    if outputs.get((root.id, 0)) != snapshot.plan.root:
        _fail(f"{path}.root_stage", "output zero must be the plan root")

    edge_by_consumer_input: dict[tuple[str, int], StageEdge] = {}
    edge_by_producer_output: dict[tuple[str, int], StageEdge] = {}
    occurrences: dict[tuple[str, str], list[tuple[int, int]]] = {}
    adjacency: dict[str, set[str]] = {stage.id: set() for stage in graph.stages}
    for edge_index, edge in enumerate(graph.edges):
        edge_path = f"{path}.edges[{edge_index}]"
        producer = stages.get(edge.producer)
        consumer = stages.get(edge.consumer)
        if producer is None or consumer is None:
            _fail(edge_path, "references an unknown producer or consumer stage")
        if edge.producer == edge.consumer:
            _fail(edge_path, "self-edges are not allowed")
        produced_node = outputs.get((edge.producer, edge.producer_output))
        if produced_node is None:
            _fail(f"{edge_path}.producer_output", "unknown producer output")
        producer_output = (edge.producer, edge.producer_output)
        if producer_output in edge_by_producer_output:
            _fail(f"{edge_path}.producer_output", "is already consumed")
        edge_by_producer_output[producer_output] = edge
        if edge.consumer_input >= len(consumer.inputs):
            _fail(f"{edge_path}.consumer_input", "is outside the consumer input list")
        if consumer.inputs[edge.consumer_input] != produced_node:
            _fail(edge_path, "producer output does not match the consumer input occurrence")
        ordinal = (edge.consumer, edge.consumer_input)
        if ordinal in edge_by_consumer_input:
            _fail(f"{edge_path}.consumer_input", "is already connected")
        edge_by_consumer_input[ordinal] = edge
        occurrences.setdefault((edge.producer, edge.consumer), []).append(
            (edge.consumer_input, edge.occurrence)
        )
        adjacency[edge.producer].add(edge.consumer)

        columns = schemas[produced_node]
        for column in edge.keys:
            if column not in columns:
                _fail(f"{edge_path}.keys", f"column {column!r} is not produced")
            if columns[column].type == DOUBLE:
                _fail(f"{edge_path}.keys", "hash routing may not consume Double")
            if columns[column].type == VOID:
                _fail(
                    f"{edge_path}.keys",
                    "void may only flow transparently to a canonical count aggregate",
                )
        for item in edge.order:
            if item.column not in columns:
                _fail(f"{edge_path}.order", f"column {item.column!r} is not produced")
            if not is_ordered_type(columns[item.column].type):
                _fail(
                    f"{edge_path}.order",
                    f"ordering type {columns[item.column].type!r} is unsupported; "
                    "modeled types are integers, String/Utf8, Date, and Decimal",
                )

    for stage in graph.stages:
        declared_outputs = {(stage.id, output.index) for output in stage.outputs}
        consumed_outputs = {
            producer_output
            for producer_output in edge_by_producer_output
            if producer_output[0] == stage.id
        }
        if stage.id == graph.root_stage:
            if consumed_outputs:
                _fail(f"stage {stage.id!r}.outputs", "root output must not feed another stage")
        elif consumed_outputs != declared_outputs:
            _fail(
                f"stage {stage.id!r}.outputs",
                "every declared output must feed exactly one edge occurrence",
            )

        stage_edges = sorted(
            (edge for edge in graph.edges if edge.consumer == stage.id),
            key=lambda edge: edge.consumer_input,
        )
        connected = [edge.consumer_input for edge in stage_edges]
        if connected != list(range(len(stage.inputs))):
            _fail(f"stage {stage.id!r}.inputs", "every input must have exactly one connection")
        closed: set[str] = set()
        previous: str | None = None
        for edge in stage_edges:
            if edge.producer != previous:
                if edge.producer in closed:
                    _fail(f"stage {stage.id!r}.inputs", "connections from one producer must be grouped")
                if previous is not None:
                    closed.add(previous)
                previous = edge.producer
    for pair, values in occurrences.items():
        ordered = [occurrence for _, occurrence in sorted(values)]
        if ordered != list(range(len(values))):
            _fail(
                f"{path}.edges",
                f"occurrences for {pair!r} must follow effective consumer input order",
            )

    visiting: set[str] = set()
    reached: set[str] = set()
    reverse: dict[str, set[str]] = {stage.id: set() for stage in graph.stages}
    for producer, consumers in adjacency.items():
        for consumer in consumers:
            reverse[consumer].add(producer)

    def visit(stage_id: str) -> None:
        if stage_id in visiting:
            _fail(f"{path}.edges", f"cycle through stage {stage_id!r}")
        if stage_id in reached:
            return
        visiting.add(stage_id)
        for producer in reverse[stage_id]:
            visit(producer)
        visiting.remove(stage_id)
        reached.add(stage_id)

    visit(graph.root_stage)
    if reached != stages.keys():
        _fail(f"{path}.stages", "every stage must reach root_stage")

    _infer_stage_task_counts(graph, path)


def _infer_stage_task_counts(graph: StageGraph, path: str) -> dict[str, int]:
    """Mirror CountComputeTasks in the bounded one/two-task model."""

    stages = graph.stage_map()
    incoming = {
        stage.id: tuple(sorted(
            (edge for edge in graph.edges if edge.consumer == stage.id),
            key=lambda edge: edge.consumer_input,
        ))
        for stage in graph.stages
    }
    counts: dict[str, int] = {}
    visiting: set[str] = set()

    def task_count(stage_id: str) -> int:
        if stage_id in counts:
            return counts[stage_id]
        if stage_id in visiting:
            _fail(f"{path}.edges", f"cycle through stage {stage_id!r}")
        visiting.add(stage_id)
        stage = stages[stage_id]
        edges = incoming[stage_id]
        if stage.source_storage is not None:
            result = 2
        elif not edges:
            result = 1
        else:
            # CountComputeTasks starts with one task. Map copies its producer's
            # count at its physical input position, while ParallelUnionAll
            # takes the running maximum. HashShuffle selects the bounded two
            # tasks only when no Map connection forces the count.
            result = 1
            has_hash = False
            force_map_tasks = False
            map_count = 0
            for edge in edges:
                producer_tasks = task_count(edge.producer)
                if edge.kind == "map":
                    result = producer_tasks
                    force_map_tasks = True
                    map_count += 1
                elif edge.kind == "hash_shuffle":
                    has_hash = True
                elif edge.kind == "union_all" and edge.parallel:
                    result = max(result, producer_tasks)

            if map_count > 1:
                _fail(
                    f"stage {stage_id!r}.inputs",
                    "only one Map connection is physically allowed",
                )
            if has_hash and not force_map_tasks:
                result = 2

            # Channel builders impose these constraints after task counting.
            for edge in edges:
                producer_tasks = task_count(edge.producer)
                if edge.kind == "map" and producer_tasks != result:
                    _fail(
                        f"stage {stage_id!r}.inputs",
                        "Map producer and consumer task counts must match",
                    )
                if edge.kind == "union_all" and not edge.parallel and result != 1:
                    _fail(
                        f"stage {stage_id!r}.inputs",
                        "serial UnionAll requires exactly one consumer task",
                    )
                if edge.kind == "merge" and result != 1:
                    _fail(
                        f"stage {stage_id!r}.inputs",
                        "Merge requires exactly one consumer task",
                    )

        if result not in {1, 2}:
            _fail(f"stage {stage_id!r}.inputs", "task count exceeds the version-one bound")
        visiting.remove(stage_id)
        counts[stage_id] = result
        return result

    task_count(graph.root_stage)
    return counts


def stage_task_counts(snapshot: Snapshot) -> dict[str, int]:
    """Return validated bounded task counts for a snapshot StageGraph."""

    if snapshot.stage_graph is None:
        raise SnapshotError("snapshot.stage_graph: expected a StageGraph")
    return _infer_stage_task_counts(snapshot.stage_graph, "snapshot.stage_graph")


def _expression_columns(expression: Expr) -> frozenset[str]:
    columns = (
        frozenset((expression.column,))
        if expression.kind == "column" and expression.column is not None
        else frozenset()
    )
    for argument in expression.args:
        columns |= _expression_columns(argument)
    return columns


def _conjuncts(expression: Expr) -> tuple[Expr, ...]:
    if expression.kind != "and":
        return (expression,)
    return tuple(
        conjunct
        for argument in expression.args
        for conjunct in _conjuncts(argument)
    )


def _validate_positive_nullable_in_binding(
    expression: Expr,
    binding: str,
    path: str,
) -> None:
    """Require a nullable-column IN result to be one positive Filter conjunct."""

    found = False
    for conjunct in _conjuncts(expression):
        if binding not in _expression_columns(conjunct):
            continue
        if conjunct.kind != "column" or conjunct.column != binding:
            _fail(
                path,
                "a nullable-column IN binding must be a direct positive "
                "Filter conjunct",
            )
        found = True
    if not found:
        _fail(
            path,
            "a nullable-column IN binding is absent from its positive "
            "Filter conjuncts",
        )


def _direct_correlation(
    predicate: Expr,
    dependency: str,
    inner_schema: Mapping[str, Column],
    path: str,
    label: str,
    inner_source: str,
    *,
    allow_inequality: bool = False,
) -> tuple[str, str]:
    """Return the direct comparison kind and inner column for one dependency."""

    dependent_conjuncts = tuple(
        conjunct
        for conjunct in _conjuncts(predicate)
        if dependency in _expression_columns(conjunct)
    )
    if len(dependent_conjuncts) != 1:
        _fail(
            path,
            f"{label} requires exactly one dependency-bearing conjunct",
        )
    correlation = dependent_conjuncts[0]
    comparison = correlation
    comparison_kind = "eq"
    if (
        allow_inequality
        and correlation.kind == "not"
        and len(correlation.args) == 1
    ):
        comparison = correlation.args[0]
        comparison_kind = "ne"
    if (
        comparison.kind != "eq"
        or comparison.null_safe
        or len(comparison.args) != 2
        or any(argument.kind != "column" for argument in comparison.args)
    ):
        requirement = (
            "one direct non-null-safe column equality or inequality"
            if allow_inequality
            else "one non-null-safe column equality"
        )
        _fail(
            path,
            f"{label} requires {requirement}",
        )
    predicate_columns = tuple(
        argument.column for argument in comparison.args
    )
    if predicate_columns.count(dependency) != 1:
        _fail(
            path,
            f"{label} comparison must reference its outer dependency once",
        )
    inner_column = next(
        column for column in predicate_columns if column != dependency
    )
    if inner_column not in inner_schema:
        _fail(
            path,
            f"inner column {inner_column!r} is not produced by {inner_source}",
        )
    return comparison_kind, inner_column


def _direct_correlation_inner_column(
    predicate: Expr,
    dependency: str,
    inner_schema: Mapping[str, Column],
    path: str,
    label: str,
    inner_source: str,
) -> str:
    """Validate the shared strict outer/inner equality contract."""

    comparison_kind, inner_column = _direct_correlation(
        predicate,
        dependency,
        inner_schema,
        path,
        label,
        inner_source,
    )
    assert comparison_kind == "eq"
    return inner_column


def _node_expression_columns(node: PlanNode) -> frozenset[str]:
    if isinstance(node, Scan):
        expressions = tuple(
            expression
            for expression in (node.predicate, node.pushed_limit)
            if expression is not None
        )
    elif isinstance(node, Project):
        expressions = tuple(column.expression for column in node.columns)
    elif isinstance(node, Filter):
        expressions = (node.predicate,)
    elif isinstance(node, Limit):
        expressions = tuple(
            expression
            for expression in (node.count, node.offset)
            if expression is not None
        )
    elif isinstance(node, Sort):
        expressions = () if node.limit is None else (node.limit,)
    elif isinstance(node, Join):
        expressions = (node.predicate,)
    else:
        expressions = ()
    result = frozenset()
    for expression in expressions:
        result |= _expression_columns(expression)
    return result


def _is_exact_nested_subplan(owner: Subplan, nested: Subplan) -> bool:
    """The deliberately narrow closed nesting admitted by the v1 model."""

    return (
        isinstance(owner, InSubplan)
        and (
            isinstance(nested, InSubplan)
            or (
                isinstance(nested, ScalarSubplan)
                and nested.dependency is None
            )
        )
    )


def validate_snapshot(snapshot: Snapshot) -> dict[str, dict[str, Column]]:
    """Validate references and types, returning every node's output schema."""

    _unique([table.name for table in snapshot.tables], "snapshot.schema.tables")
    for table in snapshot.tables:
        _unique([column.name for column in table.columns], f"table {table.name!r}")
        if any(column.type == DOUBLE for column in table.columns):
            _fail(
                f"table {table.name!r}",
                "Double is modeled only as a derived passive carrier, not a base-table value",
            )
        table_columns = table.column_map()
        for key in table.unique_keys:
            for column in key.columns:
                if column not in table_columns:
                    _fail(f"table {table.name!r}", f"unique key references unknown column {column!r}")
    tables = snapshot.table_map()

    if not snapshot.plan.nodes:
        _fail("snapshot.plan.nodes", "must not be empty")
    _unique([node.id for node in snapshot.plan.nodes], "snapshot.plan.nodes")
    nodes = snapshot.plan.node_map()
    if snapshot.plan.root not in nodes:
        _fail("snapshot.plan.root", f"unknown node {snapshot.plan.root!r}")
    if not snapshot.plan.output:
        _fail("snapshot.plan.output", "must not be empty")
    _unique(snapshot.plan.output, "snapshot.plan.output")

    if snapshot.plan.subplans and snapshot.stage_graph is not None:
        _fail(
            "snapshot.plan.subplans",
            "subplans must be fully eliminated before StageGraph capture",
        )
    _unique(
        [subplan.binding for subplan in snapshot.plan.subplans],
        "snapshot.plan.subplans",
    )
    virtual_columns: dict[str, dict[str, Column]] = {}
    for index, subplan in enumerate(snapshot.plan.subplans):
        path = f"snapshot.plan.subplans[{index}]"
        if isinstance(subplan, ScalarSubplan) and subplan.output.type == DOUBLE:
            _fail(path, "subplans may not transport Double")
        if isinstance(subplan, InSubplan) and (
            subplan.lookup.type == DOUBLE or subplan.output.type == DOUBLE
        ):
            _fail(path, "subplans may not transport Double")
        if subplan.root not in nodes:
            _fail(f"{path}.root", f"unknown node {subplan.root!r}")
        if not subplan.consumers:
            _fail(f"{path}.consumers", "subplan binding is unused")
        _unique(subplan.consumers, f"{path}.consumers")

        if isinstance(subplan, ExistsSubplan):
            _unique(subplan.dependencies, f"{path}.dependencies")
            if len(subplan.dependencies) > 2:
                _fail(
                    f"{path}.dependencies",
                    "EXISTS supports at most two outer dependencies",
                )
            if bool(subplan.dependencies) != (subplan.predicate is not None):
                _fail(
                    f"{path}.predicate",
                    "must be present exactly when EXISTS is correlated",
                )
            if len(subplan.consumers) != 1:
                _fail(
                    f"{path}.consumers",
                    "an EXISTS binding must have exactly one Filter consumer",
                )
        elif isinstance(subplan, ScalarSubplan):
            if subplan.dependency is not None and len(subplan.consumers) != 1:
                _fail(
                    f"{path}.consumers",
                    "a correlated scalar binding must have exactly one consumer",
                )
        elif isinstance(subplan, InSubplan):
            if len(subplan.consumers) != 1:
                _fail(
                    f"{path}.consumers",
                    "an IN binding must have exactly one Filter consumer",
                )
        else:
            raise AssertionError(f"unknown subplan class {type(subplan).__name__}")

        for consumer_id in subplan.consumers:
            consumer = nodes.get(consumer_id)
            if consumer is None:
                _fail(
                    f"{path}.consumers",
                    f"references unknown node {consumer_id!r}",
                )
            allowed_consumer = (
                isinstance(consumer, (Project, Filter))
                if isinstance(subplan, ScalarSubplan)
                else isinstance(consumer, Filter)
            )
            if not allowed_consumer:
                _fail(
                    f"{path}.consumers",
                    (
                        "scalar subplan consumers must be Project or Filter nodes"
                        if isinstance(subplan, ScalarSubplan)
                        else (
                            "an EXISTS consumer must be a Filter node"
                            if isinstance(subplan, ExistsSubplan)
                            else "an IN consumer must be a Filter node"
                        )
                    ),
                )
            if subplan.binding not in _node_expression_columns(consumer):
                _fail(
                    f"{path}.consumers",
                    f"node {consumer_id!r} does not reference binding "
                    f"{subplan.binding!r}",
                )
            if (
                isinstance(subplan, InSubplan)
                and (subplan.lookup.nullable or subplan.output.nullable)
            ):
                assert isinstance(consumer, Filter)
                _validate_positive_nullable_in_binding(
                    consumer.predicate,
                    subplan.binding,
                    f"{path}.consumers",
                )
            value_type = (
                ValueType(subplan.output.type, True)
                if isinstance(subplan, ScalarSubplan)
                else ValueType(BOOL, False)
            )
            virtual_columns.setdefault(consumer_id, {})[subplan.binding] = Column(
                subplan.binding,
                value_type.name,
                value_type.nullable,
            )

    schemas: dict[str, dict[str, Column]] = {}
    visiting: set[str] = set()

    def schema_for(node_id: str) -> dict[str, Column]:
        if node_id in schemas:
            return schemas[node_id]
        if node_id in visiting:
            _fail("snapshot.plan.nodes", f"cycle through node {node_id!r}")
        node = nodes.get(node_id)
        if node is None:
            _fail("snapshot.plan.nodes", f"reference to unknown node {node_id!r}")
        visiting.add(node_id)

        if isinstance(node, EmptySource):
            result = {}

        elif isinstance(node, Scan):
            table = tables.get(node.table)
            if table is None:
                _fail(f"node {node.id!r}", f"unknown table {node.table!r}")
            table_columns = table.column_map()
            _unique([column.source for column in node.columns], f"node {node.id!r} scan sources")
            _unique([column.output for column in node.columns], f"node {node.id!r} output")
            result = {}
            for column in node.columns:
                source = table_columns.get(column.source)
                if source is None:
                    _fail(f"node {node.id!r}", f"unknown source column {column.source!r}")
                result[column.output] = Column(column.output, source.type, source.nullable)
            if node.predicate is not None:
                predicate_type = _infer_expr(
                    node.predicate,
                    result,
                    f"node {node.id!r}.predicate",
                )
                if predicate_type.name != BOOL:
                    _fail(
                        f"node {node.id!r}.predicate",
                        "scan predicate must be Boolean",
                    )

        elif isinstance(node, Project):
            input_schema = schema_for(node.input)
            expression_schema = dict(input_schema)
            for binding, column in virtual_columns.get(node.id, {}).items():
                if binding in input_schema:
                    _fail(
                        f"node {node.id!r}",
                        f"subplan binding {binding!r} collides with an "
                        "input column",
                    )
                expression_schema[binding] = column
            _unique([column.output for column in node.columns], f"node {node.id!r} output")
            result = {}
            for index, column in enumerate(node.columns):
                expression_path = f"node {node.id!r}.columns[{index}]"
                if column.expression.kind == "void":
                    value_type = ValueType(VOID, False)
                elif (
                    column.expression.kind == "column"
                    and column.expression.column in expression_schema
                    and expression_schema[column.expression.column].type == VOID
                ):
                    value_type = expression_schema[column.expression.column].value_type
                else:
                    value_type = _infer_expr(
                        column.expression,
                        expression_schema,
                        expression_path,
                    )
                result[column.output] = Column(column.output, value_type.name, value_type.nullable)

        elif isinstance(node, Filter):
            result = dict(schema_for(node.input))
            expression_schema = dict(result)
            for binding, column in virtual_columns.get(node.id, {}).items():
                if binding in result:
                    _fail(
                        f"node {node.id!r}",
                        f"subplan binding {binding!r} collides with an "
                        "input column",
                    )
                expression_schema[binding] = column
            predicate_type = _infer_expr(
                node.predicate,
                expression_schema,
                f"node {node.id!r}.predicate",
            )
            if predicate_type.name != BOOL:
                _fail(f"node {node.id!r}.predicate", "filter predicate must be Boolean")

        elif isinstance(node, OuterBind):
            if node.type == DOUBLE:
                _fail(f"node {node.id!r}", "outer_bind may not transport Double")
            result = dict(schema_for(node.input))
            if node.dependency in result:
                _fail(
                    f"node {node.id!r}.dependency",
                    f"outer dependency {node.dependency!r} collides with an inner column",
                )
            result[node.dependency] = Column(
                node.dependency,
                node.type,
                node.nullable,
            )

        elif isinstance(node, Limit):
            result = dict(schema_for(node.input))

        elif isinstance(node, Sort):
            result = dict(schema_for(node.input))
            for index, item in enumerate(node.order):
                if item.column not in result:
                    _fail(
                        f"node {node.id!r}.order[{index}]",
                        f"column {item.column!r} is not available",
                    )
                if not is_ordered_type(result[item.column].type):
                    _fail(
                        f"node {node.id!r}.order[{index}]",
                        f"ordering type {result[item.column].type!r} is unsupported; "
                        "modeled types are integers, String/Utf8, Date, and Decimal",
                    )

        elif isinstance(node, Aggregate):
            input_schema = schema_for(node.input)
            _unique(node.keys, f"node {node.id!r}.keys")
            for key in node.keys:
                if key not in input_schema:
                    _fail(f"node {node.id!r}.keys", f"column {key!r} is not available")
                if input_schema[key].type == DOUBLE:
                    _fail(f"node {node.id!r}.keys", "aggregate keys may not consume Double")
            if node.distinct_all and (
                not node.keys or len(node.aggregates) != len(node.keys)
            ):
                _fail(
                    f"node {node.id!r}",
                    "DistinctAll requires one distinct trait for each ordered key",
                )
            if sum(trait.distinct for trait in node.aggregates) > 1:
                _fail(
                    f"node {node.id!r}.aggregates",
                    "at most one direct distinct aggregate trait is modeled",
                )

            output_names = (() if node.distinct_all else node.keys) + tuple(
                trait.output for trait in node.aggregates
            )
            _unique(output_names, f"node {node.id!r} output")
            result = (
                {}
                if node.distinct_all
                else {key: input_schema[key] for key in node.keys}
            )
            for index, trait in enumerate(node.aggregates):
                trait_path = f"node {node.id!r}.aggregates[{index}]"
                input_column = input_schema.get(trait.input)
                if input_column is None:
                    _fail(trait_path, f"input column {trait.input!r} is not available")
                if input_column.type == DOUBLE:
                    _fail(trait_path, "aggregate inputs may not consume Double")
                if trait.output_type == DOUBLE:
                    _fail(trait_path, "aggregates may not produce Double")
                if input_column.type == VOID and (
                    node.distinct_all
                    or trait.input in node.keys
                    or trait.function != "count"
                    or trait.distinct
                    or trait.unwrap
                ):
                    _fail(
                        trait_path,
                        "void may only flow transparently to a canonical count aggregate",
                    )
                exact_scalar_uint64_unwrap = _is_exact_scalar_uint64_unwrap(
                    node,
                    trait,
                    input_column,
                )
                if trait.unwrap and not exact_scalar_uint64_unwrap:
                    _fail(
                        trait_path,
                        "unwrap is modeled only for a keyless final "
                        "sum(Optional<Uint64>) with a raw Optional<Uint64> output",
                    )
                exact_scalar_int64_count_distinct = (
                    _is_exact_scalar_int64_count_distinct(
                        node,
                        trait,
                        input_column,
                    )
                )
                if (
                    trait.distinct
                    and not node.distinct_all
                    and not exact_scalar_int64_count_distinct
                ):
                    _fail(
                        trait_path,
                        "direct distinct is modeled only for a keyless, "
                        "phase-undefined count of non-null Int64",
                    )

                if node.distinct_all:
                    if trait.function != "distinct":
                        _fail(trait_path, "DistinctAll traits must use distinct")
                    if (
                        trait.input != node.keys[index]
                        or trait.distinct
                        or trait.unwrap
                    ):
                        _fail(
                            trait_path,
                            "DistinctAll traits must be plain distinct aliases "
                            "of their corresponding ordered keys",
                        )
                    if (
                        trait.output_type != input_column.type
                        or trait.output_nullable != input_column.nullable
                    ):
                        _fail(
                            trait_path,
                            "DistinctAll output type and nullability must "
                            "match its input key",
                        )
                elif trait.function == "distinct":
                    _fail(trait_path, "distinct aggregate requires DistinctAll")
                elif trait.function == "count":
                    if trait.output_type != "Uint64" or trait.output_nullable:
                        _fail(trait_path, "count output must be non-nullable Uint64")
                elif trait.function == "sum":
                    expected_type = _sum_type(input_column.type)
                    if expected_type is None:
                        _fail(trait_path, f"sum does not support {input_column.type!r}")
                    if trait.output_type != expected_type:
                        _fail(
                            trait_path,
                            f"sum output type must be {expected_type!r}, got {trait.output_type!r}",
                        )
                    expected_nullable = input_column.nullable
                    if (
                        not node.keys
                        and node.phase != "intermediate"
                        and not _is_final_count_sum(node, nodes, trait.input)
                    ):
                        expected_nullable = True
                    if trait.output_nullable != expected_nullable:
                        _fail(
                            trait_path,
                            "sum output nullability does not match its input, phase, and keys",
                        )
                elif trait.function in {"max", "min"}:
                    function = trait.function
                    if not decimal.is_type(input_column.type):
                        _fail(
                            trait_path,
                            f"{function} does not support {input_column.type!r}; "
                            "only Decimal is modeled",
                        )
                    if trait.output_type != input_column.type:
                        _fail(
                            trait_path,
                            f"{function} output type must exactly match its Decimal input "
                            f"{input_column.type!r}, got {trait.output_type!r}",
                        )
                    expected_nullable = input_column.nullable
                    if not node.keys and node.phase != "intermediate":
                        expected_nullable = True
                    if trait.output_nullable != expected_nullable:
                        _fail(
                            trait_path,
                            f"{function} output nullability does not match its "
                            "input, phase, and keys",
                        )
                elif trait.function == "avg":
                    if not decimal.is_type(input_column.type):
                        _fail(
                            trait_path,
                            f"avg does not support {input_column.type!r}; "
                            "only Decimal is modeled",
                        )
                    if trait.output_type != input_column.type:
                        _fail(
                            trait_path,
                            "avg output type must exactly match its Decimal input "
                            f"{input_column.type!r}, got {trait.output_type!r}",
                        )
                    expected_state = AverageStateType(
                        sum_type=decimal.sum_type(input_column.type) or "",
                        count_type="Uint64",
                        nullable=input_column.nullable,
                    )
                    if trait.state != expected_state:
                        _fail(
                            f"{trait_path}.state",
                            "avg state must be the exact "
                            f"({expected_state.sum_type}, Uint64) accumulator "
                            f"with nullable={str(expected_state.nullable).lower()}",
                        )
                    expected_nullable = input_column.nullable
                    if not node.keys and node.phase != "intermediate":
                        expected_nullable = True
                    if trait.output_nullable != expected_nullable:
                        _fail(
                            trait_path,
                            "avg output nullability does not match its input, phase, and keys",
                        )

                result[trait.output] = Column(
                    trait.output,
                    trait.output_type,
                    False
                    if exact_scalar_uint64_unwrap
                    else trait.output_nullable,
                )

        elif isinstance(node, Join):
            left = schema_for(node.left)
            right = schema_for(node.right)
            overlap = left.keys() & right.keys()
            if overlap and node.kind not in {
                "left_semi",
                "left_anti",
                "right_semi",
                "right_anti",
            }:
                _fail(
                    f"node {node.id!r}",
                    "join inputs share columns outside a one-sided join: "
                    f"{', '.join(sorted(overlap))}",
                )
            if overlap and not (
                node.predicate.kind == "literal"
                and node.predicate.result_type == BOOL
                and node.predicate.nullable is False
                and node.predicate.value is True
            ):
                _fail(
                    f"node {node.id!r}.predicate",
                    "a one-sided join with shared input columns requires "
                    "a literal true residual predicate",
                )
            for index, key in enumerate(node.keys):
                key_path = f"node {node.id!r}.keys[{index}]"
                left_column = left.get(key.left)
                if left_column is None:
                    _fail(
                        f"{key_path}.left",
                        f"column {key.left!r} is not available from the left input",
                    )
                right_column = right.get(key.right)
                if right_column is None:
                    _fail(
                        f"{key_path}.right",
                        f"column {key.right!r} is not available from the right input",
                    )
                if left_column.type == DOUBLE or right_column.type == DOUBLE:
                    _fail(key_path, "join keys may not consume Double")
                if not equality_comparison_compatible(
                    left_column.type,
                    right_column.type,
                ):
                    _fail(
                        key_path,
                        "join key equality type mismatch: "
                        f"{left_column.type!r} and {right_column.type!r}",
                    )
            predicate_type = _infer_expr(
                node.predicate,
                left | right,
                f"node {node.id!r}.predicate",
            )
            if predicate_type.name != BOOL:
                _fail(f"node {node.id!r}.predicate", "join predicate must be Boolean")

            if node.kind in {"left_semi", "left_anti"}:
                result = dict(left)
            elif node.kind in {"right_semi", "right_anti"}:
                result = dict(right)
            elif node.kind == "left":
                result = left | _nullable_columns(right)
            elif node.kind == "right":
                result = _nullable_columns(left) | right
            elif node.kind in {"full", "exclusion"}:
                result = _nullable_columns(left) | _nullable_columns(right)
            else:
                result = left | right

        elif isinstance(node, UnionAll):
            _unique(node.output, f"node {node.id!r} output")
            if any(len(item.columns) != len(node.output) for item in node.inputs):
                _fail(f"node {node.id!r}", "every union input must match the output arity")
            input_schemas = [schema_for(item.node) for item in node.inputs]
            result = {}
            for position, output_name in enumerate(node.output):
                inputs: list[Column] = []
                for item, input_schema in zip(node.inputs, input_schemas):
                    input_name = item.columns[position]
                    if input_name not in input_schema:
                        _fail(f"node {node.id!r}", f"union column {input_name!r} is not available")
                    inputs.append(input_schema[input_name])
                if len({column.type for column in inputs}) != 1:
                    _fail(f"node {node.id!r}", f"union output {output_name!r} has mismatched types")
                result[output_name] = Column(
                    output_name,
                    inputs[0].type,
                    any(column.nullable for column in inputs),
                )

        else:
            raise AssertionError(f"unknown node class {type(node).__name__}")

        visiting.remove(node_id)
        schemas[node_id] = result
        return result

    root_schema = schema_for(snapshot.plan.root)
    subplan_schemas = {
        subplan.binding: schema_for(subplan.root)
        for subplan in snapshot.plan.subplans
    }
    unreachable = nodes.keys() - schemas.keys()
    if unreachable:
        _fail(
            "snapshot.plan.nodes",
            f"nodes are not reachable from the root: {', '.join(sorted(unreachable))}",
        )
    for column in snapshot.plan.output:
        if column not in root_schema:
            _fail("snapshot.plan.output", f"column {column!r} is not produced by the root")

    def descendants(root: str) -> frozenset[str]:
        reached: set[str] = set()
        pending = [root]
        while pending:
            node_id = pending.pop()
            if node_id in reached:
                continue
            reached.add(node_id)
            pending.extend(plan_node_inputs(nodes[node_id]))
        return frozenset(reached)

    main_nodes = descendants(snapshot.plan.root)
    subplan_nodes = {
        subplan.binding: descendants(subplan.root)
        for subplan in snapshot.plan.subplans
    }
    subplans_by_binding = {
        subplan.binding: subplan
        for subplan in snapshot.plan.subplans
    }
    subplan_roots = {subplan.root for subplan in snapshot.plan.subplans}
    all_bindings = frozenset(
        subplan.binding for subplan in snapshot.plan.subplans
    )
    nested_bindings_by_owner = {
        owner_binding: frozenset(
            nested_binding
            for node_id in owner_nodes
            for nested_binding in _node_expression_columns(nodes[node_id])
            if nested_binding in all_bindings
        )
        for owner_binding, owner_nodes in subplan_nodes.items()
    }
    outer_bind_owners: dict[str, list[str]] = {
        node.id: []
        for node in snapshot.plan.nodes
        if isinstance(node, OuterBind)
    }
    for node in snapshot.plan.nodes:
        leaked_bindings = all_bindings & schemas[node.id].keys()
        if leaked_bindings:
            _fail(
                f"node {node.id!r}",
                "subplan bindings must remain virtual and may not appear "
                "in relational output: "
                + ", ".join(sorted(leaked_bindings)),
            )
    for index, subplan in enumerate(snapshot.plan.subplans):
        path = f"snapshot.plan.subplans[{index}]"
        schema = subplan_schemas[subplan.binding]
        if isinstance(subplan, ExistsSubplan) and any(
            isinstance(nodes[node_id], Limit)
            and nodes[node_id].ensure_at_most_one
            for node_id in subplan_nodes[subplan.binding]
        ):
            _fail(
                f"{path}.root",
                "EXISTS roots with observable error outcomes are not modeled",
            )
        if isinstance(subplan, InSubplan) and any(
            isinstance(nodes[node_id], Limit)
            and nodes[node_id].ensure_at_most_one
            for node_id in subplan_nodes[subplan.binding]
        ):
            _fail(
                f"{path}.root",
                "IN roots with observable error outcomes are not modeled",
            )
        if (
            isinstance(subplan, ExistsSubplan)
            and subplan.predicate is not None
            and any(
                isinstance(nodes[node_id], Limit)
                or (
                    isinstance(nodes[node_id], Sort)
                    and nodes[node_id].limit is not None
                )
                or (
                    isinstance(nodes[node_id], Scan)
                    and nodes[node_id].pushed_limit is not None
                )
                for node_id in subplan_nodes[subplan.binding]
            )
        ):
            _fail(
                f"{path}.root",
                "correlated EXISTS roots with per-invocation row selection are not modeled",
            )
        if isinstance(subplan, ScalarSubplan):
            actual = schema.get(subplan.output.column)
            if actual is None:
                _fail(
                    f"{path}.output",
                    "declared scalar output column is not produced by its root",
                )
            expected = Column(
                subplan.output.column,
                subplan.output.type,
                subplan.output.nullable,
            )
            if actual != expected:
                _fail(
                    f"{path}.output",
                    "declared scalar output schema does not match its root",
                )
            outer_binds = tuple(
                nodes[node_id]
                for node_id in subplan_nodes[subplan.binding]
                if isinstance(nodes[node_id], OuterBind)
            )
            if subplan.dependency is None:
                if outer_binds:
                    _fail(
                        f"{path}.root",
                        "an uncorrelated scalar root may not contain outer_bind",
                    )
            else:
                if len(outer_binds) != 1:
                    _fail(
                        f"{path}.root",
                        "a correlated scalar root must contain exactly one outer_bind",
                    )
                outer_bind = outer_binds[0]
                assert isinstance(outer_bind, OuterBind)
                outer_bind_owners[outer_bind.id].append(subplan.binding)
                if outer_bind.id in main_nodes:
                    _fail(
                        f"node {outer_bind.id!r}",
                        "outer_bind may not be reachable from the main plan",
                    )
                if outer_bind.dependency != subplan.dependency:
                    _fail(
                        f"{path}.dependencies",
                        "scalar dependency disagrees with outer_bind",
                    )

                correlated_nodes = subplan_nodes[subplan.binding]
                parents: dict[str, list[str]] = {
                    node_id: [] for node_id in correlated_nodes
                }
                for parent_id in correlated_nodes:
                    for child_id in plan_node_inputs(nodes[parent_id]):
                        if child_id in parents:
                            parents[child_id].append(parent_id)

                shape = nodes[subplan.root]
                aggregate_count = 0
                while isinstance(shape, (Project, Aggregate)):
                    if (
                        shape.id != subplan.root
                        and len(parents[shape.id]) != 1
                    ):
                        _fail(
                            f"{path}.root",
                            "the correlated scalar root path must not fan out",
                        )
                    if isinstance(shape, Aggregate):
                        aggregate_count += 1
                        if (
                            shape.keys
                            or shape.phase != "undefined"
                            or shape.distinct_all
                        ):
                            _fail(
                                f"{path}.root",
                                "the correlated scalar Aggregate must be "
                                "ungrouped, undefined, and non-DistinctAll",
                            )
                    shape = nodes[shape.input]
                if aggregate_count != 1:
                    _fail(
                        f"{path}.root",
                        "a correlated scalar root path must contain exactly "
                        "one Aggregate among Project wrappers",
                    )
                if (
                    not isinstance(shape, Filter)
                    or shape.input != outer_bind.id
                ):
                    _fail(
                        f"{path}.root",
                        "the correlated scalar unary path must end in Filter "
                        "over outer_bind",
                    )
                correlation_filter = shape
                if (
                    len(parents[correlation_filter.id]) != 1
                    or len(parents[outer_bind.id]) != 1
                ):
                    _fail(
                        f"{path}.root",
                        "the correlated scalar Filter and outer_bind must not fan out",
                    )
                if any(
                    isinstance(nodes[node_id], (Limit, Sort))
                    or (
                        isinstance(nodes[node_id], Scan)
                        and nodes[node_id].pushed_limit is not None
                    )
                    or (
                        isinstance(nodes[node_id], UnionAll)
                        and nodes[node_id].ordered
                    )
                    for node_id in subplan_nodes[subplan.binding]
                ):
                    _fail(
                        f"{path}.root",
                        "correlated scalar roots with per-invocation row "
                        "selection are not modeled",
                    )

                consumer = nodes[subplan.consumers[0]]
                assert isinstance(consumer, (Project, Filter))
                outer_schema = schemas[consumer.input]
                dependency = subplan.dependency
                outer_column = outer_schema.get(dependency)
                if outer_column is None:
                    _fail(
                        f"{path}.dependencies",
                        f"outer column {dependency!r} is not available to the consumer",
                    )
                declared_dependency = Column(
                    dependency,
                    outer_bind.type,
                    outer_bind.nullable,
                )
                if outer_column != declared_dependency:
                    _fail(
                        f"{path}.dependencies",
                        "outer_bind type or nullability disagrees with its consumer input",
                    )

                inner_schema = schemas[outer_bind.input]
                predicate = correlation_filter.predicate
                inner_column = _direct_correlation_inner_column(
                    predicate,
                    dependency,
                    inner_schema,
                    f"node {correlation_filter.id!r}.predicate",
                    "correlated scalar",
                    "the outer_bind input",
                )
                if outer_column.type != inner_schema[inner_column].type:
                    _fail(
                        f"node {correlation_filter.id!r}.predicate",
                        "correlated scalar equality column types must match exactly",
                    )

                for node_id in correlated_nodes:
                    candidate = nodes[node_id]
                    if candidate.id == correlation_filter.id:
                        continue
                    if isinstance(candidate, Aggregate):
                        if dependency in candidate.keys:
                            _fail(
                                f"node {candidate.id!r}.keys",
                                "a correlated scalar may not aggregate by "
                                "its outer dependency",
                            )
                        if any(
                            trait.input == dependency
                            for trait in candidate.aggregates
                        ):
                            _fail(
                                f"node {candidate.id!r}.aggregates",
                                "a correlated scalar may not aggregate "
                                "its outer dependency",
                            )
                    if isinstance(candidate, Project):
                        invalid_use = any(
                            dependency in _expression_columns(column.expression)
                            and not (
                                column.output == dependency
                                and column.expression.kind == "column"
                                and column.expression.column == dependency
                            )
                            for column in candidate.columns
                        )
                    else:
                        invalid_use = (
                            dependency
                            in _node_expression_columns(candidate)
                        )
                    if invalid_use:
                        _fail(
                            f"node {candidate.id!r}",
                            "a correlated scalar may use its outer dependency "
                            "only in the correlation Filter; exact Project "
                            "pass-through is allowed",
                        )
        elif isinstance(subplan, ExistsSubplan):
            if subplan.predicate is not None:
                assert subplan.dependencies
                consumer = nodes[subplan.consumers[0]]
                assert isinstance(consumer, Filter)
                outer_schema = schemas[consumer.input]
                predicate = subplan.predicate
                predicate_schema = dict(schema)
                correlations: list[tuple[str, str]] = []
                two_dependencies = len(subplan.dependencies) == 2
                if two_dependencies:
                    for conjunct in _conjuncts(predicate):
                        columns = _expression_columns(conjunct)
                        if sum(
                            dependency in columns
                            for dependency in subplan.dependencies
                        ) > 1:
                            _fail(
                                f"{path}.predicate",
                                "two-dependency EXISTS requires each outer "
                                "dependency in a separate conjunct",
                            )
                for dependency in subplan.dependencies:
                    outer_column = outer_schema.get(dependency)
                    if outer_column is None:
                        _fail(
                            f"{path}.dependencies",
                            f"outer column {dependency!r} is not available to the consumer",
                        )
                    if dependency in schema:
                        _fail(
                            f"{path}.dependencies",
                            f"outer column {dependency!r} collides with an inner column",
                        )
                    comparison_kind, inner_column = _direct_correlation(
                        predicate,
                        dependency,
                        schema,
                        f"{path}.predicate",
                        "correlated EXISTS",
                        "the subplan root",
                        allow_inequality=two_dependencies,
                    )
                    inner_column_type = schema[inner_column]
                    if outer_column.type != inner_column_type.type:
                        _fail(
                            f"{path}.predicate",
                            "EXISTS correlation column types must match exactly",
                        )
                    predicate_schema[dependency] = outer_column
                    correlations.append((comparison_kind, inner_column))
                if two_dependencies:
                    if {kind for kind, _inner in correlations} != {"eq", "ne"}:
                        _fail(
                            f"{path}.predicate",
                            "two-dependency EXISTS requires exactly one direct "
                            "equality and one direct inequality",
                        )
                    if len({inner for _kind, inner in correlations}) != 2:
                        _fail(
                            f"{path}.predicate",
                            "two-dependency EXISTS correlations must reference "
                            "distinct inner columns",
                        )
                predicate_type = _infer_expr(
                    predicate,
                    predicate_schema,
                    f"{path}.predicate",
                )
                if predicate_type.name != BOOL:
                    _fail(f"{path}.predicate", "EXISTS predicate must be Boolean")
        else:
            assert isinstance(subplan, InSubplan)
            actual_output = schema.get(subplan.output.column)
            expected_output = Column(
                subplan.output.column,
                subplan.output.type,
                subplan.output.nullable,
            )
            if actual_output is None:
                _fail(
                    f"{path}.output",
                    "declared IN output column is not produced by its root",
                )
            if actual_output != expected_output:
                _fail(
                    f"{path}.output",
                    "declared IN output schema does not match its root",
                )
            if any(
                isinstance(nodes[node_id], OuterBind)
                for node_id in subplan_nodes[subplan.binding]
            ):
                _fail(
                    f"{path}.root",
                    "an uncorrelated IN root may not contain outer_bind",
                )

            consumer = nodes[subplan.consumers[0]]
            assert isinstance(consumer, Filter)
            outer_schema = schemas[consumer.input]
            actual_lookup = outer_schema.get(subplan.lookup.column)
            expected_lookup = Column(
                subplan.lookup.column,
                subplan.lookup.type,
                subplan.lookup.nullable,
            )
            if actual_lookup is None:
                _fail(
                    f"{path}.lookup",
                    "declared IN lookup column is not available to the consumer",
                )
            if actual_lookup != expected_lookup:
                _fail(
                    f"{path}.lookup",
                    "declared IN lookup schema does not match the consumer input",
                )
        if subplan.root in main_nodes:
            _fail(
                f"{path}.root",
                "subplan root is nested in the main relational plan",
            )
        nested_roots = (
            subplan_nodes[subplan.binding] & subplan_roots
        ) - {subplan.root}
        if nested_roots:
            _fail(
                f"{path}.root",
                "nested subplans are not modeled",
            )
        for nested_binding in sorted(
            nested_bindings_by_owner[subplan.binding]
        ):
            nested = subplans_by_binding[nested_binding]
            if not _is_exact_nested_subplan(subplan, nested):
                _fail(
                    f"{path}.root",
                    "only an uncorrelated scalar or a one-level closed IN "
                    "binding may be consumed inside an IN subplan",
                )
            if (
                isinstance(nested, InSubplan)
                and nested_bindings_by_owner[nested.binding]
            ):
                _fail(
                    f"{path}.root",
                    "a nested IN binding must be closed and may not consume "
                    "another subplan binding",
                )
        if (
            isinstance(subplan, ExistsSubplan)
            and subplan.predicate is not None
            and _expression_columns(subplan.predicate) & all_bindings
        ):
            _fail(
                f"{path}.predicate",
                "EXISTS predicate may not reference subplan bindings",
            )
        for consumer_id in subplan.consumers:
            owner_bindings = tuple(
                owner_binding
                for owner_binding, owner_nodes in subplan_nodes.items()
                if consumer_id in owner_nodes
            )
            context_count = (
                int(consumer_id in main_nodes) + len(owner_bindings)
            )
            if context_count != 1:
                _fail(
                    f"{path}.consumers",
                    "a subplan consumer must belong to exactly one plan root",
                )
            if owner_bindings:
                owner = subplans_by_binding[owner_bindings[0]]
                if not _is_exact_nested_subplan(owner, subplan):
                    _fail(
                        f"{path}.consumers",
                        "only an uncorrelated scalar or a one-level closed "
                        "IN binding may be consumed inside an IN subplan",
                    )
    for node_id, owners in outer_bind_owners.items():
        if len(owners) != 1:
            _fail(
                f"node {node_id!r}",
                "outer_bind must belong to exactly one correlated scalar root",
            )
    _validate_average_state_dataflow(snapshot)
    _validate_void_dataflow(snapshot, schemas)
    _validate_stage_graph(snapshot, schemas)
    return schemas
