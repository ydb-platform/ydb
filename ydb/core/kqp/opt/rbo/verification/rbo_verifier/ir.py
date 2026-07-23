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
class Limit:
    id: str
    input: str
    count: Expr
    offset: Expr | None
    phase: str


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
class Join:
    id: str
    left: str
    right: str
    kind: str
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
    EmptySource | Scan | Project | Filter | Limit | Sort | Aggregate | Join | UnionAll
)


@dataclass(frozen=True, slots=True)
class Plan:
    nodes: tuple[PlanNode, ...]
    root: str
    output: tuple[str, ...]

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

    if kind in {"cast_decimal", "cast_integral"}:
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

    if kind == "opaque":
        _keys(obj, {"kind", "fingerprint", "type", "nullable", "args"}, path)
        raw_args = _array(obj["args"], f"{path}.args")
        return Expr(
            kind=kind,
            args=tuple(
                parse_child(arg, f"{path}.args[{index}]")
                for index, arg in enumerate(raw_args)
            ),
            result_type=_scalar_type(obj["type"], f"{path}.type"),
            nullable=_bool(obj["nullable"], f"{path}.nullable"),
            fingerprint=_string(obj["fingerprint"], f"{path}.fingerprint"),
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
        columns.append(
            Column(
                name=_string(column["name"], f"{column_path}.name"),
                type=_scalar_type(column["type"], f"{column_path}.type"),
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

    if operation == "limit":
        _keys(obj, {"id", "op", "input", "count", "offset", "phase"}, path)
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
        _keys(obj, {"id", "op", "left", "right", "kind", "predicate"}, path)
        kind = _string(obj["kind"], f"{path}.kind")
        if kind not in JOIN_KINDS:
            _fail(f"{path}.kind", f"unsupported join kind {kind!r}")
        return Join(
            node_id,
            _string(obj["left"], f"{path}.left"),
            _string(obj["right"], f"{path}.right"),
            kind,
            _parse_expr(obj["predicate"], f"{path}.predicate"),
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
    _keys(raw_plan, {"nodes", "root", "output"}, "snapshot.plan")
    nodes = tuple(
        _parse_node(node, f"snapshot.plan.nodes[{index}]")
        for index, node in enumerate(_array(raw_plan["nodes"], "snapshot.plan.nodes"))
    )
    output = tuple(
        _string(column, f"snapshot.plan.output[{index}]")
        for index, column in enumerate(_array(raw_plan["output"], "snapshot.plan.output"))
    )
    snapshot = Snapshot(
        tables=tables,
        plan=Plan(nodes=nodes, root=_string(raw_plan["root"], "snapshot.plan.root"), output=output),
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
            if expr.kind == "div":
                _fail(path, "div requires a Decimal result")
            if family(expr.result_type) != "int":
                _fail(path, f"{expr.kind} requires an integer result")
            if left.name != expr.result_type or right.name != expr.result_type:
                _fail(
                    path,
                    f"{expr.kind} operands and result must have exactly the same type: "
                    f"{left.name!r}, {right.name!r}, and {expr.result_type!r}",
                )
        nullable = left.nullable or right.nullable
        if expr.nullable != nullable:
            _fail(
                path,
                f"{expr.kind} nullability must equal the OR of operand nullability",
            )
        return ValueType(expr.result_type, nullable)

    if expr.kind == "cast_decimal":
        assert expr.result_type is not None and expr.nullable is not None
        argument = _infer_expr(expr.args[0], columns, f"{path}.arg", bindings)
        if family(argument.name) != "int":
            _fail(path, "Decimal cast source must be integral")
        if argument.nullable:
            _fail(path, "Decimal cast source must be non-nullable")
        result = decimal.parse_type(expr.result_type)
        if result is None:
            _fail(path, "Decimal cast result must be a canonical Decimal type")
        if result.integral_digits < 1:
            _fail(path, "Decimal cast result must have at least one integral digit")
        if expr.nullable:
            _fail(path, "exact integral Decimal cast must be non-nullable")
        return ValueType(expr.result_type, False)

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


def plan_node_inputs(node: PlanNode) -> tuple[str, ...]:
    if isinstance(node, (EmptySource, Scan)):
        return ()
    if isinstance(node, (Project, Filter, Limit, Sort, Aggregate)):
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

        if isinstance(node, (Filter, Limit, Sort)):
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
            dropped = set()
            if node.kind in {"left_semi", "left_anti"}:
                dropped = _void_columns(schemas[node.right])
            elif node.kind in {"right_semi", "right_anti"}:
                dropped = _void_columns(schemas[node.left])
            if dropped:
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


def validate_snapshot(snapshot: Snapshot) -> dict[str, dict[str, Column]]:
    """Validate references and types, returning every node's output schema."""

    _unique([table.name for table in snapshot.tables], "snapshot.schema.tables")
    for table in snapshot.tables:
        _unique([column.name for column in table.columns], f"table {table.name!r}")
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
            _unique([column.output for column in node.columns], f"node {node.id!r} output")
            result = {}
            for index, column in enumerate(node.columns):
                expression_path = f"node {node.id!r}.columns[{index}]"
                if column.expression.kind == "void":
                    value_type = ValueType(VOID, False)
                elif (
                    column.expression.kind == "column"
                    and column.expression.column in input_schema
                    and input_schema[column.expression.column].type == VOID
                ):
                    value_type = input_schema[column.expression.column].value_type
                else:
                    value_type = _infer_expr(column.expression, input_schema, expression_path)
                result[column.output] = Column(column.output, value_type.name, value_type.nullable)

        elif isinstance(node, Filter):
            result = dict(schema_for(node.input))
            predicate_type = _infer_expr(node.predicate, result, f"node {node.id!r}.predicate")
            if predicate_type.name != BOOL:
                _fail(f"node {node.id!r}.predicate", "filter predicate must be Boolean")

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

                if trait.function == "count":
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
                elif trait.function == "max":
                    if not decimal.is_type(input_column.type):
                        _fail(
                            trait_path,
                            f"max does not support {input_column.type!r}; only Decimal is modeled",
                        )
                    if trait.output_type != input_column.type:
                        _fail(
                            trait_path,
                            "max output type must exactly match its Decimal input "
                            f"{input_column.type!r}, got {trait.output_type!r}",
                        )
                    expected_nullable = input_column.nullable
                    if not node.keys and node.phase != "intermediate":
                        expected_nullable = True
                    if trait.output_nullable != expected_nullable:
                        _fail(
                            trait_path,
                            "max output nullability does not match its input, phase, and keys",
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
                    trait.output_nullable,
                )

        elif isinstance(node, Join):
            left = schema_for(node.left)
            right = schema_for(node.right)
            overlap = left.keys() & right.keys()
            if overlap:
                _fail(f"node {node.id!r}", f"join inputs share columns: {', '.join(sorted(overlap))}")
            predicate_type = _infer_expr(node.predicate, left | right, f"node {node.id!r}.predicate")
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
    unreachable = nodes.keys() - schemas.keys()
    if unreachable:
        _fail(
            "snapshot.plan.nodes",
            f"nodes are not reachable from the root: {', '.join(sorted(unreachable))}",
        )
    for column in snapshot.plan.output:
        if column not in root_schema:
            _fail("snapshot.plan.output", f"column {column!r} is not produced by the root")
    _validate_average_state_dataflow(snapshot)
    _validate_void_dataflow(snapshot, schemas)
    _validate_stage_graph(snapshot, schemas)
    return schemas
