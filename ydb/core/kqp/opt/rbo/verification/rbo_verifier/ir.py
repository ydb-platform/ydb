"""Strict version-one semantic snapshot IR.

This module intentionally contains no optimizer or solver logic.  A snapshot is
accepted only when every field is understood and every expression is well typed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Mapping, Sequence, TypeAlias

from .types import BOOL, SCALAR_TYPES, family


FORMAT = "ydb-rbo-semantic-snapshot"
VERSION = 1
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
    value: bool | int | str | None = None
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


@dataclass(frozen=True, slots=True)
class Filter:
    id: str
    input: str
    predicate: Expr


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


PlanNode: TypeAlias = EmptySource | Scan | Project | Filter | Join | UnionAll


@dataclass(frozen=True, slots=True)
class Plan:
    nodes: tuple[PlanNode, ...]
    root: str
    output: tuple[str, ...]

    def node_map(self) -> dict[str, PlanNode]:
        return {node.id: node for node in self.nodes}


@dataclass(frozen=True, slots=True)
class Snapshot:
    tables: tuple[Table, ...]
    plan: Plan
    # Version one is deliberately logical-only.  The required JSON field makes
    # accidental omission of the final StageGraph visible.
    stage_graph: None = None

    def table_map(self) -> dict[str, Table]:
        return {table.name: table for table in self.tables}

    def output_schema(self) -> tuple[Column, ...]:
        schemas = validate_snapshot(self)
        root_schema = schemas[self.plan.root]
        return tuple(root_schema[name] for name in self.plan.output)


def _fail(path: str, message: str) -> None:
    raise SnapshotError(f"{path}: {message}")


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
    if result not in SCALAR_TYPES:
        _fail(path, f"unsupported scalar type {result!r}")
    return result


def _literal(value: Any, scalar_type: str, path: str) -> bool | int | str:
    scalar_family = family(scalar_type)
    valid = (
        (scalar_family == "bool" and isinstance(value, bool))
        or (scalar_family == "int" and isinstance(value, int) and not isinstance(value, bool))
        or (scalar_family == "string" and isinstance(value, str))
    )
    if not valid:
        _fail(path, f"value does not have type {scalar_type!r}")
    return value


def _parse_expr(value: Any, path: str) -> Expr:
    obj = _object(value, path)
    kind = _string(obj.get("kind"), f"{path}.kind")

    if kind == "column":
        _keys(obj, {"kind", "column"}, path)
        return Expr(kind=kind, column=_string(obj["column"], f"{path}.column"))

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
            args=tuple(_parse_expr(arg, f"{path}.args[{index}]") for index, arg in enumerate(raw_args)),
        )

    if kind == "not":
        _keys(obj, {"kind", "arg"}, path)
        return Expr(kind=kind, args=(_parse_expr(obj["arg"], f"{path}.arg"),))

    if kind == "eq":
        _keys(obj, {"kind", "left", "right"}, path, {"null_safe"})
        return Expr(
            kind=kind,
            args=(
                _parse_expr(obj["left"], f"{path}.left"),
                _parse_expr(obj["right"], f"{path}.right"),
            ),
            null_safe=_bool(obj.get("null_safe", False), f"{path}.null_safe"),
        )

    if kind == "opaque":
        _keys(obj, {"kind", "fingerprint", "type", "nullable", "args"}, path)
        raw_args = _array(obj["args"], f"{path}.args")
        return Expr(
            kind=kind,
            args=tuple(_parse_expr(arg, f"{path}.args[{index}]") for index, arg in enumerate(raw_args)),
            result_type=_scalar_type(obj["type"], f"{path}.type"),
            nullable=_bool(obj["nullable"], f"{path}.nullable"),
            fingerprint=_string(obj["fingerprint"], f"{path}.fingerprint"),
        )

    _fail(f"{path}.kind", f"unsupported expression kind {kind!r}")


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
        _keys(obj, {"id", "op", "table", "columns"}, path)
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
        return Scan(node_id, _string(obj["table"], f"{path}.table"), tuple(columns))

    if operation == "project":
        _keys(obj, {"id", "op", "input", "columns"}, path)
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
        return Project(node_id, _string(obj["input"], f"{path}.input"), tuple(columns))

    if operation == "filter":
        _keys(obj, {"id", "op", "input", "predicate"}, path)
        return Filter(
            node_id,
            _string(obj["input"], f"{path}.input"),
            _parse_expr(obj["predicate"], f"{path}.predicate"),
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
        _keys(obj, {"id", "op", "inputs", "output"}, path)
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
        if len(inputs) < 2:
            _fail(f"{path}.inputs", "requires at least two inputs")
        output = tuple(
            _string(column, f"{path}.output[{index}]")
            for index, column in enumerate(_array(obj["output"], f"{path}.output"))
        )
        if not output:
            _fail(f"{path}.output", "must not be empty")
        return UnionAll(node_id, tuple(inputs), output)

    _fail(f"{path}.op", f"unsupported operator {operation!r}")


def parse_snapshot(value: Any) -> Snapshot:
    obj = _object(value, "snapshot")
    _keys(obj, {"format", "version", "schema", "plan", "stage_graph"}, "snapshot")
    if obj["format"] != FORMAT:
        _fail("snapshot.format", f"expected {FORMAT!r}")
    if type(obj["version"]) is not int or obj["version"] != VERSION:
        _fail("snapshot.version", f"expected version {VERSION}")
    if obj["stage_graph"] is not None:
        _fail("snapshot.stage_graph", "StageGraph is not implemented in version one")

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
    )
    validate_snapshot(snapshot)
    return snapshot


def load_snapshot(path: str | Path) -> Snapshot:
    try:
        with Path(path).open("r", encoding="utf-8") as stream:
            return parse_snapshot(json.load(stream))
    except json.JSONDecodeError as error:
        raise SnapshotError(f"{path}: invalid JSON: {error}") from error


def _unique(values: Sequence[str], path: str) -> None:
    seen: set[str] = set()
    for value in values:
        if value in seen:
            _fail(path, f"duplicate name {value!r}")
        seen.add(value)


def _infer_expr(expr: Expr, columns: Mapping[str, Column], path: str) -> ValueType:
    if expr.kind == "column":
        if expr.column not in columns:
            _fail(path, f"column {expr.column!r} is not available")
        return columns[expr.column].value_type

    if expr.kind in {"literal", "null", "opaque"}:
        assert expr.result_type is not None and expr.nullable is not None
        if expr.kind == "opaque":
            for index, arg in enumerate(expr.args):
                _infer_expr(arg, columns, f"{path}.args[{index}]")
        return ValueType(expr.result_type, expr.nullable)

    if expr.kind in {"and", "or", "not"}:
        argument_types = [
            _infer_expr(arg, columns, f"{path}.args[{index}]") for index, arg in enumerate(expr.args)
        ]
        if any(argument.name != BOOL for argument in argument_types):
            _fail(path, f"{expr.kind} requires Boolean arguments")
        return ValueType(BOOL, any(argument.nullable for argument in argument_types))

    if expr.kind == "eq":
        left = _infer_expr(expr.args[0], columns, f"{path}.left")
        right = _infer_expr(expr.args[1], columns, f"{path}.right")
        if left.name != right.name:
            _fail(path, f"equality type mismatch: {left.name!r} and {right.name!r}")
        return ValueType(BOOL, False if expr.null_safe else left.nullable or right.nullable)

    raise AssertionError(f"parser admitted unknown expression kind {expr.kind!r}")


def _nullable_columns(columns: Mapping[str, Column]) -> dict[str, Column]:
    return {name: replace(column, nullable=True) for name, column in columns.items()}


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

        elif isinstance(node, Project):
            input_schema = schema_for(node.input)
            _unique([column.output for column in node.columns], f"node {node.id!r} output")
            result = {}
            for index, column in enumerate(node.columns):
                value_type = _infer_expr(column.expression, input_schema, f"node {node.id!r}.columns[{index}]")
                result[column.output] = Column(column.output, value_type.name, value_type.nullable)

        elif isinstance(node, Filter):
            result = dict(schema_for(node.input))
            predicate_type = _infer_expr(node.predicate, result, f"node {node.id!r}.predicate")
            if predicate_type.name != BOOL:
                _fail(f"node {node.id!r}.predicate", "filter predicate must be Boolean")

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
    return schemas
