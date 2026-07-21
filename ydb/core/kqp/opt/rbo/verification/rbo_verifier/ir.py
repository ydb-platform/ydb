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
STAGE_CONNECTION_KINDS = frozenset({"map", "broadcast", "hash_shuffle", "union_all", "merge"})
HASH_FUNCTIONS = frozenset({"HashV1", "HashV2"})
AGGREGATE_PHASES = frozenset({"undefined", "intermediate", "final"})


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
class AggregateTrait:
    input: str
    function: str
    output: str
    output_type: str
    output_nullable: bool
    distinct: bool
    unwrap: bool


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


PlanNode: TypeAlias = EmptySource | Scan | Project | Filter | Aggregate | Join | UnionAll


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
class MergeOrder:
    column: str
    ascending: bool
    nulls_first: bool


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
    order: tuple[MergeOrder, ...] = ()


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
            _keys(
                trait,
                {"input", "function", "output", "type", "nullable", "distinct", "unwrap"},
                trait_path,
            )
            aggregates.append(
                AggregateTrait(
                    input=_string(trait["input"], f"{trait_path}.input"),
                    function=_string(trait["function"], f"{trait_path}.function"),
                    output=_string(trait["output"], f"{trait_path}.output"),
                    output_type=_scalar_type(trait["type"], f"{trait_path}.type"),
                    output_nullable=_bool(trait["nullable"], f"{trait_path}.nullable"),
                    distinct=_bool(trait["distinct"], f"{trait_path}.distinct"),
                    unwrap=_bool(trait["unwrap"], f"{trait_path}.unwrap"),
                )
            )
        if not aggregates:
            _fail(f"{path}.aggregates", "must not be empty")
        phase = _string(obj["phase"], f"{path}.phase")
        if phase not in AGGREGATE_PHASES:
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
        if len(inputs) != 2:
            _fail(f"{path}.inputs", "requires exactly two inputs")
        output = tuple(
            _string(column, f"{path}.output[{index}]")
            for index, column in enumerate(_array(obj["output"], f"{path}.output"))
        )
        if not output:
            _fail(f"{path}.output", "must not be empty")
        return UnionAll(node_id, tuple(inputs), output)

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
        order: list[MergeOrder] = []
        for index, raw_order in enumerate(_array(obj["order"], f"{path}.order")):
            order_path = f"{path}.order[{index}]"
            item = _object(raw_order, order_path)
            _keys(item, {"column", "ascending", "nulls_first"}, order_path)
            order.append(
                MergeOrder(
                    _string(item["column"], f"{order_path}.column"),
                    _bool(item["ascending"], f"{order_path}.ascending"),
                    _bool(item["nulls_first"], f"{order_path}.nulls_first"),
                )
            )
        if not order:
            _fail(f"{path}.order", "must not be empty")
        return StageEdge(**fields, order=tuple(order))
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


def _sum_type(input_type: str) -> str | None:
    if input_type in {"Int8", "Int16", "Int32", "Int64"}:
        return "Int64"
    if input_type in {"Uint8", "Uint16", "Uint32", "Uint64"}:
        return "Uint64"
    return None


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
    if isinstance(node, (Project, Filter, Aggregate)):
        return (node.input,)
    if isinstance(node, Join):
        return (node.left, node.right)
    if isinstance(node, UnionAll):
        return tuple(item.node for item in node.inputs)
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
        for item in edge.order:
            if item.column not in columns:
                _fail(f"{edge_path}.order", f"column {item.column!r} is not produced")

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
    _validate_stage_graph(snapshot, schemas)
    return schemas
