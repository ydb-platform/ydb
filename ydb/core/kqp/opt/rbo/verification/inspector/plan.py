"""Small, deterministic text rendering for a semantic snapshot.

The renderer has no optimizer or solver behavior.  It prints every modeled
field and deliberately fails closed when the strict version-one IR grows a new
expression, operator, or StageGraph connection.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable
from typing import TypeVar

from ..rbo_verifier import decimal, ir


T = TypeVar("T")


class InspectionError(ValueError):
    """The in-memory snapshot contains a variant this renderer cannot show."""


def snapshot_digest(snapshot: ir.Snapshot) -> str:
    """Digest the complete normalized semantic rendering used for inspection."""

    return hashlib.sha256(render_snapshot(snapshot).encode("utf-8")).hexdigest()


def _quote(value: str) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"))


def _boolean(value: bool) -> str:
    return "true" if value else "false"


def _optional(value: T | None, render: Callable[[T], str] = str) -> str:
    return "none" if value is None else render(value)


def _list(values: Iterable[T], render: Callable[[T], str] = str) -> str:
    return "[" + ", ".join(render(value) for value in values) + "]"


def _required(value: T | None, field: str) -> T:
    if value is None:
        raise InspectionError(f"expression field {field!r} is missing")
    return value


def render_expression(expression: ir.Expr) -> str:
    """Render one strict-IR expression as an unambiguous single line."""

    kind = expression.kind
    if kind == "column":
        return f"column({_quote(str(_required(expression.column, 'column')))})"
    if kind == "bound":
        depth = _required(expression.depth, "depth")
        if type(depth) is not int or depth < 0:
            raise InspectionError("expression field 'depth' is not a non-negative integer")
        return f"bound(depth={depth})"
    if kind == "void":
        return "void()"
    if kind == "literal":
        scalar_type = str(_required(expression.result_type, "type"))
        value = _required(expression.value, "value")
        if isinstance(value, decimal.Literal):
            value = decimal.literal_json(value)
        return f"literal(type={_quote(scalar_type)}, value={json.dumps(value, ensure_ascii=True)})"
    if kind == "null":
        scalar_type = str(_required(expression.result_type, "type"))
        return f"null(type={_quote(scalar_type)})"
    if kind in {"and", "or"}:
        return f"{kind}(args={_list(expression.args, render_expression)})"
    if kind == "not":
        if len(expression.args) != 1:
            raise InspectionError("not expression does not have exactly one argument")
        return f"not(arg={render_expression(expression.args[0])})"
    if kind == "exists":
        if len(expression.args) != 1:
            raise InspectionError("exists expression does not have exactly one argument")
        return f"exists(arg={render_expression(expression.args[0])})"
    if kind == "in":
        if not 2 <= len(expression.args) <= ir.MAX_STATIC_IN_ITEMS + 1:
            raise InspectionError(
                f"in expression must have between 1 and {ir.MAX_STATIC_IN_ITEMS} items"
            )
        return (
            f"in(lookup={render_expression(expression.args[0])}, "
            f"items={_list(expression.args[1:], render_expression)})"
        )
    if kind in {"eq", "lt", "lte", "gt", "gte"}:
        if len(expression.args) != 2:
            raise InspectionError(f"{kind} expression does not have exactly two arguments")
        fields = (
            f"left={render_expression(expression.args[0])}, "
            f"right={render_expression(expression.args[1])}"
        )
        if kind == "eq":
            fields += f", null_safe={_boolean(expression.null_safe)}"
        return f"{kind}({fields})"
    if kind in {"add", "sub", "mul", "div"}:
        if len(expression.args) != 2:
            raise InspectionError(f"{kind} expression does not have exactly two arguments")
        scalar_type = str(_required(expression.result_type, "type"))
        nullable = _required(expression.nullable, "nullable")
        if not isinstance(nullable, bool):
            raise InspectionError("expression field 'nullable' is not Boolean")
        return (
            f"{kind}(left={render_expression(expression.args[0])}, "
            f"right={render_expression(expression.args[1])}, "
            f"type={_quote(scalar_type)}, nullable={_boolean(nullable)})"
        )
    if kind in {"cast_decimal", "cast_integral"}:
        if len(expression.args) != 1:
            raise InspectionError(f"{kind} expression does not have exactly one argument")
        scalar_type = str(_required(expression.result_type, "type"))
        nullable = _required(expression.nullable, "nullable")
        if not isinstance(nullable, bool):
            raise InspectionError("expression field 'nullable' is not Boolean")
        argument = f"arg={render_expression(expression.args[0])}"
        if kind == "cast_decimal":
            source_type = str(_required(expression.source_type, "source_type"))
            argument += f", source_type={_quote(source_type)}"
        return (
            f"{kind}({argument}, type={_quote(scalar_type)}, "
            f"nullable={_boolean(nullable)})"
        )
    if kind == "if":
        if len(expression.args) != 3:
            raise InspectionError("if expression does not have exactly three arguments")
        scalar_type = str(_required(expression.result_type, "type"))
        nullable = _required(expression.nullable, "nullable")
        if not isinstance(nullable, bool):
            raise InspectionError("expression field 'nullable' is not Boolean")
        return (
            f"if(condition={render_expression(expression.args[0])}, "
            f"then={render_expression(expression.args[1])}, "
            f"else={render_expression(expression.args[2])}, "
            f"type={_quote(scalar_type)}, nullable={_boolean(nullable)})"
        )
    if kind == "if_present":
        if len(expression.args) != 3:
            raise InspectionError("if_present expression does not have exactly three arguments")
        scalar_type = str(_required(expression.result_type, "type"))
        nullable = _required(expression.nullable, "nullable")
        if not isinstance(nullable, bool):
            raise InspectionError("expression field 'nullable' is not Boolean")
        return (
            f"if_present(optional={render_expression(expression.args[0])}, "
            f"present={render_expression(expression.args[1])}, "
            f"missing={render_expression(expression.args[2])}, "
            f"type={_quote(scalar_type)}, nullable={_boolean(nullable)})"
        )
    if kind in {"opaque", "opaque_double"}:
        fingerprint = str(_required(expression.fingerprint, "fingerprint"))
        scalar_type = str(_required(expression.result_type, "type"))
        nullable = _required(expression.nullable, "nullable")
        if not isinstance(nullable, bool):
            raise InspectionError("expression field 'nullable' is not Boolean")
        return (
            f"{kind}(fingerprint={_quote(fingerprint)}, type={_quote(scalar_type)}, "
            f"nullable={_boolean(nullable)}, args={_list(expression.args, render_expression)})"
        )
    raise InspectionError(f"unknown expression kind {kind!r}")


def _order(item: ir.SortOrder) -> str:
    direction = "asc" if item.ascending else "desc"
    nulls = "first" if item.nulls_first else "last"
    comparison = (
        ""
        if item.comparison is None
        else f", comparison={_quote(item.comparison)}"
    )
    return (
        f"{{column={_quote(item.column)}, direction={direction}, "
        f"nulls={nulls}{comparison}}}"
    )


def _average_state(state: ir.AverageStateType | None) -> str:
    def render(item: ir.AverageStateType) -> str:
        if item.kind == "decimal":
            return (
                f"{{sum_type={_quote(item.sum_type)}, "
                f"count_type={_quote(item.count_type)}, "
                f"nullable={_boolean(item.nullable)}}}"
            )
        if item.kind == ir.INTEGRAL_DOUBLE_AVERAGE_STATE.kind:
            if (
                item.source_type is None
                or item.exact_when_count_at_most is None
            ):
                raise InspectionError(
                    "integral avg state is missing its source type or exact-count bound"
                )
            return (
                f"{{kind={_quote(item.kind)}, "
                f"source_type={_quote(item.source_type)}, "
                f"sum_type={_quote(item.sum_type)}, "
                f"count_type={_quote(item.count_type)}, "
                f"nullable={_boolean(item.nullable)}, "
                "exact_when_count_at_most="
                f"{item.exact_when_count_at_most}}}"
            )
        raise InspectionError(f"unknown avg state kind {item.kind!r}")

    return _optional(
        state,
        render,
    )


def _join_key(key: ir.JoinKey) -> str:
    return f"{{left={_quote(key.left)}, right={_quote(key.right)}}}"


def render_node(node: ir.PlanNode) -> str:
    """Render one plan operator, including all fields specific to its variant."""

    prefix = f"node {_quote(node.id)} "
    if isinstance(node, ir.EmptySource):
        return prefix + "empty_source"
    if isinstance(node, ir.Scan):
        columns = _list(
            node.columns,
            lambda item: f"{{source={_quote(item.source)}, output={_quote(item.output)}}}",
        )
        return (
            prefix + f"scan table={_quote(node.table)} columns={columns} "
            f"predicate={_optional(node.predicate, render_expression)} "
            f"pushed_limit={_optional(node.pushed_limit, render_expression)}"
        )
    if isinstance(node, ir.Project):
        columns = _list(
            node.columns,
            lambda item: (
                f"{{output={_quote(item.output)}, expression={render_expression(item.expression)}}}"
            ),
        )
        return (
            prefix + f"project input={_quote(node.input)} columns={columns} "
            f"ordered={_boolean(node.ordered)}"
        )
    if isinstance(node, ir.Filter):
        return (
            prefix + f"filter input={_quote(node.input)} "
            f"predicate={render_expression(node.predicate)}"
        )
    if isinstance(node, ir.OuterBind):
        return (
            prefix + f"outer_bind input={_quote(node.input)} "
            f"dependency={_quote(node.dependency)} "
            f"type={_quote(node.type)} nullable={_boolean(node.nullable)}"
        )
    if isinstance(node, ir.Limit):
        return (
            prefix + f"limit input={_quote(node.input)} "
            f"count={render_expression(node.count)} "
            f"offset={_optional(node.offset, render_expression)} phase={node.phase} "
            f"ensure_at_most_one={_boolean(node.ensure_at_most_one)}"
        )
    if isinstance(node, ir.Sort):
        return (
            prefix + f"sort input={_quote(node.input)} order={_list(node.order, _order)} "
            f"limit={_optional(node.limit, render_expression)} phase={node.phase}"
        )
    if isinstance(node, ir.Aggregate):
        aggregates = _list(
            node.aggregates,
            lambda item: (
                f"{{input={_quote(item.input)}, function={_quote(item.function)}, "
                f"output={_quote(item.output)}, type={_quote(item.output_type)}, "
                f"nullable={_boolean(item.output_nullable)}, distinct={_boolean(item.distinct)}, "
                f"unwrap={_boolean(item.unwrap)}, state={_average_state(item.state)}}}"
            ),
        )
        return (
            prefix + f"aggregate input={_quote(node.input)} "
            f"keys={_list(node.keys, _quote)} aggregates={aggregates} "
            f"phase={node.phase} distinct_all={_boolean(node.distinct_all)}"
        )
    if isinstance(node, ir.Join):
        return (
            prefix + f"join left={_quote(node.left)} right={_quote(node.right)} "
            f"kind={node.kind} keys={_list(node.keys, _join_key)} "
            f"predicate={render_expression(node.predicate)}"
        )
    if isinstance(node, ir.UnionAll):
        inputs = _list(
            node.inputs,
            lambda item: f"{{node={_quote(item.node)}, columns={_list(item.columns, _quote)}}}",
        )
        return (
            prefix + f"union_all inputs={inputs} output={_list(node.output, _quote)} "
            f"ordered={_boolean(node.ordered)}"
        )
    raise InspectionError(f"unknown plan node class {type(node).__name__!r}")


def render_edge(edge: ir.StageEdge) -> str:
    """Render one StageGraph connection, including all variant settings."""

    common = (
        f"edge {_quote(edge.id)} producer={_quote(edge.producer)} "
        f"producer_output={edge.producer_output} consumer={_quote(edge.consumer)} "
        f"consumer_input={edge.consumer_input} occurrence={edge.occurrence} kind={edge.kind}"
    )
    if edge.kind in {"map", "broadcast"}:
        return common
    if edge.kind == "hash_shuffle":
        use_spilling = _required(edge.use_spilling, "use_spilling")
        if not isinstance(use_spilling, bool):
            raise InspectionError("edge field 'use_spilling' is not Boolean")
        return (
            common + f" keys={_list(edge.keys, _quote)} "
            f"hash_function={_quote(str(_required(edge.hash_function, 'hash_function')))} "
            f"use_spilling={_boolean(use_spilling)}"
        )
    if edge.kind == "union_all":
        parallel = _required(edge.parallel, "parallel")
        if not isinstance(parallel, bool):
            raise InspectionError("edge field 'parallel' is not Boolean")
        return common + f" parallel={_boolean(parallel)}"
    if edge.kind == "merge":
        return common + f" order={_list(edge.order, _order)}"
    raise InspectionError(f"unknown StageGraph connection kind {edge.kind!r}")


def _column(column: ir.Column) -> str:
    nullability = "nullable" if column.nullable else "not_null"
    return f"{{name={_quote(column.name)}, type={_quote(column.type)}, {nullability}}}"


def _subplan(subplan: ir.Subplan) -> str:
    def column_metadata(column: ir.SubplanOutput) -> str:
        return (
            f"{{column={_quote(column.column)}, "
            f"type={_quote(column.type)}, "
            f"nullable={_boolean(column.nullable)}}}"
        )

    if isinstance(subplan, ir.ScalarSubplan):
        dependencies = (
            () if subplan.dependency is None else (subplan.dependency,)
        )
        return (
            f"subplan binding={_quote(subplan.binding)} kind=scalar "
            f"root={_quote(subplan.root)} "
            f"output={column_metadata(subplan.output)} "
            f"type={_quote(subplan.output.type)} nullable=true "
            f"dependencies={_list(dependencies, _quote)} "
            f"consumers={_list(subplan.consumers, _quote)}"
        )
    if isinstance(subplan, ir.ExistsSubplan):
        return (
            f"subplan binding={_quote(subplan.binding)} kind=exists "
            f"root={_quote(subplan.root)} "
            f"predicate={_optional(subplan.predicate, render_expression)} "
            f"type={_quote(ir.BOOL)} nullable=false "
            f"dependencies={_list(subplan.dependencies, _quote)} "
            f"consumers={_list(subplan.consumers, _quote)}"
        )
    if isinstance(subplan, ir.InSubplan):
        return (
            f"subplan binding={_quote(subplan.binding)} kind=in "
            f"root={_quote(subplan.root)} "
            f"lookup={column_metadata(subplan.lookup)} "
            f"output={column_metadata(subplan.output)} "
            f"type={_quote(ir.BOOL)} nullable=false dependencies=[] "
            f"consumers={_list(subplan.consumers, _quote)}"
        )
    raise InspectionError(
        f"unknown subplan class {type(subplan).__name__!r}"
    )


def render_snapshot(snapshot: ir.Snapshot) -> str:
    """Render one validated semantic snapshot, ending with exactly one newline."""

    schemas = ir.validate_snapshot(snapshot)
    output = tuple(schemas[snapshot.plan.root][name] for name in snapshot.plan.output)
    lines = [
        f"semantic_snapshot format={_quote(ir.FORMAT)} version={ir.VERSION}",
        f"schema tables={len(snapshot.tables)}",
    ]
    for table in snapshot.tables:
        keys = _list(
            table.unique_keys,
            lambda key: (
                f"{{columns={_list(key.columns, _quote)}, "
                f"nulls_distinct={_boolean(key.nulls_distinct)}}}"
            ),
        )
        lines.append(
            f"  table {_quote(table.name)} columns={_list(table.columns, _column)} "
            f"unique_keys={keys}"
        )

    lines.extend((
        f"plan root={_quote(snapshot.plan.root)} output={_list(snapshot.plan.output, _quote)} "
        f"subplans={len(snapshot.plan.subplans)}",
        f"  output_schema={_list(output, _column)}",
    ))
    lines.extend(f"  {_subplan(subplan)}" for subplan in snapshot.plan.subplans)
    lines.extend(f"  {render_node(node)}" for node in snapshot.plan.nodes)

    graph = snapshot.stage_graph
    if graph is None:
        lines.append("stage_graph none")
        return "\n".join(lines) + "\n"

    task_counts = ir.stage_task_counts(snapshot)
    lines.append(
        f"stage_graph root_stage={_quote(graph.root_stage)} "
        f"stages={len(graph.stages)} edges={len(graph.edges)} assumptions=[]"
    )
    for stage in graph.stages:
        outputs = _list(
            stage.outputs,
            lambda item: f"{{index={item.index}, node={_quote(item.node)}}}",
        )
        lines.append(
            f"  stage {_quote(stage.id)} tasks={task_counts[stage.id]} "
            f"source_storage={_optional(stage.source_storage, _quote)} "
            f"nodes={_list(stage.nodes, _quote)} inputs={_list(stage.inputs, _quote)} "
            f"outputs={outputs}"
        )
    lines.extend(f"  {render_edge(edge)}" for edge in graph.edges)
    return "\n".join(lines) + "\n"
