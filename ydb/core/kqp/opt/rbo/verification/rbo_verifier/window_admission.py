"""Admission for the three deliberately restricted window capabilities.

Expression typing lives in ir.py. These checks establish whole-plan premises
(private lineage, supported topology and no mixed window families). They are
mandatory before evaluation and do not imply support for arbitrary SQL windows.
"""

from __future__ import annotations

from typing import Mapping

from .ir import (
    Aggregate, Column, ExistsSubplan, Expr, PlanNode, Project, Snapshot, ValueType,
    MAX_WINDOW_RANKS_PER_PROJECT, MAX_WINDOW_RANKS_PER_SNAPSHOT,
    MAX_WINDOW_ROWS_PER_PROJECT, MAX_WINDOW_ROWS_PER_SNAPSHOT,
    MAX_WINDOW_ROWS_PROJECTS_PER_SNAPSHOT, Q51_PRICE_TYPE, Q51_WINDOW_NAMES,
    WHOLE_PARTITION_DECIMAL_SUM_TYPE, WINDOW_RANK_ORDER_TYPE, WINDOW_ROWS_KINDS,
    _expression_kind_count, _fail, _node_expressions, _plan_descendants, _unique,
    plan_node_inputs,
)


def validate_window_dataflow(snapshot: Snapshot, schemas: Mapping[str, Mapping[str, Column]]) -> None:
    """Establish all supported-window premises; no caller may skip a family."""

    _validate_window_rows_dataflow(snapshot, schemas)
    _validate_whole_partition_decimal_window_dataflow(snapshot, schemas)
    _validate_window_rank_dataflow(snapshot, schemas)


def _validate_whole_partition_decimal_window_dataflow(
    snapshot: Snapshot,
    schemas: Mapping[str, Mapping[str, Column]],
) -> None:
    """Admit one SUM/AVG window leaf on one private grouped-SUM corridor."""

    window_kinds = ("window_sum", "window_avg")
    owners: list[tuple[str, PlanNode]] = []
    for node in snapshot.plan.nodes:
        for expression in _node_expressions(node):
            for kind in window_kinds:
                owners.extend(
                    (kind, node)
                    for _ in range(_expression_kind_count(expression, kind))
                )
    predicate_kinds = tuple(
        kind
        for subplan in snapshot.plan.subplans
        if isinstance(subplan, ExistsSubplan) and subplan.predicate is not None
        for kind in window_kinds
        for _ in range(_expression_kind_count(subplan.predicate, kind))
    )
    if not owners and not predicate_kinds:
        return
    observed_kinds = tuple(kind for kind, _node in owners) + predicate_kinds
    if len(observed_kinds) != 1:
        label = (
            observed_kinds[0]
            if observed_kinds and len(set(observed_kinds)) == 1
            else "relation-dependent window"
        )
        _fail(
            "snapshot.plan",
            f"exactly one {label} expression is modeled",
        )
    kind = observed_kinds[0]
    if snapshot.plan.subplans:
        _fail("snapshot.plan.subplans", f"{kind} does not admit subplans")
    if predicate_kinds or not isinstance(owners[0][1], Project):
        _fail(
            "snapshot.plan",
            f"{kind} may appear only inside one main-plan Project",
        )

    project = owners[0][1]
    assert isinstance(project, Project)
    nodes = snapshot.plan.node_map()
    aggregate = nodes.get(project.input)
    if not (
        isinstance(aggregate, Aggregate)
        and aggregate.phase in {"undefined", "final"}
        and aggregate.keys
        and not aggregate.distinct_all
    ):
        _fail(
            f"node {project.id!r}.input",
            f"{kind} Project must directly consume one grouped, "
            "logical or final Aggregate",
        )

    expressions = tuple(
        projection.expression
        for projection in project.columns
        if _expression_kind_count(projection.expression, kind)
    )
    assert len(expressions) == 1

    def find(expression: Expr) -> Expr:
        if expression.kind == kind:
            return expression
        matches = tuple(
            find(argument)
            for argument in expression.args
            if _expression_kind_count(argument, kind)
        )
        assert len(matches) == 1
        return matches[0]

    window = find(expressions[0])
    assert window.window_input is not None and window.partition_by is not None
    if any(partition not in aggregate.keys for partition in window.partition_by):
        _fail(
            f"node {project.id!r}.columns",
            f"{kind} partition must contain only direct keys of its Aggregate input",
        )
    matching_traits = tuple(
        trait
        for trait in aggregate.aggregates
        if trait.output == window.window_input
    )
    if not (
        len(matching_traits) == 1
        and matching_traits[0].function == "sum"
        and not matching_traits[0].distinct
        and not matching_traits[0].unwrap
        and matching_traits[0].output_type == WHOLE_PARTITION_DECIMAL_SUM_TYPE
        and matching_traits[0].output_nullable
    ):
        _fail(
            f"node {project.id!r}.columns",
            f"{kind} input must be the direct Optional<Decimal(35,2)> "
            "SUM output of its Aggregate input",
        )

    intermediate: Aggregate | None = None
    if aggregate.phase == "final":
        candidate = nodes.get(aggregate.input)
        if not (
            isinstance(candidate, Aggregate)
            and candidate.phase == "intermediate"
            and not candidate.distinct_all
            and candidate.keys == aggregate.keys
        ):
            _fail(
                f"node {aggregate.id!r}.input",
                f"final {kind} Aggregate must directly consume one "
                "matching intermediate Aggregate",
            )
        intermediate = candidate
        state = matching_traits[0].input
        source_traits = tuple(
            trait
            for trait in intermediate.aggregates
            if (
                trait.output == state
                and trait.function == "sum"
                and not trait.distinct
                and not trait.unwrap
            )
        )
        final_uses = tuple(
            trait for trait in aggregate.aggregates if trait.input == state
        )
        if not (
            len(source_traits) == 1
            and len(final_uses) == 1
            and schemas[intermediate.id][state].value_type
            == ValueType(WHOLE_PARTITION_DECIMAL_SUM_TYPE, True)
        ):
            _fail(
                f"node {aggregate.id!r}.aggregates",
                f"final {kind} must consume exactly one matching "
                "Optional<Decimal(35,2)> intermediate SUM state",
            )

    consumers: dict[str, list[PlanNode]] = {
        node.id: [] for node in snapshot.plan.nodes
    }
    for consumer in snapshot.plan.nodes:
        for producer in plan_node_inputs(consumer):
            consumers[producer].append(consumer)
    if consumers[aggregate.id] != [project]:
        _fail(
            f"node {aggregate.id!r}",
            f"a {kind} Aggregate must have one direct Project consumer and no fanout",
        )
    if intermediate is not None and consumers[intermediate.id] != [aggregate]:
        _fail(
            f"node {intermediate.id!r}",
            f"a {kind} intermediate Aggregate must have one direct final "
            "Aggregate consumer and no fanout",
        )
    if len(consumers[project.id]) > 1:
        _fail(
            f"node {project.id!r}",
            f"a {kind} Project must not fan out",
        )

    # The ordinary type checker already established both direct operands.
    expected_partition_types = (
        {ValueType("String", True)}
        if kind == "window_sum"
        else {ValueType("Int64", True), ValueType("String", True)}
    )
    assert all(
        schemas[aggregate.id][partition].value_type in expected_partition_types
        for partition in window.partition_by
    )
    assert schemas[aggregate.id][window.window_input].value_type == ValueType(
        WHOLE_PARTITION_DECIMAL_SUM_TYPE,
        True,
    )



def _validate_window_rank_dataflow(
    snapshot: Snapshot,
    schemas: Mapping[str, Mapping[str, Column]],
) -> None:
    """Admit only q49's direct, independently sorted global Rank leaves."""

    ranks_by_project: dict[str, list[Expr]] = {}
    rank_count = 0
    whole_window_count = 0
    for node in snapshot.plan.nodes:
        for expression in _node_expressions(node):
            whole_window_count += sum(
                _expression_kind_count(expression, kind)
                for kind in ("window_sum", "window_avg")
            )
        if isinstance(node, Project):
            for index, projection in enumerate(node.columns):
                count = _expression_kind_count(
                    projection.expression,
                    "window_rank",
                )
                rank_count += count
                if not count:
                    continue
                if count != 1 or projection.expression.kind != "window_rank":
                    _fail(
                        f"node {node.id!r}.columns[{index}].expression",
                        "window_rank must be one complete top-level Project expression",
                    )
                ranks_by_project.setdefault(node.id, []).append(
                    projection.expression
                )
            continue
        if any(
            _expression_kind_count(expression, "window_rank")
            for expression in _node_expressions(node)
        ):
            _fail(
                f"node {node.id!r}",
                "window_rank may appear only as a top-level Project expression",
            )

    for index, subplan in enumerate(snapshot.plan.subplans):
        if isinstance(subplan, ExistsSubplan) and subplan.predicate is not None:
            predicate_ranks = _expression_kind_count(
                subplan.predicate,
                "window_rank",
            )
            rank_count += predicate_ranks
            whole_window_count += sum(
                _expression_kind_count(subplan.predicate, kind)
                for kind in ("window_sum", "window_avg")
            )
            if predicate_ranks:
                _fail(
                    f"snapshot.plan.subplans[{index}].predicate",
                    "window_rank may appear only as a top-level Project expression",
                )

    if not rank_count:
        return
    if snapshot.plan.subplans:
        _fail("snapshot.plan.subplans", "window_rank does not admit subplans")
    if whole_window_count:
        _fail(
            "snapshot.plan",
            "window_rank may not be mixed with aggregate-window leaves",
        )
    if rank_count > MAX_WINDOW_RANKS_PER_SNAPSHOT:
        _fail(
            "snapshot.plan",
            "window_rank count exceeds the "
            f"{MAX_WINDOW_RANKS_PER_SNAPSHOT}-leaf snapshot audit bound",
        )

    nodes = snapshot.plan.node_map()
    main_nodes = _plan_descendants(nodes, snapshot.plan.root)
    consumers: dict[str, list[PlanNode]] = {
        node.id: [] for node in snapshot.plan.nodes
    }
    for consumer in snapshot.plan.nodes:
        for producer in plan_node_inputs(consumer):
            consumers[producer].append(consumer)
    names: list[str] = []
    for project_id, ranks in ranks_by_project.items():
        if project_id not in main_nodes:
            _fail(
                f"node {project_id!r}",
                "window_rank must belong to the main result plan",
            )
        if len(consumers[project_id]) > 1:
            _fail(
                f"node {project_id!r}",
                "a window_rank Project must not fan out",
            )
        if len(ranks) > MAX_WINDOW_RANKS_PER_PROJECT:
            _fail(
                f"node {project_id!r}.columns",
                "window_rank count exceeds the "
                f"{MAX_WINDOW_RANKS_PER_PROJECT}-leaf Project audit bound",
            )
        orders = tuple(rank.execution_order for rank in ranks)
        if sorted(orders) != list(range(len(ranks))):
            _fail(
                f"node {project_id!r}.columns",
                "window_rank execution_order must be the complete distinct "
                f"range 0..{len(ranks) - 1}",
            )
        input_schema = schemas[nodes[project_id].input]
        for rank in ranks:
            assert rank.window_name is not None
            assert rank.order_by is not None and len(rank.order_by) == 1
            names.append(rank.window_name)
            key = input_schema.get(rank.order_by[0].column)
            assert key is not None
            assert key.value_type == ValueType(WINDOW_RANK_ORDER_TYPE, False)

        if snapshot.stage_graph is not None:
            stage_outputs = tuple(
                (stage.id, output.index)
                for stage in snapshot.stage_graph.stages
                for output in stage.outputs
                if output.node == project_id
            )
            outgoing = tuple(
                edge
                for stage_id, output_index in stage_outputs
                for edge in snapshot.stage_graph.edges
                if (
                    edge.producer == stage_id
                    and edge.producer_output == output_index
                )
            )
            if len(outgoing) > 1:
                _fail(
                    f"node {project_id!r}",
                    "a staged window_rank Project output must not fan out",
                )
    _unique(names, "snapshot.plan window_rank names")



def _validate_window_rows_dataflow(
    snapshot: Snapshot,
    schemas: Mapping[str, Mapping[str, Column]],
) -> None:
    """Admit only q51's private ordered-ROWS Project leaves."""

    windows_by_project: dict[str, list[Expr]] = {}
    window_count = 0
    mixed_window_count = 0
    for node in snapshot.plan.nodes:
        for expression in _node_expressions(node):
            mixed_window_count += sum(
                _expression_kind_count(expression, kind)
                for kind in ("window_sum", "window_avg", "window_rank")
            )
        if isinstance(node, Project):
            for index, projection in enumerate(node.columns):
                counts = {
                    kind: _expression_kind_count(projection.expression, kind)
                    for kind in WINDOW_ROWS_KINDS
                }
                count = sum(counts.values())
                window_count += count
                if not count:
                    continue
                if count != 1 or projection.expression.kind not in WINDOW_ROWS_KINDS:
                    _fail(
                        f"node {node.id!r}.columns[{index}].expression",
                        "q51 ROWS window must be one complete top-level Project expression",
                    )
                windows_by_project.setdefault(node.id, []).append(
                    projection.expression
                )
            continue
        if any(
            sum(
                _expression_kind_count(expression, kind)
                for kind in WINDOW_ROWS_KINDS
            )
            for expression in _node_expressions(node)
        ):
            _fail(
                f"node {node.id!r}",
                "q51 ROWS windows may appear only as top-level Project expressions",
            )

    predicate_window_count = 0
    for subplan in snapshot.plan.subplans:
        if isinstance(subplan, ExistsSubplan) and subplan.predicate is not None:
            predicate_window_count += sum(
                _expression_kind_count(subplan.predicate, kind)
                for kind in WINDOW_ROWS_KINDS
            )
            mixed_window_count += sum(
                _expression_kind_count(subplan.predicate, kind)
                for kind in ("window_sum", "window_avg", "window_rank")
            )
    window_count += predicate_window_count
    if not window_count:
        return
    if snapshot.plan.subplans:
        _fail("snapshot.plan.subplans", "q51 ROWS windows do not admit subplans")
    if predicate_window_count:
        _fail(
            "snapshot.plan",
            "q51 ROWS windows may appear only as top-level Project expressions",
        )
    if mixed_window_count:
        _fail(
            "snapshot.plan",
            "q51 ROWS windows may not be mixed with other window leaves",
        )
    if window_count != MAX_WINDOW_ROWS_PER_SNAPSHOT:
        _fail(
            "snapshot.plan",
            "q51 requires exactly four ROWS window leaves",
        )
    if len(windows_by_project) != MAX_WINDOW_ROWS_PROJECTS_PER_SNAPSHOT:
        _fail(
            "snapshot.plan",
            "q51 requires exactly three ROWS window Projects",
        )

    all_windows = tuple(
        window
        for windows in windows_by_project.values()
        for window in windows
    )
    if sum(window.kind == "window_rows_sum" for window in all_windows) != 2:
        _fail("snapshot.plan", "q51 requires exactly two running SUM leaves")
    if sum(window.kind == "window_rows_max" for window in all_windows) != 2:
        _fail("snapshot.plan", "q51 requires exactly two running MAX leaves")
    names = tuple(window.window_name for window in all_windows)
    if len(set(names)) != len(names) or set(names) != set(Q51_WINDOW_NAMES):
        _fail(
            "snapshot.plan",
            "q51 ROWS window names must be exactly "
            f"{Q51_WINDOW_NAMES!r}",
        )

    nodes = snapshot.plan.node_map()
    main_nodes = _plan_descendants(nodes, snapshot.plan.root)
    consumers: dict[str, list[PlanNode]] = {
        node.id: [] for node in snapshot.plan.nodes
    }
    for consumer in snapshot.plan.nodes:
        for producer in plan_node_inputs(consumer):
            consumers[producer].append(consumer)

    for project_id, windows in windows_by_project.items():
        project = nodes[project_id]
        assert isinstance(project, Project)
        if project_id not in main_nodes:
            _fail(
                f"node {project_id!r}",
                "q51 ROWS window must belong to the main result plan",
            )
        if len(consumers[project_id]) > 1:
            _fail(
                f"node {project_id!r}",
                "a q51 ROWS window Project must not fan out",
            )
        if len(windows) > MAX_WINDOW_ROWS_PER_PROJECT:
            _fail(
                f"node {project_id!r}.columns",
                "q51 ROWS window count exceeds the "
                f"{MAX_WINDOW_ROWS_PER_PROJECT}-leaf Project audit bound",
            )
        orders = tuple(window.execution_order for window in windows)
        if sorted(orders) != list(range(len(windows))):
            _fail(
                f"node {project_id!r}.columns",
                "q51 ROWS window execution_order must be the complete distinct "
                f"range 0..{len(windows) - 1}",
            )
        kinds = {window.kind for window in windows}
        if "window_rows_sum" in kinds and (
            len(windows) != 1 or kinds != {"window_rows_sum"}
        ):
            _fail(
                f"node {project_id!r}.columns",
                "a q51 running SUM Project must contain exactly one window leaf",
            )
        if kinds == {"window_rows_sum"}:
            if windows[0].window_name not in Q51_WINDOW_NAMES[:2]:
                _fail(
                    f"node {project_id!r}.columns",
                    "q51 running SUM names must be canonical windows 0 and 1",
                )
        elif tuple(
            window.window_name
            for window in sorted(windows, key=lambda item: item.execution_order)
        ) != Q51_WINDOW_NAMES[2:]:
            _fail(
                f"node {project_id!r}.columns",
                "q51 running MAX orders 0 and 1 must be canonical windows 2 and 3",
            )

        aggregate = nodes.get(project.input)
        for window in windows:
            assert window.window_input is not None
            assert window.partition_by is not None
            assert window.order_by is not None
            if window.kind != "window_rows_sum":
                continue
            if not (
                isinstance(aggregate, Aggregate)
                and aggregate.phase in {"undefined", "final"}
                and aggregate.keys
                and not aggregate.distinct_all
            ):
                _fail(
                    f"node {project_id!r}.input",
                    "a q51 running SUM Project must directly consume one grouped, "
                    "logical or final Aggregate",
                )
            required_keys = window.partition_by + tuple(
                item.column for item in window.order_by
            )
            if any(column not in aggregate.keys for column in required_keys):
                _fail(
                    f"node {project_id!r}.columns",
                    "q51 running SUM partition and order columns must be direct "
                    "keys of its Aggregate input",
                )
            matching_traits = tuple(
                trait
                for trait in aggregate.aggregates
                if trait.output == window.window_input
            )
            if not (
                len(matching_traits) == 1
                and matching_traits[0].function == "sum"
                and not matching_traits[0].distinct
                and not matching_traits[0].unwrap
                and matching_traits[0].output_type
                == WHOLE_PARTITION_DECIMAL_SUM_TYPE
                and matching_traits[0].output_nullable
            ):
                _fail(
                    f"node {project_id!r}.columns",
                    "q51 running SUM input must be the direct "
                    "Optional<Decimal(35,2)> SUM output of its Aggregate input",
                )
            if aggregate.phase == "undefined" and schemas[aggregate.input][
                matching_traits[0].input
            ].value_type != ValueType(Q51_PRICE_TYPE, True):
                _fail(
                    f"node {aggregate.id!r}.aggregates",
                    "logical q51 running SUM must consume Optional<Decimal(7,2)>",
                )
            if consumers[aggregate.id] != [project]:
                _fail(
                    f"node {aggregate.id!r}",
                    "a q51 running SUM Aggregate must have one direct Project "
                    "consumer and no fanout",
                )
            if aggregate.phase == "final":
                intermediate = nodes.get(aggregate.input)
                state = matching_traits[0].input
                if not (
                    isinstance(intermediate, Aggregate)
                    and intermediate.phase == "intermediate"
                    and not intermediate.distinct_all
                    and intermediate.keys == aggregate.keys
                ):
                    _fail(
                        f"node {aggregate.id!r}.input",
                        "final q51 running SUM Aggregate must directly consume "
                        "one matching intermediate Aggregate",
                    )
                source_traits = tuple(
                    trait
                    for trait in intermediate.aggregates
                    if (
                        trait.output == state
                        and trait.function == "sum"
                        and not trait.distinct
                        and not trait.unwrap
                    )
                )
                final_uses = tuple(
                    trait for trait in aggregate.aggregates if trait.input == state
                )
                if not (
                    len(source_traits) == 1
                    and len(final_uses) == 1
                    and schemas[intermediate.id][state].value_type
                    == ValueType(WHOLE_PARTITION_DECIMAL_SUM_TYPE, True)
                    and schemas[intermediate.input][source_traits[0].input].value_type
                    == ValueType(Q51_PRICE_TYPE, True)
                ):
                    _fail(
                        f"node {aggregate.id!r}.aggregates",
                        "final q51 running SUM must consume exactly one matching "
                        "Optional<Decimal(35,2)> intermediate SUM state",
                    )
                if consumers[intermediate.id] != [aggregate]:
                    _fail(
                        f"node {intermediate.id!r}",
                        "a q51 running SUM intermediate Aggregate must have one "
                        "direct final Aggregate consumer and no fanout",
                    )

    max_projects = tuple(
        nodes[project_id]
        for project_id, windows in windows_by_project.items()
        if windows[0].kind == "window_rows_max"
    )
    assert len(max_projects) == 1
    max_project = max_projects[0]
    assert isinstance(max_project, Project)
    max_windows = tuple(
        sorted(
            windows_by_project[max_project.id],
            key=lambda window: window.execution_order,
        )
    )
    if (
        max_windows[0].window_input == max_windows[1].window_input
        or max_windows[0].partition_by != max_windows[1].partition_by
        or max_windows[0].order_by != max_windows[1].order_by
    ):
        _fail(
            f"node {max_project.id!r}.columns",
            "q51 running MAX requires two distinct inputs over one identical "
            "partition/order specification",
        )
