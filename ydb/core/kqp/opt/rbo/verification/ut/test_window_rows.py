import copy
import os
import unittest
from unittest import mock

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import decimal, smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier import relation as relation_model
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    Expr,
    Project,
    Projection,
    SnapshotError,
    SortOrder,
    expression_columns,
    parse_snapshot,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import (
    Evaluator,
    RelationError,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import Encoder, Value
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import (
    build_problem,
    solve,
)


SOLVER = (
    yatest_common.binary_path("contrib/tools/z3/z3")
    if yatest_common is not None
    else os.environ.get("RBO_Z3")
)


def _column(name):
    return {"kind": "column", "column": name}


def _window(kind, input_name="amount", execution_order=0, name=None):
    return {
        "kind": kind,
        "input": input_name,
        "partition_by": ["item"],
        "order_by": [
            {
                "column": "d_date",
                "ascending": True,
                "nulls_first": True,
            }
        ],
        "frame": "rows_unbounded_preceding_current_row",
        "window_name": name or f"window{execution_order}",
        "execution_order": execution_order,
        "type": "Decimal(35,2)",
        "nullable": True,
    }


def _raw_window(snapshot, name):
    for node in snapshot["plan"]["nodes"]:
        for projection in node.get("columns", []):
            expression = projection.get("expression", {})
            if expression.get("window_name") == name:
                return expression
    raise AssertionError(f"window {name!r} is missing")


def _raw_node(snapshot, node_id):
    return next(
        node for node in snapshot["plan"]["nodes"] if node["id"] == node_id
    )


def _sum_branch(prefix, table, alias_count):
    nodes = [
        {
            "id": f"{prefix}_scan",
            "op": "scan",
            "table": table,
            "columns": [
                {"source": "item", "output": f"{prefix}_item"},
                {"source": "d_date", "output": f"{prefix}_date"},
                {"source": "amount", "output": f"{prefix}_amount"},
            ],
            "pushed_limit": None,
        },
        {
            "id": f"{prefix}_aggregate",
            "op": "aggregate",
            "input": f"{prefix}_scan",
            "keys": [f"{prefix}_item", f"{prefix}_date"],
            "aggregates": [
                {
                    "input": f"{prefix}_amount",
                    "function": "sum",
                    "output": f"{prefix}_total",
                    "type": "Decimal(35,2)",
                    "nullable": True,
                    "distinct": False,
                    "unwrap": False,
                }
            ],
            "phase": "undefined",
            "distinct_all": False,
        },
        {
            "id": f"{prefix}_window",
            "op": "project",
            "input": f"{prefix}_aggregate",
            "ordered": False,
            "columns": [
                {
                    "output": f"{prefix}_item",
                    "expression": _column(f"{prefix}_item"),
                },
                {
                    "output": f"{prefix}_date",
                    "expression": _column(f"{prefix}_date"),
                },
                {
                    "output": f"{prefix}_running",
                    "expression": {
                        **_window(
                            "window_rows_sum",
                            f"{prefix}_total",
                            name=(
                                "_yql_anonymous_window0"
                                if prefix == "web"
                                else "_yql_anonymous_window1"
                            ),
                        ),
                        "partition_by": [f"{prefix}_item"],
                        "order_by": [
                            {
                                "column": f"{prefix}_date",
                                "ascending": True,
                                "nulls_first": True,
                            }
                        ],
                    },
                },
            ],
        },
    ]
    previous = f"{prefix}_window"
    columns = (f"{prefix}_item", f"{prefix}_date", f"{prefix}_running")
    for index in range(alias_count):
        alias = f"{prefix}_alias{index}"
        nodes.append(
            {
                "id": alias,
                "op": "project",
                "input": previous,
                "ordered": False,
                "columns": [
                    {"output": name, "expression": _column(name)}
                    for name in columns
                ],
            }
        )
        previous = alias
    return nodes


def _q51_shape_snapshot(*, arm_alias_count=4, outer_alias=True):
    table = {
        "columns": [
            {"name": "item", "type": "Int64", "nullable": False},
            {"name": "d_date", "type": "Date", "nullable": True},
            {"name": "amount", "type": "Decimal(7,2)", "nullable": True},
        ],
        "unique_keys": [],
    }
    nodes = _sum_branch("web", "Web", arm_alias_count) + _sum_branch(
        "store",
        "Store",
        arm_alias_count,
    )
    web_input = (
        f"web_alias{arm_alias_count - 1}"
        if arm_alias_count
        else "web_window"
    )
    store_input = (
        f"store_alias{arm_alias_count - 1}"
        if arm_alias_count
        else "store_window"
    )
    nodes.extend(
        [
            {
                "id": "joined",
                "op": "join",
                "left": web_input,
                "right": store_input,
                "kind": "full",
                "keys": [
                    {"left": "web_item", "right": "store_item"},
                    {"left": "web_date", "right": "store_date"},
                ],
                "predicate": {"kind": "literal", "type": "Bool", "value": True},
            },
            {
                "id": "carrier",
                "op": "project",
                "input": "joined",
                "ordered": False,
                "columns": [
                    {
                        "output": "item",
                        "expression": {
                            "kind": "if",
                            "condition": {
                                "kind": "exists",
                                "arg": _column("web_item"),
                            },
                            "then": _column("web_item"),
                            "else": _column("store_item"),
                            "type": "Int64",
                            "nullable": True,
                        },
                    },
                    {
                        "output": "d_date",
                        "expression": {
                            "kind": "if",
                            "condition": {
                                "kind": "exists",
                                "arg": _column("web_date"),
                            },
                            "then": _column("web_date"),
                            "else": _column("store_date"),
                            "type": "Date",
                            "nullable": True,
                        },
                    },
                    {
                        "output": "web_running",
                        "expression": _column("web_running"),
                    },
                    {
                        "output": "store_running",
                        "expression": _column("store_running"),
                    },
                ],
            },
        ]
    )
    outer_input = "carrier"
    if outer_alias:
        nodes.append(
            {
                "id": "outer_alias",
                "op": "project",
                "input": "carrier",
                "ordered": False,
                "columns": [
                    {"output": name, "expression": _column(name)}
                    for name in ("item", "d_date", "web_running", "store_running")
                ],
            }
        )
        outer_input = "outer_alias"
    nodes.extend(
        [
            {
                "id": "outer_window",
                "op": "project",
                "input": outer_input,
                "ordered": False,
                "columns": [
                    {"output": "item", "expression": _column("item")},
                    {"output": "d_date", "expression": _column("d_date")},
                    {
                        "output": "web_running",
                        "expression": _column("web_running"),
                    },
                    {
                        "output": "store_running",
                        "expression": _column("store_running"),
                    },
                    {
                        "output": "web_max",
                        "expression": {
                            **_window(
                                "window_rows_max",
                                "web_running",
                                0,
                                "_yql_anonymous_window2",
                            ),
                            "partition_by": ["item"],
                            "order_by": [
                                {
                                    "column": "d_date",
                                    "ascending": True,
                                    "nulls_first": True,
                                }
                            ],
                        },
                    },
                    {
                        "output": "store_max",
                        "expression": {
                            **_window(
                                "window_rows_max",
                                "store_running",
                                1,
                                "_yql_anonymous_window3",
                            ),
                            "partition_by": ["item"],
                            "order_by": [
                                {
                                    "column": "d_date",
                                    "ascending": True,
                                    "nulls_first": True,
                                }
                            ],
                        },
                    },
                ],
            },
        ]
    )
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": {
            "tables": [
                {"name": "Web"} | copy.deepcopy(table),
                {"name": "Store"} | copy.deepcopy(table),
            ]
        },
        "plan": {
            "nodes": nodes,
            "root": "outer_window",
            "output": ["item", "d_date", "web_max", "store_max"],
            "subplans": [],
        },
        "stage_graph": None,
    }


def _staged_q51_snapshot(outer_shuffle_keys, *, split_aggregates=False):
    snapshot = _q51_shape_snapshot(arm_alias_count=1, outer_alias=False)
    if split_aggregates:
        for prefix in ("web", "store"):
            nodes = snapshot["plan"]["nodes"]
            aggregate = _raw_node(snapshot, f"{prefix}_aggregate")
            index = nodes.index(aggregate)
            intermediate = copy.deepcopy(aggregate)
            intermediate["id"] = f"{prefix}_intermediate"
            intermediate["phase"] = "intermediate"
            intermediate["aggregates"][0]["output"] = f"{prefix}_partial"
            final = copy.deepcopy(aggregate)
            final["input"] = f"{prefix}_intermediate"
            final["phase"] = "final"
            final["aggregates"][0]["input"] = f"{prefix}_partial"
            nodes[index : index + 1] = [intermediate, final]

    def source_stage(prefix):
        source_output = (
            f"{prefix}_intermediate" if split_aggregates else f"{prefix}_scan"
        )
        return {
            "id": f"{prefix}_source",
            "nodes": (
                [f"{prefix}_scan", f"{prefix}_intermediate"]
                if split_aggregates
                else [f"{prefix}_scan"]
            ),
            "inputs": [],
            "outputs": [{"index": 0, "node": source_output}],
            "source_storage": "column" if split_aggregates else "row",
        }

    def window_stage(prefix):
        return {
            "id": f"{prefix}_window_stage",
            "nodes": [
                f"{prefix}_aggregate",
                f"{prefix}_window",
                f"{prefix}_alias0",
            ],
            "inputs": [
                f"{prefix}_intermediate"
                if split_aggregates
                else f"{prefix}_scan"
            ],
            "outputs": [{"index": 0, "node": f"{prefix}_alias0"}],
            "source_storage": None,
        }

    snapshot["stage_graph"] = {
        "root_stage": "outer_stage",
        "stages": [
            source_stage("web"),
            window_stage("web"),
            source_stage("store"),
            window_stage("store"),
            {
                "id": "join_stage",
                "nodes": ["joined", "carrier"],
                "inputs": ["web_alias0", "store_alias0"],
                "outputs": [{"index": 0, "node": "carrier"}],
                "source_storage": None,
            },
            {
                "id": "outer_stage",
                "nodes": ["outer_window"],
                "inputs": ["carrier"],
                "outputs": [{"index": 0, "node": "outer_window"}],
                "source_storage": None,
            },
        ],
        "edges": [
            {
                "id": "web-source-window-0",
                "producer": "web_source",
                "consumer": "web_window_stage",
                "occurrence": 0,
                "producer_output": 0,
                "consumer_input": 0,
                "kind": "hash_shuffle",
                "keys": ["web_item"],
                "hash_function": "HashV2",
                "use_spilling": False,
            },
            {
                "id": "store-source-window-0",
                "producer": "store_source",
                "consumer": "store_window_stage",
                "occurrence": 0,
                "producer_output": 0,
                "consumer_input": 0,
                "kind": "hash_shuffle",
                "keys": ["store_item"],
                "hash_function": "HashV2",
                "use_spilling": False,
            },
            {
                "id": "web-window-join-0",
                "producer": "web_window_stage",
                "consumer": "join_stage",
                "occurrence": 0,
                "producer_output": 0,
                "consumer_input": 0,
                "kind": "hash_shuffle",
                "keys": ["web_item"],
                "hash_function": "HashV2",
                "use_spilling": False,
            },
            {
                "id": "store-window-join-0",
                "producer": "store_window_stage",
                "consumer": "join_stage",
                "occurrence": 0,
                "producer_output": 0,
                "consumer_input": 1,
                "kind": "hash_shuffle",
                "keys": ["store_item"],
                "hash_function": "HashV2",
                "use_spilling": False,
            },
            {
                "id": "join-outer-0",
                "producer": "join_stage",
                "consumer": "outer_stage",
                "occurrence": 0,
                "producer_output": 0,
                "consumer_input": 0,
                "kind": "hash_shuffle",
                "keys": outer_shuffle_keys,
                "hash_function": "HashV2",
                "use_spilling": False,
            },
        ],
        "assumptions": [],
    }
    return snapshot


def _value(scalar_type, value, *, nullable=False):
    is_null = value is None
    finite_bound = None
    if scalar_type.startswith("Decimal("):
        finite_bound = (
            0
            if value is None or not -decimal.INF < value < decimal.INF
            else abs(value)
        )
    return Value(
        scalar_type,
        smt.bool_value(is_null) if nullable else smt.FALSE,
        smt.int_value(0 if value is None else value),
        finite_bound,
    )


def _window_expr(kind, execution_order=0, name=None):
    return Expr(
        kind=kind,
        window_input="amount",
        partition_by=("item",),
        window_name=name or f"window{execution_order}",
        execution_order=execution_order,
        order_by=(SortOrder("d_date", True, True),),
        window_frame="rows_unbounded_preceding_current_row",
        result_type="Decimal(35,2)",
        nullable=True,
    )


def _ground(term, values=None):
    values = values or {}
    if term.operation == "symbol":
        return values[term.atom]
    if term.operation in {"bool", "int"}:
        return term.atom
    arguments = tuple(_ground(argument, values) for argument in term.arguments)
    if term.operation == "not":
        return not arguments[0]
    if term.operation == "and":
        return all(arguments)
    if term.operation == "or":
        return any(arguments)
    if term.operation == "=":
        return arguments[0] == arguments[1]
    if term.operation == "<":
        return arguments[0] < arguments[1]
    if term.operation == "ite":
        return arguments[1] if arguments[0] else arguments[2]
    if term.operation == "+":
        return sum(arguments)
    raise AssertionError(f"unsupported ground operation {term.operation!r}")


def _concrete_values(windows, slots):
    rows = tuple(
        relation_model.Row(
            smt.TRUE,
            {
                "item": _value("Int64", item, nullable=True),
                "d_date": _value("Date", d_date, nullable=True),
                "amount": _value("Decimal(35,2)", amount, nullable=True),
            },
        )
        for item, d_date, amount in slots
    )
    source = relation_model.Relation(
        (
            relation_model.Column("item", "Int64", True),
            relation_model.Column("d_date", "Date", True),
            relation_model.Column("amount", "Decimal(35,2)", True),
        ),
        rows,
    )
    node = Project(
        "window",
        "source",
        tuple(Projection(f"result{index}", window) for index, window in enumerate(windows)),
        False,
    )
    evaluator = object.__new__(Evaluator)
    script = smt.Script()
    evaluator.scalar = Encoder(script)
    evaluator.choice_scope = "test"
    return evaluator._window_rows_values(node, source, windows, 0)


class WindowRowsTest(unittest.TestCase):
    def test_exact_q51_four_leaf_three_project_shape_is_accepted(self):
        snapshot = parse_snapshot(_q51_shape_snapshot())
        leaves = tuple(
            projection.expression
            for node in snapshot.plan.nodes
            if isinstance(node, Project)
            for projection in node.columns
            if projection.expression.kind.startswith("window_rows_")
        )
        self.assertEqual(
            tuple((leaf.kind, leaf.execution_order) for leaf in leaves),
            (
                ("window_rows_sum", 0),
                ("window_rows_sum", 0),
                ("window_rows_max", 0),
                ("window_rows_max", 1),
            ),
        )
        self.assertEqual(
            expression_columns(leaves[-1]),
            frozenset({"store_running", "item", "d_date"}),
        )
        split = parse_snapshot(
            _staged_q51_snapshot(["item"], split_aggregates=True)
        )
        self.assertEqual(
            tuple(
                node.phase
                for node in split.plan.nodes
                if getattr(node, "id", "")
                in {
                    "web_intermediate",
                    "web_aggregate",
                    "store_intermediate",
                    "store_aggregate",
                }
            ),
            ("intermediate", "final", "intermediate", "final"),
        )

    def test_wire_shape_types_and_dataflow_are_closed(self):
        mutations = (
            ("nullable", False, "Optional<Decimal"),
            ("type", "Decimal(34,2)", "Optional<Decimal"),
            ("input", "missing", "window input column"),
            ("partition_by", [], "exactly one partition"),
            ("partition_by", ["d_date"], "Int64 or Optional<Int64>"),
            ("order_by", [], "must not be empty"),
            ("frame", "rows_unbounded", "frame must be"),
            ("window_name", "", "non-empty string"),
        )
        for field, value, message in mutations:
            raw = _q51_shape_snapshot()
            _raw_window(raw, "_yql_anonymous_window2")[field] = value
            with self.subTest(field=field), self.assertRaisesRegex(
                SnapshotError,
                message,
            ):
                parse_snapshot(raw)

        for field in ("ascending", "nulls_first"):
            raw = _q51_shape_snapshot()
            _raw_window(raw, "_yql_anonymous_window2")["order_by"][0][field] = False
            with self.subTest(field=field), self.assertRaisesRegex(
                SnapshotError,
                "ascending nulls-first",
            ):
                parse_snapshot(raw)

        raw = _q51_shape_snapshot()
        _raw_window(raw, "_yql_anonymous_window2")["order_by"][0][
            "column"
        ] = "item"
        with self.assertRaisesRegex(SnapshotError, "Optional<Date>"):
            parse_snapshot(raw)

        raw = _q51_shape_snapshot()
        _raw_window(raw, "_yql_anonymous_window2")["extra"] = True
        with self.assertRaisesRegex(SnapshotError, "unknown fields"):
            parse_snapshot(raw)

        nested = _q51_shape_snapshot()
        outer = _raw_node(nested, "outer_window")
        leaf = outer["columns"][4]["expression"]
        outer["columns"][4]["expression"] = {
            "kind": "decimal_abs",
            "arg": leaf,
            "type": "Decimal(35,2)",
            "nullable": True,
        }
        with self.assertRaisesRegex(SnapshotError, "complete top-level"):
            parse_snapshot(nested)

        duplicate_order = _q51_shape_snapshot()
        _raw_window(duplicate_order, "_yql_anonymous_window3")["execution_order"] = 0
        with self.assertRaisesRegex(SnapshotError, "complete distinct range"):
            parse_snapshot(duplicate_order)

        wrong_name = _q51_shape_snapshot()
        _raw_window(wrong_name, "_yql_anonymous_window3")[
            "window_name"
        ] = "_yql_anonymous_window2"
        with self.assertRaisesRegex(SnapshotError, "names must be exactly"):
            parse_snapshot(wrong_name)

        too_many = _q51_shape_snapshot()
        _raw_node(too_many, "outer_window")["columns"].append(
            {
                "output": "extra_max",
                "expression": {
                    **copy.deepcopy(
                        _raw_window(too_many, "_yql_anonymous_window3")
                    ),
                    "window_name": "_yql_anonymous_window4",
                    "execution_order": 2,
                },
            }
        )
        with self.assertRaisesRegex(SnapshotError, "exactly four"):
            parse_snapshot(too_many)

    def test_sum_provenance_mixed_windows_subplans_and_fanout_fail_closed(self):
        wrong_sum = _q51_shape_snapshot()
        wrong_sum["schema"]["tables"][0]["columns"][2]["type"] = "Decimal(35,2)"
        wrong_sum["plan"]["nodes"][1]["aggregates"][0]["function"] = "max"
        with self.assertRaisesRegex(SnapshotError, "direct Optional<Decimal"):
            parse_snapshot(wrong_sum)

        wrong_price = _q51_shape_snapshot()
        wrong_price["schema"]["tables"][0]["columns"][2]["type"] = "Decimal(15,2)"
        with self.assertRaisesRegex(SnapshotError, r"Optional<Decimal\(7,2\)>"):
            parse_snapshot(wrong_price)

        stale_sum = _q51_shape_snapshot()
        stale_sum["plan"]["nodes"].insert(
            2,
            {
                "id": "web_carrier",
                "op": "project",
                "input": "web_aggregate",
                "ordered": False,
                "columns": [
                    {"output": name, "expression": _column(name)}
                    for name in ("web_item", "web_date", "web_total")
                ],
            },
        )
        stale_sum["plan"]["nodes"][3]["input"] = "web_carrier"
        with self.assertRaisesRegex(SnapshotError, "directly consume"):
            parse_snapshot(stale_sum)

        duplicate_max_input = _q51_shape_snapshot()
        _raw_window(duplicate_max_input, "_yql_anonymous_window3")[
            "input"
        ] = "web_running"
        with self.assertRaisesRegex(SnapshotError, "two distinct inputs"):
            parse_snapshot(duplicate_max_input)

        mixed = _q51_shape_snapshot()
        _raw_node(mixed, "outer_window")["columns"].append(
            {
                "output": "whole",
                "expression": {
                    "kind": "window_avg",
                    "input": "web_running",
                    "partition_by": ["item"],
                    "type": "Decimal(35,2)",
                    "nullable": True,
                },
            }
        )
        with self.assertRaisesRegex(SnapshotError, "may not be mixed"):
            parse_snapshot(mixed)

        subplan = _q51_shape_snapshot()
        subplan["plan"]["nodes"].extend(
            [
                {
                    "id": "subscan",
                    "op": "scan",
                    "table": "Web",
                    "columns": [{"source": "item", "output": "sub_id"}],
                    "pushed_limit": None,
                },
                {
                    "id": "subproject",
                    "op": "project",
                    "input": "subscan",
                    "ordered": False,
                    "columns": [
                        {
                            "output": "scalar",
                            "expression": {
                                "kind": "literal",
                                "type": "Int64",
                                "value": 7,
                            },
                        }
                    ],
                },
            ]
        )
        _raw_node(subplan, "outer_window")["columns"].append(
            {"output": "unused", "expression": _column("$scalar")}
        )
        subplan["plan"]["subplans"] = [
            {
                "binding": "$scalar",
                "kind": "scalar",
                "root": "subproject",
                "type": "Int64",
                "nullable": True,
                "dependencies": [],
                "consumers": ["outer_window"],
                "output": {
                    "column": "scalar",
                    "type": "Int64",
                    "nullable": False,
                },
            }
        ]
        with self.assertRaisesRegex(SnapshotError, "do not admit subplans"):
            parse_snapshot(subplan)

        fanout = _q51_shape_snapshot()
        outputs = ["item", "d_date", "web_max", "store_max"]
        fanout["plan"]["nodes"].extend(
            [
                {
                    "id": branch,
                    "op": "filter",
                    "input": "outer_window",
                    "predicate": {
                        "kind": "literal",
                        "type": "Bool",
                        "value": True,
                    },
                }
                for branch in ("left", "right")
            ]
            + [
                {
                    "id": "union",
                    "op": "union_all",
                    "inputs": [
                        {"node": branch, "columns": outputs}
                        for branch in ("left", "right")
                    ],
                    "output": outputs,
                    "ordered": False,
                }
            ]
        )
        fanout["plan"]["root"] = "union"
        fanout["plan"]["output"] = outputs
        with self.assertRaisesRegex(SnapshotError, "must not fan out"):
            parse_snapshot(fanout)

    def test_peer_orders_are_independent_and_null_inputs_are_ignored(self):
        windows = (
            _window_expr("window_rows_max", 0),
            _window_expr("window_rows_max", 1),
        )
        windowed, values, enabled, choices = _concrete_values(
            windows,
            (
                (1, None, None),
                (1, 10, 10),
                (1, 10, 20),
                (1, 20, 30),
            ),
        )
        assignment = {
            choice.term.atom: ordinal
            for choice, ordinal in zip(
                choices,
                (0, 1, 2, 3, 0, 2, 1, 3),
            )
        }
        self.assertTrue(_ground(enabled, assignment))
        self.assertEqual(len(choices), 8)
        self.assertEqual(
            tuple(
                None
                if _ground(row[windows[0]].is_null, assignment)
                else _ground(row[windows[0]].value, assignment)
                for row in values
            ),
            (None, 10, 20, 30),
        )
        self.assertEqual(
            tuple(
                None
                if _ground(row[windows[1]].is_null, assignment)
                else _ground(row[windows[1]].value, assignment)
                for row in values
            ),
            (None, 20, 20, 30),
        )
        self.assertFalse(windowed.sequence)
        self.assertIsNone(windowed.order)
        self.assertIsNone(windowed.ordinals)

    def test_running_sum_peer_order_and_null_prefix_are_exact(self):
        window = _window_expr("window_rows_sum")
        _windowed, values, enabled, choices = _concrete_values(
            (window,),
            (
                (1, None, None),
                (1, 10, 2),
                (1, 10, 3),
                (1, 20, None),
            ),
        )
        assignment = {
            choice.term.atom: ordinal
            for choice, ordinal in zip(choices, (0, 2, 1, 3))
        }
        self.assertTrue(_ground(enabled, assignment))
        self.assertEqual(
            tuple(
                None
                if _ground(row[window].is_null, assignment)
                else _ground(row[window].value, assignment)
                for row in values
            ),
            (None, 5, 3, 5),
        )

    def test_null_partitions_and_decimal_specials_are_exact(self):
        max_window = _window_expr("window_rows_max")
        _windowed, values, enabled, choices = _concrete_values(
            (max_window,),
            ((None, 1, 5), (None, 2, 7), (1, 1, 10)),
        )
        assignment = {
            choice.term.atom: ordinal
            for choice, ordinal in zip(choices, (0, 1, 0))
        }
        self.assertTrue(_ground(enabled, assignment))
        self.assertEqual(
            tuple(_ground(row[max_window].value, assignment) for row in values),
            (5, 7, 10),
        )

        sum_window = _window_expr("window_rows_sum", 0, "sum")
        special_max = _window_expr("window_rows_max", 1, "max")
        _windowed, values, enabled, choices = _concrete_values(
            (sum_window, special_max),
            (
                (1, 1, decimal.INF),
                (1, 2, -decimal.INF),
                (1, 3, decimal.NAN),
            ),
        )
        assignment = {
            choice.term.atom: ordinal
            for choice, ordinal in zip(choices, (0, 1, 2, 0, 1, 2))
        }
        self.assertTrue(_ground(enabled, assignment))
        self.assertEqual(
            tuple(_ground(row[sum_window].value, assignment) for row in values),
            (decimal.INF, decimal.NAN, decimal.NAN),
        )
        self.assertEqual(
            tuple(_ground(row[special_max].value, assignment) for row in values),
            (decimal.INF, decimal.INF, decimal.NAN),
        )

    def test_pair_construction_cap_fails_closed(self):
        window = _window_expr("window_rows_max")
        with (
            mock.patch.object(relation_model, "MAX_RELATION_ROW_PAIRS", 4),
            self.assertRaisesRegex(RelationError, "q51 ROWS window requires 5"),
        ):
            _concrete_values(
                (window,),
                ((1, 1, 1), (1, 2, 2)),
            )

    def test_item_hash_proves_and_date_hash_splits_a_partition(self):
        if SOLVER is None:
            self.skipTest("Z3 is not available")
        logical = parse_snapshot(_q51_shape_snapshot())
        item_hash = parse_snapshot(_staged_q51_snapshot(["item"]))
        date_hash = parse_snapshot(_staged_q51_snapshot(["d_date"]))
        self.assertEqual(
            solve(build_problem(logical, item_hash, 1), SOLVER, 1, 10_000).status,
            "VERIFIED_BOUNDED",
        )
        self.assertEqual(
            solve(build_problem(logical, date_hash, 1), SOLVER, 1, 10_000).status,
            "COUNTEREXAMPLE",
        )


if __name__ == "__main__":
    unittest.main()
