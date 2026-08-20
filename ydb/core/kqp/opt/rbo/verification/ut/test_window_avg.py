import os
import unittest
from collections import Counter
from unittest import mock

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import decimal, smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier import relation as relation_model
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    SnapshotError,
    parse_snapshot,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import (
    Database,
    Evaluator,
    RelationError,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import Encoder
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import Value
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import (
    build_logical_kernel_problem_for_tests,
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


def _window_avg(partition_by=None):
    return {
        "kind": "window_avg",
        "input": "group_total",
        "partition_by": partition_by or ["part_i", "part_s"],
        "type": "Decimal(35,2)",
        "nullable": True,
    }


def _snapshot():
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": {
            "tables": [
                {
                    "name": "Sales",
                    "columns": [
                        {"name": "part_i", "type": "Int64", "nullable": True},
                        {"name": "part_s", "type": "String", "nullable": True},
                        {"name": "item", "type": "Int64", "nullable": False},
                        {"name": "amount", "type": "Decimal(15,2)", "nullable": True},
                    ],
                    "unique_keys": [],
                }
            ]
        },
        "plan": {
            "nodes": [
                {
                    "id": "scan",
                    "op": "scan",
                    "table": "Sales",
                    "columns": [
                        {"source": name, "output": name}
                        for name in ("part_i", "part_s", "item", "amount")
                    ],
                    "pushed_limit": None,
                },
                {
                    "id": "aggregate",
                    "op": "aggregate",
                    "input": "scan",
                    "keys": ["part_i", "part_s", "item"],
                    "aggregates": [
                        {
                            "input": "amount",
                            "function": "sum",
                            "output": "group_total",
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
                    "id": "project",
                    "op": "project",
                    "input": "aggregate",
                    "ordered": False,
                    "columns": [
                        {"output": name, "expression": _column(name)}
                        for name in ("part_i", "part_s", "item", "group_total")
                    ]
                    + [
                        {
                            "output": "window_average",
                            "expression": _window_avg(),
                        }
                    ],
                },
            ],
            "root": "project",
            "output": [
                "part_i",
                "part_s",
                "item",
                "group_total",
                "window_average",
            ],
            "subplans": [],
        },
        "stage_graph": None,
    }


def _staged_snapshot(shuffle_keys):
    snapshot = _snapshot()
    snapshot["plan"]["nodes"][1:2] = [
        {
            "id": "intermediate",
            "op": "aggregate",
            "input": "scan",
            "keys": ["part_i", "part_s", "item"],
            "aggregates": [
                {
                    "input": "amount",
                    "function": "sum",
                    "output": "partial_total",
                    "type": "Decimal(35,2)",
                    "nullable": True,
                    "distinct": False,
                    "unwrap": False,
                }
            ],
            "phase": "intermediate",
            "distinct_all": False,
        },
        {
            "id": "aggregate",
            "op": "aggregate",
            "input": "intermediate",
            "keys": ["part_i", "part_s", "item"],
            "aggregates": [
                {
                    "input": "partial_total",
                    "function": "sum",
                    "output": "group_total",
                    "type": "Decimal(35,2)",
                    "nullable": True,
                    "distinct": False,
                    "unwrap": False,
                }
            ],
            "phase": "final",
            "distinct_all": False,
        },
    ]
    snapshot["stage_graph"] = {
        "root_stage": "window",
        "stages": [
            {
                "id": "source",
                "nodes": ["scan", "intermediate"],
                "inputs": [],
                "outputs": [{"index": 0, "node": "intermediate"}],
                "source_storage": "column",
            },
            {
                "id": "final",
                "nodes": ["aggregate"],
                "inputs": ["intermediate"],
                "outputs": [{"index": 0, "node": "aggregate"}],
                "source_storage": None,
            },
            {
                "id": "window",
                "nodes": ["project"],
                "inputs": ["aggregate"],
                "outputs": [{"index": 0, "node": "project"}],
                "source_storage": None,
            },
        ],
        "edges": [
            {
                "id": "source-final-0",
                "producer": "source",
                "consumer": "final",
                "occurrence": 0,
                "producer_output": 0,
                "consumer_input": 0,
                "kind": "hash_shuffle",
                "keys": ["part_i", "part_s", "item"],
                "hash_function": "HashV2",
                "use_spilling": False,
            },
            {
                "id": "final-window-0",
                "producer": "final",
                "consumer": "window",
                "occurrence": 0,
                "producer_output": 0,
                "consumer_input": 0,
                "kind": "hash_shuffle",
                "keys": shuffle_keys,
                "hash_function": "HashV2",
                "use_spilling": False,
            }
        ],
        "assumptions": [],
    }
    return snapshot


def _q89_shaped_snapshot():
    snapshot = _snapshot()
    table_columns = snapshot["schema"]["tables"][0]["columns"]
    table_columns[0]["type"] = "String"
    table_columns[2:2] = [
        {"name": "part_s2", "type": "String", "nullable": True},
        {"name": "part_s3", "type": "String", "nullable": True},
    ]
    scan_columns = snapshot["plan"]["nodes"][0]["columns"]
    scan_columns[2:2] = [
        {"source": "part_s2", "output": "part_s2"},
        {"source": "part_s3", "output": "part_s3"},
    ]
    aggregate = snapshot["plan"]["nodes"][1]
    aggregate["keys"][2:2] = ["part_s2", "part_s3"]
    project = snapshot["plan"]["nodes"][2]
    project["columns"][2:2] = [
        {"output": "part_s2", "expression": _column("part_s2")},
        {"output": "part_s3", "expression": _column("part_s3")},
    ]
    project["columns"][-1]["expression"]["partition_by"] = [
        "part_i",
        "part_s",
        "part_s2",
        "part_s3",
    ]
    snapshot["plan"]["output"][2:2] = ["part_s2", "part_s3"]
    return snapshot


def _deviation_snapshot():
    snapshot = _snapshot()
    snapshot["plan"]["nodes"][2]["columns"][-1]["expression"] = {
        "kind": "decimal_abs",
        "arg": {
            "kind": "sub",
            "left": _column("group_total"),
            "right": _window_avg(),
            "type": "Decimal(35,2)",
            "nullable": True,
        },
        "type": "Decimal(35,2)",
        "nullable": True,
    }
    return snapshot


def _decimal_abs_snapshot():
    snapshot = _snapshot()
    snapshot["schema"]["tables"][0]["columns"] = [
        {"name": "value", "type": "Decimal(35,2)", "nullable": True}
    ]
    snapshot["plan"] = {
        "nodes": [
            {
                "id": "scan",
                "op": "scan",
                "table": "Sales",
                "columns": [{"source": "value", "output": "value"}],
                "pushed_limit": None,
            },
            {
                "id": "project",
                "op": "project",
                "input": "scan",
                "ordered": False,
                "columns": [
                    {
                        "output": "absolute",
                        "expression": {
                            "kind": "decimal_abs",
                            "arg": _column("value"),
                            "type": "Decimal(35,2)",
                            "nullable": True,
                        },
                    }
                ],
            },
        ],
        "root": "project",
        "output": ["absolute"],
        "subplans": [],
    }
    return snapshot


def _ground(term, values):
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
    if term.operation == "-":
        return arguments[0] - arguments[1]
    if term.operation == "*":
        result = 1
        for argument in arguments:
            result *= argument
        return result
    if term.operation == "div":
        return arguments[0] // arguments[1]
    if term.operation == "mod":
        return arguments[0] % arguments[1]
    raise AssertionError(f"unsupported ground operation {term.operation!r}")


def _evaluate(snapshot, slots):
    parsed = parse_snapshot(snapshot)
    script = smt.Script()
    database = Database(parsed, len(slots), script)
    relation = Evaluator(parsed, database, Encoder(script)).root().certain()
    values = {}
    table_columns = tuple(column.name for column in parsed.tables[0].columns)
    for witness, slot in zip(database.witness["Sales"], slots):
        values[witness.present.atom] = True
        for name, concrete in zip(table_columns, slot):
            cell = witness.cells[name]
            if cell.is_null.operation == "symbol":
                values[cell.is_null.atom] = concrete is None
            values[cell.value.atom] = 0 if concrete is None else concrete

    bag = Counter()
    for row in relation.rows:
        if not _ground(row.present, values):
            continue
        bag[
            tuple(
                None
                if _ground(row.values[column.name].is_null, values)
                else _ground(row.values[column.name].value, values)
                for column in relation.columns
            )
        ] += 1
    return bag


def _concrete_window_average(rows):
    """Evaluate the relational leaf over already-grouped concrete rows."""

    snapshot = parse_snapshot(_snapshot())
    expression = snapshot.plan.nodes[2].columns[-1].expression
    relation_rows = []
    for part_i, part_s, group_total in rows:
        relation_rows.append(
            relation_model.Row(
                smt.TRUE,
                {
                    "part_i": Value(
                        "Int64",
                        smt.bool_value(part_i is None),
                        smt.int_value(0 if part_i is None else part_i),
                    ),
                    "part_s": Value(
                        "String",
                        smt.bool_value(part_s is None),
                        smt.int_value(0 if part_s is None else part_s),
                    ),
                    "group_total": Value(
                        "Decimal(35,2)",
                        smt.bool_value(group_total is None),
                        smt.int_value(0 if group_total is None else group_total),
                        (
                            0
                            if group_total is None
                            or not -decimal.INF < group_total < decimal.INF
                            else abs(group_total)
                        ),
                    ),
                },
            )
        )
    source = relation_model.Relation(
        (
            relation_model.Column("part_i", "Int64", True),
            relation_model.Column("part_s", "String", True),
            relation_model.Column("group_total", "Decimal(35,2)", True),
        ),
        tuple(relation_rows),
    )
    evaluator = object.__new__(Evaluator)
    evaluator.scalar = Encoder(smt.Script())
    return evaluator._whole_partition_decimal_window_value(
        expression,
        source,
        relation_rows[0],
    )


class WindowAverageTest(unittest.TestCase):
    def test_multikey_null_partitions_all_null_inputs_and_even_ties(self):
        tied = _concrete_window_average(
            ((None, None, 100), (None, None, 101), (None, 7, 999))
        )
        self.assertFalse(_ground(tied.is_null, {}))
        self.assertEqual(_ground(tied.value, {}), 100)

        all_null = _concrete_window_average(((1, 10, None),))
        self.assertTrue(_ground(all_null.is_null, {}))

    def test_decimal_specials_flow_through_window_average(self):
        cases = (
            ((decimal.INF, 100), decimal.INF),
            ((-decimal.INF, 100), -decimal.INF),
            ((decimal.INF, -decimal.INF), decimal.NAN),
            ((decimal.NAN,), decimal.NAN),
            ((101, 102), 102),
        )
        for inputs, expected in cases:
            finite_abs_bound = sum(
                abs(value)
                for value in inputs
                if -decimal.INF < value < decimal.INF
            )
            result = relation_model._finish_decimal_average(
                tuple((smt.TRUE, smt.int_value(value)) for value in inputs),
                tuple(smt.ONE for _value in inputs),
                sum_type="Decimal(35,2)",
                count_type="Uint64",
                output_type="Decimal(35,2)",
                output_nullable=True,
                finite_abs_bound=finite_abs_bound,
                count_bound=len(inputs),
                carry_state=False,
                operation="test window avg",
            )
            with self.subTest(inputs=inputs):
                self.assertFalse(_ground(result.is_null, {}))
                self.assertEqual(_ground(result.value, {}), expected)

    def test_decimal_abs_is_exact_for_null_finite_infinities_and_nan(self):
        self.assertEqual(
            _evaluate(
                _decimal_abs_snapshot(),
                ((-123,), (45,), (-decimal.INF,), (decimal.INF,), (decimal.NAN,), (None,)),
            ),
            Counter(
                {
                    (123,): 1,
                    (45,): 1,
                    (decimal.INF,): 2,
                    (decimal.NAN,): 1,
                    (None,): 1,
                }
            ),
        )

    def test_window_avg_and_decimal_abs_json_grammars_are_closed(self):
        mixed = parse_snapshot(_snapshot())
        self.assertEqual(
            mixed.plan.nodes[2].columns[-1].expression.partition_by,
            ("part_i", "part_s"),
        )
        q89 = parse_snapshot(_q89_shaped_snapshot())
        self.assertEqual(
            q89.plan.nodes[2].columns[-1].expression.partition_by,
            ("part_i", "part_s", "part_s2", "part_s3"),
        )
        duplicate_sum = _snapshot()
        duplicate_sum["plan"]["nodes"][1]["aggregates"].append(
            {
                "input": "amount",
                "function": "sum",
                "output": "sum_sales",
                "type": "Decimal(35,2)",
                "nullable": True,
                "distinct": False,
                "unwrap": False,
            }
        )
        parse_snapshot(duplicate_sum)

        mutations = (
            ("partition_by", "part_i", "expected an array"),
            ("partition_by", [], "between 1 and 4"),
            ("partition_by", ["part_i"] * 5, "between 1 and 4"),
            ("partition_by", ["part_i", "part_i"], "duplicate name"),
            ("partition_by", ["missing"], "not available"),
            ("partition_by", ["item"], "Optional<Int64> or Optional<String>"),
            ("input", "missing", "window input column"),
            ("nullable", False, "Optional<Decimal"),
            ("type", "Decimal(34,2)", "Optional<Decimal"),
        )
        for field, value, message in mutations:
            raw = _snapshot()
            raw["plan"]["nodes"][2]["columns"][-1]["expression"][field] = value
            with self.subTest(field=field, value=value), self.assertRaisesRegex(
                SnapshotError, message
            ):
                parse_snapshot(raw)

        raw = _snapshot()
        raw["plan"]["nodes"][2]["columns"][-1]["expression"]["order_by"] = []
        with self.assertRaisesRegex(SnapshotError, "unknown fields"):
            parse_snapshot(raw)

        for field, value, message in (
            ("nullable", False, "Optional<Decimal"),
            ("type", "Decimal(34,2)", "Optional<Decimal"),
            (
                "arg",
                {"kind": "null", "type": "Int64"},
                "input must be Optional<Decimal",
            ),
        ):
            raw = _decimal_abs_snapshot()
            raw["plan"]["nodes"][1]["columns"][0]["expression"][field] = value
            with self.subTest(abs_field=field), self.assertRaisesRegex(
                SnapshotError, message
            ):
                parse_snapshot(raw)

    def test_window_avg_rejects_non_sum_stale_and_final_topologies(self):
        non_sum = _snapshot()
        non_sum["schema"]["tables"][0]["columns"][-1]["type"] = "Decimal(35,2)"
        non_sum["plan"]["nodes"][1]["aggregates"][0]["function"] = "max"
        non_sum["plan"]["nodes"][1]["aggregates"].append(
            {
                "input": "amount",
                "function": "sum",
                "output": "sum_sales",
                "type": "Decimal(35,2)",
                "nullable": True,
                "distinct": False,
                "unwrap": False,
            }
        )
        with self.assertRaisesRegex(SnapshotError, "direct Optional<Decimal.*SUM"):
            parse_snapshot(non_sum)

        stale = _snapshot()
        stale["plan"]["nodes"].insert(
            2,
            {
                "id": "carrier",
                "op": "project",
                "input": "aggregate",
                "ordered": False,
                "columns": [
                    {"output": name, "expression": _column(name)}
                    for name in ("part_i", "part_s", "item", "group_total")
                ],
            },
        )
        stale["plan"]["nodes"][3]["input"] = "carrier"
        with self.assertRaisesRegex(SnapshotError, "directly consume"):
            parse_snapshot(stale)

        mismatched_final = _staged_snapshot(["part_i", "part_s"])
        mismatched_final["stage_graph"] = None
        mismatched_final["plan"]["nodes"][1]["keys"] = [
            "part_s",
            "part_i",
            "item",
        ]
        with self.assertRaisesRegex(SnapshotError, "matching intermediate"):
            parse_snapshot(mismatched_final)

    def test_window_avg_rejects_fanout_subplans_and_multiple_occurrences(self):
        fanout = _snapshot()
        fanout["plan"]["nodes"].append(
            {
                "id": "plain",
                "op": "project",
                "input": "aggregate",
                "ordered": False,
                "columns": [
                    {"output": name, "expression": _column(name)}
                    for name in ("part_i", "part_s", "item", "group_total")
                ]
                + [{"output": "window_average", "expression": _column("group_total")}],
            }
        )
        fanout["plan"]["nodes"].append(
            {
                "id": "union",
                "op": "union_all",
                "inputs": [
                    {
                        "node": "project",
                        "columns": [
                            "part_i",
                            "part_s",
                            "item",
                            "group_total",
                            "window_average",
                        ],
                    },
                    {
                        "node": "plain",
                        "columns": [
                            "part_i",
                            "part_s",
                            "item",
                            "group_total",
                            "window_average",
                        ],
                    },
                ],
                "output": [
                    "part_i",
                    "part_s",
                    "item",
                    "group_total",
                    "window_average",
                ],
                "ordered": False,
            }
        )
        fanout["plan"]["root"] = "union"
        with self.assertRaisesRegex(SnapshotError, "no fanout"):
            parse_snapshot(fanout)

        subplan = _snapshot()
        subplan["plan"]["nodes"].extend(
            [
                {
                    "id": "subscan",
                    "op": "scan",
                    "table": "Sales",
                    "columns": [{"source": "item", "output": "sub.item"}],
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
                            "expression": {"kind": "literal", "type": "Int64", "value": 7},
                        }
                    ],
                },
            ]
        )
        subplan["plan"]["nodes"][2]["columns"].append(
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
                "consumers": ["project"],
                "output": {"column": "scalar", "type": "Int64", "nullable": False},
            }
        ]
        with self.assertRaisesRegex(SnapshotError, "does not admit subplans"):
            parse_snapshot(subplan)

        multiple = _snapshot()
        multiple["plan"]["nodes"][2]["columns"].append(
            {"output": "again", "expression": _window_avg()}
        )
        with self.assertRaisesRegex(SnapshotError, "exactly one window_avg"):
            parse_snapshot(multiple)

    def test_window_avg_pair_and_accumulator_caps_fail_closed(self):
        snapshot = parse_snapshot(_snapshot())
        script = smt.Script()
        evaluator = Evaluator(snapshot, Database(snapshot, 2, script), Encoder(script))
        with (
            mock.patch.object(relation_model, "MAX_RELATION_ROW_PAIRS", 3),
            self.assertRaisesRegex(RelationError, "window avg requires 4"),
        ):
            evaluator.root()

        with self.assertRaisesRegex(RelationError, "sum may overflow"):
            relation_model._finish_decimal_average(
                ((smt.TRUE, smt.ONE),),
                (smt.ONE,),
                sum_type="Decimal(35,2)",
                count_type="Uint64",
                output_type="Decimal(35,2)",
                output_nullable=True,
                finite_abs_bound=10**35,
                count_bound=1,
                carry_state=False,
                operation="test avg",
            )
        with self.assertRaisesRegex(RelationError, "count may wrap"):
            relation_model._finish_decimal_average(
                ((smt.TRUE, smt.ONE),),
                (smt.ONE,),
                sum_type="Decimal(35,2)",
                count_type="Uint64",
                output_type="Decimal(35,2)",
                output_nullable=True,
                finite_abs_bound=1,
                count_bound=1 << 64,
                carry_state=False,
                operation="test avg",
            )

    def test_window_avg_builds_and_solves_an_exact_obligation(self):
        if SOLVER is None:
            self.skipTest("Z3 is not available")
        # This is the exact q53/q63/q89 scalar neighborhood:
        # Abs(grouped SUM - whole-partition AVG(grouped SUM)).
        snapshot = parse_snapshot(_deviation_snapshot())
        problem = build_logical_kernel_problem_for_tests(snapshot, snapshot, 2)
        self.assertIn("check-sat", problem.formula())
        self.assertEqual(
            solve(problem, SOLVER, 2, 10_000).status,
            "VERIFIED_BOUNDED",
        )

    def test_partition_routing_proves_and_wrong_routing_has_a_counterexample(self):
        if SOLVER is None:
            self.skipTest("Z3 is not available")
        logical = parse_snapshot(_snapshot())
        colocated = parse_snapshot(_staged_snapshot(["part_i", "part_s"]))
        split = parse_snapshot(
            _staged_snapshot(["part_i", "part_s", "item"])
        )
        self.assertEqual(
            solve(build_problem(logical, colocated, 2), SOLVER, 2, 10_000).status,
            "VERIFIED_BOUNDED",
        )
        self.assertEqual(
            solve(build_problem(logical, split, 2), SOLVER, 2, 10_000).status,
            "COUNTEREXAMPLE",
        )

    def test_q89_shaped_four_key_partition_mutation_has_a_counterexample(self):
        if SOLVER is None:
            self.skipTest("Z3 is not available")
        before = parse_snapshot(_q89_shaped_snapshot())
        mutated = _q89_shaped_snapshot()
        mutated["plan"]["nodes"][2]["columns"][-1]["expression"][
            "partition_by"
        ] = ["part_i", "part_s", "part_s2"]
        after = parse_snapshot(mutated)
        self.assertEqual(
            solve(
                build_logical_kernel_problem_for_tests(before, after, 2),
                SOLVER,
                2,
                10_000,
            ).status,
            "COUNTEREXAMPLE",
        )


if __name__ == "__main__":
    unittest.main()
