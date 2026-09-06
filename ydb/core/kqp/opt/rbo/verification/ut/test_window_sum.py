import os
import unittest
from collections import Counter
from unittest import mock

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import smt
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
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import (
    build_problem,
    build_logical_kernel_problem_for_tests,
    solve,
)


SOLVER = (
    yatest_common.binary_path("contrib/tools/z3/z3")
    if yatest_common is not None
    else os.environ.get("RBO_Z3")
)


def _column(name):
    return {"kind": "column", "column": name}


def _window_sum():
    return {
        "kind": "window_sum",
        "input": "group_total",
        "partition_by": "class",
        "type": "Decimal(35,2)",
        "nullable": True,
    }


def _snapshot():
    window = _window_sum()
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": {
            "tables": [
                {
                    "name": "Sales",
                    "columns": [
                        {"name": "class", "type": "String", "nullable": True},
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
                        {"source": "class", "output": "class"},
                        {"source": "item", "output": "item"},
                        {"source": "amount", "output": "amount"},
                    ],
                    "pushed_limit": None,
                },
                {
                    "id": "aggregate",
                    "op": "aggregate",
                    "input": "scan",
                    "keys": ["class", "item"],
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
                        {"output": "class", "expression": _column("class")},
                        {"output": "item", "expression": _column("item")},
                        {
                            "output": "group_total",
                            "expression": _column("group_total"),
                        },
                        {
                            "output": "window_total",
                            "expression": {
                                "kind": "add",
                                "left": window,
                                "right": {
                                    "kind": "literal",
                                    "type": "Decimal(35,2)",
                                    "value": {"kind": "finite", "scaled": "0"},
                                },
                                "type": "Decimal(35,2)",
                                "nullable": True,
                            },
                        },
                    ],
                },
            ],
            "root": "project",
            "output": ["class", "item", "group_total", "window_total"],
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
            "keys": ["class", "item"],
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
            "keys": ["class", "item"],
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
                "id": "window",
                "nodes": ["aggregate", "project"],
                "inputs": ["intermediate"],
                "outputs": [{"index": 0, "node": "project"}],
                "source_storage": None,
            },
        ],
        "edges": [
            {
                "id": "source-window-0",
                "producer": "source",
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


class WindowSumTest(unittest.TestCase):
    def test_nested_window_sum_preserves_groups_bags_nulls_and_null_partition(self):
        snapshot = parse_snapshot(_snapshot())
        script = smt.Script()
        database = Database(snapshot, 5, script)
        relation = Evaluator(snapshot, database, Encoder(script)).root().certain()
        slots = (
            (1, 1, 100),
            (1, 1, 100),
            (1, 2, 300),
            (None, 3, 200),
            (None, 4, None),
        )
        values = {}
        for witness, slot in zip(database.witness["Sales"], slots):
            values[witness.present.atom] = True
            for name, concrete in zip(("class", "item", "amount"), slot):
                cell = witness.cells[name]
                if cell.is_null.operation == "symbol":
                    values[cell.is_null.atom] = concrete is None
                values[cell.value.atom] = 0 if concrete is None else concrete

        bag = Counter()
        for row in relation.rows:
            if not _ground(row.present, values):
                continue
            output = []
            for column in relation.columns:
                value = row.values[column.name]
                output.append(
                    None if _ground(value.is_null, values) else _ground(value.value, values)
                )
            bag[tuple(output)] += 1

        self.assertEqual(
            bag,
            Counter(
                {
                    (1, 1, 200, 500): 1,
                    (1, 2, 300, 500): 1,
                    (None, 3, 200, 200): 1,
                    (None, 4, None, 200): 1,
                }
            ),
        )
        self.assertFalse(relation.sequence)
        self.assertEqual(len(relation.rows), 5)

    def test_window_sum_builds_and_solves_an_exact_obligation(self):
        if SOLVER is None:
            self.skipTest("Z3 is not available")
        snapshot = parse_snapshot(_snapshot())
        problem = build_logical_kernel_problem_for_tests(snapshot, snapshot, 2)
        self.assertIn("check-sat", problem.formula())
        self.assertEqual(solve(problem, SOLVER, 2, 10_000).status, "VERIFIED_BOUNDED")

    def test_window_sum_json_shape_is_closed(self):
        for field, value, message in (
            ("nullable", False, "Optional<Decimal"),
            ("type", "Decimal(34,2)", "Optional<Decimal"),
            ("input", "missing", "window input column"),
            ("partition_by", "item", "Optional<String>"),
        ):
            raw = _snapshot()
            window = raw["plan"]["nodes"][2]["columns"][3]["expression"]["left"]
            window[field] = value
            with self.subTest(field=field), self.assertRaisesRegex(SnapshotError, message):
                parse_snapshot(raw)

        raw = _snapshot()
        window = raw["plan"]["nodes"][2]["columns"][3]["expression"]["left"]
        window["order_by"] = []
        with self.assertRaisesRegex(SnapshotError, "unknown fields"):
            parse_snapshot(raw)

    def test_window_sum_rejects_stale_shape_and_fanout(self):
        stale = _snapshot()
        stale["plan"]["nodes"].insert(
            2,
            {
                "id": "carrier",
                "op": "project",
                "input": "aggregate",
                "ordered": False,
                "columns": [
                    {"output": "class", "expression": _column("class")},
                    {"output": "item", "expression": _column("item")},
                    {"output": "group_total", "expression": _column("group_total")},
                ],
            },
        )
        stale["plan"]["nodes"][3]["input"] = "carrier"
        with self.assertRaisesRegex(SnapshotError, "directly consume"):
            parse_snapshot(stale)

        fanout = _snapshot()
        fanout["plan"]["nodes"].extend(
            [
                {
                    "id": "plain",
                    "op": "project",
                    "input": "aggregate",
                    "ordered": False,
                    "columns": [
                        {"output": "class", "expression": _column("class")},
                        {"output": "item", "expression": _column("item")},
                        {"output": "group_total", "expression": _column("group_total")},
                        {"output": "window_total", "expression": _column("group_total")},
                    ],
                },
                {
                    "id": "union",
                    "op": "union_all",
                    "inputs": [
                        {
                            "node": "project",
                            "columns": ["class", "item", "group_total", "window_total"],
                        },
                        {
                            "node": "plain",
                            "columns": ["class", "item", "group_total", "window_total"],
                        },
                    ],
                    "output": ["class", "item", "group_total", "window_total"],
                    "ordered": False,
                },
            ]
        )
        fanout["plan"]["root"] = "union"
        with self.assertRaisesRegex(SnapshotError, "no fanout"):
            parse_snapshot(fanout)

    def test_hashing_all_aggregate_keys_can_split_a_window_partition(self):
        if SOLVER is None:
            self.skipTest("Z3 is not available")
        logical = parse_snapshot(_snapshot())
        split_partition = parse_snapshot(_staged_snapshot(["class", "item"]))
        split_result = solve(
            build_problem(logical, split_partition, 2),
            SOLVER,
            2,
            10_000,
        )
        self.assertEqual(split_result.status, "COUNTEREXAMPLE")

    def test_hashing_the_window_partition_preserves_the_logical_result(self):
        if SOLVER is None:
            self.skipTest("Z3 is not available")
        logical = parse_snapshot(_snapshot())
        colocated_partition = parse_snapshot(_staged_snapshot(["class"]))
        colocated_result = solve(
            build_problem(logical, colocated_partition, 2),
            SOLVER,
            2,
            10_000,
        )
        self.assertEqual(colocated_result.status, "VERIFIED_BOUNDED")

    def test_window_sum_rejects_subplans(self):
        raw = _snapshot()
        raw["plan"]["nodes"].extend(
            [
                {
                    "id": "subscan",
                    "op": "scan",
                    "table": "Sales",
                    "columns": [
                        {"source": "item", "output": "sub.item"},
                    ],
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
        raw["plan"]["nodes"][2]["columns"].append(
            {"output": "unused", "expression": _column("$scalar")}
        )
        raw["plan"]["subplans"] = [
            {
                "binding": "$scalar",
                "kind": "scalar",
                "root": "subproject",
                "type": "Int64",
                "nullable": True,
                "dependencies": [],
                "consumers": ["project"],
                "output": {
                    "column": "scalar",
                    "type": "Int64",
                    "nullable": False,
                },
            }
        ]
        with self.assertRaisesRegex(SnapshotError, "separate from subplan evaluation"):
            parse_snapshot(raw)

    def test_window_sum_rejects_multiple_occurrences_and_pair_overflow(self):
        multiple = _snapshot()
        multiple["plan"]["nodes"][2]["columns"].append(
            {"output": "again", "expression": _window_sum()}
        )
        with self.assertRaisesRegex(SnapshotError, "exactly one window_sum"):
            parse_snapshot(multiple)

        snapshot = parse_snapshot(_snapshot())
        script = smt.Script()
        evaluator = Evaluator(snapshot, Database(snapshot, 2, script), Encoder(script))
        with (
            mock.patch.object(relation_model, "MAX_RELATION_ROW_PAIRS", 3),
            self.assertRaisesRegex(RelationError, "window sum requires 4"),
        ):
            evaluator.root()


if __name__ == "__main__":
    unittest.main()
