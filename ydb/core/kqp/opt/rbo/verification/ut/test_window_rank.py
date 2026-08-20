import os
import unittest

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import decimal, smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier import relation as relation_model
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    Expr,
    SnapshotError,
    parse_snapshot,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import (
    Evaluator,
    Outcome,
    RelationFamily,
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


def _rank(name, execution_order, key):
    return {
        "kind": "window_rank",
        "window_name": name,
        "execution_order": execution_order,
        "partition_by": [],
        "order_by": [
            {
                "column": key,
                "ascending": True,
                "nulls_first": True,
            }
        ],
        "frame": "rows_unbounded_preceding_current_row",
        "type": "Uint64",
        "nullable": False,
    }


def _snapshot(rank_count=2):
    ranks = [
        _rank(f"window{index}", index, "key_a" if index == 0 else "key_b")
        for index in range(rank_count)
    ]
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": {
            "tables": [
                {
                    "name": "Rows",
                    "columns": [
                        {"name": "id", "type": "Int64", "nullable": False},
                        {
                            "name": "key_a",
                            "type": "Decimal(15,4)",
                            "nullable": False,
                        },
                        {
                            "name": "key_b",
                            "type": "Decimal(15,4)",
                            "nullable": False,
                        },
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
                    "table": "Rows",
                    "columns": [
                        {"source": name, "output": name}
                        for name in ("id", "key_a", "key_b")
                    ],
                    "pushed_limit": None,
                },
                {
                    "id": "rank",
                    "op": "project",
                    "input": "scan",
                    "ordered": False,
                    "columns": [
                        {"output": name, "expression": _column(name)}
                        for name in ("id", "key_a", "key_b")
                    ]
                    + [
                        {
                            "output": f"rank_{index}",
                            "expression": rank,
                        }
                        for index, rank in enumerate(ranks)
                    ],
                },
            ],
            "root": "rank",
            "output": ["id"]
            + [f"rank_{index}" for index in range(rank_count)],
            "subplans": [],
        },
        "stage_graph": None,
    }


def _staged_snapshot(connection, rank_count=2):
    snapshot = _snapshot(rank_count)
    snapshot["stage_graph"] = {
        "root_stage": "rank_stage",
        "stages": [
            {
                "id": "source",
                "nodes": ["scan"],
                "inputs": [],
                "outputs": [{"index": 0, "node": "scan"}],
                "source_storage": "row",
            },
            {
                "id": "rank_stage",
                "nodes": ["rank"],
                "inputs": ["scan"],
                "outputs": [{"index": 0, "node": "rank"}],
                "source_storage": None,
            },
        ],
        "edges": [
            {
                "id": "source-rank-0",
                "producer": "source",
                "consumer": "rank_stage",
                "occurrence": 0,
                "producer_output": 0,
                "consumer_input": 0,
            }
            | connection
        ],
        "assumptions": [],
    }
    return snapshot


def _cast_snapshot(result_type="Decimal(15,4)"):
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": {
            "tables": [
                {
                    "name": "Rows",
                    "columns": [
                        {
                            "name": "source",
                            "type": "Decimal(35,2)",
                            "nullable": False,
                        }
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
                    "table": "Rows",
                    "columns": [{"source": "source", "output": "source"}],
                    "pushed_limit": None,
                },
                {
                    "id": "cast",
                    "op": "project",
                    "input": "scan",
                    "ordered": False,
                    "columns": [
                        {
                            "output": "result",
                            "expression": {
                                "kind": "cast_decimal",
                                "arg": _column("source"),
                                "source_type": "Decimal(35,2)",
                                "type": result_type,
                                "nullable": False,
                            },
                        }
                    ],
                },
            ],
            "root": "cast",
            "output": ["result"],
            "subplans": [],
        },
        "stage_graph": None,
    }


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
    if term.operation == "-":
        return arguments[0] - arguments[1]
    if term.operation == "*":
        result = 1
        for argument in arguments:
            result *= argument
        return result
    raise AssertionError(f"unsupported ground operation {term.operation!r}")


def _concrete_rank_values(keys):
    snapshot = parse_snapshot(_snapshot())
    project = snapshot.plan.nodes[1]
    ranks = tuple(
        projection.expression
        for projection in project.columns
        if projection.expression.kind == "window_rank"
    )
    rows = tuple(
        relation_model.Row(
            smt.TRUE,
            {
                "id": Value("Int64", smt.FALSE, smt.int_value(index)),
                "key_a": Value(
                    "Decimal(15,4)",
                    smt.FALSE,
                    smt.int_value(key_a),
                ),
                "key_b": Value(
                    "Decimal(15,4)",
                    smt.FALSE,
                    smt.int_value(key_b),
                ),
            },
        )
        for index, (key_a, key_b) in enumerate(keys)
    )
    source = relation_model.Relation(
        (
            relation_model.Column("id", "Int64", False),
            relation_model.Column("key_a", "Decimal(15,4)", False),
            relation_model.Column("key_b", "Decimal(15,4)", False),
        ),
        rows,
    )
    evaluator = object.__new__(Evaluator)
    script = smt.Script()
    evaluator.scalar = Encoder(script)
    evaluator.choice_scope = "test"
    ranked, values, enabled, choices = evaluator._window_rank_values(
        project,
        source,
        ranks,
        0,
    )
    return ranks, ranked, values, enabled, choices, script


class WindowRankTest(unittest.TestCase):
    def test_decimal_rank_key_cast_thresholds_and_specials_are_exact(self):
        threshold = 10**13
        cases = (
            (-decimal.INF, -decimal.INF),
            (-threshold, -decimal.INF),
            (-threshold + 1, (-threshold + 1) * 100),
            (0, 0),
            (threshold - 1, (threshold - 1) * 100),
            (threshold, decimal.INF),
            (decimal.INF, decimal.INF),
            (decimal.NAN, decimal.NAN),
        )
        for source, expected in cases:
            with self.subTest(source=source):
                actual = decimal.cast_decimal(
                    smt.int_value(source),
                    "Decimal(35,2)",
                    "Decimal(15,4)",
                )
                self.assertEqual(_ground(actual), expected)

        expression = parse_snapshot(_cast_snapshot()).plan.nodes[1].columns[
            0
        ].expression
        encoded = Encoder(smt.Script()).evaluate(
            expression,
            {
                "source": Value(
                    "Decimal(35,2)",
                    smt.FALSE,
                    smt.ZERO,
                    10**35 - 1,
                )
            },
        )
        self.assertEqual(
            encoded.decimal_finite_abs_bound,
            (10**13 - 1) * 100,
        )
        with self.assertRaisesRegex(SnapshotError, "exact Decimal"):
            parse_snapshot(_cast_snapshot("Decimal(15,3)"))

    def test_rank_json_and_dataflow_gates_are_closed(self):
        parsed = parse_snapshot(_snapshot())
        ranks = tuple(
            projection.expression
            for projection in parsed.plan.nodes[1].columns
            if projection.expression.kind == "window_rank"
        )
        self.assertEqual(
            tuple(
                (rank.window_name, rank.execution_order)
                for rank in ranks
            ),
            (("window0", 0), ("window1", 1)),
        )

        mutations = (
            ("window_name", "", "non-empty string"),
            ("partition_by", ["id"], "empty partition"),
            ("frame", "rows_unbounded", "frame must be"),
            ("type", "Int64", "non-null Uint64"),
            ("nullable", True, "non-null Uint64"),
        )
        for field, value, message in mutations:
            raw = _snapshot()
            raw["plan"]["nodes"][1]["columns"][3]["expression"][field] = value
            with self.subTest(field=field), self.assertRaisesRegex(
                SnapshotError,
                message,
            ):
                parse_snapshot(raw)

        for order_field, value in (
            ("ascending", False),
            ("nulls_first", False),
        ):
            raw = _snapshot()
            raw["plan"]["nodes"][1]["columns"][3]["expression"][
                "order_by"
            ][0][order_field] = value
            with self.subTest(order_field=order_field), self.assertRaisesRegex(
                SnapshotError,
                "ascending nulls-first",
            ):
                parse_snapshot(raw)

        duplicate_name = _snapshot()
        duplicate_name["plan"]["nodes"][1]["columns"][4]["expression"][
            "window_name"
        ] = "window0"
        with self.assertRaisesRegex(SnapshotError, "duplicate name"):
            parse_snapshot(duplicate_name)

        duplicate_order = _snapshot()
        duplicate_order["plan"]["nodes"][1]["columns"][4]["expression"][
            "execution_order"
        ] = 0
        with self.assertRaisesRegex(SnapshotError, "complete distinct range"):
            parse_snapshot(duplicate_order)

        nested = _snapshot(1)
        rank = nested["plan"]["nodes"][1]["columns"][3]["expression"]
        nested["plan"]["nodes"][1]["columns"][3]["expression"] = {
            "kind": "add",
            "left": rank,
            "right": {"kind": "literal", "type": "Uint64", "value": 0},
            "type": "Uint64",
            "nullable": False,
        }
        with self.assertRaisesRegex(SnapshotError, "complete top-level"):
            parse_snapshot(nested)

        too_many = _snapshot()
        too_many["plan"]["nodes"][1]["columns"].append(
            {
                "output": "rank_2",
                "expression": _rank("window2", 2, "key_a"),
            }
        )
        too_many["plan"]["output"].append("rank_2")
        with self.assertRaisesRegex(SnapshotError, "Project audit bound"):
            parse_snapshot(too_many)

        nullable_key = _snapshot()
        nullable_key["schema"]["tables"][0]["columns"][1]["nullable"] = True
        with self.assertRaisesRegex(SnapshotError, r"non-null Decimal\(15,4\)"):
            parse_snapshot(nullable_key)

        fanout = _snapshot()
        projected = ["id", "key_a", "key_b", "rank_0", "rank_1"]
        for branch in ("left", "right"):
            fanout["plan"]["nodes"].append(
                {
                    "id": branch,
                    "op": "filter",
                    "input": "rank",
                    "predicate": {
                        "kind": "literal",
                        "type": "Bool",
                        "value": True,
                    },
                }
            )
        fanout["plan"]["nodes"].append(
            {
                "id": "union",
                "op": "union_all",
                "inputs": [
                    {"node": branch, "columns": projected}
                    for branch in ("left", "right")
                ],
                "output": projected,
                "ordered": False,
            }
        )
        fanout["plan"]["root"] = "union"
        with self.assertRaisesRegex(SnapshotError, "must not fan out"):
            parse_snapshot(fanout)

    def test_rank_ties_have_gaps_and_duplicate_nan_language_is_independent(self):
        ranks, ranked, values, enabled, choices, _script = _concrete_rank_values(
            ((10, 10), (10, 20), (20, 20))
        )
        assignment = {
            choice.term.atom: ordinal
            for choice, ordinal in zip(choices, (0, 1, 2, 0, 1, 2))
        }
        self.assertTrue(_ground(enabled, assignment))
        self.assertEqual(
            tuple(_ground(row[ranks[0]].value, assignment) for row in values),
            (1, 1, 3),
        )
        self.assertEqual(
            tuple(_ground(row[ranks[1]].value, assignment) for row in values),
            (1, 2, 2),
        )
        self.assertFalse(ranked.sequence)
        self.assertIsNone(ranked.order)
        self.assertIsNone(ranked.ordinals)

        ranks, _ranked, values, enabled, choices, script = _concrete_rank_values(
            ((decimal.NAN, decimal.NAN), (decimal.NAN, decimal.NAN))
        )
        # The first unstable sort places row 0 first; the second independently
        # places row 1 first.  Both choices satisfy one sequential q49 Project.
        assignment = {
            choice.term.atom: ordinal
            for choice, ordinal in zip(choices, (0, 1, 1, 0))
        }
        self.assertTrue(_ground(enabled, assignment))
        self.assertEqual(
            tuple(
                (
                    _ground(row[ranks[0]].value, assignment),
                    _ground(row[ranks[1]].value, assignment),
                )
                for row in values
            ),
            ((1, 2), (2, 1)),
        )

        family = RelationFamily(
            (
                Outcome(
                    enabled,
                    _ranked,
                    smt.FALSE,
                    choices=tuple(choices),
                ),
            )
        )
        limited = relation_model.limit_family(
            family,
            Expr(
                kind="literal",
                result_type="Uint64",
                nullable=False,
                value=1,
            ),
            None,
            script,
            "test:take",
        ).outcomes[0]
        self.assertEqual(limited.choices[:-1], tuple(choices))
        selector = limited.choices[-1]
        self.assertNotIn(selector, choices)
        self.assertEqual(len(limited.relation.rows), 1)
        output = limited.relation.rows[0]
        # The last Rank sort put row 1 first above, but CalcOverWindow declares
        # no output order.  Unordered Take(1) must therefore retain its own
        # fresh selection and may independently return either input row.
        for selected, expected_id in ((0, 0), (1, 1)):
            selected_assignment = assignment | {selector.term.atom: selected}
            with self.subTest(selected=selected):
                self.assertTrue(_ground(limited.enabled, selected_assignment))
                self.assertTrue(_ground(output.present, selected_assignment))
                self.assertEqual(
                    _ground(output.values["id"].value, selected_assignment),
                    expected_id,
                )

    def test_global_rank_serial_gather_proves_and_hash_split_is_wrong(self):
        if SOLVER is None:
            self.skipTest("Z3 is not available")
        # Keep this oracle about StageGraph routing.  Independent rank leaves
        # have a separate exact-language test above.
        logical = parse_snapshot(_snapshot(1))
        serial = parse_snapshot(
            _staged_snapshot(
                {"kind": "union_all", "parallel": False},
                rank_count=1,
            )
        )
        wrong_hash = parse_snapshot(
            _staged_snapshot(
                {
                    "kind": "hash_shuffle",
                    "keys": ["id"],
                    "hash_function": "HashV2",
                    "use_spilling": False,
                },
                rank_count=1,
            )
        )
        self.assertEqual(
            solve(build_problem(logical, serial, 2), SOLVER, 2, 10_000).status,
            "VERIFIED_BOUNDED",
        )
        self.assertEqual(
            solve(
                build_problem(logical, wrong_hash, 2),
                SOLVER,
                2,
                10_000,
            ).status,
            "COUNTEREXAMPLE",
        )


if __name__ == "__main__":
    unittest.main()
