import os
import unittest

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier import verify as verifier
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    Column,
    Expr,
    OPAQUE_FINGERPRINT_PREFIX,
    RESTRICTED_CONCAT_FINGERPRINT_PREFIX,
    SnapshotError,
    checked_concat_corridor,
    parse_snapshot,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import (
    Database,
    Evaluator,
    Outcome,
    Relation,
    RelationFamily,
    Row,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import (
    Encoder,
    Value,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import (
    VerificationError,
    build_problem,
    solve,
)


FINGERPRINT = f"{RESTRICTED_CONCAT_FINGERPRINT_PREFIX}q84-customer-name"
SOLVER = (
    yatest_common.binary_path("contrib/tools/z3/z3")
    if yatest_common is not None
    else os.environ.get("RBO_Z3")
)


def _column(name):
    return {"kind": "column", "column": name}


def _literal(scalar_type, value):
    return {"kind": "literal", "type": scalar_type, "value": value}


def _checked_concat(*columns, fingerprint=FINGERPRINT):
    return {
        "kind": "checked_concat",
        "fingerprint": fingerprint,
        "type": "String",
        "nullable": False,
        "args": [_column(column) for column in columns],
    }


def _table(name="A"):
    return {
        "name": name,
        "columns": [
            {"name": "id", "type": "Int64", "nullable": False},
            {"name": "first", "type": "String", "nullable": True},
            {"name": "last", "type": "String", "nullable": True},
        ],
        "unique_keys": [],
    }


def _logical_snapshot():
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": {"tables": [_table()]},
        "plan": {
            "nodes": [
                {
                    "id": "scan",
                    "op": "scan",
                    "table": "A",
                    "columns": [
                        {"source": "id", "output": "a.id"},
                        {"source": "first", "output": "a.first"},
                        {"source": "last", "output": "a.last"},
                    ],
                    "pushed_limit": None,
                },
                {
                    "id": "checked",
                    "op": "project",
                    "input": "scan",
                    "ordered": False,
                    "columns": [
                        {"output": "id", "expression": _column("a.id")},
                        {
                            "output": "name",
                            "expression": _checked_concat("a.last", "a.first"),
                        },
                    ],
                },
                {
                    "id": "sort",
                    "op": "sort",
                    "input": "checked",
                    "order": [
                        {
                            "column": "id",
                            "ascending": True,
                            "nulls_first": False,
                        }
                    ],
                    "limit": None,
                    "phase": "undefined",
                },
                {
                    "id": "alias",
                    "op": "project",
                    "input": "sort",
                    "ordered": True,
                    "columns": [
                        {"output": "id", "expression": _column("id")},
                        {"output": "result", "expression": _column("name")},
                    ],
                },
                {
                    "id": "limit",
                    "op": "limit",
                    "input": "alias",
                    "count": _literal("Uint64", 100),
                    "offset": None,
                    "phase": "undefined",
                },
            ],
            "root": "limit",
            "output": ["id", "result"],
            "subplans": [],
        },
        "stage_graph": None,
    }


def _root_project_snapshot():
    raw = _logical_snapshot()
    raw["plan"]["nodes"] = raw["plan"]["nodes"][:2]
    raw["plan"]["root"] = "checked"
    raw["plan"]["output"] = ["id", "name"]
    return raw


def _staged_snapshot():
    raw = _root_project_snapshot()
    raw["stage_graph"] = {
        "root_stage": "root",
        "stages": [
            {
                "id": "source",
                "nodes": ["scan"],
                "inputs": [],
                "outputs": [{"index": 0, "node": "scan"}],
                "source_storage": "column",
            },
            {
                "id": "root",
                "nodes": ["checked"],
                "inputs": ["scan"],
                "outputs": [{"index": 0, "node": "checked"}],
                "source_storage": None,
            },
        ],
        "edges": [
            {
                "id": "input",
                "producer": "source",
                "consumer": "root",
                "occurrence": 0,
                "producer_output": 0,
                "consumer_input": 0,
                "kind": "map",
            }
        ],
        "assumptions": [],
    }
    return raw


def _six_scan_snapshot():
    tables = [_table("T0")]
    tables.extend(
        {
            "name": f"T{index}",
            "columns": [
                {"name": "id", "type": "Int64", "nullable": False}
            ],
            "unique_keys": [],
        }
        for index in range(1, 6)
    )
    scans = [
        {
            "id": f"scan{index}",
            "op": "scan",
            "table": f"T{index}",
            "columns": (
                [
                    {"source": "id", "output": "t0.id"},
                    {"source": "first", "output": "t0.first"},
                    {"source": "last", "output": "t0.last"},
                ]
                if index == 0
                else [{"source": "id", "output": f"t{index}.id"}]
            ),
            "pushed_limit": None,
        }
        for index in range(6)
    ]
    joins = []
    left = "scan0"
    for index in range(1, 6):
        node_id = f"join{index}"
        joins.append(
            {
                "id": node_id,
                "op": "join",
                "left": left,
                "right": f"scan{index}",
                "kind": "cross",
                "keys": [],
                "predicate": _literal("Bool", True),
            }
        )
        left = node_id
    nodes = [
        *scans,
        *joins,
        {
            "id": "filter",
            "op": "filter",
            "input": left,
            "predicate": _literal("Bool", True),
        },
        {
            "id": "checked",
            "op": "project",
            "input": "filter",
            "ordered": False,
            "columns": [
                {"output": "id", "expression": _column("t0.id")},
                {
                    "output": "name",
                    "expression": _checked_concat("t0.last", "t0.first"),
                },
            ],
        },
        {
            "id": "sort",
            "op": "sort",
            "input": "checked",
            "order": [
                {
                    "column": "id",
                    "ascending": True,
                    "nulls_first": False,
                }
            ],
            "limit": None,
            "phase": "undefined",
        },
        {
            "id": "limit",
            "op": "limit",
            "input": "sort",
            "count": _literal("Uint64", 100),
            "offset": None,
            "phase": "undefined",
        },
    ]
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": {"tables": tables},
        "plan": {
            "nodes": nodes,
            "root": "limit",
            "output": ["id", "name"],
            "subplans": [],
        },
        "stage_graph": None,
    }


class CheckedConcatIrTest(unittest.TestCase):
    def test_exact_logical_corridor_is_admitted(self):
        snapshot = parse_snapshot(_logical_snapshot())
        corridor = checked_concat_corridor(snapshot)

        self.assertIsNotNone(corridor)
        self.assertEqual(corridor.producer.id, "checked")
        self.assertEqual(corridor.output, "name")
        self.assertEqual(
            [selector.id for selector in corridor.selectors],
            ["limit"],
        )

    def test_exact_staged_root_is_admitted(self):
        snapshot = parse_snapshot(_staged_snapshot())
        corridor = checked_concat_corridor(snapshot)

        self.assertIsNotNone(corridor)
        self.assertEqual(corridor.producer.id, "checked")
        self.assertEqual(corridor.selectors, ())

    def test_result_fingerprint_and_arguments_are_strict(self):
        mutations = (
            (
                "result type",
                lambda expression: expression.update(type="Utf8"),
                "result must be non-null String",
            ),
            (
                "nullable result",
                lambda expression: expression.update(nullable=True),
                "result must be non-null String",
            ),
            (
                "no arguments",
                lambda expression: expression.update(args=[]),
                "one or two stored-String arguments",
            ),
            (
                "three arguments",
                lambda expression: expression.update(
                    args=[_column("a.last"), _column("a.first"), _column("a.id")]
                ),
                "one or two stored-String arguments",
            ),
            (
                "generic opaque root",
                lambda expression: expression.update(
                    fingerprint=(
                        f"{OPAQUE_FINGERPRINT_PREFIX}"
                        "node:8:callable;content:4:Just;identity"
                    )
                ),
                "audited root-Concat fingerprint prefix",
            ),
            (
                "empty identity",
                lambda expression: expression.update(
                    fingerprint=RESTRICTED_CONCAT_FINGERPRINT_PREFIX
                ),
                "non-empty identity suffix",
            ),
            (
                "scalar argument",
                lambda expression: expression["args"].__setitem__(
                    0, _literal("String", "last")
                ),
                "direct column references",
            ),
            (
                "duplicate argument",
                lambda expression: expression.update(
                    args=[_column("a.last"), _column("a.last")]
                ),
                "distinct columns",
            ),
            (
                "non-String argument",
                lambda expression: expression["args"].__setitem__(
                    0, _column("a.id")
                ),
                "stored String columns",
            ),
        )
        for label, mutate, message in mutations:
            with self.subTest(label=label):
                raw = _root_project_snapshot()
                expression = raw["plan"]["nodes"][1]["columns"][1]["expression"]
                mutate(expression)
                with self.assertRaisesRegex(SnapshotError, message):
                    parse_snapshot(raw)

    def test_expression_must_be_one_complete_top_level_projection(self):
        raw = _root_project_snapshot()
        projection = raw["plan"]["nodes"][1]["columns"][1]
        checked = projection["expression"]
        projection["expression"] = {
            "kind": "opaque",
            "fingerprint": "wrapper",
            "type": "String",
            "nullable": False,
            "args": [checked],
        }

        with self.assertRaisesRegex(
            SnapshotError,
            "one complete top-level Project expression",
        ):
            parse_snapshot(raw)

    def test_arguments_must_be_physical_project_inputs(self):
        raw = _root_project_snapshot()
        raw["schema"]["tables"].append(
            {
                "name": "B",
                "columns": [
                    {"name": "text", "type": "String", "nullable": True}
                ],
                "unique_keys": [],
            }
        )
        raw["plan"]["nodes"].append(
            {
                "id": "sub_scan",
                "op": "scan",
                "table": "B",
                "columns": [{"source": "text", "output": "sub.text"}],
                "pushed_limit": None,
            }
        )
        raw["plan"]["nodes"][1]["columns"][1]["expression"]["args"][0] = (
            _column("$scalar")
        )
        raw["plan"]["subplans"].append(
            {
                "binding": "$scalar",
                "kind": "scalar",
                "root": "sub_scan",
                "output": {
                    "column": "sub.text",
                    "type": "String",
                    "nullable": True,
                },
                "type": "String",
                "nullable": True,
                "dependencies": [],
                "consumers": ["checked"],
            }
        )

        with self.assertRaisesRegex(
            SnapshotError,
            "checked_concat arguments must be physical Project input columns",
        ):
            parse_snapshot(raw)

    def test_expression_is_rejected_outside_a_project(self):
        raw = _root_project_snapshot()
        raw["plan"]["nodes"][1]["columns"][1]["expression"] = _column(
            "a.last"
        )
        raw["plan"]["nodes"].insert(
            1,
            {
                "id": "filter",
                "op": "filter",
                "input": "scan",
                "predicate": {
                    "kind": "opaque",
                    "fingerprint": "observes-checked-concat",
                    "type": "Bool",
                    "nullable": False,
                    "args": [_checked_concat("a.last", "a.first")],
                },
            },
        )
        raw["plan"]["nodes"][2]["input"] = "filter"

        with self.assertRaisesRegex(
            SnapshotError,
            "only as a top-level Project expression",
        ):
            parse_snapshot(raw)

    def test_logical_corridor_rejects_observation_or_row_discard(self):
        mutations = (
            (
                "sort key",
                lambda raw: raw["plan"]["nodes"][2]["order"][0].update(
                    column="name"
                ),
                "may not be consumed as a Sort key",
            ),
            (
                "computed alias",
                lambda raw: raw["plan"]["nodes"][3]["columns"][1].update(
                    expression={
                        "kind": "opaque",
                        "fingerprint": "observe-name",
                        "type": "String",
                        "nullable": False,
                        "args": [_column("name")],
                    }
                ),
                "one direct column transport",
            ),
            (
                "zero offset",
                lambda raw: raw["plan"]["nodes"][4].update(
                    offset=_literal("Uint64", 0)
                ),
                "does not cross a rootward Limit with an offset",
            ),
            (
                "positive offset",
                lambda raw: raw["plan"]["nodes"][4].update(
                    offset=_literal("Uint64", 1)
                ),
                "does not cross a rootward Limit with an offset",
            ),
            (
                "cardinality error",
                lambda raw: raw["plan"]["nodes"][4].update(
                    ensure_at_most_one=True
                ),
                "does not cross an error-bearing Limit",
            ),
            (
                "not returned",
                lambda raw: raw["plan"].update(output=["id"]),
                "output must be returned",
            ),
        )
        for label, mutate, message in mutations:
            with self.subTest(label=label):
                raw = _logical_snapshot()
                mutate(raw)
                with self.assertRaisesRegex(SnapshotError, message):
                    parse_snapshot(raw)

    def test_staged_checked_concat_must_be_the_result_project(self):
        raw = _staged_snapshot()
        raw["plan"]["nodes"].append(
            {
                "id": "alias",
                "op": "project",
                "input": "checked",
                "ordered": False,
                "columns": [
                    {"output": "id", "expression": _column("id")},
                    {"output": "result", "expression": _column("name")},
                ],
            }
        )
        raw["plan"]["root"] = "alias"
        raw["plan"]["output"] = ["id", "result"]

        with self.assertRaisesRegex(
            SnapshotError,
            "staged checked_concat Project must be the plan root",
        ):
            parse_snapshot(raw)

    def test_corridor_rejects_filter_join_and_fanout_consumers(self):
        def filtered():
            raw = _root_project_snapshot()
            raw["plan"]["nodes"].append(
                {
                    "id": "filter",
                    "op": "filter",
                    "input": "checked",
                    "predicate": _literal("Bool", True),
                }
            )
            raw["plan"]["root"] = "filter"
            return raw

        def joined(*, fanout):
            raw = _root_project_snapshot()
            if fanout:
                raw["plan"]["nodes"].append(
                    {
                        "id": "branch",
                        "op": "project",
                        "input": "checked",
                        "ordered": False,
                        "columns": [
                            {"output": "b.id", "expression": _column("id")},
                            {
                                "output": "b.name",
                                "expression": _column("name"),
                            },
                        ],
                    }
                )
                right = "branch"
            else:
                raw["schema"]["tables"].append(
                    {
                        "name": "B",
                        "columns": [
                            {"name": "id", "type": "Int64", "nullable": False}
                        ],
                        "unique_keys": [],
                    }
                )
                raw["plan"]["nodes"].append(
                    {
                        "id": "right",
                        "op": "scan",
                        "table": "B",
                        "columns": [{"source": "id", "output": "b.id"}],
                        "pushed_limit": None,
                    }
                )
                right = "right"
            raw["plan"]["nodes"].append(
                {
                    "id": "join",
                    "op": "join",
                    "left": "checked",
                    "right": right,
                    "kind": "cross",
                    "keys": [],
                    "predicate": _literal("Bool", True),
                }
            )
            raw["plan"]["root"] = "join"
            return raw

        for label, raw, message in (
            (
                "filter",
                filtered(),
                "supports only direct Projects, Sort, and offset-free Limit",
            ),
            (
                "join",
                joined(fanout=False),
                "supports only direct Projects, Sort, and offset-free Limit",
            ),
            (
                "fanout",
                joined(fanout=True),
                "must have exactly one consumer",
            ),
        ):
            with self.subTest(label=label):
                with self.assertRaisesRegex(SnapshotError, message):
                    parse_snapshot(raw)


class CheckedConcatScalarTest(unittest.TestCase):
    @staticmethod
    def _expression(kind="checked_concat", fingerprint=FINGERPRINT, args=None):
        return Expr(
            kind=kind,
            fingerprint=fingerprint,
            result_type="String",
            nullable=False,
            args=(
                args
                if args is not None
                else (
                    Expr(kind="column", column="last"),
                    Expr(kind="column", column="first"),
                )
            ),
        )

    def test_value_identity_is_shared_with_the_same_opaque_concat(self):
        script = smt.Script()
        scalar = Encoder(script)
        row = {
            "last": Value("String", smt.FALSE, smt.int_value(7)),
            "first": Value("String", smt.TRUE, smt.int_value(91)),
        }
        checked = scalar.evaluate(self._expression(), row)
        opaque = scalar.evaluate(self._expression(kind="opaque"), row)

        self.assertEqual(checked, opaque)
        self.assertIs(checked.is_null, smt.FALSE)

    def test_failure_is_shared_and_uses_canonical_ordered_arguments(self):
        script = smt.Script()
        scalar = Encoder(script)
        expression = self._expression()
        first_row = {
            "last": Value("String", smt.TRUE, smt.int_value(7)),
            "first": Value("String", smt.FALSE, smt.int_value(3)),
        }
        same_canonical_row = {
            "last": Value("String", smt.TRUE, smt.int_value(99)),
            "first": Value("String", smt.FALSE, smt.int_value(3)),
        }

        failure = scalar.checked_concat_failure(expression, first_row)
        self.assertEqual(
            failure,
            scalar.checked_concat_failure(expression, same_canonical_row),
        )
        self.assertEqual(failure.sort, smt.BOOL)

        renamed = self._expression(
            args=(
                Expr(kind="column", column="family"),
                Expr(kind="column", column="given"),
            )
        )
        renamed_row = {
            "family": first_row["last"],
            "given": first_row["first"],
        }
        self.assertEqual(
            failure,
            scalar.checked_concat_failure(renamed, renamed_row),
        )
        self.assertEqual(
            scalar.evaluate(expression, first_row),
            scalar.evaluate(renamed, renamed_row),
        )

        reversed_arguments = self._expression(
            args=tuple(reversed(expression.args))
        )
        self.assertNotEqual(
            failure,
            scalar.checked_concat_failure(reversed_arguments, first_row),
        )
        self.assertNotEqual(
            failure.operation,
            scalar.checked_concat_failure(
                self._expression(
                    fingerprint=f"{RESTRICTED_CONCAT_FINGERPRINT_PREFIX}other"
                ),
                first_row,
            ).operation,
        )


class CheckedConcatEvaluationTest(unittest.TestCase):
    def test_project_failure_is_eager_for_each_present_input_row(self):
        snapshot = parse_snapshot(_root_project_snapshot())
        script = smt.Script()
        scalar = Encoder(script)
        present = script.fresh_constant("present", smt.BOOL)
        source_row = Row(
            present,
            {
                "a.id": Value("Int64", smt.FALSE, smt.ONE),
                "a.first": Value("String", smt.TRUE, smt.int_value(17)),
                "a.last": Value("String", smt.FALSE, smt.int_value(29)),
            },
        )
        source = RelationFamily(
            (
                Outcome(
                    smt.TRUE,
                    Relation(
                        (
                            Column("a.id", "Int64", False),
                            Column("a.first", "String", True),
                            Column("a.last", "String", True),
                        ),
                        (source_row,),
                    ),
                    smt.FALSE,
                ),
            )
        )
        outcome = Evaluator(
            snapshot,
            Database(snapshot, 0, script),
            scalar,
            node_overrides={"scan": source},
        ).root().outcomes[0]
        expression = snapshot.plan.node_map()["checked"].columns[1].expression

        self.assertEqual(
            outcome.error,
            smt.and_(
                present,
                scalar.checked_concat_failure(expression, source_row.values),
            ),
        )
        result = outcome.relation.rows[0].values["name"]
        opaque = Expr(
            kind="opaque",
            fingerprint=expression.fingerprint,
            result_type="String",
            nullable=False,
            args=expression.args,
        )
        self.assertEqual(result, scalar.evaluate(opaque, source_row.values))

    def test_eager_failure_survives_a_limiting_sort_that_discards_the_row(self):
        # WideTopSort drains every input field before selecting its output.
        raw = _root_project_snapshot()
        raw["plan"]["nodes"].append(
            {
                "id": "tail_sort",
                "op": "sort",
                "input": "checked",
                "order": [
                    {
                        "column": "id",
                        "ascending": True,
                        "nulls_first": False,
                    }
                ],
                "limit": _literal("Uint64", 0),
                "phase": "undefined",
            }
        )
        raw["plan"]["root"] = "tail_sort"
        snapshot = parse_snapshot(raw)
        script = smt.Script()
        scalar = Encoder(script)
        present = script.fresh_constant("present", smt.BOOL)
        source_row = Row(
            present,
            {
                "a.id": Value("Int64", smt.FALSE, smt.ONE),
                "a.first": Value("String", smt.FALSE, smt.int_value(17)),
                "a.last": Value("String", smt.FALSE, smt.int_value(29)),
            },
        )
        source = RelationFamily(
            (
                Outcome(
                    smt.TRUE,
                    Relation(
                        (
                            Column("a.id", "Int64", False),
                            Column("a.first", "String", True),
                            Column("a.last", "String", True),
                        ),
                        (source_row,),
                    ),
                    smt.FALSE,
                ),
            )
        )
        family = Evaluator(
            snapshot,
            Database(snapshot, 0, script),
            scalar,
            node_overrides={"scan": source},
        ).root()
        expression = snapshot.plan.node_map()["checked"].columns[1].expression
        expected_error = smt.and_(
            present,
            scalar.checked_concat_failure(expression, source_row.values),
        )

        self.assertTrue(family.outcomes)
        self.assertTrue(
            all(outcome.error == expected_error for outcome in family.outcomes)
        )
        self.assertTrue(
            all(
                all(row.present is smt.FALSE for row in outcome.relation.rows)
                for outcome in family.outcomes
            )
        )


class CheckedConcatCardinalityGateTest(unittest.TestCase):
    def test_q84_six_scan_bound_admits_two_rows_and_rejects_three(self):
        snapshot = parse_snapshot(_six_scan_snapshot())

        verifier._check_checked_concat_eager_bound(snapshot, 2)
        with self.assertRaisesRegex(
            VerificationError,
            "729 rows at row bound 3.*exceeding Limit 'limit' bound 100",
        ):
            verifier._check_checked_concat_eager_bound(snapshot, 3)

    def test_limiting_sort_uses_the_same_bound(self):
        raw = _six_scan_snapshot()
        raw["plan"]["nodes"][-2]["limit"] = _literal("Uint64", 100)
        raw["plan"]["nodes"].pop()
        raw["plan"]["root"] = "sort"
        snapshot = parse_snapshot(raw)

        verifier._check_checked_concat_eager_bound(snapshot, 2)
        with self.assertRaisesRegex(
            VerificationError,
            "exceeding TopSort 'sort' bound 100",
        ):
            verifier._check_checked_concat_eager_bound(snapshot, 3)

    def test_unknown_producer_spine_fails_closed(self):
        raw = _logical_snapshot()
        raw["plan"]["nodes"].insert(
            1,
            {
                "id": "input_project",
                "op": "project",
                "input": "scan",
                "ordered": False,
                "columns": [
                    {"output": "a.id", "expression": _column("a.id")},
                    {"output": "a.first", "expression": _column("a.first")},
                    {"output": "a.last", "expression": _column("a.last")},
                ],
            },
        )
        raw["plan"]["nodes"][2]["input"] = "input_project"
        snapshot = parse_snapshot(raw)

        with self.assertRaisesRegex(
            VerificationError,
            "q84 Scan/Filter/Cross-or-Inner-Join producer spine.*Project",
        ):
            verifier._check_checked_concat_eager_bound(snapshot, 2)

    def test_public_problem_builder_applies_the_eager_bound(self):
        before = parse_snapshot(_six_scan_snapshot())
        after = parse_snapshot(_staged_snapshot())

        with self.assertRaisesRegex(
            VerificationError,
            "729 rows at row bound 3.*exceeding Limit 'limit' bound 100",
        ):
            build_problem(before, after, 3)


@unittest.skipUnless(SOLVER, "run through ya or set RBO_Z3 for solver tests")
class CheckedConcatProofTest(unittest.TestCase):
    def test_shared_checked_value_and_failure_prove_across_stage_gather(self):
        # The final Map is evaluated independently in both source tasks and its
        # errors are gathered at the staged root.  Sharing the audited value and
        # failure UFs makes that physical representation equal to the logical
        # root Project for every bounded row and routing choice.
        result = solve(
            build_problem(
                parse_snapshot(_root_project_snapshot()),
                parse_snapshot(_staged_snapshot()),
                1,
                10_000,
            ),
            SOLVER,
            1,
            10_000,
        )

        self.assertEqual(result.status, "VERIFIED_BOUNDED")

    def test_dropping_checked_failure_has_a_counterexample(self):
        final = _staged_snapshot()
        final["plan"]["nodes"][1]["columns"][1]["expression"]["kind"] = (
            "opaque"
        )
        result = solve(
            build_problem(
                parse_snapshot(_root_project_snapshot()),
                parse_snapshot(final),
                1,
                10_000,
            ),
            SOLVER,
            1,
            10_000,
        )

        self.assertEqual(result.status, "COUNTEREXAMPLE")


if __name__ == "__main__":
    unittest.main()
