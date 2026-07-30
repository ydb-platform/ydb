import unittest
from itertools import product

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    Column,
    SnapshotError,
    parse_snapshot,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import (
    Database,
    Evaluator,
    Outcome,
    Relation,
    RelationFamily,
    Row,
    Value,
    family_equal,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import (
    Encoder as ScalarEncoder,
)


def _column(name):
    return {"kind": "column", "column": name}


def _literal(scalar_type, value):
    return {"kind": "literal", "type": scalar_type, "value": value}


def _snapshot(*, marked=True):
    projection = {
        "output": "result",
        "expression": _column("a.text"),
    }
    if marked is not None:
        projection["error_on_null"] = marked
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": {
            "tables": [
                {
                    "name": "A",
                    "columns": [
                        {
                            "name": "text",
                            "type": "String",
                            "nullable": True,
                        },
                    ],
                    "unique_keys": [],
                },
            ],
        },
        "plan": {
            "nodes": [
                {
                    "id": "scan",
                    "op": "scan",
                    "table": "A",
                    "columns": [
                        {"source": "text", "output": "a.text"},
                    ],
                },
                {
                    "id": "project",
                    "op": "project",
                    "input": "scan",
                    "columns": [projection],
                    "ordered": False,
                },
            ],
            "root": "project",
            "output": ["result"],
            "subplans": [],
        },
        "stage_graph": None,
    }


def _left_semi_snapshot():
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": {
            "tables": [
                {
                    "name": "Left",
                    "columns": [
                        {
                            "name": "key",
                            "type": "String",
                            "nullable": False,
                        },
                    ],
                    "unique_keys": [],
                },
                {
                    "name": "Right",
                    "columns": [
                        {
                            "name": "checked",
                            "type": "String",
                            "nullable": True,
                        },
                        {
                            "name": "key",
                            "type": "String",
                            "nullable": False,
                        },
                    ],
                    "unique_keys": [],
                },
            ],
        },
        "plan": {
            "nodes": [
                {
                    "id": "left_scan",
                    "op": "scan",
                    "table": "Left",
                    "columns": [
                        {"source": "key", "output": "left.key"},
                    ],
                },
                {
                    "id": "right_scan",
                    "op": "scan",
                    "table": "Right",
                    "columns": [
                        {
                            "source": "checked",
                            "output": "right.checked",
                        },
                        {"source": "key", "output": "right.key"},
                    ],
                },
                {
                    "id": "checked_project",
                    "op": "project",
                    "input": "right_scan",
                    "ordered": False,
                    "columns": [
                        {
                            "output": "checked",
                            "expression": _column("right.checked"),
                            "error_on_null": True,
                        },
                        {
                            "output": "right.key",
                            "expression": _column("right.key"),
                        },
                    ],
                },
                {
                    "id": "join",
                    "op": "join",
                    "left": "left_scan",
                    "right": "checked_project",
                    "kind": "left_semi",
                    "keys": [
                        {"left": "left.key", "right": "checked"},
                    ],
                    "predicate": _literal("Bool", True),
                },
            ],
            "root": "join",
            "output": ["left.key"],
            "subplans": [],
        },
        "stage_graph": None,
    }


def _ground(term, constants):
    if term.operation == "symbol":
        return constants[term.atom]
    if term.operation in {"bool", "int"}:
        return term.atom
    if term.operation == "not":
        return not _ground(term.arguments[0], constants)
    if term.operation == "and":
        return all(_ground(argument, constants) for argument in term.arguments)
    if term.operation == "or":
        return any(_ground(argument, constants) for argument in term.arguments)
    if term.operation == "=":
        return _ground(term.arguments[0], constants) == _ground(
            term.arguments[1],
            constants,
        )
    if term.operation == "<":
        return _ground(term.arguments[0], constants) < _ground(
            term.arguments[1],
            constants,
        )
    if term.operation == "ite":
        condition, present, absent = term.arguments
        return _ground(
            present if _ground(condition, constants) else absent,
            constants,
        )
    if term.operation == "+":
        return sum(_ground(argument, constants) for argument in term.arguments)
    raise AssertionError(f"unsupported ground SMT operation {term.operation!r}")


def _source_family(
    columns,
    rows,
    *,
    error=smt.FALSE,
):
    return RelationFamily(
        (
            Outcome(
                smt.TRUE,
                Relation(columns, tuple(rows)),
                error,
            ),
        )
    )


def _evaluate_override(raw, script, source):
    snapshot = parse_snapshot(raw)
    database = Database(snapshot, 0, script)
    return Evaluator(
        snapshot,
        database,
        ScalarEncoder(script),
        node_overrides={"scan": source},
    ).root()


class ErrorOnNullSchemaTest(unittest.TestCase):
    def test_marker_defaults_false_and_true_makes_output_non_nullable(self):
        absent = parse_snapshot(_snapshot(marked=None))
        marked = parse_snapshot(_snapshot(marked=True))

        self.assertFalse(absent.plan.nodes[1].columns[0].error_on_null)
        self.assertTrue(marked.plan.nodes[1].columns[0].error_on_null)
        self.assertTrue(absent.output_schema()[0].nullable)
        self.assertFalse(marked.output_schema()[0].nullable)

    def test_unknown_and_non_boolean_markers_are_rejected(self):
        unknown = _snapshot(marked=None)
        unknown["plan"]["nodes"][1]["columns"][0]["on_null_error"] = True
        with self.assertRaisesRegex(SnapshotError, "unknown fields: on_null_error"):
            parse_snapshot(unknown)

        for marker in (0, 1, "true", None):
            with self.subTest(marker=marker):
                malformed = _snapshot(marked=None)
                malformed["plan"]["nodes"][1]["columns"][0][
                    "error_on_null"
                ] = marker
                with self.assertRaisesRegex(SnapshotError, "expected a Boolean"):
                    parse_snapshot(malformed)

    def test_marker_requires_exact_nullable_string_input_type(self):
        for scalar_type, nullable in (
            ("String", False),
            ("Utf8", True),
            ("Int64", True),
        ):
            with self.subTest(scalar_type=scalar_type, nullable=nullable):
                malformed = _snapshot()
                source = malformed["schema"]["tables"][0]["columns"][0]
                source["type"] = scalar_type
                source["nullable"] = nullable
                with self.assertRaisesRegex(
                    SnapshotError,
                    "direct nullable String input column",
                ):
                    parse_snapshot(malformed)

    def test_marker_rejects_scalar_expression_shape(self):
        malformed = _snapshot()
        malformed["plan"]["nodes"][1]["columns"][0]["expression"] = _literal(
            "String",
            "fallback",
        )

        with self.assertRaisesRegex(
            SnapshotError,
            "direct nullable String input column",
        ):
            parse_snapshot(malformed)

    def test_marker_rejects_a_virtual_subplan_binding(self):
        malformed = _snapshot()
        malformed["schema"]["tables"].append(
            {
                "name": "B",
                "columns": [
                    {
                        "name": "text",
                        "type": "String",
                        "nullable": True,
                    },
                ],
                "unique_keys": [],
            }
        )
        malformed["plan"]["nodes"].append(
            {
                "id": "sub_scan",
                "op": "scan",
                "table": "B",
                "columns": [
                    {"source": "text", "output": "sub.text"},
                ],
            }
        )
        projection = malformed["plan"]["nodes"][1]["columns"][0]
        projection["expression"] = _column("$scalar")
        malformed["plan"]["subplans"].append(
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
                "consumers": ["project"],
            }
        )

        with self.assertRaisesRegex(
            SnapshotError,
            "direct nullable String input column",
        ):
            parse_snapshot(malformed)


class ErrorOnNullTopologyTest(unittest.TestCase):
    def assert_rejected(self, raw):
        with self.assertRaisesRegex(
            SnapshotError,
            "private direct right input of one keyed left_semi Join",
        ):
            parse_snapshot(raw)

    def test_observed_root_and_private_keyed_left_semi_rhs_are_admitted(self):
        parse_snapshot(_snapshot())
        parsed = parse_snapshot(_left_semi_snapshot())

        project = parsed.plan.node_map()["checked_project"]
        self.assertTrue(project.columns[0].error_on_null)

    def test_left_semi_rhs_error_is_eager_when_left_row_is_absent(self):
        snapshot = parse_snapshot(_left_semi_snapshot())
        script = smt.Script()
        database = Database(snapshot, 0, script)
        left = _source_family(
            (Column("left.key", "String", False),),
            (
                Row(
                    smt.FALSE,
                    {
                        "left.key": Value(
                            "String",
                            smt.FALSE,
                            smt.ZERO,
                        )
                    },
                ),
            ),
        )
        right = _source_family(
            (
                Column("right.checked", "String", True),
                Column("right.key", "String", False),
            ),
            (
                Row(
                    smt.TRUE,
                    {
                        "right.checked": Value(
                            "String",
                            smt.TRUE,
                            smt.ZERO,
                        ),
                        "right.key": Value(
                            "String",
                            smt.FALSE,
                            smt.ONE,
                        ),
                    },
                ),
            ),
        )
        outcome = Evaluator(
            snapshot,
            database,
            ScalarEncoder(script),
            node_overrides={
                "left_scan": left,
                "right_scan": right,
            },
        ).root().outcomes[0]

        self.assertIs(outcome.error, smt.TRUE)

    def test_root_must_observe_every_marked_output(self):
        raw = _snapshot()
        raw["plan"]["nodes"][1]["columns"].append(
            {
                "output": "other",
                "expression": _column("a.text"),
            }
        )
        raw["plan"]["output"] = ["other"]

        self.assert_rejected(raw)

    def test_filter_and_limit_consumers_are_rejected(self):
        filter_raw = _snapshot()
        filter_raw["plan"]["nodes"].append(
            {
                "id": "filter",
                "op": "filter",
                "input": "project",
                "predicate": _literal("Bool", False),
            }
        )
        filter_raw["plan"]["root"] = "filter"

        limit_raw = _snapshot()
        limit_raw["plan"]["nodes"].append(
            {
                "id": "limit",
                "op": "limit",
                "input": "project",
                "count": _literal("Uint64", 1),
                "offset": None,
                "phase": "undefined",
            }
        )
        limit_raw["plan"]["root"] = "limit"

        for label, raw in (("filter", filter_raw), ("limit", limit_raw)):
            with self.subTest(label=label):
                self.assert_rejected(raw)

    def test_join_must_be_left_semi_with_project_on_the_right(self):
        inner = _left_semi_snapshot()
        inner["plan"]["nodes"][3]["kind"] = "inner"

        left = _left_semi_snapshot()
        join = left["plan"]["nodes"][3]
        join["left"] = "checked_project"
        join["right"] = "left_scan"
        join["keys"] = [{"left": "checked", "right": "left.key"}]
        left["plan"]["output"] = ["checked"]

        for label, raw in (("inner", inner), ("left", left)):
            with self.subTest(label=label):
                self.assert_rejected(raw)

    def test_every_marked_output_must_be_an_exact_right_key(self):
        unkeyed = _left_semi_snapshot()
        unkeyed["plan"]["nodes"][3]["keys"] = []

        wrong_key = _left_semi_snapshot()
        wrong_key["plan"]["nodes"][3]["keys"][0]["right"] = "right.key"

        for label, raw in (("unkeyed", unkeyed), ("wrong_key", wrong_key)):
            with self.subTest(label=label):
                self.assert_rejected(raw)

    def test_project_must_have_exactly_one_plan_consumer(self):
        raw = _left_semi_snapshot()
        raw["schema"]["tables"].append(
            {
                "name": "Left2",
                "columns": [
                    {
                        "name": "key",
                        "type": "String",
                        "nullable": False,
                    },
                ],
                "unique_keys": [],
            }
        )
        raw["plan"]["nodes"].extend(
            (
                {
                    "id": "left_scan_2",
                    "op": "scan",
                    "table": "Left2",
                    "columns": [
                        {"source": "key", "output": "left2.key"},
                    ],
                },
                {
                    "id": "join_2",
                    "op": "join",
                    "left": "left_scan_2",
                    "right": "checked_project",
                    "kind": "left_semi",
                    "keys": [
                        {"left": "left2.key", "right": "checked"},
                    ],
                    "predicate": _literal("Bool", True),
                },
                {
                    "id": "union",
                    "op": "union_all",
                    "inputs": [
                        {"node": "join", "columns": ["left.key"]},
                        {"node": "join_2", "columns": ["left2.key"]},
                    ],
                    "output": ["result"],
                    "ordered": False,
                },
            )
        )
        raw["plan"]["root"] = "union"
        raw["plan"]["output"] = ["result"]

        self.assert_rejected(raw)


class ErrorOnNullEvaluationTest(unittest.TestCase):
    def test_error_is_exactly_present_and_null_for_every_source_row(self):
        script = smt.Script()
        present = tuple(
            script.fresh_constant(f"present {index}", smt.BOOL)
            for index in range(2)
        )
        is_null = tuple(
            script.fresh_constant(f"null {index}", smt.BOOL)
            for index in range(2)
        )
        values = tuple(
            script.fresh_constant(f"value {index}", smt.INT)
            for index in range(2)
        )
        source = _source_family(
            (Column("a.text", "String", True),),
            (
                Row(
                    present[0],
                    {"a.text": Value("String", is_null[0], values[0])},
                ),
                Row(
                    present[1],
                    {"a.text": Value("String", is_null[1], values[1])},
                ),
            ),
        )
        outcome = _evaluate_override(
            _snapshot(),
            script,
            source,
        ).outcomes[0]

        for assignment in product((False, True), repeat=4):
            with self.subTest(assignment=assignment):
                constants = {
                    present[0].atom: assignment[0],
                    is_null[0].atom: assignment[1],
                    present[1].atom: assignment[2],
                    is_null[1].atom: assignment[3],
                }
                self.assertEqual(
                    _ground(outcome.error, constants),
                    (
                        (assignment[0] and assignment[1])
                        or (assignment[2] and assignment[3])
                    ),
                )

        for row, source_value in zip(
            outcome.relation.rows,
            values,
        ):
            result = row.values["result"]
            self.assertIs(result.is_null, smt.FALSE)
            self.assertIs(result.value, source_value)

    def test_multiple_marked_columns_contribute_to_one_error(self):
        raw = _snapshot()
        raw["schema"]["tables"][0]["columns"].append(
            {"name": "other", "type": "String", "nullable": True}
        )
        raw["plan"]["nodes"][0]["columns"].append(
            {"source": "other", "output": "a.other"}
        )
        raw["plan"]["nodes"][1]["columns"].append(
            {
                "output": "other_result",
                "expression": _column("a.other"),
                "error_on_null": True,
            }
        )
        raw["plan"]["output"].append("other_result")

        script = smt.Script()
        present = script.fresh_constant("present", smt.BOOL)
        first_null = script.fresh_constant("first null", smt.BOOL)
        second_null = script.fresh_constant("second null", smt.BOOL)
        source = _source_family(
            tuple(
                Column(name, "String", True)
                for name in ("a.text", "a.other")
            ),
            (
                Row(
                    present,
                    {
                        "a.text": Value("String", first_null, smt.ZERO),
                        "a.other": Value("String", second_null, smt.ONE),
                    },
                ),
            ),
        )
        outcome = _evaluate_override(raw, script, source).outcomes[0]

        for assignment in product((False, True), repeat=3):
            constants = {
                present.atom: assignment[0],
                first_null.atom: assignment[1],
                second_null.atom: assignment[2],
            }
            self.assertEqual(
                _ground(outcome.error, constants),
                assignment[0] and (assignment[1] or assignment[2]),
            )

    def test_inherited_and_local_errors_are_disjoined(self):
        script = smt.Script()
        inherited = script.fresh_constant("inherited error", smt.BOOL)
        present = script.fresh_constant("present", smt.BOOL)
        is_null = script.fresh_constant("null", smt.BOOL)
        source = _source_family(
            (Column("a.text", "String", True),),
            (
                Row(
                    present,
                    {"a.text": Value("String", is_null, smt.ZERO)},
                ),
            ),
            error=inherited,
        )
        outcome = _evaluate_override(_snapshot(), script, source).outcomes[0]

        for assignment in product((False, True), repeat=3):
            constants = {
                inherited.atom: assignment[0],
                present.atom: assignment[1],
                is_null.atom: assignment[2],
            }
            self.assertEqual(
                _ground(outcome.error, constants),
                assignment[0] or (assignment[1] and assignment[2]),
            )

    def test_local_and_scalar_subplan_errors_are_both_observable(self):
        raw = _snapshot()
        raw["schema"]["tables"].append(
            {
                "name": "B",
                "columns": [
                    {"name": "number", "type": "Int64", "nullable": False}
                ],
                "unique_keys": [],
            }
        )
        raw["plan"]["nodes"].append(
            {
                "id": "sub_scan",
                "op": "scan",
                "table": "B",
                "columns": [
                    {"source": "number", "output": "sub.number"},
                ],
            }
        )
        raw["plan"]["nodes"][1]["columns"].append(
            {
                "output": "ignored",
                "expression": _column("$scalar"),
            }
        )
        raw["plan"]["subplans"].append(
            {
                "binding": "$scalar",
                "kind": "scalar",
                "root": "sub_scan",
                "output": {
                    "column": "sub.number",
                    "type": "Int64",
                    "nullable": False,
                },
                "type": "Int64",
                "nullable": True,
                "dependencies": [],
                "consumers": ["project"],
            }
        )
        snapshot = parse_snapshot(raw)
        script = smt.Script()
        database = Database(snapshot, 2, script)
        outcome = Evaluator(
            snapshot,
            database,
            ScalarEncoder(script),
        ).root().outcomes[0]

        def constants(main_present, main_null, sub_present):
            result = {}
            for index, row in enumerate(database.witness["A"]):
                result[row.present.atom] = (
                    main_present if index == 0 else False
                )
                cell = row.cells["text"]
                result[cell.is_null.atom] = (
                    main_null if index == 0 else False
                )
                result[cell.value.atom] = index
            for index, row in enumerate(database.witness["B"]):
                result[row.present.atom] = sub_present[index]
                result[row.cells["number"].value.atom] = index
            return result

        cases = (
            ((False, True, (False, False)), False),
            ((True, False, (False, False)), False),
            ((True, True, (False, False)), True),
            ((True, False, (True, True)), True),
            ((True, True, (True, True)), True),
        )
        for arguments, expected in cases:
            with self.subTest(arguments=arguments):
                self.assertEqual(
                    _ground(outcome.error, constants(*arguments)),
                    expected,
                )


class ErrorOnNullEqualityTest(unittest.TestCase):
    @staticmethod
    def _family(*, marked, is_null, value, script):
        raw = _snapshot(marked=marked)
        source = _source_family(
            (Column("a.text", "String", True),),
            (
                Row(
                    smt.TRUE,
                    {
                        "a.text": Value(
                            "String",
                            smt.bool_value(is_null),
                            smt.int_value(value),
                        )
                    },
                ),
            ),
        )
        return _evaluate_override(raw, script, source)

    def test_error_and_success_are_not_equivalent(self):
        script = smt.Script()
        error = self._family(
            marked=True,
            is_null=True,
            value=1,
            script=script,
        )
        success = self._family(
            marked=False,
            is_null=True,
            value=1,
            script=script,
        )

        self.assertFalse(
            _ground(
                family_equal(error, success, ScalarEncoder(script)),
                {},
            )
        )

    def test_error_payloads_are_unobservable(self):
        script = smt.Script()
        first = self._family(
            marked=True,
            is_null=True,
            value=1,
            script=script,
        )
        second = self._family(
            marked=True,
            is_null=True,
            value=2,
            script=script,
        )

        self.assertTrue(
            _ground(
                family_equal(first, second, ScalarEncoder(script)),
                {},
            )
        )


if __name__ == "__main__":
    unittest.main()
