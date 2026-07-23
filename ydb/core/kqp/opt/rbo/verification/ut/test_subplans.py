import copy
import unittest

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    SnapshotError,
    parse_snapshot,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import (
    Database,
    Evaluator,
    family_equal,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import (
    Encoder as ScalarEncoder,
)


BINDING = "$scalar"


def _literal(scalar_type, value):
    return {"kind": "literal", "type": scalar_type, "value": value}


def _base_snapshot(expression=None):
    expression = expression or _literal("Int64", 7)
    output_type = expression["type"]
    output_nullable = expression["kind"] == "null"
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": {"tables": []},
        "plan": {
            "nodes": [
                {"id": "main_source", "op": "empty_source"},
                {
                    "id": "main_project",
                    "op": "project",
                    "input": "main_source",
                    "ordered": False,
                    "columns": [
                        {
                            "output": "result",
                            "expression": {
                                "kind": "column",
                                "column": BINDING,
                            },
                        }
                    ],
                },
                {"id": "sub_source", "op": "empty_source"},
                {
                    "id": "sub_value",
                    "op": "project",
                    "input": "sub_source",
                    "ordered": False,
                    "columns": [
                        {
                            "output": "sub.value",
                            "expression": expression,
                        }
                    ],
                },
            ],
            "root": "main_project",
            "output": ["result"],
            "subplans": [
                {
                    "binding": BINDING,
                    "kind": "scalar",
                    "root": "sub_value",
                    "output": {
                        "column": "sub.value",
                        "type": output_type,
                        "nullable": output_nullable,
                    },
                    "type": output_type,
                    "nullable": True,
                    "dependencies": [],
                    "consumers": ["main_project"],
                }
            ],
        },
        "stage_graph": None,
    }


def _evaluate(raw, row_bound=0, observer=None):
    snapshot = parse_snapshot(raw)
    script = smt.Script()
    database = Database(snapshot, row_bound, script)
    evaluator = Evaluator(
        snapshot,
        database,
        ScalarEncoder(script),
        node_observer=observer,
    )
    return evaluator, evaluator.root().certain()


def _global_decimal_sum_snapshot():
    raw = _base_snapshot(
        {
            "kind": "literal",
            "type": "Decimal(35,2)",
            "value": {"kind": "finite", "scaled": "0"},
        }
    )
    raw["schema"]["tables"] = [
        {
            "name": "A",
            "columns": [
                {"name": "amount", "type": "Decimal(7,2)", "nullable": True}
            ],
            "unique_keys": [],
        }
    ]
    raw["plan"]["nodes"][2:] = [
        {
            "id": "sub_scan",
            "op": "scan",
            "table": "A",
            "columns": [{"source": "amount", "output": "sub.amount"}],
            "predicate": None,
            "pushed_limit": None,
        },
        {
            "id": "sub_value",
            "op": "aggregate",
            "input": "sub_scan",
            "keys": [],
            "aggregates": [
                {
                    "input": "sub.amount",
                    "function": "sum",
                    "output": "sub.value",
                    "type": "Decimal(35,2)",
                    "nullable": True,
                    "distinct": False,
                    "unwrap": False,
                }
            ],
            "phase": "undefined",
            "distinct_all": False,
        },
    ]
    raw["plan"]["subplans"][0]["output"]["nullable"] = True
    return raw


def _scan_scalar_snapshot():
    raw = _base_snapshot()
    raw["schema"]["tables"] = [
        {
            "name": "A",
            "columns": [
                {"name": "x", "type": "Int64", "nullable": False}
            ],
            "unique_keys": [],
        }
    ]
    raw["plan"]["nodes"][2:] = [
        {
            "id": "sub_scan",
            "op": "scan",
            "table": "A",
            "columns": [{"source": "x", "output": "sub.value"}],
            "predicate": None,
            "pushed_limit": None,
        }
    ]
    raw["plan"]["subplans"][0]["root"] = "sub_scan"
    return raw


def _lowered_scalar_snapshot(*, check_mode, empty_outer=False):
    if check_mode not in {"gated", "eager", "none"}:
        raise AssertionError(f"unknown scalar check mode {check_mode!r}")
    nodes = [{"id": "main_source", "op": "empty_source"}]
    outer = "main_source"
    if empty_outer:
        nodes.append(
            {
                "id": "empty_outer",
                "op": "filter",
                "input": outer,
                "predicate": _literal("Bool", False),
            }
        )
        outer = "empty_outer"
    nodes.append(
        {
            "id": "sub_scan",
            "op": "scan",
            "table": "A",
            "columns": [{"source": "x", "output": "sub.value"}],
            "predicate": None,
            "pushed_limit": None,
        }
    )
    checked_input = "sub_scan"
    if check_mode == "gated":
        nodes.extend(
            [
                {
                    "id": "outer_gate",
                    "op": "limit",
                    "input": outer,
                    "count": _literal("Uint64", 1),
                    "offset": None,
                    "phase": "undefined",
                },
                {
                    "id": "scalar_bound",
                    "op": "limit",
                    "input": "sub_scan",
                    "count": _literal("Uint64", 2),
                    "offset": None,
                    "phase": "undefined",
                },
                {
                    "id": "gate_cross",
                    "op": "join",
                    "left": "outer_gate",
                    "right": "scalar_bound",
                    "kind": "cross",
                    "predicate": _literal("Bool", True),
                },
            ]
        )
        checked_input = "gate_cross"
    nodes.extend(
        [
            {
                "id": "checked",
                "op": "limit",
                "input": checked_input,
                "count": _literal("Uint64", 2),
                "offset": None,
                "phase": "undefined",
                "ensure_at_most_one": check_mode != "none",
            },
            {
                "id": "descriptor",
                "op": "project",
                "input": "checked",
                "ordered": False,
                "columns": [
                    {
                        "output": "sub.value",
                        "expression": {
                            "kind": "column",
                            "column": "sub.value",
                        },
                    }
                ],
            },
            {"id": "fallback_source", "op": "empty_source"},
            {
                "id": "fallback",
                "op": "project",
                "input": "fallback_source",
                "ordered": False,
                "columns": [
                    {
                        "output": "sub.value",
                        "expression": {"kind": "null", "type": "Int64"},
                    }
                ],
            },
            {
                "id": "value_or_null",
                "op": "union_all",
                "inputs": [
                    {"node": "descriptor", "columns": ["sub.value"]},
                    {"node": "fallback", "columns": ["sub.value"]},
                ],
                "output": ["sub.value"],
                "ordered": True,
            },
            {
                "id": "first",
                "op": "limit",
                "input": "value_or_null",
                "count": _literal("Uint64", 1),
                "offset": None,
                "phase": "undefined",
            },
            {
                "id": "cross",
                "op": "join",
                "left": outer,
                "right": "first",
                "kind": "cross",
                "predicate": _literal("Bool", True),
            },
            {
                "id": "result",
                "op": "project",
                "input": "cross",
                "ordered": False,
                "columns": [
                    {
                        "output": "result",
                        "expression": {
                            "kind": "column",
                            "column": "sub.value",
                        },
                    }
                ],
            },
        ]
    )
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": _scan_scalar_snapshot()["schema"],
        "plan": {
            "nodes": nodes,
            "root": "result",
            "output": ["result"],
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
        branch = (
            term.arguments[1]
            if _ground(term.arguments[0], constants)
            else term.arguments[2]
        )
        return _ground(branch, constants)
    if term.operation == "+":
        return sum(_ground(argument, constants) for argument in term.arguments)
    raise AssertionError(f"unsupported ground SMT operation {term.operation!r}")


def _database_constants(database, present, values=(10, 20)):
    constants = {}
    for row, is_present, value in zip(
        database.witness["A"],
        present,
        values,
    ):
        constants[row.present.atom] = is_present
        constants[row.cells["x"].value.atom] = value
    return constants


class ScalarSubplanEvaluationTest(unittest.TestCase):
    def test_present_scalar_value_is_injected_only_into_expression_scope(self):
        evaluator, relation = _evaluate(_base_snapshot())
        self.assertEqual(tuple(column.name for column in relation.columns), ("result",))
        self.assertEqual(set(relation.rows[0].values), {"result"})
        result = relation.rows[0].values["result"]
        self.assertEqual(result.type, "Int64")
        self.assertEqual(result.is_null, smt.FALSE)
        self.assertEqual(result.value, smt.int_value(7))
        self.assertEqual(set(evaluator.scalar_subplan_families), {BINDING})

    def test_nullable_scalar_value_remains_null(self):
        raw = _base_snapshot({"kind": "null", "type": "Int64"})
        _, relation = _evaluate(raw)
        result = relation.rows[0].values["result"]
        self.assertEqual(result.type, "Int64")
        self.assertEqual(result.is_null, smt.TRUE)

    def test_empty_scalar_relation_yields_typed_null(self):
        raw = _base_snapshot()
        raw["plan"]["nodes"].insert(
            3,
            {
                "id": "sub_filter",
                "op": "filter",
                "input": "sub_source",
                "predicate": _literal("Bool", False),
            },
        )
        raw["plan"]["nodes"][4]["input"] = "sub_filter"
        _, relation = _evaluate(raw)
        result = relation.rows[0].values["result"]
        self.assertEqual(result.type, "Int64")
        self.assertEqual(result.is_null, smt.TRUE)

    def test_repeated_use_resolves_one_shared_scalar_value(self):
        raw = _base_snapshot()
        raw["plan"]["nodes"][1]["columns"][0]["expression"] = {
            "kind": "add",
            "left": {"kind": "column", "column": BINDING},
            "right": {"kind": "column", "column": BINDING},
            "type": "Int64",
            "nullable": True,
        }
        observed = []
        evaluator, relation = _evaluate(
            raw,
            observer=lambda scope, node, family: observed.append(node),
        )
        result = relation.rows[0].values["result"]
        self.assertEqual(result.is_null, smt.FALSE)
        self.assertEqual(result.value, smt.int_value(14))
        self.assertEqual(observed.count("sub_value"), 1)
        self.assertEqual(len(evaluator.scalar_subplan_families), 1)

    def test_nondeterministic_binding_is_shared_across_consumers(self):
        raw = _scan_scalar_snapshot()
        raw["plan"]["nodes"][1]["columns"] = [
            {
                "output": "first",
                "expression": {"kind": "column", "column": BINDING},
            }
        ]
        raw["plan"]["nodes"].extend(
            [
                {
                    "id": "sub_one",
                    "op": "limit",
                    "input": "sub_scan",
                    "count": _literal("Uint64", 1),
                    "offset": None,
                    "phase": "undefined",
                },
                {
                    "id": "main_second",
                    "op": "project",
                    "input": "main_project",
                    "ordered": False,
                    "columns": [
                        {
                            "output": "result",
                            "expression": {
                                "kind": "eq",
                                "left": {"kind": "column", "column": "first"},
                                "right": {"kind": "column", "column": BINDING},
                            },
                        }
                    ],
                },
            ]
        )
        raw["plan"]["root"] = "main_second"
        raw["plan"]["output"] = ["result"]
        raw["plan"]["subplans"][0]["root"] = "sub_one"
        raw["plan"]["subplans"][0]["consumers"] = [
            "main_project",
            "main_second",
        ]

        snapshot = parse_snapshot(raw)
        script = smt.Script()
        database = Database(snapshot, 2, script)
        family = Evaluator(
            snapshot,
            database,
            ScalarEncoder(script),
        ).root()
        constants = _database_constants(database, (True, True))
        enabled = [
            outcome
            for outcome in family.outcomes
            if _ground(outcome.enabled, constants)
        ]

        self.assertEqual(len(enabled), 2)
        for outcome in enabled:
            self.assertFalse(_ground(outcome.error, constants))
            self.assertTrue(
                _ground(
                    outcome.relation.rows[0].values["result"].value,
                    constants,
                )
            )

    def test_enumerated_sequence_binding_is_shared_across_consumers(self):
        raw = _scan_scalar_snapshot()
        raw["plan"]["nodes"][1]["columns"] = [
            {
                "output": "first",
                "expression": {"kind": "column", "column": BINDING},
            }
        ]
        raw["plan"]["nodes"].extend(
            [
                {"id": "sub_empty_source", "op": "empty_source"},
                {
                    "id": "sub_empty",
                    "op": "filter",
                    "input": "sub_empty_source",
                    "predicate": _literal("Bool", False),
                },
                {
                    "id": "sub_empty_value",
                    "op": "project",
                    "input": "sub_empty",
                    "ordered": False,
                    "columns": [
                        {
                            "output": "sub.value",
                            "expression": _literal("Int64", 0),
                        }
                    ],
                },
                {
                    "id": "sub_ordered",
                    "op": "union_all",
                    "inputs": [
                        {"node": "sub_scan", "columns": ["sub.value"]},
                        {"node": "sub_empty_value", "columns": ["sub.value"]},
                    ],
                    "output": ["sub.value"],
                    "ordered": True,
                },
                {
                    "id": "sub_one",
                    "op": "limit",
                    "input": "sub_ordered",
                    "count": _literal("Uint64", 1),
                    "offset": None,
                    "phase": "undefined",
                },
                {
                    "id": "main_second",
                    "op": "project",
                    "input": "main_project",
                    "ordered": False,
                    "columns": [
                        {
                            "output": "result",
                            "expression": {
                                "kind": "eq",
                                "left": {"kind": "column", "column": "first"},
                                "right": {"kind": "column", "column": BINDING},
                            },
                        }
                    ],
                },
            ]
        )
        raw["plan"]["root"] = "main_second"
        raw["plan"]["output"] = ["result"]
        raw["plan"]["subplans"][0]["root"] = "sub_one"
        raw["plan"]["subplans"][0]["consumers"] = [
            "main_project",
            "main_second",
        ]

        snapshot = parse_snapshot(raw)
        script = smt.Script()
        database = Database(snapshot, 2, script)
        family = Evaluator(
            snapshot,
            database,
            ScalarEncoder(script),
        ).root()
        constants = _database_constants(database, (True, True))
        enabled = [
            outcome
            for outcome in family.outcomes
            if _ground(outcome.enabled, constants)
        ]

        self.assertEqual(len(enabled), 2)
        for outcome in enabled:
            self.assertFalse(_ground(outcome.error, constants))
            self.assertTrue(
                _ground(
                    outcome.relation.rows[0].values["result"].value,
                    constants,
                )
            )

    def test_global_aggregate_preserves_null_and_decimal_proof_metadata(self):
        raw = _global_decimal_sum_snapshot()
        evaluator, relation = _evaluate(raw, row_bound=2)
        scalar_value = relation.rows[0].values["result"]
        aggregate_value = (
            evaluator.node("sub_value")
            .certain()
            .rows[0]
            .values["sub.value"]
        )
        self.assertEqual(scalar_value.is_null, aggregate_value.is_null)
        self.assertEqual(scalar_value.value, aggregate_value.value)
        self.assertEqual(
            scalar_value.decimal_finite_abs_bound,
            aggregate_value.decimal_finite_abs_bound,
        )
        self.assertIsNotNone(scalar_value.decimal_finite_abs_bound)

    def test_filter_consumer_uses_binding_without_exposing_it(self):
        raw = _base_snapshot(_literal("Bool", False))
        raw["plan"]["nodes"][0] = {
            "id": "main_source",
            "op": "project",
            "input": "main_unit",
            "ordered": False,
            "columns": [
                {
                    "output": "carrier",
                    "expression": _literal("Int64", 1),
                }
            ],
        }
        raw["plan"]["nodes"].insert(
            0,
            {"id": "main_unit", "op": "empty_source"},
        )
        raw["plan"]["nodes"][2] = {
            "id": "main_project",
            "op": "filter",
            "input": "main_source",
            "predicate": {"kind": "column", "column": BINDING},
        }
        raw["plan"]["output"] = ["carrier"]
        _, relation = _evaluate(raw)
        self.assertEqual(tuple(column.name for column in relation.columns), ("carrier",))
        self.assertEqual(set(relation.rows[0].values), {"carrier"})
        self.assertEqual(relation.rows[0].present, smt.FALSE)

    def test_sort_and_limit_wrappers_over_one_candidate_are_deterministic(self):
        wrappers = (
            (
                "sort",
                {
                    "id": "sub_wrapper",
                    "op": "sort",
                    "input": "sub_value",
                    "order": [
                        {
                            "column": "sub.value",
                            "ascending": True,
                            "nulls_first": True,
                        }
                    ],
                    "limit": None,
                    "phase": "undefined",
                },
                False,
            ),
            (
                "limit-one",
                {
                    "id": "sub_wrapper",
                    "op": "limit",
                    "input": "sub_value",
                    "count": _literal("Uint64", 1),
                    "offset": None,
                    "phase": "undefined",
                },
                False,
            ),
            (
                "limit-offset-one",
                {
                    "id": "sub_wrapper",
                    "op": "limit",
                    "input": "sub_value",
                    "count": _literal("Uint64", 1),
                    "offset": _literal("Uint64", 1),
                    "phase": "undefined",
                },
                True,
            ),
        )
        for name, wrapper, expected_null in wrappers:
            with self.subTest(name=name):
                raw = _base_snapshot()
                raw["plan"]["nodes"].append(wrapper)
                raw["plan"]["subplans"][0]["root"] = "sub_wrapper"

                _, relation = _evaluate(raw)

                result = relation.rows[0].values["result"]
                self.assertEqual(result.is_null, smt.bool_value(expected_null))
                if not expected_null:
                    self.assertEqual(result.value, smt.int_value(7))

    def test_general_scalar_observes_null_value_and_multirow_error(self):
        raw = _scan_scalar_snapshot()
        snapshot = parse_snapshot(raw)
        script = smt.Script()
        database = Database(snapshot, 2, script)
        family = Evaluator(
            snapshot,
            database,
            ScalarEncoder(script),
        ).root()
        self.assertEqual(len(family.outcomes), 1)
        outcome = family.outcomes[0]
        result = outcome.relation.rows[0].values["result"]

        for present, error, is_null, value in (
            ((False, False), False, True, None),
            ((True, False), False, False, 10),
            ((False, True), False, False, 20),
            ((True, True), True, False, None),
        ):
            constants = _database_constants(database, present)
            with self.subTest(present=present):
                self.assertEqual(_ground(outcome.error, constants), error)
                if not error:
                    self.assertEqual(_ground(result.is_null, constants), is_null)
                    if value is not None:
                        self.assertEqual(_ground(result.value, constants), value)

    def test_scalar_error_is_suppressed_without_a_consumer_input_row(self):
        raw = _scan_scalar_snapshot()
        raw["plan"]["nodes"].insert(
            1,
            {
                "id": "empty_outer",
                "op": "filter",
                "input": "main_source",
                "predicate": _literal("Bool", False),
            },
        )
        raw["plan"]["nodes"][2]["input"] = "empty_outer"
        snapshot = parse_snapshot(raw)
        script = smt.Script()
        database = Database(snapshot, 2, script)
        outcome = Evaluator(
            snapshot,
            database,
            ScalarEncoder(script),
        ).root().outcomes[0]

        self.assertFalse(
            _ground(
                outcome.error,
                _database_constants(database, (True, True)),
            )
        )

    def test_inherited_scalar_error_is_observed_without_a_consumer_input_row(self):
        raw = _scan_scalar_snapshot()
        raw["plan"]["nodes"].append(
            {
                "id": "inner_cardinality_error",
                "op": "limit",
                "input": "sub_scan",
                "count": _literal("Uint64", 2),
                "offset": None,
                "phase": "undefined",
                "ensure_at_most_one": True,
            }
        )
        raw["plan"]["subplans"][0]["root"] = "inner_cardinality_error"
        raw["plan"]["nodes"].insert(
            1,
            {
                "id": "empty_outer",
                "op": "filter",
                "input": "main_source",
                "predicate": _literal("Bool", False),
            },
        )
        raw["plan"]["nodes"][2]["input"] = "empty_outer"
        snapshot = parse_snapshot(raw)
        script = smt.Script()
        database = Database(snapshot, 2, script)
        family = Evaluator(
            snapshot,
            database,
            ScalarEncoder(script),
        ).root()
        constants = _database_constants(database, (True, True))
        enabled = [
            outcome
            for outcome in family.outcomes
            if _ground(outcome.enabled, constants)
        ]

        self.assertTrue(enabled)
        self.assertTrue(
            all(_ground(outcome.error, constants) for outcome in enabled)
        )

    def test_multiple_bindings_keep_inherited_and_local_errors_separate(self):
        raw = _scan_scalar_snapshot()
        raw["plan"]["nodes"].insert(
            1,
            {
                "id": "empty_outer",
                "op": "filter",
                "input": "main_source",
                "predicate": _literal("Bool", False),
            },
        )
        raw["plan"]["nodes"][2]["input"] = "empty_outer"
        raw["plan"]["nodes"][2]["columns"][0]["expression"] = {
            "kind": "add",
            "left": {"kind": "column", "column": BINDING},
            "right": {"kind": "column", "column": "$scalar2"},
            "type": "Int64",
            "nullable": True,
        }
        raw["plan"]["nodes"].extend(
            [
                {
                    "id": "sub_scan_two",
                    "op": "scan",
                    "table": "A",
                    "columns": [{"source": "x", "output": "sub2.value"}],
                    "predicate": None,
                    "pushed_limit": None,
                },
                {
                    "id": "inner_cardinality_error",
                    "op": "limit",
                    "input": "sub_scan_two",
                    "count": _literal("Uint64", 2),
                    "offset": None,
                    "phase": "undefined",
                    "ensure_at_most_one": True,
                },
            ]
        )
        second = copy.deepcopy(raw["plan"]["subplans"][0])
        second.update(
            {
                "binding": "$scalar2",
                "root": "inner_cardinality_error",
                "output": {
                    "column": "sub2.value",
                    "type": "Int64",
                    "nullable": False,
                },
            }
        )
        raw["plan"]["subplans"].append(second)

        snapshot = parse_snapshot(raw)
        script = smt.Script()
        database = Database(snapshot, 2, script)
        family = Evaluator(
            snapshot,
            database,
            ScalarEncoder(script),
        ).root()
        constants = _database_constants(database, (True, True))
        enabled = [
            outcome
            for outcome in family.outcomes
            if _ground(outcome.enabled, constants)
        ]

        self.assertTrue(enabled)
        self.assertTrue(
            all(_ground(outcome.error, constants) for outcome in enabled)
        )

    def test_scalar_error_is_consumer_eager_across_dead_if_branch(self):
        raw = _scan_scalar_snapshot()
        raw["plan"]["nodes"][1]["columns"][0]["expression"] = {
            "kind": "if",
            "condition": _literal("Bool", False),
            "then": {"kind": "column", "column": BINDING},
            "else": _literal("Int64", 7),
            "type": "Int64",
            "nullable": True,
        }
        snapshot = parse_snapshot(raw)
        script = smt.Script()
        database = Database(snapshot, 2, script)
        outcome = Evaluator(
            snapshot,
            database,
            ScalarEncoder(script),
        ).root().outcomes[0]

        self.assertTrue(
            _ground(
                outcome.error,
                _database_constants(database, (True, True)),
            )
        )

    def test_checked_scalar_lowering_matches_a_demanded_initial_binding(self):
        before = parse_snapshot(_scan_scalar_snapshot())
        after = parse_snapshot(
            _lowered_scalar_snapshot(check_mode="gated")
        )
        script = smt.Script()
        database = Database(before, 2, script)
        scalar = ScalarEncoder(script)
        equality = family_equal(
            Evaluator(
                before,
                database,
                scalar,
                choice_scope="before",
            ).root(),
            Evaluator(
                after,
                database,
                scalar,
                choice_scope="after",
            ).root(),
            scalar,
        )

        for present in (
            (False, False),
            (True, False),
            (False, True),
            (True, True),
        ):
            with self.subTest(present=present):
                self.assertTrue(
                    _ground(equality, _database_constants(database, present))
                )

    def test_missing_and_eager_checks_are_counterexamples(self):
        before = parse_snapshot(_scan_scalar_snapshot())

        def equal_to(raw_after):
            script = smt.Script()
            database = Database(before, 2, script)
            scalar = ScalarEncoder(script)
            equality = family_equal(
                Evaluator(
                    before,
                    database,
                    scalar,
                    choice_scope="before",
                ).root(),
                Evaluator(
                    parse_snapshot(raw_after),
                    database,
                    scalar,
                    choice_scope="after",
                ).root(),
                scalar,
            )
            return _ground(
                equality,
                _database_constants(database, (True, True)),
            )

        self.assertFalse(
            equal_to(_lowered_scalar_snapshot(check_mode="none"))
        )

        empty_before_raw = _scan_scalar_snapshot()
        empty_before_raw["plan"]["nodes"].insert(
            1,
            {
                "id": "empty_outer",
                "op": "filter",
                "input": "main_source",
                "predicate": _literal("Bool", False),
            },
        )
        empty_before_raw["plan"]["nodes"][2]["input"] = "empty_outer"
        empty_before = parse_snapshot(empty_before_raw)
        script = smt.Script()
        database = Database(empty_before, 2, script)
        scalar = ScalarEncoder(script)
        equality = family_equal(
            Evaluator(empty_before, database, scalar).root(),
            Evaluator(
                parse_snapshot(
                    _lowered_scalar_snapshot(
                        check_mode="eager",
                        empty_outer=True,
                    )
                ),
                database,
                scalar,
            ).root(),
            scalar,
        )
        self.assertFalse(
            _ground(
                equality,
                _database_constants(database, (True, True)),
            )
        )

    def test_gated_check_matches_an_empty_consumer(self):
        before_raw = _scan_scalar_snapshot()
        before_raw["plan"]["nodes"].insert(
            1,
            {
                "id": "empty_outer",
                "op": "filter",
                "input": "main_source",
                "predicate": _literal("Bool", False),
            },
        )
        before_raw["plan"]["nodes"][2]["input"] = "empty_outer"
        before = parse_snapshot(before_raw)
        after = parse_snapshot(
            _lowered_scalar_snapshot(
                check_mode="gated",
                empty_outer=True,
            )
        )
        script = smt.Script()
        database = Database(before, 2, script)
        scalar = ScalarEncoder(script)
        equality = family_equal(
            Evaluator(
                before,
                database,
                scalar,
                choice_scope="before",
            ).root(),
            Evaluator(
                after,
                database,
                scalar,
                choice_scope="after",
            ).root(),
            scalar,
        )

        for present in (
            (False, False),
            (True, False),
            (False, True),
            (True, True),
        ):
            with self.subTest(present=present):
                self.assertTrue(
                    _ground(
                        equality,
                        _database_constants(database, present),
                    )
                )


class ScalarSubplanValidationTest(unittest.TestCase):
    def test_legacy_plan_without_subplans_defaults_to_empty(self):
        raw = _base_snapshot()
        raw["plan"]["nodes"][1]["columns"][0]["expression"] = _literal(
            "Int64",
            7,
        )
        del raw["plan"]["nodes"][2:]
        del raw["plan"]["subplans"]

        snapshot = parse_snapshot(raw)

        self.assertEqual(snapshot.plan.subplans, ())

    def test_descriptor_and_output_objects_are_strict(self):
        for target in ("descriptor", "output"):
            with self.subTest(target=target):
                raw = _base_snapshot()
                obj = (
                    raw["plan"]["subplans"][0]
                    if target == "descriptor"
                    else raw["plan"]["subplans"][0]["output"]
                )
                obj["surprise"] = True
                with self.assertRaisesRegex(SnapshotError, "unknown fields: surprise"):
                    parse_snapshot(raw)

    def test_descriptor_type_and_nullability_are_exact(self):
        mutations = (
            ("type", "Uint64", "must exactly match"),
            ("nullable", False, "zero rows yield NULL"),
        )
        for field, value, message in mutations:
            with self.subTest(field=field):
                raw = _base_snapshot()
                raw["plan"]["subplans"][0][field] = value
                with self.assertRaisesRegex(SnapshotError, message):
                    parse_snapshot(raw)

    def test_dependencies_and_non_scalar_kinds_fail_closed(self):
        raw = _base_snapshot()
        raw["plan"]["subplans"][0]["dependencies"] = ["outer.x"]
        with self.assertRaisesRegex(SnapshotError, "correlated scalar"):
            parse_snapshot(raw)

        raw = _base_snapshot()
        raw["plan"]["subplans"][0]["kind"] = "exists"
        with self.assertRaisesRegex(SnapshotError, "unsupported subplan kind"):
            parse_snapshot(raw)

    def test_consumers_are_exact_project_or_filter_references(self):
        raw = _base_snapshot()
        raw["plan"]["subplans"][0]["consumers"] = []
        with self.assertRaisesRegex(SnapshotError, "binding is unused"):
            parse_snapshot(raw)

        raw = _base_snapshot()
        raw["plan"]["nodes"][1]["columns"][0]["expression"] = _literal("Int64", 0)
        with self.assertRaisesRegex(SnapshotError, "does not reference binding"):
            parse_snapshot(raw)

        raw = _base_snapshot()
        raw["plan"]["subplans"][0]["consumers"] = ["main_source"]
        with self.assertRaisesRegex(SnapshotError, "Project or Filter"):
            parse_snapshot(raw)

        raw = _base_snapshot()
        raw["plan"]["subplans"][0]["consumers"] = ["missing"]
        with self.assertRaisesRegex(SnapshotError, "unknown node"):
            parse_snapshot(raw)

    def test_undeclared_binding_reference_is_rejected(self):
        raw = _base_snapshot()
        raw["plan"]["nodes"].insert(
            2,
            {
                "id": "extra_project",
                "op": "project",
                "input": "main_source",
                "ordered": False,
                "columns": [
                    {
                        "output": "extra",
                        "expression": {"kind": "column", "column": BINDING},
                    }
                ],
            },
        )
        raw["plan"]["nodes"].append(
            {
                "id": "main_union",
                "op": "union_all",
                "inputs": [
                    {"node": "main_project", "columns": ["result"]},
                    {"node": "extra_project", "columns": ["extra"]},
                ],
                "output": ["result"],
                "ordered": False,
            }
        )
        raw["plan"]["root"] = "main_union"
        with self.assertRaisesRegex(SnapshotError, "column '\\$scalar' is not available"):
            parse_snapshot(raw)

    def test_duplicate_and_input_colliding_bindings_are_rejected(self):
        raw = _base_snapshot()
        raw["plan"]["subplans"].append(
            copy.deepcopy(raw["plan"]["subplans"][0])
        )
        with self.assertRaisesRegex(SnapshotError, "duplicate name"):
            parse_snapshot(raw)

        raw = _base_snapshot()
        raw["plan"]["nodes"][0] = {
            "id": "main_source",
            "op": "project",
            "input": "main_unit",
            "ordered": False,
            "columns": [
                {
                    "output": BINDING,
                    "expression": _literal("Int64", 1),
                }
            ],
        }
        raw["plan"]["nodes"].append(
            {"id": "main_unit", "op": "empty_source"}
        )
        with self.assertRaisesRegex(SnapshotError, "collides with an input column"):
            parse_snapshot(raw)

    def test_binding_cannot_leak_into_main_relational_output(self):
        raw = _base_snapshot()
        raw["plan"]["nodes"][1]["columns"][0]["output"] = BINDING
        raw["plan"]["output"] = [BINDING]
        with self.assertRaisesRegex(SnapshotError, "must remain virtual"):
            parse_snapshot(raw)

    def test_output_schema_and_reachability_are_exact(self):
        raw = _base_snapshot()
        raw["plan"]["subplans"][0]["output"]["column"] = "missing"
        with self.assertRaisesRegex(SnapshotError, "is not produced by its root"):
            parse_snapshot(raw)

        raw = _base_snapshot()
        raw["plan"]["subplans"][0]["output"]["nullable"] = True
        with self.assertRaisesRegex(SnapshotError, "does not match its root"):
            parse_snapshot(raw)

        raw = _base_snapshot()
        raw["plan"]["subplans"][0]["type"] = "Uint64"
        raw["plan"]["subplans"][0]["output"]["type"] = "Uint64"
        with self.assertRaisesRegex(SnapshotError, "does not match its root"):
            parse_snapshot(raw)

        raw = _base_snapshot()
        raw["plan"]["nodes"].append(
            {"id": "orphan", "op": "empty_source"}
        )
        with self.assertRaisesRegex(SnapshotError, "nodes are not reachable"):
            parse_snapshot(raw)

    def test_scalar_root_may_retain_non_result_columns(self):
        raw = _base_snapshot()
        raw["plan"]["nodes"][2] = {
            "id": "sub_source",
            "op": "project",
            "input": "sub_unit",
            "ordered": False,
            "columns": [
                {
                    "output": "sub.auxiliary",
                    "expression": _literal("Int64", 9),
                }
            ],
        }
        raw["plan"]["nodes"].append(
            {"id": "sub_unit", "op": "empty_source"}
        )
        raw["plan"]["nodes"][3]["columns"].insert(
            0,
            {
                "output": "sub.auxiliary",
                "expression": {
                    "kind": "column",
                    "column": "sub.auxiliary",
                },
            },
        )

        evaluator, relation = _evaluate(raw)
        self.assertEqual(
            tuple(evaluator.schemas["sub_value"]),
            ("sub.auxiliary", "sub.value"),
        )
        result = relation.rows[0].values["result"]
        self.assertEqual(result.is_null, smt.FALSE)
        self.assertEqual(result.value, smt.int_value(7))

    def test_subplans_must_not_survive_into_stage_graph(self):
        raw = _base_snapshot()
        raw["stage_graph"] = {
            "root_stage": "ignored",
            "stages": [],
            "edges": [],
            "assumptions": [],
        }
        with self.assertRaisesRegex(SnapshotError, "fully eliminated"):
            parse_snapshot(raw)

    def test_nested_subplan_binding_reference_is_rejected(self):
        raw = _base_snapshot()
        raw["plan"]["nodes"][3]["columns"][0]["expression"] = {
            "kind": "column",
            "column": BINDING,
        }
        raw["plan"]["subplans"][0]["output"]["nullable"] = True
        raw["plan"]["subplans"][0]["consumers"].append("sub_value")
        with self.assertRaisesRegex(
            SnapshotError,
            "subplan expressions may not reference",
        ):
            parse_snapshot(raw)

    def test_general_scalar_shapes_pass_schema_validation(self):
        raw = _base_snapshot()
        raw["schema"]["tables"] = [
            {
                "name": "A",
                "columns": [
                    {"name": "x", "type": "Int64", "nullable": False}
                ],
                "unique_keys": [],
            }
        ]
        raw["plan"]["nodes"][2:] = [
            {
                "id": "sub_value",
                "op": "scan",
                "table": "A",
                "columns": [{"source": "x", "output": "sub.value"}],
                "predicate": None,
                "pushed_limit": None,
            }
        ]
        parse_snapshot(raw)

        raw = _global_decimal_sum_snapshot()
        raw["plan"]["nodes"][-1]["phase"] = "intermediate"
        parse_snapshot(raw)

        raw = _base_snapshot()
        raw["plan"]["nodes"].append(
            {
                "id": "sub_limit",
                "op": "limit",
                "input": "sub_value",
                "count": _literal("Uint64", 2),
                "offset": None,
                "phase": "undefined",
            }
        )
        raw["plan"]["subplans"][0]["root"] = "sub_limit"
        parse_snapshot(raw)

        for operation in ("join", "union_all"):
            with self.subTest(operation=operation):
                raw = _base_snapshot()
                if operation == "join":
                    raw["plan"]["nodes"][3] = {
                        "id": "sub_value",
                        "op": "join",
                        "left": "sub_left",
                        "right": "sub_right",
                        "kind": "left_semi",
                        "predicate": _literal("Bool", True),
                    }
                    raw["plan"]["nodes"].extend(
                        [
                            {
                                "id": "sub_left",
                                "op": "project",
                                "input": "sub_source",
                                "ordered": False,
                                "columns": [
                                    {
                                        "output": "sub.value",
                                        "expression": _literal("Int64", 1),
                                    }
                                ],
                            },
                            {
                                "id": "sub_right",
                                "op": "project",
                                "input": "sub_source",
                                "ordered": False,
                                "columns": [
                                    {
                                        "output": "right.value",
                                        "expression": _literal("Int64", 2),
                                    }
                                ],
                            },
                        ]
                    )
                else:
                    raw["plan"]["nodes"][3] = {
                        "id": "sub_value",
                        "op": "union_all",
                        "inputs": [
                            {"node": "sub_left", "columns": ["value"]},
                            {"node": "sub_right", "columns": ["value"]},
                        ],
                        "output": ["sub.value"],
                        "ordered": False,
                    }
                    raw["plan"]["nodes"].extend(
                        [
                            {
                                "id": "sub_left",
                                "op": "project",
                                "input": "sub_source",
                                "ordered": False,
                                "columns": [
                                    {
                                        "output": "value",
                                        "expression": _literal("Int64", 1),
                                    }
                                ],
                            },
                            {
                                "id": "sub_right",
                                "op": "project",
                                "input": "sub_source",
                                "ordered": False,
                                "columns": [
                                    {
                                        "output": "value",
                                        "expression": _literal("Int64", 2),
                                    }
                                ],
                            },
                        ]
                    )
                parse_snapshot(raw)

    def test_limit_one_shape_preserves_runtime_choices(self):
        raw = _base_snapshot()
        raw["schema"]["tables"] = [
            {
                "name": "A",
                "columns": [
                    {"name": "x", "type": "Int64", "nullable": False}
                ],
                "unique_keys": [],
            }
        ]
        raw["plan"]["nodes"][2:] = [
            {
                "id": "sub_scan",
                "op": "scan",
                "table": "A",
                "columns": [{"source": "x", "output": "sub.value"}],
                "predicate": None,
                "pushed_limit": None,
            },
            {
                "id": "sub_value",
                "op": "limit",
                "input": "sub_scan",
                "count": _literal("Uint64", 1),
                "offset": None,
                "phase": "undefined",
            },
        ]
        deterministic_empty = copy.deepcopy(raw)
        deterministic_empty["plan"]["nodes"][-1]["count"] = _literal(
            "Uint64",
            0,
        )
        _, relation = _evaluate(deterministic_empty, row_bound=2)
        self.assertEqual(
            relation.rows[0].values["result"].is_null,
            smt.TRUE,
        )

        snapshot = parse_snapshot(raw)
        script = smt.Script()
        evaluator = Evaluator(
            snapshot,
            Database(snapshot, 2, script),
            ScalarEncoder(script),
        )
        family = evaluator.root()
        self.assertGreater(len(family.outcomes), 1)
        self.assertTrue(
            all(outcome.decisions for outcome in family.outcomes)
        )


if __name__ == "__main__":
    unittest.main()
