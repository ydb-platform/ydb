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
    RelationError,
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


class ScalarSubplanEvaluationTest(unittest.TestCase):
    def test_present_scalar_value_is_injected_only_into_expression_scope(self):
        evaluator, relation = _evaluate(_base_snapshot())
        self.assertEqual(tuple(column.name for column in relation.columns), ("result",))
        self.assertEqual(set(relation.rows[0].values), {"result"})
        result = relation.rows[0].values["result"]
        self.assertEqual(result.type, "Int64")
        self.assertEqual(result.is_null, smt.FALSE)
        self.assertEqual(result.value, smt.int_value(7))
        self.assertEqual(set(evaluator.scalar_subplan_values), {BINDING})

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
        self.assertEqual(len(evaluator.scalar_subplan_values), 1)

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

    def test_static_shape_rejects_scan_join_union_and_intermediate_aggregate(self):
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
        with self.assertRaisesRegex(SnapshotError, "at most one row"):
            parse_snapshot(raw)

        raw = _global_decimal_sum_snapshot()
        raw["plan"]["nodes"][-1]["phase"] = "intermediate"
        with self.assertRaisesRegex(SnapshotError, "at most one row"):
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
        with self.assertRaisesRegex(SnapshotError, "at most one row"):
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
                with self.assertRaisesRegex(SnapshotError, "at most one row"):
                    parse_snapshot(raw)

    def test_limit_one_shape_with_runtime_choices_fails_closed(self):
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
        with self.assertRaisesRegex(
            RelationError,
            "conditional relation outcomes",
        ):
            evaluator.root()


if __name__ == "__main__":
    unittest.main()
