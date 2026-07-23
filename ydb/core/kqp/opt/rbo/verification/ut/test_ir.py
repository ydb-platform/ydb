import copy
import tempfile
import unittest
from itertools import product
from pathlib import Path

from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    MAX_BOUND_DEPTH,
    MAX_EXPR_DEPTH,
    MAX_EXPR_NODES,
    SnapshotError,
    load_snapshot,
    parse_snapshot,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.types import (
    INTEGER_TYPES,
    integer_bounds,
    static_in_comparison_compatible,
)


def minimal_snapshot():
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": {
            "tables": [
                {
                    "name": "A",
                    "columns": [
                        {"name": "k", "type": "Int64", "nullable": False},
                        {"name": "flag", "type": "Bool", "nullable": True},
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
                    "table": "A",
                    "columns": [
                        {"source": "k", "output": "a.k"},
                        {"source": "flag", "output": "a.flag"},
                    ],
                    "predicate": None,
                    "pushed_limit": None,
                },
                {
                    "id": "filter",
                    "op": "filter",
                    "input": "scan",
                    "predicate": {"kind": "column", "column": "a.flag"},
                },
            ],
            "root": "filter",
            "output": ["a.k"],
            "subplans": [],
        },
        "stage_graph": None,
    }


def count_star_snapshot():
    value = minimal_snapshot()
    value["plan"]["nodes"] = [
        value["plan"]["nodes"][0],
        {
            "id": "count_input",
            "op": "project",
            "input": "scan",
            "ordered": False,
            "columns": [
                {
                    "output": "_count_input",
                    "expression": {"kind": "void"},
                }
            ],
        },
        {
            "id": "aggregate",
            "op": "aggregate",
            "input": "count_input",
            "keys": [],
            "aggregates": [
                {
                    "input": "_count_input",
                    "function": "count",
                    "output": "result",
                    "type": "Uint64",
                    "nullable": False,
                    "distinct": False,
                    "unwrap": False,
                }
            ],
            "phase": "undefined",
            "distinct_all": False,
        },
    ]
    value["plan"]["root"] = "aggregate"
    value["plan"]["output"] = ["result"]
    return value


def decimal_div_snapshot(
    right_type="Decimal(7,2)",
    *,
    left_nullable=False,
    right_nullable=False,
    result_nullable=None,
):
    value = minimal_snapshot()
    value["schema"]["tables"][0]["columns"] = [
        {
            "name": "amount",
            "type": "Decimal(7,2)",
            "nullable": left_nullable,
        },
        {
            "name": "divisor",
            "type": right_type,
            "nullable": right_nullable,
        },
    ]
    value["plan"]["nodes"][0]["columns"] = [
        {"source": "amount", "output": "a.amount"},
        {"source": "divisor", "output": "a.divisor"},
    ]
    value["plan"]["nodes"][1]["predicate"] = {
        "kind": "eq",
        "left": {
            "kind": "div",
            "left": {"kind": "column", "column": "a.amount"},
            "right": {"kind": "column", "column": "a.divisor"},
            "type": "Decimal(7,2)",
            "nullable": (
                left_nullable or right_nullable
                if result_nullable is None
                else result_nullable
            ),
        },
        "right": {
            "kind": "literal",
            "type": "Decimal(7,2)",
            "value": {"kind": "finite", "scaled": "0"},
        },
    }
    value["plan"]["output"] = ["a.amount"]
    return value


def integral_safe_cast_snapshot(
    source_type="Int64",
    target_type="Int32",
    *,
    source_nullable=True,
    result_nullable=True,
):
    value = minimal_snapshot()
    value["schema"]["tables"][0]["columns"][0].update(
        type=source_type,
        nullable=source_nullable,
    )
    value["plan"]["nodes"][1]["predicate"] = {
        "kind": "exists",
        "arg": {
            "kind": "cast_integral",
            "arg": {"kind": "column", "column": "a.k"},
            "type": target_type,
            "nullable": result_nullable,
        },
    }
    return value


def integral_conversion_may_fail(source_type, target_type):
    source_lower, source_upper = integer_bounds(source_type)
    target_lower, target_upper = integer_bounds(target_type)
    return not (
        target_lower <= source_lower and source_upper <= target_upper
    )


def if_present_snapshot():
    value = minimal_snapshot()
    value["schema"]["tables"][0]["columns"][0]["nullable"] = True
    value["plan"]["nodes"][1]["predicate"] = {
        "kind": "eq",
        "left": {
            "kind": "if_present",
            "optional": {"kind": "column", "column": "a.k"},
            "present": {"kind": "bound", "depth": 0},
            "missing": {"kind": "literal", "type": "Int64", "value": 0},
            "type": "Int64",
            "nullable": False,
        },
        "right": {"kind": "literal", "type": "Int64", "value": 1},
    }
    return value


def if_snapshot():
    value = minimal_snapshot()
    value["plan"]["nodes"][1]["predicate"] = {
        "kind": "if",
        "condition": {
            "kind": "exists",
            "arg": {"kind": "column", "column": "a.flag"},
        },
        "then": {"kind": "literal", "type": "Bool", "value": True},
        "else": {"kind": "literal", "type": "Bool", "value": False},
        "type": "Bool",
        "nullable": False,
    }
    return value


def boolean_expression_with_nodes(nodes):
    if nodes == 1:
        return {"kind": "column", "column": "a.flag"}
    return {
        "kind": "and",
        "args": [
            {"kind": "column", "column": "a.flag"}
            for _ in range(nodes - 1)
        ],
    }


def boolean_expression_with_depth(depth):
    expression = {"kind": "column", "column": "a.flag"}
    for _ in range(depth - 1):
        expression = {"kind": "not", "arg": expression}
    return expression


class SnapshotTest(unittest.TestCase):
    def test_valid_snapshot_has_inferred_root_schema(self):
        snapshot = parse_snapshot(minimal_snapshot())
        self.assertEqual([(column.name, column.type, column.nullable) for column in snapshot.output_schema()], [
            ("a.k", "Int64", False)
        ])

    def test_expression_expanded_node_budget_has_exact_boundary(self):
        accepted = minimal_snapshot()
        accepted["plan"]["nodes"][1]["predicate"] = boolean_expression_with_nodes(
            MAX_EXPR_NODES
        )
        parse_snapshot(accepted)

        rejected = minimal_snapshot()
        rejected["plan"]["nodes"][1]["predicate"] = boolean_expression_with_nodes(
            MAX_EXPR_NODES + 1
        )
        with self.assertRaisesRegex(
            SnapshotError,
            "expanded expression node count exceeds the audit limit of 1024",
        ):
            parse_snapshot(rejected)

    def test_expression_structural_depth_budget_has_exact_boundary(self):
        accepted = minimal_snapshot()
        accepted["plan"]["nodes"][1]["predicate"] = boolean_expression_with_depth(
            MAX_EXPR_DEPTH
        )
        parse_snapshot(accepted)

        rejected = minimal_snapshot()
        rejected["plan"]["nodes"][1]["predicate"] = boolean_expression_with_depth(
            MAX_EXPR_DEPTH + 1
        )
        with self.assertRaisesRegex(
            SnapshotError,
            "expression structural depth exceeds the audit limit of 128",
        ):
            parse_snapshot(rejected)

    def test_expression_node_budget_is_shared_by_sibling_subtrees(self):
        value = minimal_snapshot()
        subtree_nodes = MAX_EXPR_NODES // 2
        value["plan"]["nodes"][1]["predicate"] = {
            "kind": "and",
            "args": [
                boolean_expression_with_nodes(subtree_nodes),
                boolean_expression_with_nodes(subtree_nodes),
            ],
        }
        with self.assertRaisesRegex(
            SnapshotError,
            "expanded expression node count exceeds the audit limit of 1024",
        ):
            parse_snapshot(value)

    def test_expression_budget_resets_between_complete_roots(self):
        value = minimal_snapshot()
        scan = value["plan"]["nodes"][0]
        value["plan"]["nodes"] = [
            scan,
            {
                "id": "filter",
                "op": "filter",
                "input": "scan",
                "predicate": boolean_expression_with_nodes(MAX_EXPR_NODES),
            },
            {
                "id": "project",
                "op": "project",
                "input": "filter",
                "ordered": False,
                "columns": [
                    {
                        "output": "first",
                        "expression": boolean_expression_with_nodes(MAX_EXPR_NODES),
                    },
                    {
                        "output": "second",
                        "expression": boolean_expression_with_nodes(MAX_EXPR_NODES),
                    },
                ],
            },
            {
                "id": "limit",
                "op": "limit",
                "input": "project",
                "count": {"kind": "literal", "type": "Uint64", "value": 10},
                "offset": {"kind": "literal", "type": "Uint64", "value": 1},
                "phase": "undefined",
            },
        ]
        value["plan"]["root"] = "limit"
        value["plan"]["output"] = ["first", "second"]
        parse_snapshot(value)

    def test_deep_json_file_fails_with_snapshot_error(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "deep.json"
            path.write_text("[" * 10000 + "0" + "]" * 10000, encoding="utf-8")
            with self.assertRaisesRegex(
                SnapshotError,
                "JSON nesting exceeds the decoder limit",
            ):
                load_snapshot(path)

    def test_legacy_v1_scan_without_pushdowns_defaults_to_none(self):
        value = minimal_snapshot()
        del value["plan"]["nodes"][0]["predicate"]
        del value["plan"]["nodes"][0]["pushed_limit"]
        snapshot = parse_snapshot(value)
        self.assertIsNone(snapshot.plan.nodes[0].predicate)
        self.assertIsNone(snapshot.plan.nodes[0].pushed_limit)

    def test_legacy_v1_plan_without_subplans_defaults_to_empty(self):
        value = minimal_snapshot()
        del value["plan"]["subplans"]

        snapshot = parse_snapshot(value)

        self.assertEqual(snapshot.plan.subplans, ())

    def test_pushed_scan_predicate_is_strict_typed_and_column_only(self):
        value = minimal_snapshot()
        value["plan"]["nodes"][0]["predicate"] = {
            "kind": "gte",
            "left": {"kind": "column", "column": "a.k"},
            "right": {"kind": "literal", "type": "Int64", "value": 30},
        }
        value["stage_graph"] = {
            "root_stage": "source",
            "stages": [
                {
                    "id": "source",
                    "nodes": ["scan", "filter"],
                    "inputs": [],
                    "outputs": [{"index": 0, "node": "filter"}],
                    "source_storage": "column",
                }
            ],
            "edges": [],
            "assumptions": [],
        }
        snapshot = parse_snapshot(value)
        self.assertEqual(snapshot.plan.nodes[0].predicate.kind, "gte")

        row_source = copy.deepcopy(value)
        row_source["plan"]["nodes"] = [row_source["plan"]["nodes"][0]]
        row_source["plan"]["root"] = "scan"
        row_source["stage_graph"]["stages"][0]["nodes"] = ["scan"]
        row_source["stage_graph"]["stages"][0]["outputs"][0]["node"] = "scan"
        row_source["stage_graph"]["stages"][0]["source_storage"] = "row"
        with self.assertRaisesRegex(SnapshotError, "pushed scan predicate or limit"):
            parse_snapshot(row_source)

        unavailable = copy.deepcopy(value)
        unavailable["plan"]["nodes"][0]["predicate"]["left"]["column"] = "missing"
        with self.assertRaisesRegex(SnapshotError, "column 'missing' is not available"):
            parse_snapshot(unavailable)

        non_boolean = copy.deepcopy(value)
        non_boolean["plan"]["nodes"][0]["predicate"] = {
            "kind": "column",
            "column": "a.k",
        }
        with self.assertRaisesRegex(SnapshotError, "scan predicate must be Boolean"):
            parse_snapshot(non_boolean)

    def test_ordered_comparison_requires_supported_scalar_families(self):
        value = minimal_snapshot()
        value["plan"]["nodes"][1]["predicate"] = {
            "kind": "lt",
            "left": {"kind": "column", "column": "a.k"},
            "right": {"kind": "literal", "type": "Uint64", "value": 1},
        }
        parse_snapshot(value)

        value["schema"]["tables"][0]["columns"][0]["type"] = "String"
        value["plan"]["nodes"][1]["predicate"]["right"] = {
            "kind": "literal",
            "type": "String",
            "value": "1",
        }
        parse_snapshot(value)

        value["plan"]["nodes"][1]["predicate"]["right"]["type"] = "Utf8"
        parse_snapshot(value)

        value = minimal_snapshot()
        value["plan"]["nodes"][1]["predicate"] = {
            "kind": "gte",
            "left": {"kind": "column", "column": "a.k"},
            "right": {"kind": "literal", "type": "Int64", "value": 1},
            "null_safe": True,
        }
        with self.assertRaisesRegex(SnapshotError, "valid only for equality"):
            parse_snapshot(value)

    def test_every_integral_pair_is_admitted_by_every_ordinary_comparison(self):
        for left_type in sorted(INTEGER_TYPES):
            for right_type in sorted(INTEGER_TYPES):
                for kind, null_safe in (
                    ("eq", False),
                    ("eq", True),
                    ("lt", False),
                    ("lte", False),
                    ("gt", False),
                    ("gte", False),
                ):
                    value = minimal_snapshot()
                    value["schema"]["tables"][0]["columns"][0].update(
                        type=left_type,
                        nullable=True,
                    )
                    value["plan"]["nodes"][1]["predicate"] = {
                        "kind": kind,
                        "left": {"kind": "column", "column": "a.k"},
                        "right": {
                            "kind": "literal",
                            "type": right_type,
                            "value": 0,
                        },
                    }
                    if null_safe:
                        value["plan"]["nodes"][1]["predicate"]["null_safe"] = True
                    with self.subTest(
                        left_type=left_type,
                        right_type=right_type,
                        kind=kind,
                        null_safe=null_safe,
                    ):
                        parse_snapshot(value)

    def test_static_in_keeps_its_narrow_lossless_integer_gate(self):
        for left_type in sorted(INTEGER_TYPES):
            for right_type in sorted(INTEGER_TYPES):
                left_signed = left_type.startswith("Int")
                right_signed = right_type.startswith("Int")
                if left_signed == right_signed:
                    expected = True
                else:
                    signed_type = left_type if left_signed else right_type
                    unsigned_type = right_type if left_signed else left_type
                    expected = int(signed_type[3:]) > int(unsigned_type[4:])
                with self.subTest(left_type=left_type, right_type=right_type):
                    self.assertEqual(
                        static_in_comparison_compatible(left_type, right_type),
                        expected,
                    )

    def test_integer_literals_must_fit_their_declared_width(self):
        cases = {
            "Int8": (-(1 << 7), 1 << 7),
            "Int16": (-(1 << 15), 1 << 15),
            "Int32": (-(1 << 31), 1 << 31),
            "Int64": (-(1 << 63), 1 << 63),
            "Uint8": (0, 1 << 8),
            "Uint16": (0, 1 << 16),
            "Uint32": (0, 1 << 32),
            "Uint64": (0, 1 << 64),
        }
        for scalar_type, (lower, upper) in cases.items():
            for literal in (lower, upper - 1):
                value = minimal_snapshot()
                value["schema"]["tables"][0]["columns"][0]["type"] = scalar_type
                value["plan"]["nodes"][1]["predicate"] = {
                    "kind": "eq",
                    "left": {"kind": "column", "column": "a.k"},
                    "right": {"kind": "literal", "type": scalar_type, "value": literal},
                }
                with self.subTest(scalar_type=scalar_type, literal=literal):
                    parse_snapshot(value)

            for literal in (lower - 1, upper):
                value = minimal_snapshot()
                value["schema"]["tables"][0]["columns"][0]["type"] = scalar_type
                value["plan"]["nodes"][1]["predicate"] = {
                    "kind": "eq",
                    "left": {"kind": "column", "column": "a.k"},
                    "right": {"kind": "literal", "type": scalar_type, "value": literal},
                }
                with self.subTest(scalar_type=scalar_type, literal=literal):
                    with self.assertRaisesRegex(SnapshotError, "literal is outside"):
                        parse_snapshot(value)

    def test_string_literals_must_be_valid_unicode(self):
        for scalar_type in ("String", "Utf8"):
            value = minimal_snapshot()
            value["schema"]["tables"][0]["columns"][0]["type"] = scalar_type
            value["plan"]["nodes"][1]["predicate"] = {
                "kind": "eq",
                "left": {"kind": "column", "column": "a.k"},
                "right": {
                    "kind": "literal",
                    "type": scalar_type,
                    "value": "bad\ud800literal",
                },
            }
            with self.subTest(scalar_type=scalar_type):
                with self.assertRaisesRegex(SnapshotError, "not valid Unicode"):
                    parse_snapshot(value)

    def test_unknown_field_is_rejected(self):
        value = minimal_snapshot()
        value["plan"]["nodes"][0]["estimate"] = 42
        with self.assertRaisesRegex(SnapshotError, "unknown fields: estimate"):
            parse_snapshot(value)

    def test_unavailable_expression_column_is_rejected(self):
        value = minimal_snapshot()
        value["plan"]["nodes"][1]["predicate"] = {"kind": "column", "column": "missing"}
        with self.assertRaisesRegex(SnapshotError, "column 'missing' is not available"):
            parse_snapshot(value)

    def test_expression_types_are_checked(self):
        value = minimal_snapshot()
        value["plan"]["nodes"][1]["predicate"] = {
            "kind": "eq",
            "left": {"kind": "column", "column": "a.k"},
            "right": {"kind": "literal", "type": "String", "value": "1"},
        }
        with self.assertRaisesRegex(SnapshotError, "equality type mismatch"):
            parse_snapshot(value)

    def test_integer_arithmetic_schema_and_inference_are_strict(self):
        value = minimal_snapshot()
        value["plan"]["nodes"][1]["predicate"] = {
            "kind": "eq",
            "left": {
                "kind": "add",
                "left": {"kind": "column", "column": "a.k"},
                "right": {"kind": "literal", "type": "Int64", "value": 1},
                "type": "Int64",
                "nullable": False,
            },
            "right": {"kind": "literal", "type": "Int64", "value": 2},
        }
        snapshot = parse_snapshot(value)
        expression = snapshot.plan.nodes[1].predicate.args[0]
        self.assertEqual((expression.kind, expression.result_type, expression.nullable), (
            "add", "Int64", False
        ))

        unknown = copy.deepcopy(value)
        unknown["plan"]["nodes"][1]["predicate"]["left"]["args"] = []
        with self.assertRaisesRegex(SnapshotError, "unknown fields: args"):
            parse_snapshot(unknown)

        missing = copy.deepcopy(value)
        del missing["plan"]["nodes"][1]["predicate"]["left"]["nullable"]
        with self.assertRaisesRegex(SnapshotError, "missing fields: nullable"):
            parse_snapshot(missing)

        mismatched = copy.deepcopy(value)
        mismatched["plan"]["nodes"][1]["predicate"]["left"]["right"]["type"] = "Int32"
        with self.assertRaisesRegex(SnapshotError, "operands and result must have exactly the same type"):
            parse_snapshot(mismatched)

        non_integer = copy.deepcopy(value)
        arithmetic = non_integer["plan"]["nodes"][1]["predicate"]["left"]
        arithmetic["left"] = {"kind": "literal", "type": "Bool", "value": True}
        arithmetic["right"] = {"kind": "literal", "type": "Bool", "value": False}
        arithmetic["type"] = "Bool"
        with self.assertRaisesRegex(SnapshotError, "add requires an integer result"):
            parse_snapshot(non_integer)

        nullable = copy.deepcopy(value)
        nullable["schema"]["tables"][0]["columns"][0]["nullable"] = True
        with self.assertRaisesRegex(SnapshotError, "nullability must equal the OR"):
            parse_snapshot(nullable)

    def test_nullable_integral_safe_cast_accepts_exactly_partial_integer_pairs(self):
        for source_type in sorted(INTEGER_TYPES):
            for target_type in sorted(INTEGER_TYPES):
                for source_nullable in (False, True):
                    with self.subTest(
                        source_type=source_type,
                        target_type=target_type,
                        source_nullable=source_nullable,
                    ):
                        value = integral_safe_cast_snapshot(
                            source_type,
                            target_type,
                            source_nullable=source_nullable,
                        )
                        if not integral_conversion_may_fail(source_type, target_type):
                            with self.assertRaisesRegex(
                                SnapshotError,
                                "must be a partial conversion",
                            ):
                                parse_snapshot(value)
                            continue

                        snapshot = parse_snapshot(value)
                        expression = snapshot.plan.nodes[1].predicate.args[0]
                        self.assertEqual(
                            (
                                expression.kind,
                                expression.result_type,
                                expression.nullable,
                                expression.args[0].column,
                            ),
                            ("cast_integral", target_type, True, "a.k"),
                        )

    def test_nullable_integral_safe_cast_schema_and_types_fail_closed(self):
        valid = integral_safe_cast_snapshot()

        for field in ("arg", "type", "nullable"):
            malformed = copy.deepcopy(valid)
            del malformed["plan"]["nodes"][1]["predicate"]["arg"][field]
            with self.subTest(missing=field):
                with self.assertRaisesRegex(SnapshotError, f"missing fields: {field}"):
                    parse_snapshot(malformed)

        extra = copy.deepcopy(valid)
        extra["plan"]["nodes"][1]["predicate"]["arg"]["source_type"] = "Int64"
        with self.assertRaisesRegex(SnapshotError, "unknown fields: source_type"):
            parse_snapshot(extra)

        non_boolean = copy.deepcopy(valid)
        non_boolean["plan"]["nodes"][1]["predicate"]["arg"]["nullable"] = 1
        with self.assertRaisesRegex(SnapshotError, "expected a Boolean"):
            parse_snapshot(non_boolean)

        non_nullable = integral_safe_cast_snapshot(result_nullable=False)
        with self.assertRaisesRegex(SnapshotError, "result must be nullable"):
            parse_snapshot(non_nullable)

        for source_type in ("Bool", "Date", "String", "Decimal(5,2)"):
            with self.subTest(source_type=source_type):
                with self.assertRaisesRegex(SnapshotError, "source must be an integer"):
                    parse_snapshot(integral_safe_cast_snapshot(source_type=source_type))

        for target_type in ("Bool", "Date", "String", "Decimal(5,2)"):
            with self.subTest(target_type=target_type):
                with self.assertRaisesRegex(SnapshotError, "result must be an integer"):
                    parse_snapshot(integral_safe_cast_snapshot(target_type=target_type))

    def test_if_present_has_exact_scoped_unary_handler_semantics(self):
        snapshot = parse_snapshot(if_present_snapshot())
        expression = snapshot.plan.nodes[1].predicate.args[0]
        self.assertEqual(
            (expression.kind, expression.result_type, expression.nullable),
            ("if_present", "Int64", False),
        )
        self.assertEqual(expression.args[1].depth, 0)

        nullable = if_present_snapshot()
        nullable["plan"]["nodes"][1]["predicate"] = {
            "kind": "if_present",
            "optional": {"kind": "column", "column": "a.k"},
            "present": {"kind": "column", "column": "a.flag"},
            "missing": {"kind": "null", "type": "Bool"},
            "type": "Bool",
            "nullable": True,
        }
        parse_snapshot(nullable)

        opaque = if_present_snapshot()
        opaque["plan"]["nodes"][1]["predicate"]["left"]["present"] = {
            "kind": "opaque",
            "fingerprint": "use-bound",
            "type": "Int64",
            "nullable": False,
            "args": [{"kind": "bound", "depth": 0}],
        }
        parse_snapshot(opaque)

    def test_if_and_exists_have_exact_types_and_nullability(self):
        snapshot = parse_snapshot(if_snapshot())
        expression = snapshot.plan.nodes[1].predicate
        self.assertEqual(
            (expression.kind, expression.result_type, expression.nullable),
            ("if", "Bool", False),
        )
        self.assertEqual(expression.args[0].kind, "exists")

        optional_condition = if_snapshot()
        optional_condition["plan"]["nodes"][1]["predicate"]["condition"] = {
            "kind": "column",
            "column": "a.flag",
        }
        optional_condition["plan"]["nodes"][1]["predicate"]["nullable"] = True
        parse_snapshot(optional_condition)

    def test_if_and_exists_fail_closed_for_invalid_contracts(self):
        wrong_condition = if_snapshot()
        wrong_condition["plan"]["nodes"][1]["predicate"]["condition"] = {
            "kind": "column",
            "column": "a.k",
        }
        with self.assertRaisesRegex(SnapshotError, "If condition must be Boolean"):
            parse_snapshot(wrong_condition)

        wrong_branch = if_snapshot()
        wrong_branch["plan"]["nodes"][1]["predicate"]["then"] = {
            "kind": "literal",
            "type": "Int64",
            "value": 1,
        }
        with self.assertRaisesRegex(SnapshotError, "branch types"):
            parse_snapshot(wrong_branch)

        wrong_nullability = if_snapshot()
        wrong_nullability["plan"]["nodes"][1]["predicate"]["condition"] = {
            "kind": "column",
            "column": "a.flag",
        }
        with self.assertRaisesRegex(SnapshotError, "If nullability"):
            parse_snapshot(wrong_nullability)

        extra = if_snapshot()
        extra["plan"]["nodes"][1]["predicate"]["condition"]["type"] = "Bool"
        with self.assertRaisesRegex(SnapshotError, "unknown fields: type"):
            parse_snapshot(extra)

    def test_nested_if_present_uses_lexical_de_bruijn_depth(self):
        value = if_present_snapshot()
        value["schema"]["tables"][0]["columns"].append(
            {"name": "other", "type": "Int64", "nullable": True}
        )
        value["plan"]["nodes"][0]["columns"].append(
            {"source": "other", "output": "a.other"}
        )
        outer = value["plan"]["nodes"][1]["predicate"]["left"]
        outer["present"] = {
            "kind": "if_present",
            "optional": {"kind": "column", "column": "a.other"},
            "present": {
                "kind": "add",
                "left": {"kind": "bound", "depth": 1},
                "right": {"kind": "bound", "depth": 0},
                "type": "Int64",
                "nullable": False,
            },
            "missing": {"kind": "bound", "depth": 0},
            "type": "Int64",
            "nullable": False,
        }

        snapshot = parse_snapshot(value)
        inner = snapshot.plan.nodes[1].predicate.args[0].args[1]
        self.assertEqual(inner.args[1].args[0].depth, 1)
        self.assertEqual(inner.args[1].args[1].depth, 0)
        self.assertEqual(inner.args[2].depth, 0)

    def test_bound_nodes_cannot_escape_their_exact_handler_scope(self):
        for field in ("optional", "missing"):
            value = if_present_snapshot()
            value["plan"]["nodes"][1]["predicate"]["left"][field] = {
                "kind": "bound",
                "depth": 0,
            }
            with self.subTest(field=field):
                with self.assertRaisesRegex(SnapshotError, "enclosing IfPresent handler"):
                    parse_snapshot(value)

        for depth in (-1, True, 1):
            value = if_present_snapshot()
            value["plan"]["nodes"][1]["predicate"]["left"]["present"]["depth"] = depth
            with self.subTest(depth=depth):
                with self.assertRaises(SnapshotError):
                    parse_snapshot(value)

        value = minimal_snapshot()
        value["plan"]["nodes"][1]["predicate"] = {"kind": "bound", "depth": 0}
        with self.assertRaisesRegex(SnapshotError, "enclosing IfPresent handler"):
            parse_snapshot(value)

    def test_if_present_binding_depth_has_an_explicit_audit_limit(self):
        def nested(count):
            value = if_present_snapshot()
            expression = {"kind": "literal", "type": "Int64", "value": 1}
            for _ in range(count):
                expression = {
                    "kind": "if_present",
                    "optional": {"kind": "column", "column": "a.k"},
                    "present": expression,
                    "missing": {"kind": "literal", "type": "Int64", "value": 0},
                    "type": "Int64",
                    "nullable": False,
                }
            value["plan"]["nodes"][1]["predicate"]["left"] = expression
            return value

        parse_snapshot(nested(MAX_BOUND_DEPTH))
        with self.assertRaisesRegex(SnapshotError, "binding depth exceeds"):
            parse_snapshot(nested(MAX_BOUND_DEPTH + 1))

    def test_if_present_annotations_must_match_exactly(self):
        non_optional = if_present_snapshot()
        non_optional["schema"]["tables"][0]["columns"][0]["nullable"] = False
        with self.assertRaisesRegex(SnapshotError, "optional must be nullable"):
            parse_snapshot(non_optional)

        wrong_present = if_present_snapshot()
        wrong_present["plan"]["nodes"][1]["predicate"]["left"]["present"] = {
            "kind": "null",
            "type": "Int64",
        }
        with self.assertRaisesRegex(SnapshotError, "handler type and nullability"):
            parse_snapshot(wrong_present)

        wrong_missing = if_present_snapshot()
        wrong_missing["plan"]["nodes"][1]["predicate"]["left"]["missing"] = {
            "kind": "literal",
            "type": "Int32",
            "value": 0,
        }
        with self.assertRaisesRegex(SnapshotError, "missing type and nullability"):
            parse_snapshot(wrong_missing)

        wrong_result = if_present_snapshot()
        wrong_result["plan"]["nodes"][1]["predicate"]["left"]["type"] = "Int32"
        with self.assertRaisesRegex(SnapshotError, "handler type and nullability"):
            parse_snapshot(wrong_result)

    def test_if_present_and_bound_node_schemas_are_closed(self):
        for field in ("optional", "present", "missing", "type", "nullable"):
            value = if_present_snapshot()
            del value["plan"]["nodes"][1]["predicate"]["left"][field]
            with self.subTest(missing=field):
                with self.assertRaisesRegex(SnapshotError, f"missing fields: {field}"):
                    parse_snapshot(value)

        extra = if_present_snapshot()
        extra["plan"]["nodes"][1]["predicate"]["left"]["args"] = []
        with self.assertRaisesRegex(SnapshotError, "unknown fields: args"):
            parse_snapshot(extra)

        bound_extra = if_present_snapshot()
        bound_extra["plan"]["nodes"][1]["predicate"]["left"]["present"]["type"] = "Int64"
        with self.assertRaisesRegex(SnapshotError, "unknown fields: type"):
            parse_snapshot(bound_extra)

    def test_decimal_division_accepts_only_its_exact_operand_shapes(self):
        for right_type in ("Decimal(7,2)", *sorted(INTEGER_TYPES)):
            with self.subTest(right_type=right_type):
                snapshot = parse_snapshot(decimal_div_snapshot(right_type))
                expression = snapshot.plan.nodes[1].predicate.args[0]
                self.assertEqual(
                    (
                        expression.kind,
                        expression.result_type,
                        expression.nullable,
                    ),
                    ("div", "Decimal(7,2)", False),
                )

        rejected_right_types = (
            "Decimal(7,1)",
            "Decimal(8,2)",
            "Bool",
            "Date",
            "String",
        )
        for right_type in rejected_right_types:
            with self.subTest(right_type=right_type):
                with self.assertRaisesRegex(
                    SnapshotError,
                    "right operand must exactly match.*or be integral",
                ):
                    parse_snapshot(decimal_div_snapshot(right_type))

    def test_decimal_division_rejects_result_or_left_type_broadening(self):
        integer_result = decimal_div_snapshot("Int64")
        division = integer_result["plan"]["nodes"][1]["predicate"]["left"]
        division["left"] = {"kind": "column", "column": "a.divisor"}
        division["type"] = "Int64"
        integer_result["plan"]["nodes"][1]["predicate"]["right"] = {
            "kind": "literal",
            "type": "Int64",
            "value": 0,
        }
        with self.assertRaisesRegex(SnapshotError, "div requires a Decimal result"):
            parse_snapshot(integer_result)

        for result_type in ("Decimal(6,2)", "Decimal(8,2)", "Decimal(7,3)"):
            broadened = decimal_div_snapshot()
            division = broadened["plan"]["nodes"][1]["predicate"]["left"]
            division["type"] = result_type
            broadened["plan"]["nodes"][1]["predicate"]["right"]["type"] = result_type
            with self.subTest(result_type=result_type):
                with self.assertRaisesRegex(
                    SnapshotError,
                    "left operand must exactly match its result type",
                ):
                    parse_snapshot(broadened)

    def test_decimal_division_nullability_is_exactly_the_operand_or(self):
        for left_nullable in (False, True):
            for right_nullable in (False, True):
                expected = left_nullable or right_nullable
                with self.subTest(
                    left_nullable=left_nullable,
                    right_nullable=right_nullable,
                ):
                    snapshot = parse_snapshot(
                        decimal_div_snapshot(
                            "Int16",
                            left_nullable=left_nullable,
                            right_nullable=right_nullable,
                        )
                    )
                    expression = snapshot.plan.nodes[1].predicate.args[0]
                    self.assertEqual(expression.nullable, expected)

                    wrong = decimal_div_snapshot(
                        "Int16",
                        left_nullable=left_nullable,
                        right_nullable=right_nullable,
                        result_nullable=not expected,
                    )
                    with self.assertRaisesRegex(
                        SnapshotError,
                        "div nullability must equal the OR",
                    ):
                        parse_snapshot(wrong)

    def test_decimal_division_node_schema_is_closed(self):
        value = decimal_div_snapshot()
        for field in ("left", "right", "type", "nullable"):
            malformed = copy.deepcopy(value)
            del malformed["plan"]["nodes"][1]["predicate"]["left"][field]
            with self.subTest(missing=field):
                with self.assertRaisesRegex(SnapshotError, f"missing fields: {field}"):
                    parse_snapshot(malformed)

        unknown = copy.deepcopy(value)
        division = unknown["plan"]["nodes"][1]["predicate"]["left"]
        division["rounding"] = "half_even"
        with self.assertRaisesRegex(SnapshotError, "unknown fields: rounding"):
            parse_snapshot(unknown)

        non_boolean = copy.deepcopy(value)
        division = non_boolean["plan"]["nodes"][1]["predicate"]["left"]
        division["nullable"] = 0
        with self.assertRaisesRegex(SnapshotError, "expected a Boolean"):
            parse_snapshot(non_boolean)

    def test_abstract_scalar_names_are_rejected(self):
        value = minimal_snapshot()
        value["schema"]["tables"][0]["columns"][0]["type"] = "int"
        with self.assertRaisesRegex(SnapshotError, "unsupported scalar type 'int'"):
            parse_snapshot(value)

    def test_date_and_decimal_have_exact_ordering_semantics(self):
        value = minimal_snapshot()
        value["schema"]["tables"][0]["columns"].extend([
            {"name": "date", "type": "Date", "nullable": True},
            {"name": "amount", "type": "Decimal(5,2)", "nullable": True},
        ])
        value["plan"]["nodes"][0]["columns"].extend([
            {"source": "date", "output": "a.date"},
            {"source": "amount", "output": "a.amount"},
        ])
        value["plan"]["output"] = ["a.date", "a.amount"]
        snapshot = parse_snapshot(value)
        self.assertEqual(
            [(column.type, column.nullable) for column in snapshot.output_schema()],
            [("Date", True), ("Decimal(5,2)", True)],
        )

        value["plan"]["nodes"][1]["predicate"] = {
            "kind": "eq",
            "left": {"kind": "column", "column": "a.date"},
            "right": {"kind": "column", "column": "a.date"},
        }
        parse_snapshot(value)

        for kind in ("lt", "lte", "gt", "gte"):
            ordered = copy.deepcopy(value)
            ordered["plan"]["nodes"][1]["predicate"] = {
                "kind": kind,
                "left": {"kind": "column", "column": "a.date"},
                "right": {"kind": "literal", "type": "Date", "value": 49_672},
            }
            with self.subTest(kind=kind):
                parse_snapshot(ordered)

        for literal_value in (0, 49_672):
            literal = copy.deepcopy(value)
            literal["plan"]["nodes"][1]["predicate"] = {
                "kind": "eq",
                "left": {"kind": "column", "column": "a.date"},
                "right": {"kind": "literal", "type": "Date", "value": literal_value},
            }
            with self.subTest(literal_value=literal_value):
                parse_snapshot(literal)

        for literal_value in (-1, 49_673, True, "0", "2000-01-01"):
            literal = copy.deepcopy(value)
            literal["plan"]["nodes"][1]["predicate"] = {
                "kind": "eq",
                "left": {"kind": "column", "column": "a.date"},
                "right": {"kind": "literal", "type": "Date", "value": literal_value},
            }
            with self.subTest(invalid_literal=literal_value):
                with self.assertRaisesRegex(
                    SnapshotError,
                    "value does not have type 'Date'",
                ):
                    parse_snapshot(literal)

        wrong_type = copy.deepcopy(value)
        wrong_type["plan"]["nodes"][1]["predicate"] = {
            "kind": "lt",
            "left": {"kind": "column", "column": "a.date"},
            "right": {"kind": "literal", "type": "Uint16", "value": 1},
        }
        with self.assertRaisesRegex(SnapshotError, "comparison type mismatch"):
            parse_snapshot(wrong_type)

        decimal_order = copy.deepcopy(value)
        decimal_order["plan"]["nodes"][1]["predicate"] = {
            "kind": "lt",
            "left": {"kind": "column", "column": "a.amount"},
            "right": {"kind": "column", "column": "a.amount"},
        }
        parse_snapshot(decimal_order)

    def test_decimal_type_identity_is_canonical_and_validated(self):
        for scalar_type in (
            "Decimal",
            "Decimal(0,0)",
            "Decimal(05,2)",
            "Decimal(5,02)",
            "Decimal(5,6)",
            "Decimal(36,2)",
            "Decimal(5, 2)",
        ):
            with self.subTest(scalar_type=scalar_type):
                value = minimal_snapshot()
                value["schema"]["tables"][0]["columns"][0]["type"] = scalar_type
                with self.assertRaisesRegex(SnapshotError, "unsupported scalar type"):
                    parse_snapshot(value)

    def test_aggregate_contract_is_strict_and_typed(self):
        value = minimal_snapshot()
        value["plan"]["nodes"][-1] = {
            "id": "aggregate",
            "op": "aggregate",
            "input": "scan",
            "keys": [],
            "aggregates": [
                {
                    "input": "a.k",
                    "function": "count",
                    "output": "result",
                    "type": "Uint64",
                    "nullable": False,
                    "distinct": False,
                    "unwrap": False,
                }
            ],
            "phase": "undefined",
            "distinct_all": False,
        }
        value["plan"]["root"] = "aggregate"
        value["plan"]["output"] = ["result"]
        snapshot = parse_snapshot(value)
        self.assertEqual(
            [(column.name, column.type, column.nullable) for column in snapshot.output_schema()],
            [("result", "Uint64", False)],
        )

        wrong_type = copy.deepcopy(value)
        wrong_type["plan"]["nodes"][-1]["aggregates"][0]["type"] = "Int64"
        with self.assertRaisesRegex(SnapshotError, "count output must"):
            parse_snapshot(wrong_type)

        unknown_field = copy.deepcopy(value)
        unknown_field["plan"]["nodes"][-1]["aggregates"][0]["state"] = "opaque"
        with self.assertRaisesRegex(SnapshotError, "unknown fields: state"):
            parse_snapshot(unknown_field)

        bad_phase = copy.deepcopy(value)
        bad_phase["plan"]["nodes"][-1]["phase"] = "partial"
        with self.assertRaisesRegex(SnapshotError, "unsupported aggregate phase"):
            parse_snapshot(bad_phase)

    def test_decimal_sum_widens_to_max_precision_and_preserves_scale(self):
        value = minimal_snapshot()
        value["schema"]["tables"][0]["columns"][0]["type"] = "Decimal(7,2)"
        value["plan"]["nodes"][-1] = {
            "id": "aggregate",
            "op": "aggregate",
            "input": "scan",
            "keys": [],
            "aggregates": [
                {
                    "input": "a.k",
                    "function": "sum",
                    "output": "result",
                    "type": "Decimal(35,2)",
                    "nullable": True,
                    "distinct": False,
                    "unwrap": False,
                }
            ],
            "phase": "undefined",
            "distinct_all": False,
        }
        value["plan"]["root"] = "aggregate"
        value["plan"]["output"] = ["result"]
        snapshot = parse_snapshot(value)
        self.assertEqual(
            [
                (column.name, column.type, column.nullable)
                for column in snapshot.output_schema()
            ],
            [("result", "Decimal(35,2)", True)],
        )

        for wrong_type in ("Decimal(34,2)", "Decimal(35,3)"):
            malformed = copy.deepcopy(value)
            malformed["plan"]["nodes"][-1]["aggregates"][0][
                "type"
            ] = wrong_type
            with self.subTest(wrong_type=wrong_type):
                with self.assertRaisesRegex(
                    SnapshotError,
                    "sum output type must be 'Decimal\\(35,2\\)'",
                ):
                    parse_snapshot(malformed)

    def test_decimal_max_requires_exact_type_and_phase_aware_nullability(self):
        def snapshot_value(*, phase, grouped, input_nullable, output_nullable):
            value = minimal_snapshot()
            value["schema"]["tables"][0]["columns"][0].update(
                type="Decimal(7,2)",
                nullable=input_nullable,
            )
            keys = ["a.flag"] if grouped else []
            value["plan"]["nodes"][-1] = {
                "id": "aggregate",
                "op": "aggregate",
                "input": "scan",
                "keys": keys,
                "aggregates": [
                    {
                        "input": "a.k",
                        "function": "max",
                        "output": "result",
                        "type": "Decimal(7,2)",
                        "nullable": output_nullable,
                        "distinct": False,
                        "unwrap": False,
                    }
                ],
                "phase": phase,
                "distinct_all": False,
            }
            value["plan"]["root"] = "aggregate"
            value["plan"]["output"] = keys + ["result"]
            return value

        for phase, grouped, input_nullable in product(
            ("undefined", "intermediate", "final"),
            (False, True),
            (False, True),
        ):
            output_nullable = input_nullable or (
                not grouped and phase != "intermediate"
            )
            with self.subTest(
                phase=phase,
                grouped=grouped,
                input_nullable=input_nullable,
            ):
                parse_snapshot(
                    snapshot_value(
                        phase=phase,
                        grouped=grouped,
                        input_nullable=input_nullable,
                        output_nullable=output_nullable,
                    )
                )
                with self.assertRaisesRegex(SnapshotError, "max output nullability"):
                    parse_snapshot(
                        snapshot_value(
                            phase=phase,
                            grouped=grouped,
                            input_nullable=input_nullable,
                            output_nullable=not output_nullable,
                        )
                    )

        wrong_type = snapshot_value(
            phase="undefined",
            grouped=False,
            input_nullable=False,
            output_nullable=True,
        )
        wrong_type["plan"]["nodes"][-1]["aggregates"][0]["type"] = "Decimal(8,2)"
        with self.assertRaisesRegex(SnapshotError, "must exactly match its Decimal input"):
            parse_snapshot(wrong_type)

        non_decimal = snapshot_value(
            phase="undefined",
            grouped=False,
            input_nullable=False,
            output_nullable=True,
        )
        non_decimal["schema"]["tables"][0]["columns"][0]["type"] = "Int64"
        non_decimal["plan"]["nodes"][-1]["aggregates"][0]["type"] = "Int64"
        with self.assertRaisesRegex(SnapshotError, "only Decimal is modeled"):
            parse_snapshot(non_decimal)

    def test_decimal_avg_requires_explicit_sum_count_state_and_exact_phase_lineage(self):
        def trait(input_name, output_name):
            return {
                "input": input_name,
                "function": "avg",
                "output": output_name,
                "type": "Decimal(7,2)",
                "nullable": True,
                "distinct": False,
                "unwrap": False,
                "state": {
                    "sum_type": "Decimal(35,2)",
                    "count_type": "Uint64",
                    "nullable": True,
                },
            }

        logical = minimal_snapshot()
        logical["schema"]["tables"][0]["columns"][0].update(
            type="Decimal(7,2)",
            nullable=True,
        )
        logical_aggregate = {
            "id": "aggregate",
            "op": "aggregate",
            "input": "scan",
            "keys": [],
            "aggregates": [trait("a.k", "result")],
            "phase": "undefined",
            "distinct_all": False,
        }
        logical["plan"].update(
            nodes=[logical["plan"]["nodes"][0], logical_aggregate],
            root="aggregate",
            output=["result"],
        )
        parsed = parse_snapshot(logical)
        state = parsed.plan.nodes[-1].aggregates[0].state
        self.assertEqual(
            (state.sum_type, state.count_type, state.nullable),
            ("Decimal(35,2)", "Uint64", True),
        )

        for path, replacement, reason in (
            (("sum_type",), "Decimal(34,2)", "exact .* accumulator"),
            (("count_type",), "Int64", "exact .* accumulator"),
            (("nullable",), False, "nullable=true"),
        ):
            with self.subTest(path=path):
                malformed = copy.deepcopy(logical)
                malformed["plan"]["nodes"][-1]["aggregates"][0]["state"][
                    path[0]
                ] = replacement
                with self.assertRaisesRegex(SnapshotError, reason):
                    parse_snapshot(malformed)

        missing_state = copy.deepcopy(logical)
        del missing_state["plan"]["nodes"][-1]["aggregates"][0]["state"]
        with self.assertRaisesRegex(SnapshotError, "missing fields: state"):
            parse_snapshot(missing_state)

        partial = copy.deepcopy(logical_aggregate)
        partial.update(id="partial", phase="intermediate")
        partial["aggregates"][0].update(output="_state")
        final = copy.deepcopy(logical_aggregate)
        final.update(id="final", input="partial", phase="final")
        final["aggregates"][0].update(input="_state")
        split = copy.deepcopy(logical)
        split["plan"].update(
            nodes=[split["plan"]["nodes"][0], partial, final],
            root="final",
            output=["result"],
        )
        parse_snapshot(split)

        no_final = copy.deepcopy(split)
        no_final["plan"].update(root="partial", output=["_state"])
        no_final["plan"]["nodes"].pop()
        with self.assertRaisesRegex(
            SnapshotError,
            "intermediate avg state must have one direct final",
        ):
            parse_snapshot(no_final)

        wrong_source = copy.deepcopy(split)
        wrong_source["plan"]["nodes"][-1]["aggregates"][0]["input"] = "result"
        with self.assertRaisesRegex(SnapshotError, "input column 'result'.*not available"):
            parse_snapshot(wrong_source)

        leaked_state = copy.deepcopy(split)
        leaked_state["plan"]["nodes"][-1]["aggregates"].append({
            "input": "_state",
            "function": "sum",
            "output": "state_sum",
            "type": "Decimal(35,2)",
            "nullable": True,
            "distinct": False,
            "unwrap": False,
        })
        with self.assertRaisesRegex(
            SnapshotError,
            "intermediate avg state must be used only",
        ):
            parse_snapshot(leaked_state)

        routed_state = copy.deepcopy(split)
        routed_state["stage_graph"] = {
            "root_stage": "root",
            "stages": [
                {
                    "id": "source",
                    "nodes": ["scan", "partial"],
                    "inputs": [],
                    "outputs": [{"index": 0, "node": "partial"}],
                    "source_storage": "column",
                },
                {
                    "id": "root",
                    "nodes": ["final"],
                    "inputs": ["partial"],
                    "outputs": [{"index": 0, "node": "final"}],
                    "source_storage": None,
                },
            ],
            "edges": [
                {
                    "id": "state_shuffle",
                    "producer": "source",
                    "consumer": "root",
                    "occurrence": 0,
                    "producer_output": 0,
                    "consumer_input": 0,
                    "kind": "hash_shuffle",
                    "keys": ["_state"],
                    "hash_function": "HashV1",
                    "use_spilling": False,
                }
            ],
            "assumptions": [],
        }
        with self.assertRaisesRegex(
            SnapshotError,
            "intermediate avg state may only be transported as payload",
        ):
            parse_snapshot(routed_state)

    def test_void_is_an_exact_non_nullable_count_input_expression(self):
        value = count_star_snapshot()
        snapshot = parse_snapshot(value)
        expression = snapshot.plan.nodes[1].columns[0].expression
        self.assertEqual((expression.kind, expression.result_type, expression.nullable), (
            "void",
            "Void",
            False,
        ))

        for extra in ("type", "nullable", "value", "args"):
            with self.subTest(extra=extra):
                malformed = copy.deepcopy(value)
                malformed["plan"]["nodes"][1]["columns"][0]["expression"][extra] = None
                with self.assertRaisesRegex(SnapshotError, f"unknown fields: {extra}"):
                    parse_snapshot(malformed)

        typed_void = copy.deepcopy(value)
        typed_void["plan"]["nodes"][1]["columns"][0]["expression"] = {
            "kind": "literal",
            "type": "Void",
            "value": 0,
        }
        with self.assertRaisesRegex(SnapshotError, "unsupported scalar type 'Void'"):
            parse_snapshot(typed_void)

        predicate_void = minimal_snapshot()
        predicate_void["plan"]["nodes"][1]["predicate"] = {"kind": "void"}
        with self.assertRaisesRegex(SnapshotError, "canonical count aggregate"):
            parse_snapshot(predicate_void)

    def test_void_may_pass_through_a_join_to_count_star(self):
        value = count_star_snapshot()
        value["schema"]["tables"].append({
            "name": "B",
            "columns": [{"name": "k", "type": "Int64", "nullable": False}],
            "unique_keys": [],
        })
        value["plan"]["nodes"][1]["columns"].append({
            "output": "a.k",
            "expression": {"kind": "column", "column": "a.k"},
        })
        value["plan"]["nodes"].insert(2, {
            "id": "scan_b",
            "op": "scan",
            "table": "B",
            "columns": [{"source": "k", "output": "b.k"}],
            "predicate": None,
            "pushed_limit": None,
        })
        value["plan"]["nodes"].insert(3, {
            "id": "join",
            "op": "join",
            "left": "count_input",
            "right": "scan_b",
            "kind": "inner",
            "predicate": {
                "kind": "eq",
                "left": {"kind": "column", "column": "a.k"},
                "right": {"kind": "column", "column": "b.k"},
            },
        })
        value["plan"]["nodes"][4]["input"] = "join"
        parse_snapshot(value)

    def test_void_is_rejected_outside_a_canonical_count_input(self):
        root_void = count_star_snapshot()
        root_void["plan"]["nodes"] = root_void["plan"]["nodes"][:2]
        root_void["plan"]["root"] = "count_input"
        root_void["plan"]["output"] = ["_count_input"]

        equality_void = count_star_snapshot()
        equality_void["plan"]["nodes"][1]["columns"][0]["expression"] = {
            "kind": "eq",
            "left": {"kind": "void"},
            "right": {"kind": "void"},
        }

        opaque_void = count_star_snapshot()
        opaque_void["plan"]["nodes"][1]["columns"][0]["expression"] = {
            "kind": "opaque",
            "fingerprint": "opaque-with-void",
            "type": "Int64",
            "nullable": False,
            "args": [{"kind": "void"}],
        }

        grouped_void = count_star_snapshot()
        grouped_void["plan"]["nodes"][2]["keys"] = ["_count_input"]

        non_count_void = count_star_snapshot()
        non_count_void["plan"]["nodes"][2]["aggregates"][0].update({
            "function": "sum",
            "type": "Int64",
        })

        distinct_count_void = count_star_snapshot()
        distinct_count_void["plan"]["nodes"][2]["aggregates"][0]["distinct"] = True

        for label, malformed in (
            ("root", root_void),
            ("equality", equality_void),
            ("opaque argument", opaque_void),
            ("group key", grouped_void),
            ("non-count aggregate", non_count_void),
            ("distinct count", distinct_count_void),
        ):
            with self.subTest(location=label):
                with self.assertRaisesRegex(
                    SnapshotError,
                    "canonical count aggregate",
                ):
                    parse_snapshot(malformed)

    def test_incomplete_stage_graph_is_rejected(self):
        value = copy.deepcopy(minimal_snapshot())
        value["stage_graph"] = {"stages": []}
        with self.assertRaisesRegex(SnapshotError, "missing fields: assumptions, edges, root_stage"):
            parse_snapshot(value)

    def test_strict_stage_graph_is_accepted(self):
        value = copy.deepcopy(minimal_snapshot())
        value["stage_graph"] = {
            "root_stage": "s0",
            "stages": [
                {
                    "id": "s0",
                    "nodes": ["scan", "filter"],
                    "inputs": [],
                    "outputs": [{"index": 0, "node": "filter"}],
                    "source_storage": "column",
                }
            ],
            "edges": [],
            "assumptions": [],
        }
        snapshot = parse_snapshot(value)
        self.assertEqual(snapshot.stage_graph.root_stage, "s0")

    def test_row_storage_source_stage_must_contain_only_the_scan(self):
        value = copy.deepcopy(minimal_snapshot())
        value["stage_graph"] = {
            "root_stage": "s0",
            "stages": [
                {
                    "id": "s0",
                    "nodes": ["scan", "filter"],
                    "inputs": [],
                    "outputs": [{"index": 0, "node": "filter"}],
                    "source_storage": "row",
                }
            ],
            "edges": [],
            "assumptions": [],
        }
        with self.assertRaisesRegex(SnapshotError, "row-storage source stage.*only its scan"):
            parse_snapshot(value)

    def test_repeated_shuffle_keys_preserve_order(self):
        value = copy.deepcopy(minimal_snapshot())
        value["stage_graph"] = {
            "root_stage": "consumer",
            "stages": [
                {
                    "id": "source",
                    "nodes": ["scan"],
                    "inputs": [],
                    "outputs": [{"index": 0, "node": "scan"}],
                    "source_storage": "row",
                },
                {
                    "id": "consumer",
                    "nodes": ["filter"],
                    "inputs": ["scan"],
                    "outputs": [{"index": 0, "node": "filter"}],
                    "source_storage": None,
                },
            ],
            "edges": [
                {
                    "id": "edge",
                    "producer": "source",
                    "consumer": "consumer",
                    "occurrence": 0,
                    "producer_output": 0,
                    "consumer_input": 0,
                    "kind": "hash_shuffle",
                    "keys": ["a.k", "a.k"],
                    "hash_function": "HashV1",
                    "use_spilling": False,
                }
            ],
            "assumptions": [],
        }
        snapshot = parse_snapshot(value)
        self.assertEqual(snapshot.stage_graph.edges[0].keys, ("a.k", "a.k"))

        value["stage_graph"]["edges"][0]["hash_function"] = "ColumnShardHashV1"
        with self.assertRaisesRegex(SnapshotError, "unsupported hash function"):
            parse_snapshot(value)

    def test_occurrences_follow_effective_consumer_order(self):
        value = copy.deepcopy(minimal_snapshot())
        value["plan"] = {
            "nodes": [
                value["plan"]["nodes"][0],
                {
                    "id": "union",
                    "op": "union_all",
                    "inputs": [
                        {"node": "scan", "columns": ["a.k"]},
                        {"node": "scan", "columns": ["a.k"]},
                    ],
                    "output": ["u.k"],
                    "ordered": False,
                },
            ],
            "root": "union",
            "output": ["u.k"],
            "subplans": [],
        }
        value["stage_graph"] = {
            "root_stage": "consumer",
            "stages": [
                {
                    "id": "source",
                    "nodes": ["scan"],
                    "inputs": [],
                    "outputs": [
                        {"index": 0, "node": "scan"},
                        {"index": 1, "node": "scan"},
                    ],
                    "source_storage": "row",
                },
                {
                    "id": "consumer",
                    "nodes": ["union"],
                    "inputs": ["scan", "scan"],
                    "outputs": [{"index": 0, "node": "union"}],
                    "source_storage": None,
                },
            ],
            "edges": [
                {
                    "id": "first",
                    "producer": "source",
                    "consumer": "consumer",
                    "occurrence": 1,
                    "producer_output": 0,
                    "consumer_input": 0,
                    "kind": "union_all",
                    "parallel": True,
                },
                {
                    "id": "second",
                    "producer": "source",
                    "consumer": "consumer",
                    "occurrence": 0,
                    "producer_output": 1,
                    "consumer_input": 1,
                    "kind": "union_all",
                    "parallel": True,
                },
            ],
            "assumptions": [],
        }
        with self.assertRaisesRegex(SnapshotError, "effective consumer input order"):
            parse_snapshot(value)

    def test_union_inputs_must_cross_stage_boundaries(self):
        value = copy.deepcopy(minimal_snapshot())
        value["plan"] = {
            "nodes": [
                value["plan"]["nodes"][0],
                {
                    "id": "union",
                    "op": "union_all",
                    "inputs": [
                        {"node": "scan", "columns": ["a.k"]},
                        {"node": "scan", "columns": ["a.k"]},
                    ],
                    "output": ["u.k"],
                    "ordered": False,
                },
            ],
            "root": "union",
            "output": ["u.k"],
            "subplans": [],
        }
        value["stage_graph"] = {
            "root_stage": "stage",
            "stages": [
                {
                    "id": "stage",
                    "nodes": ["scan", "union"],
                    "inputs": [],
                    "outputs": [{"index": 0, "node": "union"}],
                    "source_storage": "row",
                }
            ],
            "edges": [],
            "assumptions": [],
        }
        with self.assertRaisesRegex(SnapshotError, "must cross stage boundaries"):
            parse_snapshot(value)

    def test_union_all_is_exactly_binary(self):
        value = copy.deepcopy(minimal_snapshot())
        value["plan"] = {
            "nodes": [
                value["plan"]["nodes"][0],
                {
                    "id": "union",
                    "op": "union_all",
                    "inputs": [
                        {"node": "scan", "columns": ["a.k"]},
                        {"node": "scan", "columns": ["a.k"]},
                        {"node": "scan", "columns": ["a.k"]},
                    ],
                    "output": ["u.k"],
                    "ordered": False,
                },
            ],
            "root": "union",
            "output": ["u.k"],
            "subplans": [],
        }
        with self.assertRaisesRegex(SnapshotError, "requires exactly two inputs"):
            parse_snapshot(value)

    def test_union_all_ordered_flag_is_required_and_strict(self):
        value = copy.deepcopy(minimal_snapshot())
        value["plan"] = {
            "nodes": [
                value["plan"]["nodes"][0],
                {
                    "id": "union",
                    "op": "union_all",
                    "inputs": [
                        {"node": "scan", "columns": ["a.k"]},
                        {"node": "scan", "columns": ["a.k"]},
                    ],
                    "output": ["u.k"],
                    "ordered": True,
                },
            ],
            "root": "union",
            "output": ["u.k"],
            "subplans": [],
        }
        value["stage_graph"] = None
        parsed = parse_snapshot(value)
        self.assertTrue(parsed.plan.nodes[-1].ordered)

        missing = copy.deepcopy(value)
        del missing["plan"]["nodes"][1]["ordered"]
        with self.assertRaisesRegex(SnapshotError, "missing fields: ordered"):
            parse_snapshot(missing)

        wrong_type = copy.deepcopy(value)
        wrong_type["plan"]["nodes"][1]["ordered"] = 1
        with self.assertRaisesRegex(SnapshotError, "ordered: expected a Boolean"):
            parse_snapshot(wrong_type)

        unknown = copy.deepcopy(value)
        unknown["plan"]["nodes"][1]["parallel"] = False
        with self.assertRaisesRegex(SnapshotError, "unknown fields: parallel"):
            parse_snapshot(unknown)

    def test_boolean_is_not_accepted_as_version_one(self):
        value = minimal_snapshot()
        value["version"] = True
        with self.assertRaisesRegex(SnapshotError, "expected version 1"):
            parse_snapshot(value)

    def test_disconnected_nodes_are_rejected(self):
        value = minimal_snapshot()
        value["plan"]["nodes"].append(
            {
                "id": "unused",
                "op": "scan",
                "table": "A",
                "columns": [{"source": "k", "output": "unused.k"}],
                "pushed_limit": None,
            }
        )
        with self.assertRaisesRegex(SnapshotError, "not reachable from the root: unused"):
            parse_snapshot(value)


if __name__ == "__main__":
    unittest.main()
