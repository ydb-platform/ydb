"""Query inputs are shared typed symbols, independent of plan choices."""

import copy
import os
import unittest
from unittest import mock

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import bundle, decimal, smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    Expr, SnapshotError, ValueType, parse_snapshot, query_parameters,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import Encoder, date_domain, integer_domain
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import (
    VerificationError, build_logical_kernel_problem_for_tests, build_transformation_pair_problem, solve,
)

SOLVER = (yatest_common.binary_path("contrib/tools/z3/z3")
          if yatest_common is not None else os.environ.get("RBO_Z3"))
TYPES = ("Bool", "Int8", "Int16", "Int32", "Int64", "Uint8", "Uint16", "Uint32", "Uint64",
         "Date", "Datetime", "Timestamp", "Timestamp64", "String", "Utf8", "Decimal(15,2)", "Decimal(35,35)")


def parameter(name="$p", scalar_type="Int32", nullable=False):
    return {"kind": "parameter", "name": name, "type": scalar_type, "nullable": nullable}


def literal(value, scalar_type="Int32"):
    return {"kind": "literal", "type": scalar_type, "value": value}


def snapshot(expression, extra=None):
    columns = [{"output": "answer", "expression": expression}]
    if extra is not None:
        columns.append({"output": "unused", "expression": extra})
    return {
        "format": "ydb-rbo-semantic-snapshot", "version": 1,
        "schema": {"tables": [{"name": "T", "columns": [
            {"name": "k", "type": "Int32", "nullable": False}], "unique_keys": []}]},
        "plan": {"nodes": [
            {"id": "scan", "op": "scan", "table": "T",
             "columns": [{"source": "k", "output": "key"}], "pushed_limit": None},
            {"id": "project", "op": "project", "input": "scan", "ordered": False,
             "columns": columns}], "root": "project", "output": ["answer"], "subplans": []},
        "stage_graph": None,
    }


def problem(before, after):
    return build_logical_kernel_problem_for_tests(parse_snapshot(before), parse_snapshot(after), 2, 2000)


class ParameterAdmissionTest(unittest.TestCase):
    def test_exact_supported_types_and_nullability(self):
        for scalar_type in TYPES:
            for nullable in (False, True):
                with self.subTest(type=scalar_type, nullable=nullable):
                    parsed = parse_snapshot(snapshot(parameter(scalar_type=scalar_type, nullable=nullable)))
                    self.assertEqual(query_parameters(parsed), {"$p": ValueType(scalar_type, nullable)})

    def test_rejects_ambiguous_or_structured_parameter_contracts(self):
        for update in ({"name": ""}, {"name": "$"}, {"name": "p"}, {"name": 3},
                       {"name": "%kqp_physical_tx_param_binding_0"},
                       {"name": "%kqp%tx_result_binding_0_0"}, {"nullable": 1}, {"extra": "ignored"},
                       {"type": "Double"}, {"type": "List<Int32>"}, {"type": "Optional<Int32>"},
                       {"type": "Struct<k:Int32>"}, {"type": "PgInt4"}, {"type": "Void"}):
            changed = parameter()
            changed.update(update)
            with self.subTest(update=update):
                with self.assertRaises(SnapshotError):
                    parse_snapshot(snapshot(changed))

    def test_same_name_conflicts_rejected_inside_snapshot_even_if_output_is_pruned(self):
        for conflicting in (parameter(scalar_type="Uint32"), parameter(nullable=True)):
            with self.assertRaisesRegex(SnapshotError, "inconsistent declared types"):
                parse_snapshot(snapshot(parameter(), conflicting))

    def test_same_name_conflicts_rejected_across_snapshots_without_evaluation(self):
        for conflicting in (parameter(scalar_type="Uint32"), parameter(nullable=True)):
            # The parameter is omitted from the root output; the output schema
            # cannot accidentally detect these contradictory declarations.
            with self.assertRaisesRegex(VerificationError, "inconsistent declared types"):
                problem(snapshot(literal(7), parameter()), snapshot(literal(7), conflicting))

    def test_transformation_anchor_type_is_retained_across_a_pruned_middle_boundary(self):
        anchor = parse_snapshot(snapshot(literal(7), parameter()))
        middle = parse_snapshot(snapshot(literal(7)))
        for conflicting in (parameter(scalar_type="Uint32"), parameter(nullable=True)):
            after = parse_snapshot(snapshot(literal(7), conflicting))
            with self.assertRaisesRegex(VerificationError, "inconsistent declared types across transformation"):
                build_transformation_pair_problem(middle, after, 2, observation_snapshot=anchor)

    def test_parameter_present_on_only_one_side_is_still_registered_and_witnessed(self):
        result = problem(snapshot(literal(7), parameter(nullable=True)), snapshot(literal(7)))
        self.assertEqual(set(result.parameters), {"$p"})
        value = result.parameters["$p"]
        self.assertIn(value.value, result.witness_values())
        self.assertIn(value.is_null, result.witness_values())

    def test_bundle_registers_pruned_inputs_and_rejects_cross_result_conflicts(self):
        # Only bypass the boundary-role rule; this test targets bundle-wide
        # registration before any evaluator or per-result filter can erase use.
        before = parse_snapshot(snapshot(literal(7), parameter()))
        after = parse_snapshot(snapshot(literal(7)))
        conflict = parse_snapshot(snapshot(literal(7), parameter(scalar_type="Uint32")))
        with mock.patch.object(bundle, "_check_boundary_roles"):
            built = bundle.build_bundle_problem(((before, after), (before, after)), 2)
            self.assertEqual(set(built.parameters), {"$p"})
            self.assertIn(built.parameters["$p"].value, built.witness_values())
            with self.assertRaisesRegex(VerificationError, "inconsistent declared types"):
                bundle.build_bundle_problem(((before, after), (conflict, after)), 2)

    def test_encoder_shares_same_input_across_rows_and_preserves_distinct_names(self):
        encoder = Encoder(smt.Script())
        first = Expr(kind="parameter", parameter="$p", result_type="Int32", nullable=True)
        second = Expr(kind="parameter", parameter="$q", result_type="Int32", nullable=True)
        value = encoder.evaluate(first, {})
        self.assertIs(value, encoder.evaluate(copy.deepcopy(first), {"irrelevant": value}))
        self.assertNotEqual(value.value, encoder.evaluate(second, {}).value)
        with self.assertRaisesRegex(smt.SmtError, "inconsistent declared types"):
            encoder.register_parameters({"$p": ValueType("Uint32", True)})

    def test_storage_domains_are_global_and_keep_decimal_specials(self):
        for scalar_type, domain in (("Int8", integer_domain), ("Uint64", integer_domain),
                                   ("Date", date_domain), ("Timestamp", date_domain), ("Timestamp64", date_domain),
                                   ("Decimal(15,2)", decimal.domain)):
            script = smt.Script()
            encoder = Encoder(script)
            encoder.register_parameters({"$p": ValueType(scalar_type, True)})
            value = encoder.parameters["$p"]
            self.assertIn(domain(value.value, scalar_type), script.assertions)
            self.assertEqual(value.is_null.sort, smt.BOOL)
        for special in (-decimal.INF, decimal.INF, decimal.NAN):
            self.assertEqual(decimal.domain(smt.int_value(special), "Decimal(15,2)"), smt.TRUE)

    def test_timestamp64_parameter_domain_contains_signed_endpoints(self):
        lower, upper = -4_611_669_897_600_000_000, 4_611_669_811_199_999_999
        for value in (lower, -1, 0, upper):
            self.assertEqual(date_domain(smt.int_value(value), "Timestamp64"), smt.TRUE)
        for value in (lower - 1, upper + 1):
            self.assertEqual(date_domain(smt.int_value(value), "Timestamp64"), smt.FALSE)


@unittest.skipUnless(SOLVER, "run through ya or set RBO_Z3")
class ParameterProofTest(unittest.TestCase):
    def verify(self, before, after):
        return solve(problem(before, after), SOLVER, 2, 2000)

    def test_arithmetic_identity_holds_for_all_input_values_and_nullness(self):
        for scalar_type in ("Int8", "Int64", "Uint64"):
            for nullable in (False, True):
                value = parameter(scalar_type=scalar_type, nullable=nullable)
                added = {"kind": "add", "left": value, "right": literal(0, scalar_type),
                         "type": scalar_type, "nullable": nullable}
                self.assertEqual(self.verify(snapshot(value), snapshot(added)).status, "VERIFIED_BOUNDED")

    def test_pruning_unused_input_neither_fixes_its_value_nor_breaks_equivalence(self):
        before = snapshot(literal(7), parameter(nullable=True))
        after = snapshot(literal(7))
        self.assertEqual(self.verify(before, after).status, "VERIFIED_BOUNDED")

    def test_renaming_an_input_is_a_counterexample_with_both_values(self):
        for scalar_type in ("Bool", "Int32", "Date", "Timestamp64", "String", "Utf8", "Decimal(15,2)"):
            result = self.verify(snapshot(parameter("$p", scalar_type)), snapshot(parameter("$q", scalar_type)))
            self.assertEqual(result.status, "COUNTEREXAMPLE")
            self.assertEqual(set(result.parameters), {"$p", "$q"})
            self.assertNotEqual(result.parameters["$p"], result.parameters["$q"])
            if scalar_type in {"String", "Utf8"}:
                self.assertIsInstance(result.parameters["$p"], str)
            self.assertEqual(result.to_json()["parameters"], result.parameters)

    def test_null_input_remains_a_reachable_counterexample(self):
        before = snapshot({"kind": "if", "type": "Int32", "nullable": False,
                           "condition": {"kind": "exists", "arg": parameter(nullable=True)},
                           "then": literal(7), "else": literal(9)})
        result = self.verify(before, snapshot(literal(7)))
        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertIsNone(result.parameters["$p"])

    def test_decimal_nan_input_prevents_invalid_reflexivity_proof(self):
        value = parameter(scalar_type="Decimal(15,2)")
        reflexive = {"kind": "eq", "left": value, "right": copy.deepcopy(value)}
        result = self.verify(snapshot(reflexive), snapshot(literal(True, "Bool")))
        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertEqual(result.parameters["$p"], decimal.NAN)

    def test_fixed_width_parameter_domain_does_not_add_impossible_counterexamples(self):
        value = parameter(scalar_type="Uint8")
        lower_bound = {"kind": "gte", "left": value, "right": literal(0, "Uint8")}
        self.assertEqual(self.verify(snapshot(lower_bound), snapshot(literal(True, "Bool"))).status,
                         "VERIFIED_BOUNDED")

    def test_timestamp64_endpoint_values_and_null_remain_reachable(self):
        lower, upper = -4_611_669_897_600_000_000, 4_611_669_811_199_999_999
        for value in (lower, -1, upper):
            equality = {"kind": "eq", "left": parameter(scalar_type="Timestamp64"),
                        "right": literal(value, "Timestamp64")}
            result = self.verify(snapshot(equality), snapshot(literal(False, "Bool")))
            self.assertEqual(result.status, "COUNTEREXAMPLE")
            self.assertEqual(result.parameters["$p"], value)
        existence = {"kind": "exists", "arg": parameter(scalar_type="Timestamp64", nullable=True)}
        result = self.verify(snapshot(existence), snapshot(literal(True, "Bool")))
        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertIsNone(result.parameters["$p"])

    def test_timestamp64_values_outside_the_runtime_domain_are_excluded(self):
        for operation, boundary in (("lt", -4_611_669_897_600_000_000),
                                    ("gt", 4_611_669_811_199_999_999)):
            outside = {"kind": operation, "left": parameter(scalar_type="Timestamp64"),
                       "right": literal(boundary, "Timestamp64")}
            result = self.verify(snapshot(outside), snapshot(literal(False, "Bool")))
            self.assertEqual(result.status, "VERIFIED_BOUNDED")
