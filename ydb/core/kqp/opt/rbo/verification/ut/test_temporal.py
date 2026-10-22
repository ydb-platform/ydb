"""Exact temporal storage domains; conversions between units are excluded."""

import copy
import os
import unittest

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import Expr, SnapshotError, parse_snapshot
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import Encoder, date_domain
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import build_logical_kernel_problem_for_tests, solve

SOLVER = (yatest_common.binary_path("contrib/tools/z3/z3")
          if yatest_common is not None else os.environ.get("RBO_Z3"))
# Independent runtime constants: inclusive lower and exclusive upper bounds.
BOUNDS = {
    "Date": (0, 49_673),
    "Datetime": (0, 4_291_747_200),
    "Timestamp": (0, 4_291_747_200_000_000),
    "Timestamp64": (-4_611_669_897_600_000_000, 4_611_669_811_200_000_000),
}


def snapshot(kind, value, scalar_type):
    return {
        "format": "ydb-rbo-semantic-snapshot", "version": 1,
        "schema": {"tables": [{"name": "T", "columns": [
            {"name": "time", "type": scalar_type, "nullable": True}], "unique_keys": []}]},
        "plan": {"nodes": [
            {"id": "scan", "op": "scan", "table": "T",
             "columns": [{"source": "time", "output": "t.time"}], "pushed_limit": None},
            {"id": "filter", "op": "filter", "input": "scan", "predicate": {
                "kind": kind, "left": {"kind": "column", "column": "t.time"},
                "right": {"kind": "literal", "type": scalar_type, "value": value}}}],
            "root": "filter", "output": ["t.time"], "subplans": []},
        "stage_graph": None,
    }


class TemporalTest(unittest.TestCase):
    def test_literal_boundaries_and_mixed_units(self):
        for scalar_type, (lower, upper) in BOUNDS.items():
            for value in (lower, 0, upper - 1):
                parse_snapshot(snapshot("lt", value, scalar_type))
            for value in (lower - 1, upper, True, "0"):
                with self.subTest(type=scalar_type, value=value):
                    with self.assertRaises(SnapshotError):
                        parse_snapshot(snapshot("lt", value, scalar_type))
            for other in BOUNDS.keys() - {scalar_type}:
                raw = snapshot("lt", 1, scalar_type)
                raw["plan"]["nodes"][1]["predicate"]["right"]["type"] = other
                with self.assertRaises(SnapshotError):
                    parse_snapshot(raw)

    def test_non_null_opaque_results_have_their_own_domain(self):
        for scalar_type, (lower, upper) in BOUNDS.items():
            for nullable in (False, True):
                script = smt.Script()
                result = Encoder(script).evaluate(Expr(
                    kind="opaque", result_type=scalar_type, nullable=nullable,
                    fingerprint="temporal-result"), {})
                self.assertIn(smt.or_(result.is_null, date_domain(result.value, scalar_type)),
                              script.assertions)
            for value, expected in ((lower - 1, False), (lower, True), (0, True),
                                    (upper - 1, True), (upper, False)):
                self.assertEqual(date_domain(smt.int_value(value), scalar_type),
                                 smt.bool_value(expected))

    @unittest.skipUnless(SOLVER, "run through ya or set RBO_Z3")
    def test_filter_equivalence_and_boundary_mutation(self):
        for scalar_type, (lower, upper) in BOUNDS.items():
            before = snapshot("lt", upper - 1, scalar_type)
            equivalent = snapshot("lte", upper - 2, scalar_type)
            changed = snapshot("lte", upper - 1, scalar_type)
            for after, status in ((equivalent, "VERIFIED_BOUNDED"), (changed, "COUNTEREXAMPLE")):
                with self.subTest(type=scalar_type, status=status):
                    problem = build_logical_kernel_problem_for_tests(
                        parse_snapshot(before), parse_snapshot(after), 1, 10_000)
                    self.assertEqual(solve(problem, SOLVER, 1, 10_000).status, status)

    def test_timestamp64_mixed_equality_arithmetic_casts_and_pickle_are_rejected(self):
        for other in ("Date", "Datetime", "Timestamp", "Int64"):
            raw = snapshot("eq", 0, "Timestamp64")
            raw["plan"]["nodes"][1]["predicate"]["right"]["type"] = other
            with self.subTest(other=other), self.assertRaises(SnapshotError):
                parse_snapshot(raw)
        column = {"kind": "column", "column": "t.time"}
        unsupported = [
            {"kind": "add", "left": column, "right": {
                "kind": "literal", "type": "Timestamp64", "value": 1},
             "type": "Timestamp64", "nullable": True},
            {"kind": "cast_integral", "arg": column, "type": "Int64", "nullable": True},
            {"kind": "stable_pickle", "arg": column, "source_type": "Timestamp64",
             "type": "String", "nullable": False},
        ]
        for expression in unsupported:
            raw = snapshot("eq", 0, "Timestamp64")
            raw["plan"]["nodes"][1]["predicate"] = {"kind": "exists", "arg": expression}
            with self.subTest(kind=expression["kind"]), self.assertRaises(SnapshotError):
                parse_snapshot(raw)

    @unittest.skipUnless(SOLVER, "run through ya or set RBO_Z3")
    def test_timestamp64_negative_and_null_boundary_mutations(self):
        lower, upper = BOUNDS["Timestamp64"]
        for boundary in (lower + 1, -1, 0, upper - 1):
            before = snapshot("lt", boundary, "Timestamp64")
            equivalent = snapshot("lte", boundary - 1, "Timestamp64")
            changed = snapshot("lte", boundary, "Timestamp64")
            for after, status in ((equivalent, "VERIFIED_BOUNDED"), (changed, "COUNTEREXAMPLE")):
                with self.subTest(boundary=boundary, status=status):
                    problem = build_logical_kernel_problem_for_tests(
                        parse_snapshot(before), parse_snapshot(after), 1, 10_000)
                    self.assertEqual(solve(problem, SOLVER, 1, 10_000).status, status)
        before = snapshot("eq", 0, "Timestamp64")
        before["plan"]["nodes"][1]["predicate"] = {
            "kind": "exists", "arg": {"kind": "column", "column": "t.time"}}
        after = copy.deepcopy(before)
        after["plan"]["nodes"][1]["predicate"] = {"kind": "literal", "type": "Bool", "value": True}
        problem = build_logical_kernel_problem_for_tests(
            parse_snapshot(before), parse_snapshot(after), 1, 10_000)
        self.assertEqual(solve(problem, SOLVER, 1, 10_000).status, "COUNTEREXAMPLE")

    @unittest.skipUnless(SOLVER, "run through ya or set RBO_Z3")
    def test_timestamp64_direct_projection_preserves_values_and_optional_nulls(self):
        for nullable in (False, True):
            before = snapshot("eq", 0, "Timestamp64")
            before["schema"]["tables"][0]["columns"][0]["nullable"] = nullable
            before["plan"]["nodes"].pop()
            before["plan"]["root"] = "scan"
            after = copy.deepcopy(before)
            after["plan"]["nodes"].append({
                "id": "project", "op": "project", "input": "scan", "ordered": False,
                "columns": [{"output": "t.time", "expression": {"kind": "column", "column": "t.time"}}]})
            after["plan"].update(root="project", output=["t.time"])
            changed = copy.deepcopy(after)
            replacement = {"kind": "literal", "type": "Timestamp64", "value": 0}
            if nullable:
                # Retain the declared optional output type while replacing the
                # actual payload and removing the source's reachable NULL.
                replacement = {
                    "kind": "if", "condition": {"kind": "literal", "type": "Bool", "value": True},
                    "then": replacement, "else": {"kind": "null", "type": "Timestamp64"},
                    "type": "Timestamp64", "nullable": True}
            changed["plan"]["nodes"][-1]["columns"][0]["expression"] = replacement
            for candidate, status in ((after, "VERIFIED_BOUNDED"), (changed, "COUNTEREXAMPLE")):
                problem = build_logical_kernel_problem_for_tests(
                    parse_snapshot(before), parse_snapshot(candidate), 2, 10_000)
                self.assertEqual(solve(problem, SOLVER, 2, 10_000).status, status)

    @unittest.skipUnless(SOLVER, "run through ya or set RBO_Z3")
    def test_timestamp64_primary_key_justifies_distinct_elimination(self):
        for unique, status in ((True, "VERIFIED_BOUNDED"), (False, "COUNTEREXAMPLE")):
            before = snapshot("eq", 0, "Timestamp64")
            table = before["schema"]["tables"][0]
            table["columns"][0]["nullable"] = False
            table["unique_keys"] = [{"columns": ["time"], "nulls_distinct": False}] if unique else []
            before["plan"]["nodes"].pop()
            before["plan"]["root"] = "scan"
            after = copy.deepcopy(before)
            after["plan"]["nodes"].append({
                "id": "distinct", "op": "aggregate", "input": "scan", "keys": ["t.time"],
                "aggregates": [], "phase": "undefined", "distinct_all": False})
            after["plan"]["root"] = "distinct"
            problem = build_logical_kernel_problem_for_tests(
                parse_snapshot(before), parse_snapshot(after), 2, 10_000)
            self.assertEqual(solve(problem, SOLVER, 2, 10_000).status, status)
