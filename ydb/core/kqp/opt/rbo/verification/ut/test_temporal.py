"""Exact temporal storage domains; conversions between units are excluded."""

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
# Independent constants from the runtime's udf_data_type.h, exclusive bounds.
BOUNDS = {"Date": 49_673, "Datetime": 4_291_747_200, "Timestamp": 4_291_747_200_000_000}


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
        for scalar_type, upper in BOUNDS.items():
            for value in (0, upper - 1):
                parse_snapshot(snapshot("lt", value, scalar_type))
            for value in (-1, upper, True, "0"):
                with self.subTest(type=scalar_type, value=value):
                    with self.assertRaises(SnapshotError):
                        parse_snapshot(snapshot("lt", value, scalar_type))
            for other in BOUNDS.keys() - {scalar_type}:
                raw = snapshot("lt", 1, scalar_type)
                raw["plan"]["nodes"][1]["predicate"]["right"]["type"] = other
                with self.assertRaises(SnapshotError):
                    parse_snapshot(raw)

    def test_non_null_opaque_results_have_their_own_domain(self):
        for scalar_type, upper in BOUNDS.items():
            for nullable in (False, True):
                script = smt.Script()
                result = Encoder(script).evaluate(Expr(
                    kind="opaque", result_type=scalar_type, nullable=nullable,
                    fingerprint="temporal-result"), {})
                self.assertIn(smt.or_(result.is_null, date_domain(result.value, scalar_type)),
                              script.assertions)
            for value, expected in ((-1, False), (0, True), (upper - 1, True), (upper, False)):
                self.assertEqual(date_domain(smt.int_value(value), scalar_type),
                                 smt.bool_value(expected))

    @unittest.skipUnless(SOLVER, "run through ya or set RBO_Z3")
    def test_filter_equivalence_and_boundary_mutation(self):
        for scalar_type, upper in BOUNDS.items():
            before = snapshot("lt", upper - 1, scalar_type)
            equivalent = snapshot("lte", upper - 2, scalar_type)
            changed = snapshot("lte", upper - 1, scalar_type)
            for after, status in ((equivalent, "VERIFIED_BOUNDED"), (changed, "COUNTEREXAMPLE")):
                with self.subTest(type=scalar_type, status=status):
                    problem = build_logical_kernel_problem_for_tests(
                        parse_snapshot(before), parse_snapshot(after), 1, 10_000)
                    self.assertEqual(solve(problem, SOLVER, 1, 10_000).status, status)
