"""StablePickle equality, including NULL, through the relational verifier."""

import copy
import os
import subprocess
import unittest
from itertools import product

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import SnapshotError, parse_snapshot
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.stable_pickle import Kernel, TYPES
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import build_logical_kernel_problem_for_tests, solve

SOLVER = (yatest_common.binary_path("contrib/tools/z3/z3")
          if yatest_common is not None else os.environ.get("RBO_Z3"))


def column(name):
    return {"kind": "column", "column": name}


def pickle_expr(arg, source_type):
    return {"kind": "stable_pickle", "arg": arg, "source_type": source_type,
            "type": "String", "nullable": False}


def snapshot(source_type="Int32", nullable=(True, True), *, encoded=False,
             null_safe=True, kind="inner"):
    tables = [{"name": name, "columns": [
        {"name": "key", "type": source_type, "nullable": optional}], "unique_keys": []}
        for name, optional in zip(("L", "R"), nullable)]
    nodes = [{"id": name, "op": "scan", "table": name,
              "columns": [{"source": "key", "output": name + ".key"}], "pushed_limit": None}
             for name in ("L", "R")]
    args = [column(name + ".key") for name in ("L", "R")]
    if encoded:
        args = [pickle_expr(arg, source_type) for arg in args]
    nodes.append({"id": "join", "op": "join", "left": "L", "right": "R", "kind": kind,
                  "predicate": {"kind": "eq", "left": args[0], "right": args[1],
                                "null_safe": null_safe if not encoded else False}})
    return {"format": "ydb-rbo-semantic-snapshot", "version": 1,
            "schema": {"tables": tables},
            "plan": {"nodes": nodes, "root": "join", "output": ["L.key", "R.key"], "subplans": []},
            "stage_graph": None}


class StablePickleTest(unittest.TestCase):
    def test_parser_accepts_only_reviewed_exact_source_and_result_types(self):
        for source_type in TYPES:
            for optional in (False, True):
                parse_snapshot(snapshot(source_type, (optional, optional), encoded=True))
        base = snapshot(encoded=True)
        for updates in ({"source_type": "Double"}, {"source_type": "String"},
                        {"source_type": "Utf8"}, {"source_type": "Decimal(22,9)"},
                        {"source_type": "Uint32"}, {"nullable": True},
                        {"nullable": 0}, {"type": "Utf8"}, {"extra": 1}):
            changed = copy.deepcopy(base)
            changed["plan"]["nodes"][-1]["predicate"]["left"].update(updates)
            with self.subTest(updates=updates):
                with self.assertRaises(SnapshotError):
                    parse_snapshot(changed)

    @unittest.skipUnless(SOLVER, "run through ya or set RBO_Z3")
    def test_encoded_joins_match_null_safe_equality(self):
        for source_type, nullable, kind in product(
                ("Bool", "Int32", "Date", "String", "Utf8"), product((False, True), repeat=2),
                ("inner", "left", "full")):
            with self.subTest(type=source_type, nullable=nullable, kind=kind):
                initial = parse_snapshot(snapshot(source_type, nullable, kind=kind))
                final = parse_snapshot(snapshot(source_type, nullable, kind=kind, encoded=True))
                problem = build_logical_kernel_problem_for_tests(initial, final, 2, 2000)
                self.assertEqual(solve(problem, SOLVER, 2, 2000).status, "VERIFIED_BOUNDED")

    @unittest.skipUnless(SOLVER, "run through ya or set RBO_Z3")
    def test_encoded_nulls_are_not_ordinary_sql_join_nulls(self):
        for kind in ("inner", "left", "full"):
            with self.subTest(kind=kind):
                initial = parse_snapshot(snapshot(null_safe=False, kind=kind))
                changed = parse_snapshot(snapshot(encoded=True, kind=kind))
                problem = build_logical_kernel_problem_for_tests(initial, changed, 2, 2000)
                self.assertEqual(solve(problem, SOLVER, 2, 2000).status, "COUNTEREXAMPLE")

    @unittest.skipUnless(SOLVER, "run through ya or set RBO_Z3")
    def test_choice_injectivity_is_valid_under_rebinding(self):
        script = smt.Script(timeout_ms=2000)
        kernel = Kernel(script)
        choice = script.fresh_constant("choice", smt.INT)
        script.register_quantified_choice(choice, 2)
        packed = kernel.encode("Int32", smt.FALSE, choice)
        zero = kernel.encode("Int32", smt.FALSE, smt.ZERO)
        script.seal_string_order()
        script.assert_obligation(smt.exists((choice,), smt.and_(
            smt.eq(choice, smt.ONE), smt.eq(packed, zero))))
        result = subprocess.run([SOLVER, "-in", "-smt2"], input=script.render(),
                                text=True, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "unsat")

    @unittest.skipUnless(SOLVER, "run through ya or set RBO_Z3")
    def test_cross_type_collisions_remain_possible(self):
        # Integer encodings are not tagged by their source type. Independent
        # functions must not accidentally impose cross-type injectivity.
        script = smt.Script(timeout_ms=2000)
        kernel = Kernel(script)
        signed = kernel.encode("Int32", smt.FALSE, smt.int_value(7))
        unsigned = kernel.encode("Uint64", smt.FALSE, smt.int_value(14))
        script.seal_string_order()
        script.assert_obligation(smt.eq(signed, unsigned))
        result = subprocess.run([SOLVER, "-in", "-smt2"], input=script.render(),
                                text=True, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "sat")


if __name__ == "__main__":
    unittest.main()
