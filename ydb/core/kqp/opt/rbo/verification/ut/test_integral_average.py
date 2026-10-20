"""Integer AVG preserves required carriers, empty groups and source ranges."""

import copy
import os
import unittest
from itertools import product

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import aggregate, floating, smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import AggregateTrait, AverageStateType, parse_snapshot
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import Encoder, Value
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.types import INTEGER_TYPES, integer_bounds
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import build_logical_kernel_problem_for_tests, solve

SOLVER = yatest_common.binary_path("contrib/tools/z3/z3") if yatest_common is not None else os.environ.get("RBO_Z3")


def snapshot(source_type, nullable, grouped=False, split=False, binary64=False):
    state = {"kind": "integral_double_v1", "source_type": source_type, "sum_type": "Double",
             "count_type": "Uint64", "nullable": nullable, "exact_when_count_at_most": 2}

    def node(name, input_name, column, phase, output):
        return {"id": name, "op": "aggregate", "input": input_name, "keys": ["k"] if grouped else [],
                "phase": phase, "distinct_all": False,
                "aggregates": [{"input": column, "function": "avg", "output": output, "type": "Double",
                                "nullable": nullable or (not grouped and phase != "intermediate"),
                                "distinct": False, "unwrap": False, "state": state}]}
    nodes = [{"id": "scan", "op": "scan", "table": "T", "pushed_limit": None,
              "columns": [{"source": name, "output": name} for name in ("k", "v")]}]
    if split:
        nodes.append(node("partial", "scan", "v", "intermediate", "state"))
        nodes.append(node("result", "partial", "state", "final", "avg"))
    else:
        nodes.append(node("result", "scan", "v", "undefined", "avg"))
    result = {"format": "ydb-rbo-semantic-snapshot", "version": 1,
              "schema": {"tables": [{"name": "T", "columns": [
                  {"name": "k", "type": "Int32", "nullable": False},
                  {"name": "v", "type": source_type, "nullable": nullable}], "unique_keys": []}]},
              "plan": {"nodes": nodes, "root": "result", "output": (["k"] if grouped else []) + ["avg"], "subplans": []},
              "stage_graph": None}
    if binary64:
        result["semantic_mode"] = floating.SEMANTIC_MODE
    return result


class IntegralAverageTest(unittest.TestCase):
    def test_all_integer_types_and_nullabilities_parse_with_exact_phase_types(self):
        for source_type, nullable, grouped, split in product(INTEGER_TYPES, (False, True), (False, True), (False, True)):
            parse_snapshot(snapshot(source_type, nullable, grouped, split))

    def test_required_empty_intermediate_count_still_finalizes_to_null(self):
        for source_type in INTEGER_TYPES:
            state = AverageStateType("Double", "Uint64", False, "integral_double_v1", source_type, 2)
            partial_trait = AggregateTrait("v", "avg", "state", "Double", False, False, False, state)
            final_trait = AggregateTrait("state", "avg", "result", "Double", True, False, False, state)
            encoder = Encoder(smt.Script())
            partial = aggregate.integral_average_value(partial_trait, "intermediate", (), (), encoder.integral_integer_average)
            self.assertEqual(partial.is_null, smt.FALSE)
            final = aggregate.integral_average_value(final_trait, "final", (partial,), (smt.TRUE,), encoder.integral_integer_average)
            self.assertEqual(final.is_null, smt.TRUE)
            self.assertEqual(final.average_metadata.count, smt.ZERO)

    def test_extrema_use_full_source_range_including_high_uint64(self):
        for source_type in INTEGER_TYPES:
            bounds = integer_bounds(source_type)
            state = AverageStateType("Double", "Uint64", False, "integral_double_v1", source_type, 2)
            trait = AggregateTrait("v", "avg", "state", "Double", False, False, False, state)
            for value in (bounds[0], bounds[1] - 1):
                actual = aggregate.integral_average_value(
                    trait, "intermediate", (Value(source_type, smt.FALSE, smt.int_value(value)),),
                    (smt.TRUE,), Encoder(smt.Script()).integral_integer_average)
                self.assertEqual(actual.average_metadata.minimum, smt.int_value(value))
                self.assertEqual(actual.average_metadata.maximum, smt.int_value(value))

    @unittest.skipUnless(SOLVER, "run through ya or set RBO_Z3")
    def test_logical_and_split_average_agree_for_all_integer_contracts(self):
        for source_type, nullable, grouped in product(INTEGER_TYPES, (False, True), (False, True)):
            with self.subTest(type=source_type, nullable=nullable, grouped=grouped):
                before = parse_snapshot(snapshot(source_type, nullable, grouped))
                after = parse_snapshot(snapshot(source_type, nullable, grouped, True))
                problem = build_logical_kernel_problem_for_tests(before, after, 2, 2000)
                self.assertEqual(solve(problem, SOLVER, 2, 2000).status, "VERIFIED_BOUNDED")

    @unittest.skipUnless(SOLVER, "run through ya or set RBO_Z3")
    def test_count_above_two_remains_an_explicit_gap(self):
        for source_type in ("Int8", "Uint64"):
            before = parse_snapshot(snapshot(source_type, False))
            problem = build_logical_kernel_problem_for_tests(before, before, 3, 2000)
            verdict = solve(problem, SOLVER, 3, 2000)
            self.assertEqual(verdict.status, "UNKNOWN")
            self.assertIn("model-domain exclusion is reachable", verdict.reason)

    @unittest.skipUnless(SOLVER, "run through ya or set RBO_Z3")
    def test_omitted_scalar_row_is_an_abstract_mismatch_candidate(self):
        before = snapshot("Uint64", False)
        changed = copy.deepcopy(before)
        changed["plan"]["nodes"].append({
            "id": "omit", "op": "filter", "input": "result",
            "predicate": {"kind": "literal", "type": "Bool", "value": False}})
        changed["plan"]["root"] = "omit"
        problem = build_logical_kernel_problem_for_tests(parse_snapshot(before), parse_snapshot(changed), 2, 2000)
        verdict = solve(problem, SOLVER, 2, 2000)
        self.assertEqual(verdict.status, "UNKNOWN")
        self.assertIn("abstract integral AVG carrier admits a possible mismatch", verdict.reason)

    @unittest.skipUnless(SOLVER, "run through ya or set RBO_Z3")
    def test_binary64_mode_uses_the_source_integer_conversion(self):
        for source_type, nullable, grouped in product(("Int8", "Uint64"), (False, True), (False, True)):
            with self.subTest(type=source_type, nullable=nullable, grouped=grouped):
                before = parse_snapshot(snapshot(source_type, nullable, grouped, binary64=True))
                after = parse_snapshot(snapshot(source_type, nullable, grouped, split=True, binary64=True))
                problem = build_logical_kernel_problem_for_tests(before, after, 2, 2000)
                self.assertEqual(solve(problem, SOLVER, 2, 2000).status, "VERIFIED_BOUNDED")
                self.assertIn("from_" + source_type, problem.formula())
