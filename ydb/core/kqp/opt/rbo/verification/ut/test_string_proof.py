import os
import unittest

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import StageEdge, parse_snapshot
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import Row
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import Value
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.stages import Router
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import (
    build_logical_kernel_problem_for_tests,
    solve,
)


SOLVER = (
    yatest_common.binary_path("contrib/tools/z3/z3")
    if yatest_common is not None
    else os.environ.get("RBO_Z3")
)


def _hash_edge(version):
    return StageEdge(
        id=f"edge-{version}",
        producer="source",
        consumer="root",
        occurrence=0,
        producer_output=0,
        consumer_input=0,
        kind="hash_shuffle",
        keys=("key",),
        hash_function=version,
    )


def _text_order_snapshot(value_type, literal_type, filter_before_sort, ascending):
    scan = {
        "id": "scan",
        "op": "scan",
        "table": "T",
        "columns": [{"source": "key", "output": "t.key"}],
        "pushed_limit": None,
    }
    filter_node = {
        "id": "filter",
        "op": "filter",
        "input": "scan" if filter_before_sort else "sort",
        "predicate": {
            "kind": "lt",
            "left": {"kind": "column", "column": "t.key"},
            "right": {"kind": "literal", "type": literal_type, "value": "m"},
        },
    }
    sort = {
        "id": "sort",
        "op": "sort",
        "input": "filter" if filter_before_sort else "scan",
        "order": [
            {
                "column": "t.key",
                "ascending": ascending,
                "nulls_first": False,
            }
        ],
        "limit": None,
        "phase": "undefined",
    }
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": {
                "tables": [
                    {
                        "name": "T",
                        "columns": [
                            {"name": "key", "type": value_type, "nullable": True}
                        ],
                        "unique_keys": [],
                    }
                ]
            },
            "plan": {
                "nodes": [scan, filter_node, sort],
                "root": "sort" if filter_before_sort else "filter",
                "output": ["t.key"],
                "subplans": [],
            },
            "stage_graph": None,
        }
    )


class StringHashCorrelationTest(unittest.TestCase):
    def test_string_and_utf8_share_only_their_raw_byte_hash_contract(self):
        script = smt.Script()
        router = Router(script)
        is_null = script.fresh_constant("key_is_null", smt.BOOL)
        rank = script.fresh_constant("raw_byte_rank", smt.INT)

        def hash_term(value_type, version="HashV1"):
            row = Row(
                smt.TRUE,
                {"key": Value(value_type, is_null, rank)},
            )
            return router.hash_task(_hash_edge(version), row)

        string_hash = hash_term("String")
        utf8_hash = hash_term("Utf8")
        self.assertEqual(string_hash, utf8_hash)

        hash_v2 = hash_term("String", "HashV2")
        int64_hash = hash_term("Int64")
        uint64_hash = hash_term("Uint64")
        self.assertNotEqual(string_hash.operation, hash_v2.operation)
        self.assertNotEqual(string_hash.operation, int64_hash.operation)
        self.assertNotEqual(int64_hash.operation, uint64_hash.operation)


@unittest.skipUnless(SOLVER, "run through ya or set RBO_Z3 for solver tests")
class StringOrderingSolverTest(unittest.TestCase):
    def test_filter_sort_commutation_and_direction_mutation(self):
        for value_type, literal_type in (("String", "Utf8"), ("Utf8", "String")):
            with self.subTest(value_type=value_type, literal_type=literal_type):
                before = _text_order_snapshot(
                    value_type,
                    literal_type,
                    filter_before_sort=True,
                    ascending=True,
                )
                after = _text_order_snapshot(
                    value_type,
                    literal_type,
                    filter_before_sort=False,
                    ascending=True,
                )
                equivalent = solve(
                    build_logical_kernel_problem_for_tests(before, after, 2, 10_000),
                    SOLVER,
                    2,
                    10_000,
                )
                self.assertEqual(equivalent.status, "VERIFIED_BOUNDED")

                descending = _text_order_snapshot(
                    value_type,
                    literal_type,
                    filter_before_sort=False,
                    ascending=False,
                )
                mutation = solve(
                    build_logical_kernel_problem_for_tests(
                        before,
                        descending,
                        2,
                        10_000,
                    ),
                    SOLVER,
                    2,
                    10_000,
                )
                self.assertEqual(mutation.status, "COUNTEREXAMPLE")
                self.assertEqual(len(mutation.witness["T"]), 2)


if __name__ == "__main__":
    unittest.main()
