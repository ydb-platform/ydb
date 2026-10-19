"""Grouping-only aggregates deduplicate keys, including NULL groups."""
import copy
import unittest

from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import SnapshotError, parse_snapshot
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import build_problem, query_solver
if __package__:
    from .test_verify import SOLVER, SCAN_A, _edge, _snapshot_with_stage_graph, _stage, _stage_schema
else:
    from test_verify import SOLVER, SCAN_A, _edge, _snapshot_with_stage_graph, _stage, _stage_schema


def _snapshot(staged=False, *, type_name="Int64", extra_key=False):
    schema = _stage_schema("A")
    schema["tables"][0]["columns"][0].update(type=type_name, nullable=True)
    aggregate = {"id": "aggregate", "op": "aggregate", "input": "a", "keys": ["a.k"],
                 "aggregates": [], "phase": "undefined", "distinct_all": False}
    nodes = [copy.deepcopy(SCAN_A), aggregate]
    stages = edges = None
    if extra_key:
        aggregate["keys"].append("a.x")
    if staged:
        aggregate.update(id="partial", phase="intermediate")
        final = copy.deepcopy(aggregate)
        final.update(id="final", input="partial", phase="final")
        nodes.append(final)
        stages = [_stage("source", ["a", "partial"], [], ["partial"], "column"),
                  _stage("root", ["final"], ["partial"], ["final"])]
        edges = [_edge("groups", "source", "root", 0, 0, "hash_shuffle",
                       keys=["a.k"], hash_function="HashV1", use_spilling=False)]
    return _snapshot_with_stage_graph(schema, nodes, nodes[-1]["id"], ["a.k"], stages, edges)


class KeyOnlyAggregateValidationTest(unittest.TestCase):
    def test_keyless_empty_traits_are_rejected(self):
        raw = _snapshot()
        raw["plan"]["nodes"][-1]["keys"] = []
        with self.assertRaisesRegex(SnapshotError, "nonempty grouping keys"):
            parse_snapshot(raw)

    def test_distinct_all_requires_explicit_aliases(self):
        raw = _snapshot()
        raw["plan"]["nodes"][-1]["distinct_all"] = True
        with self.assertRaisesRegex(SnapshotError, "distinct_all:false"):
            parse_snapshot(raw)

    def test_missing_and_duplicate_keys_are_rejected(self):
        for keys in (["missing"], ["a.k", "a.k"]):
            raw = _snapshot()
            raw["plan"]["nodes"][-1]["keys"] = keys
            with self.subTest(keys=keys), self.assertRaises(SnapshotError):
                parse_snapshot(raw)


@unittest.skipUnless(SOLVER, "requires the pinned solver")
class KeyOnlyAggregateProofTest(unittest.TestCase):
    def test_two_stage_deduplication_preserves_nullable_groups(self):
        for type_name in ("Int64", "String", "Utf8"):
            for bound in (0, 1, 2):
                before = parse_snapshot(_snapshot(type_name=type_name))
                after = parse_snapshot(_snapshot(True, type_name=type_name))
                with self.subTest(type_name=type_name, bound=bound):
                    problem = build_problem(before, after, bound)
                    self.assertEqual(query_solver(problem, SOLVER, timeout_ms=10_000).status, "unsat")

    def test_extra_grouping_key_preserves_incorrect_duplicates(self):
        before = parse_snapshot(_snapshot())
        after = parse_snapshot(_snapshot(True, extra_key=True))
        problem = build_problem(before, after, 2)
        self.assertEqual(query_solver(problem, SOLVER, timeout_ms=10_000).status, "sat")


if __name__ == "__main__":
    unittest.main()
