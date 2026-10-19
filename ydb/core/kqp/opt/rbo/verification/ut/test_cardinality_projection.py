import subprocess
import unittest
from dataclasses import replace
from itertools import product
from unittest import mock

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import analysis, smt, verify
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import Column, SnapshotError, parse_snapshot
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import (
    BoundedChoice, Database, Evaluator, RelationError, RelationFamily, Row, Value,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import Encoder as ScalarEncoder
if __package__:
    from .test_project_error import _ground, _source_family
    from .test_subplans import _exists_snapshot
    from .test_verify import SOLVER
else:
    from test_project_error import _ground, _source_family
    from test_subplans import _exists_snapshot
    from test_verify import SOLVER


def _column(name):
    return {"kind": "column", "column": name}


def _literal(kind, value):
    return {"kind": "literal", "type": kind, "value": value}


def _snapshot(*, safe=False, discard=False, staged=False, changed_value=False):
    nodes = [{"id": "scan", "op": "scan", "table": "T", "columns": [
        {"source": name, "output": name} for name in ("n", "v", "w")]}]
    producer = "scan"
    if safe:
        nodes.append({"id": "safe", "op": "filter", "input": producer,
                      "predicate": {"kind": "lte", "left": _column("n"), "right": _literal("Uint64", 1)}})
        producer = "safe"
    nodes.append({"id": "project", "op": "project", "input": producer, "ordered": True,
                  "columns": [{"output": "result", "expression": _column("w" if changed_value else "v"),
                               "require_at_most_one": "n"}]})
    root = "project"
    if discard:
        nodes.append({"id": "discard", "op": "filter", "input": root, "predicate": _literal("Bool", False)})
        root = "discard"
    graph = None
    if staged:
        graph = {"root_stage": "consumer", "assumptions": [], "stages": [
            {"id": "source", "nodes": ["scan"] + (["safe"] if safe else []), "inputs": [],
             "outputs": [{"index": 0, "node": producer}], "source_storage": "column"},
            {"id": "consumer", "nodes": ["project"] + (["discard"] if discard else []),
             "inputs": [producer], "outputs": [{"index": 0, "node": root}], "source_storage": None},
        ], "edges": [{
            "id": "edge", "producer": "source", "consumer": "consumer", "occurrence": 0,
            "producer_output": 0, "consumer_input": 0, "kind": "union_all", "parallel": False,
        }]}
    return {"format": "ydb-rbo-semantic-snapshot", "version": 1,
            "schema": {"tables": [{"name": "T", "columns": [
                {"name": "n", "type": "Uint64", "nullable": False},
                {"name": "v", "type": "Int32", "nullable": False},
                {"name": "w", "type": "Int32", "nullable": False},
            ], "unique_keys": []}]},
            "plan": {"nodes": nodes, "root": root, "output": ["result"], "subplans": []},
            "stage_graph": graph}


class CardinalityProjectionValidationTest(unittest.TestCase):
    def test_guard_is_strictly_a_nonempty_physical_uint64_column(self):
        for marker in (None, False, True, 1, [], {}, "", "missing", "v"):
            with self.subTest(marker=marker):
                raw = _snapshot()
                raw["plan"]["nodes"][1]["columns"][0]["require_at_most_one"] = marker
                with self.assertRaises(SnapshotError):
                    parse_snapshot(raw)
        for kind, nullable in (("Uint64", True), ("Int64", False), ("Uint32", False)):
            raw = _snapshot()
            raw["schema"]["tables"][0]["columns"][0].update(type=kind, nullable=nullable)
            with self.assertRaisesRegex(SnapshotError, "physical non-null Uint64"):
                parse_snapshot(raw)

    def test_value_must_be_physical_and_cannot_combine_error_markers(self):
        for change in ({"expression": _literal("Int32", 0)}, {"expression": _column("missing")},
                       {"error_on_null": True}, {"require_total": True}):
            with self.subTest(change=change):
                raw = _snapshot()
                raw["plan"]["nodes"][1]["columns"][0].update(change)
                with self.assertRaises(SnapshotError):
                    parse_snapshot(raw)

    def test_guards_in_subplans_are_rejected(self):
        raw = _exists_snapshot(correlated=False)
        raw["schema"]["tables"][0]["columns"][1].update(type="Uint64", nullable=False)
        raw["plan"]["nodes"].append({"id": "guard", "op": "project", "input": "inner_scan", "ordered": True,
                                     "columns": [{"output": "inner.x", "expression": _column("inner.x"),
                                                  "require_at_most_one": "inner.x"}]})
        raw["plan"]["subplans"][0]["root"] = "guard"
        with self.assertRaisesRegex(SnapshotError, "cardinality guards inside subplans"):
            parse_snapshot(raw)

    def test_analysis_marks_guard_without_changing_value_type(self):
        snapshot = parse_snapshot(_snapshot())
        prepared = analysis.analyze_snapshot(snapshot)
        self.assertTrue(prepared.project_effects["project"].require_totality)
        self.assertEqual(snapshot.plan.nodes[1].columns[0].require_at_most_one, "n")


class CardinalityProjectionObligationTest(unittest.TestCase):
    def test_observer_is_mandatory_before_even_a_discarded_result(self):
        snapshot = parse_snapshot(_snapshot(discard=True))
        script = smt.Script()
        database = Database(snapshot, 0, script)
        source = _source_family((Column("n", "Uint64", False), Column("v", "Int32", False), Column("w", "Int32", False)), (
            Row(smt.TRUE, {"n": Value("Uint64", smt.FALSE, smt.int_value(2)),
                           "v": Value("Int32", smt.FALSE, smt.ZERO), "w": Value("Int32", smt.FALSE, smt.ONE)}),))
        args = dict(edge_inputs={("project", 0): source})
        with self.assertRaisesRegex(RelationError, "mandatory totality observer"):
            Evaluator(snapshot, database, ScalarEncoder(script), **args).root()
        exclusions = []
        observer = verify._required_totality_observer(snapshot, "before", script, exclusions)
        evaluator = Evaluator(snapshot, database, ScalarEncoder(script), project_input_observer=observer, **args)
        result = evaluator.root()
        evaluator.root()
        self.assertEqual(len(exclusions), 1)
        self.assertIs(exclusions[0].predicate, smt.TRUE)
        self.assertTrue(all(row.present is smt.FALSE for row in result.outcomes[0].relation.rows))

    def test_guard_checks_every_enabled_successful_present_input_and_legal_choice(self):
        snapshot = parse_snapshot(_snapshot())
        script = smt.Script()
        enabled, error, present = (script.fresh_constant(name, smt.BOOL) for name in ("enabled", "error", "present"))
        count = script.fresh_constant("count", smt.INT)
        choice = BoundedChoice(script.fresh_constant("schedule", smt.INT), 2)
        source = _source_family((Column("n", "Uint64", False),), (
            Row(present, {"n": Value("Uint64", smt.FALSE, count)}),), error=error)
        source = RelationFamily((replace(
            source.outcomes[0],
            enabled=smt.and_(enabled, smt.eq(choice.term, smt.ONE)), choices=(choice,),
        ),))
        exclusions = []
        verify._required_totality_observer(snapshot, "after", script, exclusions)("scope", "project", source)
        predicate = exclusions[0].predicate
        self.assertEqual(predicate.operation, "exists")
        for flags in product((False, True), repeat=3):
            for value in (0, 1, 2, 2**64 - 1):
                constants = {term.atom: flag for term, flag in zip((enabled, error, present), flags)}
                constants[count.atom] = value
                reachable = any(_ground(predicate.arguments[-1], constants | {choice.term.atom: index})
                                for index in range(choice.bound))
                self.assertEqual(reachable, flags[0] and not flags[1] and flags[2] and value > 1)

    def test_domain_failure_blocks_result_check_and_witness(self):
        problem = verify.build_problem(parse_snapshot(_snapshot(discard=True)),
                                       parse_snapshot(_snapshot(discard=True, staged=True)), 2)
        self.assertEqual(len(problem.soundness_exclusions), 2)
        with mock.patch.object(verify, "_run_solver", return_value=subprocess.CompletedProcess(["z3"], 0, "sat\n", "")) as run:
            result = verify.query_solver(problem, "z3", problem.witness_values())
        self.assertEqual(result.status, "unknown")
        self.assertEqual(result.phase, "soundness")
        self.assertFalse(result.values)
        self.assertEqual(run.call_count, 1)

    @unittest.skipUnless(SOLVER, "requires the pinned solver")
    def test_solver_proves_total_guards_and_keeps_real_value_counterexamples(self):
        for safe, discard, changed in ((False, True, False), (True, False, False), (True, False, True)):
            with self.subTest(safe=safe, discard=discard, changed=changed):
                problem = verify.build_problem(
                    parse_snapshot(_snapshot(safe=safe, discard=discard)),
                    parse_snapshot(_snapshot(safe=safe, discard=discard, staged=True, changed_value=changed)), 2,
                )
                result = verify.query_solver(problem, SOLVER, timeout_ms=10_000)
                self.assertEqual(result.status, "unknown" if not safe else "sat" if changed else "unsat")
                if not safe:
                    self.assertEqual(result.phase, "soundness")


if __name__ == "__main__":
    unittest.main()
