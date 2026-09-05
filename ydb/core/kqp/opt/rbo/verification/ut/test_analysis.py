"""Plan facts are immutable and shared; symbolic evaluation state is not."""

import unittest
from dataclasses import replace
from unittest.mock import patch

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import analysis, relation, smt, stages, verify
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    Column, Expr, Filter, Limit, Plan, Project, Projection, Scan, ScanColumn,
    Snapshot, Stage, StageEdge, StageGraph, StageOutput, Table, UnionAll, UnionInput,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import Encoder


def _snapshot(*, staged=False):
    scan = Scan("scan", "A", (ScanColumn("k", "a.k"),), None, None)
    predicate = Expr("literal", value=True, result_type="Bool", nullable=False)
    plan = Plan((scan, Filter("filter", "scan", predicate)), "filter", ("a.k",), ())
    graph = None
    if staged:
        graph = StageGraph("result", (
            Stage("source", ("scan",), (), (StageOutput(0, "scan"),), "row"),
            Stage("result", ("filter",), ("scan",), (StageOutput(0, "filter"),), None),
        ), (StageEdge("edge", "source", "result", 0, 0, 0, "map"),))
    return Snapshot((Table("A", (Column("k", "Int64", False),), ()),), plan, graph)


class AnalyzedPlanTest(unittest.TestCase):
    def test_root_schema_mismatch_precedes_relational_fanout_rejection(self):
        base = _snapshot()
        count = Expr("literal", value=1, result_type="Uint64", nullable=False)
        fanout = replace(base, plan=Plan((
            base.plan.nodes[0],
            Limit("left", "scan", count, None, "undefined"),
            Limit("right", "scan", count, None, "undefined"),
            UnionAll("union", (
                UnionInput("left", ("a.k",)),
                UnionInput("right", ("a.k",)),
            ), ("a.k",), False),
        ), "union", ("a.k",), ()))
        # Both snapshots pass strict IR/schema validation. Relational fanout is
        # unsupported, but must not hide a directly observable schema mismatch.
        self.assertEqual(analysis.validate_snapshot(fanout)["union"]["a.k"].type, "Int64")
        mutations = (
            ("type", Expr("literal", value=True, result_type="Bool", nullable=False)),
            ("nullability", Expr("null", result_type="Int64", nullable=True)),
        )
        for difference, expression in mutations:
            project = Project("project", base.plan.root,
                              (Projection("a.k", expression),), False)
            changed = replace(base, plan=replace(
                base.plan, nodes=base.plan.nodes + (project,), root="project"))
            for before, after in ((fanout, changed), (changed, fanout)):
                with self.subTest(difference=difference, fanout_before=before is fanout):
                    with self.assertRaisesRegex(verify.SchemaMismatch, difference):
                        verify.build_logical_kernel_problem_for_tests(before, after, 1)
        with self.assertRaisesRegex(verify.VerificationError, "correlated fan-out"):
            verify.build_logical_kernel_problem_for_tests(fanout, base, 1)

    def test_problem_validates_each_side_once(self):
        before, after = _snapshot(), _snapshot(staged=True)
        with patch.object(analysis, "validate_snapshot", wraps=analysis.validate_snapshot) as validate:
            verify.build_problem(before, after, 1)
        self.assertEqual(validate.call_count, 2)

    def test_facts_are_deeply_read_only(self):
        validated = analysis.ValidatedPlan(_snapshot())
        facts = analysis.analyze_validated(validated)
        self.assertEqual(validated.output_schema, (Column("a.k", "Int64", False),))
        self.assertEqual(facts.parents["scan"], frozenset(("filter",)))
        for mapping in (facts.nodes, facts.schemas, facts.schemas["scan"], facts.parents,
                        facts.subplans_by_consumer, facts.scalar_outer_binds):
            with self.subTest(mapping=mapping), self.assertRaises(TypeError):
                mapping["injected"] = None

    def test_reuse_does_not_share_evaluation_caches(self):
        snapshot = _snapshot()
        facts = analysis.analyze_snapshot(snapshot)
        script = smt.Script()
        database = relation.Database(snapshot, 1, script)
        evaluators = tuple(relation.Evaluator(snapshot, database, Encoder(script), _context=facts)
                           for _ in range(2))
        evaluators[0].root()
        self.assertEqual(evaluators[1].cache, {})
        self.assertIs(evaluators[0].schemas, evaluators[1].schemas)
        with self.assertRaisesRegex(relation.RelationError, "one snapshot"):
            relation.Evaluator(replace(snapshot), database, Encoder(script), _context=facts)

    def test_all_stage_tasks_share_one_validation(self):
        snapshot = _snapshot(staged=True)
        script = smt.Script()
        database = relation.Database(snapshot, 1, script)
        with patch.object(analysis, "validate_snapshot", wraps=analysis.validate_snapshot) as validate:
            evaluator = stages.Evaluator(snapshot, database, Encoder(script), stages.Router(script))
            evaluator.root()
        self.assertEqual(validate.call_count, 1)
        self.assertEqual(evaluator.task_counts, {"source": 2, "result": 2})
        with self.assertRaisesRegex(stages.StageError, "one snapshot"):
            stages.Evaluator(replace(snapshot), database, Encoder(script), stages.Router(script),
                             _context=evaluator._context)
