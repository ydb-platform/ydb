"""Plan facts are immutable and shared; symbolic evaluation state is not."""

import unittest
from dataclasses import FrozenInstanceError, replace
from unittest.mock import patch

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import analysis, relation, smt, stages, verify
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    Aggregate, AggregateTrait, Column, Expr, Filter, Limit, Plan, Project, Projection, Scan, ScanColumn,
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
    def test_limit_fanout_ignores_only_cardinality_certified_takes(self):
        base = _snapshot()

        def literal(value):
            return Expr("literal", value=value, result_type="Uint64", nullable=False)
        aggregate = Aggregate("aggregate", "scan", (), (
            AggregateTrait("a.k", "count", "n", "Uint64", False, False, False),
        ), "undefined", False)
        for grouped in (False, True):
            for count in (0, 1, 2):
                producer = replace(aggregate, keys=("a.k",) if grouped else ())
                plan = Plan((
                    base.plan.nodes[0], producer,
                    Limit("left", producer.id, literal(count), None, "undefined"),
                    Limit("right", producer.id, literal(count), None, "undefined"),
                    UnionAll("union", (UnionInput("left", ("n",)), UnionInput("right", ("n",))),
                             ("n",), False),
                ), "union", ("n",), ())
                snapshot = replace(base, plan=plan)
                with self.subTest(grouped=grouped, count=count):
                    if grouped and count:
                        with self.assertRaisesRegex(analysis.AnalysisError, "correlated fan-out"):
                            analysis.analyze_snapshot(snapshot)
                    else:
                        analysis.analyze_snapshot(snapshot)
                        problem = verify.build_logical_kernel_problem_for_tests(snapshot, snapshot, 2)
                        self.assertIsNotNone(problem.semantic_mismatch)

        # A global aggregate emits one row per producer task, not one overall.
        # Broadcasting both rows to one task cannot certify its LIMIT 1.
        plan = Plan((
            base.plan.nodes[0], aggregate,
            Limit("right", "aggregate", literal(1), None, "undefined"),
        ), "right", ("n",), ())
        graph = StageGraph("result", (
            Stage("source", ("scan",), (), (StageOutput(0, "scan"),), "row"),
            Stage("groups", ("aggregate",), ("scan",), (StageOutput(0, "aggregate"),), None),
            Stage("result", ("right",), ("aggregate",), (StageOutput(0, "right"),), None),
        ), (
            StageEdge("aggregate_input", "source", "groups", 0, 0, 0, "map"),
            StageEdge("copied_input", "groups", "result", 0, 0, 0, "broadcast"),
        ))
        snapshot = Snapshot((Table("A", (Column("k", "Uint64", False),), ()),), plan, graph)
        analysis.validate_snapshot(snapshot)
        self.assertNotIn("right", analysis._order_insensitive_limits(snapshot, plan.node_map()))
        wide = replace(plan, nodes=tuple(
            replace(node, count=literal(2)) if node.id == "right" else node for node in plan.nodes
        ))
        snapshot = replace(snapshot, plan=wide)
        analysis.validate_snapshot(snapshot)
        self.assertIn("right", analysis._order_insensitive_limits(snapshot, wide.node_map()))

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
                        facts.subplans_by_consumer, facts.scalar_outer_binds, facts.project_effects):
            with self.subTest(mapping=mapping), self.assertRaises(TypeError):
                mapping["injected"] = None

    def test_project_effects_are_immutable_and_shared_without_discharging_totality(self):
        base = _snapshot()
        project = Project("project", "filter", (
            Projection("a.k", Expr("column", column="a.k"), error_on_null=True, require_total=True),
        ), False)
        snapshot = replace(
            base,
            tables=(Table("A", (Column("k", "String", True),), ()),),
            plan=replace(base.plan, nodes=base.plan.nodes + (project,), root=project.id),
        )
        with patch.object(analysis, "_project_effects", wraps=analysis._project_effects) as classify:
            facts = analysis.analyze_snapshot(snapshot)
            effects = facts.project_effects[project.id]
            self.assertEqual(effects, analysis.ProjectEffects(None, (), (), True, ("a.k",), ()))
            with self.assertRaises(FrozenInstanceError):
                effects.require_totality = False
            for _ in range(2):
                script = smt.Script()
                evaluator = relation.Evaluator(snapshot, relation.Database(snapshot, 1, script),
                                               Encoder(script), _context=facts)
                with self.assertRaisesRegex(relation.RelationError, "mandatory totality observer"):
                    evaluator.root()
            self.assertEqual(classify.call_count, 1)

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
