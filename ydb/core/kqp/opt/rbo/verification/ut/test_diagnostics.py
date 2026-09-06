import io
import json
import os
import subprocess
import unittest
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from itertools import product
from unittest import mock

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import bundle, cli, diagnostics, smt, verify
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import Expr, Filter, StageOutput
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import (
    BoundedChoice, MismatchBranch, Outcome, Relation, RelationFamily, Row,
)

try:
    from .test_verify import aggregate_stage_snapshot, passthrough_stage_snapshot
except ImportError:
    from test_verify import aggregate_stage_snapshot, passthrough_stage_snapshot


SOLVER = (
    yatest_common.binary_path("contrib/tools/z3/z3")
    if yatest_common is not None else os.environ.get("RBO_Z3")
)


def _family(present=smt.TRUE, *, enabled=smt.TRUE, error=smt.FALSE, choices=(), decisions=()):
    return RelationFamily((Outcome(
        enabled, Relation((), (Row(present, {}),)), error, decisions, choices,
    ),))


def _problem():
    script = smt.Script()
    script.assert_obligation(smt.FALSE)
    return verify.Problem(script, {})


class NonemptyDiagnosticTests(unittest.TestCase):
    def test_complete_success_not_just_candidate_rows_and_result_marginals(self):
        for enabled, error, present, second_error in product((False, True), repeat=4):
            observer = diagnostics.NonemptyOutputObserver()
            for side in ("before", "after"):
                observer(side, _family(
                    smt.bool_value(present), enabled=smt.bool_value(enabled), error=smt.bool_value(error),
                ))
                observer(side, _family(smt.FALSE, error=smt.bool_value(second_error)))
            for branch in observer.predicates(smt.Script()):
                self.assertEqual(branch.predicate, smt.bool_value(enabled and present and not error and not second_error))

    def test_observing_normal_and_bundle_builds_does_not_change_canonical_formula(self):
        pair = (passthrough_stage_snapshot(), passthrough_stage_snapshot({"kind": "map"}))
        for builder, args in ((verify.build_problem, pair), (bundle.build_bundle_problem, ((pair, pair),))):
            observer = diagnostics.NonemptyOutputObserver()
            ordinary = builder(*args, 2)
            observed = builder(*args, 2, boundary_observer=observer)
            canonical = observed.formula()
            self.assertEqual(ordinary.formula(), canonical)
            observer.predicates(observed.script)
            self.assertEqual(observed.formula(), canonical)

    def test_domain_exclusions_are_prerequisites_not_assumed_constraints(self):
        for domain_status in ("sat", "unknown", "unsat"):
            problem = _problem()
            exclusion = MismatchBranch("domain", smt.TRUE)
            problem = replace(problem, soundness_exclusion=exclusion)
            observer = diagnostics.NonemptyOutputObserver()
            observer("before", _family())
            observer("after", _family())
            outputs = (domain_status, "sat", "unsat")
            with mock.patch.object(verify, "_run_solver", side_effect=[
                subprocess.CompletedProcess(["solver"], 0, status + "\n", "") for status in outputs
            ]) as run:
                result = diagnostics.diagnose_nonempty_outputs(problem, observer, "solver", 2)
            self.assertEqual(result["model_domain"]["status"], domain_status.upper())
            self.assertEqual(run.call_count, 3 if domain_status == "unsat" else 1)
            self.assertEqual(result["before"]["status"], "SAT" if domain_status == "unsat" else "UNKNOWN")
            self.assertEqual(result["after"]["status"], "UNSAT" if domain_status == "unsat" else "UNKNOWN")

    def test_raw_queries_replace_mismatch_and_do_not_apply_abstract_sat_classification(self):
        problem = replace(_problem(), semantic_mode="binary64_uf_universal_v1", abstract_integral_average=True)
        observer = diagnostics.NonemptyOutputObserver()
        for side in ("before", "after"):
            observer(side, _family())
        canonical = problem.formula()
        with mock.patch.object(verify, "_run_solver", return_value=subprocess.CompletedProcess(
            ["solver"], 0, "sat\n", "",
        )) as run:
            result = diagnostics.diagnose_nonempty_outputs(problem, observer, "solver", 2)
        self.assertEqual(result["before"]["status"], "SAT")
        self.assertEqual(result["scope"], "bounded_model")
        self.assertIn("not a runtime witness", result["caveat"])
        self.assertEqual(problem.formula(), canonical)
        for call in run.call_args_list:
            self.assertIn("(assert true)", call.args[1])
            self.assertNotIn("(assert false)", call.args[1])
            self.assertNotIn("get-value", call.args[1])

    def test_diagnostic_deadline_is_shared_and_protocol_errors_are_separate(self):
        observer = diagnostics.NonemptyOutputObserver()
        for side in ("before", "after"):
            observer(side, _family())
        now = [0.0]

        def late_solver(*_args):
            now[0] = 2.0
            return subprocess.CompletedProcess(["solver"], 0, "unsat\n", "")

        with (
            mock.patch.object(verify.time, "monotonic", side_effect=lambda: now[0]),
            mock.patch.object(verify, "_run_solver", side_effect=late_solver) as run,
        ):
            result = diagnostics.diagnose_nonempty_outputs(_problem(), observer, "solver", 2, 1000)
        self.assertEqual(run.call_count, 1)
        self.assertEqual(result["before"]["status"], "UNKNOWN")
        self.assertEqual(result["after"]["status"], "UNKNOWN")
        with mock.patch.object(verify, "_run_solver", return_value=subprocess.CompletedProcess(
            ["solver"], 0, "unsat\nsat\n", "",
        )):
            result = diagnostics.diagnose_nonempty_outputs(_problem(), observer, "solver", 2)
        self.assertIn("exactly one solver status", result["error"])
        self.assertEqual(result["before"]["status"], "UNKNOWN")

    def test_optional_cli_diagnostic_never_changes_verdict_or_exit_code(self):
        for status, exit_code in (("VERIFIED_BOUNDED", 0), ("COUNTEREXAMPLE", 1), ("UNKNOWN", 2)):
            with (
                mock.patch.object(cli, "load_snapshot"),
                mock.patch.object(cli, "build_problem", return_value=_problem()),
                mock.patch.object(cli, "solve", return_value=verify.Result(status, 2)),
                mock.patch.object(cli, "diagnose_nonempty_outputs", side_effect=RuntimeError("diagnostic failed")) as diagnostic,
            ):
                for enabled in (False, True):
                    output = io.StringIO()
                    with redirect_stdout(output):
                        actual = cli.main(["before", "after", "--solver", "solver"] + (
                            ["--diagnose-nonempty-output"] if enabled else []
                        ))
                    result = json.loads(output.getvalue())
                    self.assertEqual(actual, exit_code)
                    self.assertEqual(result["status"], status)
                    self.assertEqual("nonempty_output_diagnostic" in result, enabled)
                    if enabled:
                        self.assertEqual(result["nonempty_output_diagnostic"]["status"], "ERROR")
                diagnostic.assert_called_once()
        for args in (
            ["--emit-smt", "unused"],
            ["--solver", "solver", "--diagnostic-timeout-ms", "0"],
        ):
            with redirect_stderr(io.StringIO()):
                self.assertEqual(cli.main(["before", "after", "--diagnose-nonempty-output", *args]), 2)

    @unittest.skipUnless(SOLVER, "requires Z3")
    def test_choice_bounds_and_joint_decisions_are_preserved(self):
        for in_range in (True, False):
            problem = _problem()
            choice = problem.script.fresh_constant("choice", smt.INT)
            observer = diagnostics.NonemptyOutputObserver()
            for side in ("before", "after"):
                observer(side, _family(
                    smt.eq(choice, smt.int_value(1 if in_range else 2)), choices=(BoundedChoice(choice, 2),),
                ))
            result = diagnostics.diagnose_nonempty_outputs(problem, observer, SOLVER, 2)
            self.assertEqual(result["before"]["status"], "SAT" if in_range else "UNSAT")
        observer = diagnostics.NonemptyOutputObserver()
        for side in ("before", "after"):
            observer(side, RelationFamily((
                _family(decisions=(("shared", 0),)).outcomes[0],
                _family(smt.FALSE, decisions=(("shared", 1),)).outcomes[0],
            )))
            observer(side, _family(smt.FALSE, decisions=(("shared", 1),)))
        result = diagnostics.diagnose_nonempty_outputs(_problem(), observer, SOLVER, 2)
        # Only the incompatible decision 0 produces a row. Independent
        # per-result reachability would incorrectly report SAT here.
        self.assertEqual(result["before"]["status"], "UNSAT")

    @unittest.skipUnless(SOLVER, "requires Z3")
    def test_having_count_requires_an_adequate_bound_and_detects_threshold_mutation(self):
        def snapshot(threshold, staged=False):
            # The existing partial/final fixture counts a required column,
            # exactly COUNT(*), without introducing a new routing contract.
            base = aggregate_stage_snapshot("count", False, staged)
            having = Filter("having", base.plan.root, Expr("gt", args=(
                Expr("column", column="result"),
                Expr("literal", value=threshold, result_type="Uint64", nullable=False),
            )))
            plan = replace(base.plan, nodes=base.plan.nodes + (having,), root="having")
            graph = base.stage_graph
            if graph is not None:
                graph = replace(graph, stages=tuple(
                    replace(stage, nodes=stage.nodes + ("having",), outputs=(StageOutput(0, "having"),))
                    if stage.id == graph.root_stage else stage for stage in graph.stages
                ))
            return replace(base, plan=plan, stage_graph=graph)

        for rows, expected in ((2, "UNSAT"), (3, "SAT")):
            observer = diagnostics.NonemptyOutputObserver()
            problem = verify.build_problem(snapshot(2), snapshot(2, True), rows, boundary_observer=observer)
            self.assertEqual(verify.solve(problem, SOLVER, rows).status, "VERIFIED_BOUNDED")
            result = diagnostics.diagnose_nonempty_outputs(problem, observer, SOLVER, rows)
            self.assertEqual((result["before"]["status"], result["after"]["status"]), (expected, expected))
            mutated = verify.build_problem(snapshot(2), snapshot(3, True), rows)
            self.assertEqual(verify.solve(mutated, SOLVER, rows).status,
                             "VERIFIED_BOUNDED" if rows == 2 else "COUNTEREXAMPLE")

    @unittest.skipUnless(SOLVER, "requires Z3")
    def test_nonempty_positive_and_payload_mutation_are_distinct_questions(self):
        before = passthrough_stage_snapshot()
        after = passthrough_stage_snapshot({"kind": "map"})
        observer = diagnostics.NonemptyOutputObserver()
        problem = verify.build_problem(before, after, 2, boundary_observer=observer)
        self.assertEqual(verify.solve(problem, SOLVER, 2).status, "VERIFIED_BOUNDED")
        result = diagnostics.diagnose_nonempty_outputs(problem, observer, SOLVER, 2)
        self.assertEqual((result["before"]["status"], result["after"]["status"]), ("SAT", "SAT"))

        project = after.plan.nodes[-1]
        mutated = replace(after, plan=replace(after.plan, nodes=(
            *after.plan.nodes[:-1],
            replace(project, columns=(replace(project.columns[0], expression=Expr(
                "add", args=(project.columns[0].expression, Expr("literal", result_type="Int64", value=1, nullable=False)),
                result_type="Int64", nullable=False,
            )),)),
        )))
        observer = diagnostics.NonemptyOutputObserver()
        problem = verify.build_problem(before, mutated, 2, boundary_observer=observer)
        self.assertEqual(verify.solve(problem, SOLVER, 2).status, "COUNTEREXAMPLE")
        result = diagnostics.diagnose_nonempty_outputs(problem, observer, SOLVER, 2)
        self.assertEqual((result["before"]["status"], result["after"]["status"]), ("SAT", "SAT"))
