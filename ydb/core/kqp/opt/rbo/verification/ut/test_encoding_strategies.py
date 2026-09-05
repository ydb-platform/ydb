"""Differential checks select encodings without changing their symbolic inputs."""

import os
import subprocess
import unittest
from dataclasses import replace

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import relation, smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    Column, Expr, Filter, Join, Plan, Scan, ScanColumn, Snapshot, SortOrder, Table, UniqueKey,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import Encoder, Value


SOLVER = (
    yatest_common.binary_path("contrib/tools/z3/z3")
    if yatest_common is not None else os.environ.get("RBO_Z3")
)


def _sort_source(script, *, unique=False):
    rows = tuple(relation.Row(
        script.fresh_constant(f"present:{index}", smt.BOOL),
        {"k": Value("Int64", smt.FALSE, script.fresh_constant(f"key:{index}", smt.INT))},
    ) for index in range(2))
    if unique:
        script.assert_global(smt.not_(smt.and_(
            rows[0].present, rows[1].present,
            smt.eq(rows[0].values["k"].value, rows[1].values["k"].value),
        )))
    return relation.single(relation.Relation(
        (Column("k", "Int64", False),), rows,
        null_safe_unique_key=frozenset(("k",)) if unique else None,
    ))


def _filter_snapshot():
    tables = tuple(Table(
        name, (Column("k", "Int64", False),),
        (UniqueKey(("k",), False),) if name == "R" else (),
    ) for name in ("L", "R"))
    scans = tuple(Scan(
        name, name, (ScanColumn("k", f"{name}.k"),), None, None,
    ) for name in ("L", "R"))
    cross = Join("cross", "L", "R", "cross", (), Expr(
        "literal", value=True, result_type="Bool", nullable=False,
    ))
    predicate = Expr("eq", args=(Expr("column", column="L.k"), Expr("column", column="R.k")))
    filtered = Filter("filter", "cross", predicate)
    return Snapshot(tables, Plan(scans + (cross, filtered), "filter", ("L.k", "R.k"), ()))


class EncodingStrategiesTest(unittest.TestCase):
    def assertUnsat(self, script):
        result = subprocess.run(
            [SOLVER, "-in"], input=script.render(), text=True,
            capture_output=True, timeout=30, check=True,
        )
        self.assertEqual(result.stdout.strip(), "unsat", result.stderr)

    @unittest.skipUnless(SOLVER, "set RBO_Z3 to compare symbolic sequence languages")
    def test_forced_sort_encodings_match_enumerated_reference(self):
        for unique, encoding in (
            (False, "auto"), (False, "ordinals"), (False, "network"),
            (True, "unique"), (True, "network"),
        ):
            with self.subTest(unique=unique, encoding=encoding):
                script = smt.Script()
                scalar = Encoder(script)
                source = _sort_source(script, unique=unique)
                order = (SortOrder("k", True, True),)
                baseline = relation.sort_family(source, order, script, "baseline", encoding="enumerated")
                alternate = relation.sort_family(source, order, script, "alternate", encoding=encoding)
                script.assert_term(smt.not_(relation.family_equal(baseline, alternate, scalar)))
                self.assertUnsat(script)

    def test_forced_sort_encoding_admission_and_selection_purity(self):
        script = smt.Script()
        source = _sort_source(script)
        order = (SortOrder("k", True, True),)
        before = script.render()
        selected = relation._choose_sort_encoding(source, order, compact_prefix=False, requested="auto")
        self.assertEqual(selected.encoding, "enumerated")
        self.assertEqual(script.render(), before)
        with self.assertRaisesRegex(relation.RelationError, "certified complete order key"):
            relation.sort_family(source, order, script, "unique", encoding="unique")
        wide = relation.single(replace(source.certain(), rows=source.certain().rows * 2))
        with self.assertRaisesRegex(relation.RelationError, "one tiny outcome"):
            relation.sort_family(wide, order, script, "enumerated", encoding="enumerated")

    @unittest.skipUnless(SOLVER, "set RBO_Z3 to compare Filter input encodings")
    def test_factored_filter_matches_baseline_on_shared_database(self):
        snapshot = _filter_snapshot()
        script = smt.Script()
        scalar = Encoder(script)
        database = relation.Database(snapshot, 2, script)
        baseline_evaluator = relation.Evaluator(snapshot, database, scalar)
        factored_evaluator = relation.Evaluator(snapshot, database, scalar)
        node = snapshot.plan.node_map()["filter"]
        baseline = baseline_evaluator._filter(node, encoding="baseline")
        factored = factored_evaluator._filter(node, encoding="factored")
        self.assertEqual(len(baseline.certain().rows), 4)
        self.assertEqual(len(factored.certain().rows), 2)
        script.assert_term(smt.not_(relation.family_equal(baseline, factored, scalar)))
        self.assertUnsat(script)

    def test_filter_plan_is_read_only_and_forced_factoring_rejects_observed_spine(self):
        snapshot = _filter_snapshot()
        script = smt.Script()
        evaluator = relation.Evaluator(snapshot, relation.Database(snapshot, 2, script), Encoder(script))
        node = snapshot.plan.node_map()["filter"]
        before = script.render()
        self.assertIsNotNone(evaluator._plan_delayed_cross_filter(node))
        self.assertEqual(evaluator.cache, {})
        self.assertEqual(script.render(), before)
        evaluator.node("cross")
        with self.assertRaisesRegex(relation.RelationError, "admitted private Cross spine"):
            evaluator._filter(node, encoding="factored")
