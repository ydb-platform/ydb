"""The same inputs exercise the baseline and admitted Join encodings."""

import os
import subprocess
import unittest
from collections import Counter
from dataclasses import replace
from itertools import product

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import join, smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    Column, Expr, Join, JoinKey, Plan, Scan, ScanColumn, Snapshot, Table, UniqueKey,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import (
    Database, Evaluator, PartitionFact, RelationError, bag_equal,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import Encoder


SOLVER = (
    yatest_common.binary_path("contrib/tools/z3/z3")
    if yatest_common is not None else os.environ.get("RBO_Z3")
)
ABSENT = object()
KINDS = (
    "cross", "inner", "left", "right", "full",
    "left_semi", "right_semi", "left_anti", "right_anti", "exclusion",
)


def _snapshot(kind="inner", *, unique=True):
    tables = tuple(
        Table(
            name,
            (Column("k", "Int64", name == "L"),),
            (UniqueKey(("k",), False),) if name == "R" and unique else (),
        )
        for name in ("L", "R")
    )
    scans = tuple(
        Scan(name, name, (ScanColumn("k", f"{name}.k"),), None, None)
        for name in ("L", "R")
    )
    node = Join(
        "join", "L", "R", kind,
        (JoinKey("L.k", "R.k"),),
        Expr("literal", value=True, result_type="Bool", nullable=False),
    )
    if kind in {"left_semi", "left_anti"}:
        output = ("L.k",)
    elif kind in {"right_semi", "right_anti"}:
        output = ("R.k",)
    else:
        output = ("L.k", "R.k")
    return Snapshot(tables, Plan(scans + (node,), "join", output, ()))


def _evaluator(snapshot):
    script = smt.Script()
    scalar = Encoder(script)
    evaluator = Evaluator(snapshot, Database(snapshot, 2, script), scalar)
    return script, scalar, evaluator, snapshot.plan.node_map()["join"]


def _reference(kind, left, right):
    """Concrete bag oracle: no symbolic terms or production join policy."""

    pairs = [
        (i, j)
        for i, l in enumerate(left) for j, r in enumerate(right)
        if l is not ABSENT and r is not ABSENT
        and (kind == "cross" or (l is not None and r is not None and l == r))
    ]
    result = Counter()
    if kind in {"cross", "inner", "left", "right", "full"}:
        result.update((left[i], right[j]) for i, j in pairs)
    for i, value in enumerate(left):
        if value is ABSENT:
            continue
        matched = any(li == i for li, _ in pairs)
        if (kind == "left_semi" and matched) or (kind == "left_anti" and not matched):
            result[(value,)] += 1
        if kind in {"left", "full", "exclusion"} and not matched:
            result[(value, None)] += 1
    for j, value in enumerate(right):
        if value is ABSENT:
            continue
        matched = any(ri == j for _, ri in pairs)
        if (kind == "right_semi" and matched) or (kind == "right_anti" and not matched):
            result[(value,)] += 1
        if kind in {"right", "full", "exclusion"} and not matched:
            result[(None, value)] += 1
    return result


class JoinKernelTest(unittest.TestCase):
    def test_every_kind_matches_independent_small_bag_oracle(self):
        for kind in KINDS:
            for left, right in product(product((ABSENT, None, 0, 1), repeat=2), repeat=2):
                conditions = tuple(
                    tuple(smt.bool_value(
                        kind == "cross" or (
                            left_value is not None and right_value is not None
                            and left_value == right_value
                        )
                    ) for right_value in right)
                    for left_value in left
                )
                rows = join.reference_rows(
                    join.shape(kind),
                    tuple(smt.bool_value(value is not ABSENT) for value in left),
                    tuple(smt.bool_value(value is not ABSENT) for value in right),
                    conditions,
                )
                actual = Counter()
                for row in rows:
                    if row.present == smt.FALSE:
                        continue
                    self.assertEqual(row.present, smt.TRUE)
                    left_value = None if row.left is None else left[row.left]
                    right_value = None if row.right is None else right[row.right]
                    if kind.startswith("left_"):
                        values = (left_value,)
                    elif kind.startswith("right_"):
                        values = (right_value,)
                    else:
                        values = (left_value, right_value)
                    actual[values] += 1
                self.assertEqual(actual, _reference(kind, left, right), (kind, left, right))

    def test_condition_dimensions_and_kind_fail_closed(self):
        with self.assertRaises(ValueError):
            join.reference_rows(join.shape("full"), (smt.TRUE,), (), ())
        with self.assertRaises(ValueError):
            join.shape("unknown")


class JoinEncodingTest(unittest.TestCase):
    @unittest.skipUnless(SOLVER, "set RBO_Z3 to check symbolic encoding equivalence")
    def test_baseline_auto_and_forced_compact_share_symbolic_inputs(self):
        for kind in ("inner", "left"):
            script, scalar, evaluator, node = _evaluator(_snapshot(kind))
            left, right = (evaluator.node(name).certain() for name in ("L", "R"))
            baseline = evaluator._join(node, left, right, encoding="baseline")
            compact = evaluator._join(node, left, right, encoding="compact")
            self.assertEqual(evaluator._join(node, left, right), compact)
            self.assertEqual(len(compact.rows), 2)
            self.assertGreater(len(baseline.rows), len(compact.rows))
            script.assert_term(smt.not_(bag_equal(baseline, compact, scalar)))
            solved = subprocess.run(
                [SOLVER, "-in"], input=script.render(), text=True,
                capture_output=True, timeout=30, check=True,
            )
            self.assertEqual(solved.stdout.strip(), "unsat", solved.stderr)

    def test_forced_compact_requires_both_shape_and_row_admission(self):
        for kind, unique in (("right", True), ("full", True), ("inner", False)):
            _, _, evaluator, node = _evaluator(_snapshot(kind, unique=unique))
            left, right = (evaluator.node(name).certain() for name in ("L", "R"))
            with self.assertRaisesRegex(RelationError, "provenance-checked unique RHS"):
                evaluator._join(node, left, right, encoding="compact")
            self.assertEqual(
                evaluator._join(node, left, right),
                evaluator._join(node, left, right, encoding="baseline"),
            )
        _, _, evaluator, node = _evaluator(_snapshot())
        left, right = (evaluator.node(name).certain() for name in ("L", "R"))
        duplicate = replace(right, rows=(right.rows[0], right.rows[0]))
        with self.assertRaisesRegex(RelationError, "provenance-checked unique RHS"):
            evaluator._join(node, left, duplicate, encoding="compact")

    def test_baseline_annotations_follow_only_retained_input_occurrences(self):
        _, _, evaluator, node = _evaluator(_snapshot("full"))
        left, right = (evaluator.node(name).certain() for name in ("L", "R"))
        lf = frozenset((PartitionFact(smt.symbol("left_route", smt.BOOL), True),))
        rf = frozenset((PartitionFact(smt.symbol("right_route", smt.BOOL), False),))
        left = replace(left, rows=(replace(
            left.rows[0], partition_facts=lf,
            present=smt.and_(left.rows[0].present, next(iter(lf)).term),
        ),))
        right = replace(right, rows=(replace(
            right.rows[0], partition_facts=rf,
            present=smt.and_(right.rows[0].present, smt.not_(next(iter(rf)).term)),
        ),))
        matched, left_only, right_only = evaluator._join(node, left, right, encoding="baseline").rows
        self.assertEqual(
            tuple(row.partition_facts for row in (matched, left_only, right_only)),
            (lf | rf, lf, rf),
        )
        self.assertEqual(
            tuple(row.occurrence.operation for row in (matched, left_only, right_only)),
            ("join_match", "join_full_left", "join_full_right"),
        )
        self.assertEqual(matched.occurrence.inputs, (left.rows[0].occurrence, right.rows[0].occurrence))
        self.assertEqual(left_only.occurrence.inputs, (left.rows[0].occurrence,))
        self.assertEqual(right_only.occurrence.inputs, (right.rows[0].occurrence,))
