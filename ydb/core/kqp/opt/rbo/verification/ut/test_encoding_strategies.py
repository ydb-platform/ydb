"""Differential checks select encodings without changing their symbolic inputs."""

import os
import subprocess
import unittest
from dataclasses import replace

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import aggregate, decimal, relation, smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    Column, Expr, Filter, Join, Plan, Scan, ScanColumn, Snapshot, SortOrder, Table, UniqueKey,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import Encoder, Value


SOLVER = (
    yatest_common.binary_path("contrib/tools/z3/z3")
    if yatest_common is not None else os.environ.get("RBO_Z3")
)


def _all_candidates_bag_equal(left, right, scalar):
    """Original quadratic reference: check every possible output tuple."""
    left_names = tuple(column.name for column in left.columns)
    right_names = tuple(column.name for column in right.columns)

    def count(rows, names, candidate, candidate_names):
        return smt.add(*(smt.ite(
            smt.and_(row.present, *(scalar.not_distinct(
                row.values[name], candidate.values[candidate_name],
            ) for name, candidate_name in zip(names, candidate_names))),
            smt.ONE, smt.ZERO,
        ) for row in rows))

    return smt.and_(*(smt.or_(
        smt.not_(candidate.present),
        smt.eq(count(left.rows, left_names, candidate, names),
               count(right.rows, right_names, candidate, names)),
    ) for rows, names in ((left.rows, left_names), (right.rows, right_names))
        for candidate in rows))


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

    @unittest.skipUnless(SOLVER, "set RBO_Z3 to compare Decimal AVG encodings")
    def test_decimal_average_count_cases_match_general_division(self):
        for bound in (0, 1, 2, 4, 8):
            for value in range(bound + 1):
                with self.subTest(bound=bound, count=value):
                    script = smt.Script()
                    total = script.fresh_constant("sum", smt.INT)
                    count = script.fresh_constant("count", smt.INT)
                    optimized = aggregate._divide_decimal_average(total, count, "Decimal(35,2)", "Uint64", bound)
                    reference = decimal.divide(total, count, "Decimal(35,2)", "Uint64")
                    script.assert_term(smt.eq(count, smt.int_value(value)))
                    # No restriction on total: signed finite values, both
                    # infinities and NaN all participate in the same proof.
                    script.assert_term(smt.not_(smt.eq(optimized, reference)))
                    self.assertUnsat(script)

    @unittest.skipUnless(SOLVER, "set RBO_Z3 to compare bag encodings")
    def test_cardinality_and_one_side_match_all_candidate_multiplicities(self):
        schemas = ((), ("Int64",), ("Bool", "Int64", "Decimal(7,2)", "String", "Utf8", "Double"))
        for left_size, right_size in ((0, 0), (0, 3), (3, 0), (1, 4), (4, 1), (2, 2), (3, 3)):
            for types in schemas:
                with self.subTest(left_size=left_size, right_size=right_size, types=types):
                    script = smt.Script()
                    scalar = Encoder(script)

                    def source(prefix, size):
                        columns = tuple(Column(f"{prefix}:{i}", kind, True) for i, kind in enumerate(types))
                        rows = tuple(relation.Row(
                            script.fresh_constant(f"{prefix}:present:{index}", smt.BOOL),
                            {column.name: Value(
                                column.type,
                                script.fresh_constant(f"{prefix}:null:{index}:{i}", smt.BOOL),
                                script.fresh_constant(f"{prefix}:value:{index}:{i}",
                                                      smt.BOOL if column.type == "Bool" else smt.INT),
                            ) for i, column in enumerate(columns)},
                        ) for index in range(size))
                        return relation.Relation(columns, rows)

                    left, right = source("left", left_size), source("right", right_size)
                    compact = relation.bag_equal(left, right, scalar)
                    reference = _all_candidates_bag_equal(left, right, scalar)
                    script.assert_term(smt.not_(smt.eq(compact, reference)))
                    self.assertUnsat(script)

    @unittest.skipUnless(SOLVER, "set RBO_Z3 to compare bag encodings")
    def test_absent_bag_slots_match_unpruned_symbolic_reference(self):
        for left_size, right_size in ((0, 0), (0, 3), (1, 2), (3, 1), (3, 3)):
            for width in (0, 1, 2):
                with self.subTest(left_size=left_size, right_size=right_size, width=width):
                    script = smt.Script()
                    scalar = Encoder(script)
                    absent = script.fresh_constant("absent", smt.BOOL)
                    script.assert_global(smt.not_(absent))

                    def source(prefix, size):
                        columns = tuple(Column(f"{prefix}:{i}", "Int64", True) for i in range(width))
                        rows = tuple(relation.Row(
                            smt.FALSE if index == 0 else script.fresh_constant(f"{prefix}:present:{index}", smt.BOOL),
                            {column.name: Value(
                                "Int64", script.fresh_constant(f"{prefix}:null:{index}:{i}", smt.BOOL),
                                script.fresh_constant(f"{prefix}:value:{index}:{i}", smt.INT),
                            ) for i, column in enumerate(columns)},
                        ) for index in range(size))
                        return relation.Relation(columns, rows)

                    def unpruned(source):
                        # The same absent slots have symbolic guards fixed to
                        # false. They therefore retain the original full
                        # multiplicity/candidate construction without pruning.
                        return replace(source, rows=tuple(
                            replace(row, present=absent) if row.present == smt.FALSE else row
                            for row in source.rows
                        ))

                    left, right = source("left", left_size), source("right", right_size)
                    compact = relation.bag_equal(left, right, scalar)
                    baseline = relation.bag_equal(unpruned(left), unpruned(right), scalar)
                    script.assert_term(smt.not_(smt.eq(compact, baseline)))
                    self.assertUnsat(script)

    def test_absent_bag_slots_still_require_compatible_output_types(self):
        scalar = Encoder(smt.Script())
        left = relation.Relation((Column("left", "Int64", True),), (
            relation.Row(smt.FALSE, {"left": Value("Int64", smt.TRUE, smt.ZERO)}),
        ))
        right = relation.Relation((Column("right", "Bool", True),), (
            relation.Row(smt.FALSE, {"right": Value("Bool", smt.TRUE, smt.FALSE)}),
        ))
        self.assertEqual(relation.bag_equal(left, right, scalar), smt.FALSE)
        self.assertEqual(relation.bag_equal(left, relation.Relation((), ()), scalar), smt.FALSE)

    @unittest.skipUnless(SOLVER, "set RBO_Z3 to compare quantified result languages")
    def test_result_equality_is_symmetric_with_symbolic_rows_choices_and_errors(self):
        for ordered in (False, True):
            for scalar_type in (None, "Int64", "Utf8"):
                with self.subTest(ordered=ordered, scalar_type=scalar_type):
                    script = smt.Script()
                    scalar = Encoder(script)

                    def family(prefix, size):
                        columns = () if scalar_type is None else (Column(prefix, scalar_type, True),)
                        outcomes = []
                        for index in range(size):
                            name = f"{prefix}:{index}"
                            choice = relation.BoundedChoice(script.fresh_constant(name + ":choice", smt.INT), 2)
                            first = smt.eq(choice.term, smt.ZERO)
                            rows = []
                            for slot in range(1 + index % 2):
                                values = {}
                                if scalar_type is not None:
                                    one, two = (
                                        (smt.int_value(slot), smt.int_value(slot + 1))
                                        if scalar_type == "Int64"
                                        else (script.string_atom(str(slot)), script.string_atom(str(slot + 1)))
                                    )
                                    values[prefix] = Value(
                                        scalar_type, script.fresh_constant(f"{name}:null:{slot}", smt.BOOL),
                                        smt.ite(first, one, two),
                                    )
                                rows.append(relation.Row(
                                    script.fresh_constant(f"{name}:present:{slot}", smt.BOOL), values,
                                ))
                            outcomes.append(relation.Outcome(
                                script.fresh_constant(name + ":enabled", smt.BOOL),
                                relation.Relation(columns, tuple(rows), sequence=ordered),
                                smt.and_(first, script.fresh_constant(name + ":error", smt.BOOL)),
                                choices=(choice,),
                            ))
                        return relation.RelationFamily(tuple(outcomes))

                    comparison = relation.compare_families(family("left", 2), family("right", 3), scalar)
                    # Independent construction of the old reverse comparison:
                    # retain its row order and swapped operand expressions.
                    reverse = relation._outcome_equal_matrix(
                        comparison.right, comparison.left, scalar, ordered,
                    )
                    # Prove equality pointwise for all valuations, before
                    # either side's choices are bound by language inclusion.
                    script.assert_term(smt.or_(
                        *(smt.not_(smt.eq(term, reverse[j][i]))
                          for i, row in enumerate(comparison.pair_equal) for j, term in enumerate(row)),
                    ))
                    self.assertUnsat(script)

    @unittest.skipUnless(SOLVER, "set RBO_Z3 to compare quantified result languages")
    def test_reused_equality_retains_both_language_inclusions_and_query_errors(self):
        for ordered in (False, True):
            for may_error in (False, True):
                for mutation in ("equal", "extra", "missing"):
                    with self.subTest(ordered=ordered, may_error=may_error, mutation=mutation):
                        script = smt.Script()
                        scalar = Encoder(script)
                        choice = relation.BoundedChoice(script.fresh_constant("left-choice", smt.INT), 2)

                        def outcome(value, *, null=False, error=smt.FALSE, choices=()):
                            return relation.Outcome(smt.TRUE, relation.Relation(
                                (Column("x", "Int64", True),),
                                (relation.Row(smt.FALSE, {"x": Value("Int64", smt.FALSE, smt.int_value(99))}),
                                 relation.Row(smt.TRUE, {"x": Value("Int64", smt.bool_value(null), value)})),
                                sequence=ordered,
                            ), error, choices=choices)

                        first_error = smt.eq(choice.term, smt.ZERO) if may_error else smt.FALSE
                        left = relation.RelationFamily((
                            outcome(choice.term, error=first_error, choices=(choice,)),
                            outcome(smt.ZERO, null=True),
                        ))
                        right = [outcome(smt.ZERO, error=smt.bool_value(may_error)), outcome(smt.ONE)]
                        if mutation != "missing":
                            right.append(outcome(smt.ZERO, null=True))
                        if mutation == "extra":
                            right.append(outcome(smt.int_value(2)))
                        mismatch = relation.family_mismatch(left, relation.RelationFamily(tuple(right)), scalar)
                        script.assert_term(mismatch.counterexample)
                        result = subprocess.run(
                            [SOLVER, "-in"], input=script.render(), text=True,
                            capture_output=True, timeout=30, check=True,
                        )
                        self.assertEqual(result.stdout.strip(), "unsat" if mutation == "equal" else "sat")

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
