"""Tiny independent bag oracle and mocked protocol checks; no real SMT AST."""

from collections import Counter
from dataclasses import dataclass, replace
from itertools import product
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import relation as rel, verify
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import Column
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import Value


@dataclass(frozen=True)
class Tiny:
    operation: str
    arguments: tuple = ()
    atom: object = None
    sort: str = "Bool"


def constant(value):
    return Tiny("constant", atom=value, sort="Bool" if type(value) is bool else "Int")


class TinySmt:
    TRUE, FALSE = constant(True), constant(False)
    ONE, ZERO = constant(1), constant(0)
    BOOL, INT = "Bool", "Int"

    @staticmethod
    def and_(*args):
        return Tiny("and", args)

    @staticmethod
    def not_(arg):
        return Tiny("not", (arg,))

    @staticmethod
    def eq(*args):
        return Tiny("eq", args)

    @staticmethod
    def ite(*args):
        return Tiny("ite", args, sort=args[1].sort)

    @staticmethod
    def add(*args):
        return Tiny("add", args, sort="Int")


def evaluate(term, assignment):
    if term.operation == "constant":
        return term.atom
    if term.operation == "symbol":
        return assignment[term.atom]
    args = [evaluate(arg, assignment) for arg in term.arguments]
    if term.operation == "and":
        return all(args)
    if term.operation == "not":
        return not args[0]
    if term.operation == "eq":
        return args[0] == args[1]
    if term.operation == "ite":
        return args[1] if args[0] else args[2]
    if term.operation == "add":
        return sum(args)
    if term.operation == "same_value":
        return (args[0] and args[2]) or (not args[0] and not args[2] and args[1] == args[3])
    raise AssertionError(term.operation)


class TinyScript:
    def __init__(self):
        self.declarations = []
        self.string_universe = ("existing", "representatives")

    def fresh_constant(self, hint, sort):
        term = Tiny("symbol", atom=hint, sort=sort)
        self.declarations.append(term)
        return term


def scalar():
    return SimpleNamespace(
        script=TinyScript(), binary64=None, uses_abstract_integral_average=False,
        not_distinct=lambda a, b: Tiny("same_value", (a.is_null, a.value, b.is_null, b.value)),
    )


def family(rows, types=("Int32",), present=None, names=None):
    names = names or tuple(f"c{i}" for i in range(len(types)))
    columns = tuple(Column(name, typ, True) for name, typ in zip(names, types))
    modeled = tuple(rel.Row(
        constant(True if present is None else present[index]),
        {name: Value(typ, constant(value is None), constant(False if typ == "Bool" else 999)
                     if value is None else constant(value))
         for name, typ, value in zip(names, types, row)},
    ) for index, row in enumerate(rows))
    return rel.RelationFamily((rel.Outcome(TinySmt.TRUE, rel.Relation(columns, modeled), TinySmt.FALSE),))


def assign_probe(values, types=None):
    return {
        f"endpoint_bag_probe:{i}:{part}": value
        for i, item in enumerate(values)
        for part, value in (
            ("null", item is None),
            ("value", (False if types and types[i] == "Bool" else 999) if item is None else item),
        )
    }


class EndpointBagProbeTests(unittest.TestCase):
    def setUp(self):
        self.smt_patch = patch.object(rel, "smt", TinySmt)
        self.threshold_patch = patch.object(rel, "_MIN_ENDPOINT_BAG_COMPARISON_PAIRS", 0)
        self.audit_patch = patch.object(rel, "_comparison_inputs", side_effect=lambda a, b, *args: (a, b, False))
        self.smt_patch.start()
        self.threshold_patch.start()
        self.audit = self.audit_patch.start()
        self.addCleanup(self.smt_patch.stop)
        self.addCleanup(self.threshold_patch.stop)
        self.addCleanup(self.audit_patch.stop)

    def assert_projection(self, left, right, types, probes, left_present=None, right_present=None):
        result = rel._endpoint_bag_mismatch(
            family(left, types, left_present), family(right, types, right_present), scalar())
        self.assertIsNotNone(result)
        observed = []
        for probe in product(probes, repeat=len(types)):
            observed.append(evaluate(result.counterexample, assign_probe(probe, types)))
        lbag = Counter(row for i, row in enumerate(left) if left_present is None or left_present[i])
        rbag = Counter(row for i, row in enumerate(right) if right_present is None or right_present[i])
        self.assertEqual(any(observed), lbag != rbag, (left, right))
        if any(observed):
            # Projecting any satisfying probe drops only the private tuple;
            # the original source rows still form an actual unequal bag pair.
            self.assertNotEqual(lbag, rbag)

    def test_exhaustive_nullable_bags_with_duplicates(self):
        bags = [tuple((v,) for v in values)
                for n in range(4) for values in product((None, 0, 1), repeat=n)]
        for left, right in product(bags, repeat=2):
            self.assert_projection(left, right, ("Int32",), (None, 0, 1, 98765))

    def test_presence_masks_do_not_leak_absent_payloads(self):
        rows = [((a,), (b,)) for a, b in product((None, 0, 1), repeat=2)]
        masks = tuple(product((False, True), repeat=2))
        for left, right in product(rows, repeat=2):
            for lm, rm in product(masks, repeat=2):
                self.assert_projection(left, right, ("Int32",), (None, 0, 1, -77), lm, rm)

    def test_correlated_multicolumn_tuples(self):
        domain = ((None, 0), (0, None), (0, 0), (1, 1))
        bags = [tuple(values) for n in range(3) for values in product(domain, repeat=n)]
        for left, right in product(bags, repeat=2):
            self.assert_projection(left, right, ("Int32", "Int32"), (None, 0, 1, 9999))
        self.assert_projection(((0, 0), (1, 1)), ((0, 1), (1, 0)),
                               ("Int32", "Int32"), (None, 0, 1, 9999))

    def test_decimal_raw_nan_infinities_and_null(self):
        infinity = 10 ** 35
        domain = (None, -infinity, -1, 0, infinity, infinity + 1)
        bags = [tuple((v,) for v in values)
                for n in range(3) for values in product(domain, repeat=n)]
        for left, right in product(bags, repeat=2):
            self.assert_projection(left, right, ("Decimal(10,0)",), domain + (infinity + 100,))

    def test_bool_and_string_probes_add_no_new_domain(self):
        for typ, domain in (("Bool", (None, False, True)), ("String", (None, 0, 1)),
                            ("Utf8", (None, 0, 1))):
            bags = [tuple((v,) for v in values)
                    for n in range(3) for values in product(domain, repeat=n)]
            for left, right in product(bags, repeat=2):
                self.assert_projection(left, right, (typ,), domain)
        enc = scalar()
        old_universe = enc.script.string_universe
        rel._endpoint_bag_mismatch(family(((0,),), ("String",)), family(((1,),), ("String",)), enc)
        self.assertIs(enc.script.string_universe, old_universe)
        self.assertEqual([term.sort for term in enc.script.declarations], ["Bool", "Int"])

    def test_zero_column_bags_and_empty_results(self):
        for a, b in product(range(4), repeat=2):
            self.assert_projection(((),) * a, ((),) * b, (), (None,))

    def test_rejects_nontrivial_outcome_languages_without_allocating(self):
        base = family(((0,),))
        outcome = base.outcomes[0]
        mutations = (
            replace(outcome, enabled=TinySmt.FALSE),
            replace(outcome, enabled=Tiny("symbol", atom="enabled")),
            replace(outcome, error=TinySmt.TRUE),
            replace(outcome, error=Tiny("symbol", atom="error")),
            replace(outcome, choices=(object(),)),
            replace(outcome, decisions=(("shared", 1),)),
            replace(outcome, relation=replace(outcome.relation, sequence=True)),
            replace(outcome, relation=replace(outcome.relation, order=())),
            replace(outcome, relation=replace(outcome.relation, ordinals=(constant(0),))),
        )
        for changed in mutations:
            for left, right in ((rel.RelationFamily((changed,)), base),
                                (base, rel.RelationFamily((changed,)))):
                enc = scalar()
                self.assertIsNone(rel._endpoint_bag_mismatch(left, right, enc))
                self.assertFalse(enc.script.declarations)
        self.assertIsNone(rel._endpoint_bag_mismatch(rel.RelationFamily((outcome, outcome)), base, scalar()))

    def test_rejects_types_hidden_state_and_abstract_modes(self):
        base = family(((0,),))
        for typ in ("Double", "Void"):
            self.assertIsNone(rel._endpoint_bag_mismatch(
                family(((0,),), (typ,)), family(((0,),), (typ,)), scalar()))
        self.assertIsNone(rel._endpoint_bag_mismatch(base, family(((0,),), ("Int64",)), scalar()))
        for field in ("average_metadata", "decimal_sum_state", "binary64_state"):
            row = base.outcomes[0].relation.rows[0]
            value = replace(row.values["c0"], **{field: object()})
            relation = replace(base.outcomes[0].relation, rows=(replace(row, values={"c0": value}),))
            changed = rel.RelationFamily((replace(base.outcomes[0], relation=relation),))
            self.assertIsNone(rel._endpoint_bag_mismatch(changed, base, scalar()))
        for field in ("binary64", "uses_abstract_integral_average"):
            enc = scalar()
            setattr(enc, field, True)
            self.assertIsNone(rel._endpoint_bag_mismatch(base, base, enc))

    def test_schema_tags_nullability_and_value_types_are_checked(self):
        base = family(((0,),))
        outcome = base.outcomes[0]
        column = outcome.relation.columns[0]
        for changed in (replace(column, nullable=False), replace(column, integral_avg_rank=True)):
            relation = replace(outcome.relation, columns=(changed,))
            mutated = rel.RelationFamily((replace(outcome, relation=relation),))
            self.assertIsNone(rel._endpoint_bag_mismatch(mutated, base, scalar()))
        row = outcome.relation.rows[0]
        wrong_value = replace(row.values["c0"], type="Int64")
        relation = replace(outcome.relation, rows=(replace(row, values={"c0": wrong_value}),))
        mutated = rel.RelationFamily((replace(outcome, relation=relation),))
        self.assertIsNone(rel._endpoint_bag_mismatch(mutated, base, scalar()))

    def test_probe_predicate_is_not_a_pointwise_equality_replacement(self):
        result = rel._endpoint_bag_mismatch(family(((0,),)), family(((1,),)), scalar())
        self.assertTrue(evaluate(result.counterexample, assign_probe((0,))))
        self.assertFalse(evaluate(result.counterexample, assign_probe((12345,))))
        # The existential closure, not each arbitrary probe valuation, is the
        # mismatch. This distinction requires the standalone-endpoint gate.

    def test_one_global_probe_cannot_cross_opponent_choices(self):
        empty = family(())
        variants = (family(((0,),)), family(((1,),)))
        predicates = [rel._endpoint_bag_mismatch(empty, variant, scalar()).counterexample
                      for variant in variants]
        probes = tuple(assign_probe((value,)) for value in (None, 0, 1, 9999))
        self.assertTrue(all(any(evaluate(term, value) for value in probes) for term in predicates))
        self.assertFalse(any(all(evaluate(term, value) for term in predicates) for value in probes))
        # For every opponent choice there is a distinguishing tuple; there is
        # no one tuple distinguishing all choices. Admission must reject both
        # choice-bearing outcomes and enumerated multiple-outcome families.

    def test_threshold_ignores_statically_absent_rows(self):
        with patch.object(rel, "_MIN_ENDPOINT_BAG_COMPARISON_PAIRS", 4096):
            small = family(((0,),) * 32)
            self.assertIsNone(rel._endpoint_bag_mismatch(small, small, scalar()))
            large = family(((0,),) * 46)
            self.assertIsNotNone(rel._endpoint_bag_mismatch(large, large, scalar()))
            absent = family(((0,),) * 46, present=(False,) * 46)
            self.assertIsNone(rel._endpoint_bag_mismatch(absent, large, scalar()))

    def test_dependency_audit_precedes_probe_declarations(self):
        enc = scalar()
        self.audit.side_effect = rel.RelationError("un-carried bounded choice")
        with self.assertRaisesRegex(rel.RelationError, "un-carried"):
            rel._endpoint_bag_mismatch(family(((0,),)), family(((1,),)), enc)
        self.assertFalse(enc.script.declarations)

    def test_renamed_output_positions_and_single_complete_branch(self):
        result = rel._endpoint_bag_mismatch(
            family(((0,),), names=("left",)), family(((1,),), names=("right",)), scalar())
        self.assertEqual(result.branches, result.preferred_branches)
        self.assertEqual(len(result.branches), 1)
        self.assertIs(result.branches[0].predicate, result.counterexample)
        self.assertTrue(evaluate(result.counterexample, assign_probe((0,))))


class EndpointProbeHookTests(unittest.TestCase):
    def test_only_normal_endpoint_enables_probe(self):
        with patch.object(verify, "_check_boundary_roles"), patch.object(verify, "_build_problem") as build:
            verify.build_problem(object(), object(), 2)
            self.assertIs(build.call_args.kwargs["allow_endpoint_bag_probe"], True)

    def test_observers_diagnostics_certificates_and_declined_probe_keep_generic_path(self):
        names = ("before_node_observer", "after_node_observer", "after_edge_observer",
                 "boundary_observer", "comparison_observer")
        cases = [(False, None, None, False, False), (True, None, None, False, False)]
        cases += [(True, name, None, False, False) for name in names]
        cases.append((True, None, False, False, False))  # Explicit diagnostic observation.
        cases.append((True, None, None, True, False))  # An ineligible endpoint retains generic comparison.
        cases.append((True, None, None, False, True))  # A terminal order certificate stays separate.
        for enabled, observer, observed_order, declined, certified in cases:
            source = SimpleNamespace(semantic_mode=None)
            enc = SimpleNamespace(register_parameters=Mock(), semantic_mode=None,
                                  uses_abstract_integral_average=False, parameters={}, list_parameters={})
            exclusion = object()
            roots = iter((object(), object()))

            def boundary(*args, **kwargs):
                args[6].append(exclusion)
                return next(roots)
            kwargs = {name: Mock() if name == observer else None for name in names}
            probe_result, generic_result = object(), object()
            certificate = SimpleNamespace(before=SimpleNamespace(snapshot=source),
                                          after=SimpleNamespace(snapshot=source))
            with (
                patch.multiple(
                    verify,
                    _problem_snapshots=lambda x: x, _validate_pair=Mock(return_value=(object(), object())),
                    Database=Mock(), ScalarEncoder=Mock(return_value=enc), Router=Mock(),
                    query_parameters=Mock(return_value={}), _evaluate_boundary=boundary,
                    smt=SimpleNamespace(Script=Mock(), SmtError=ValueError),
                ),
                patch.object(verify, "_endpoint_bag_mismatch", return_value=None if declined else probe_result) as probe,
                patch.object(verify, "family_mismatch", return_value=generic_result),
                patch.object(verify, "compare_families", return_value=SimpleNamespace(mismatch=generic_result)),
                patch("ydb.core.kqp.opt.rbo.verification.rbo_verifier.terminal_order.certify", return_value=certificate),
                patch.object(verify, "_finish_problem") as finish,
            ):
                verify._build_problem(source, source, 2, 100, **kwargs,
                                      allow_endpoint_bag_probe=enabled, observed_order=observed_order,
                                      allow_terminal_order_certificate=certified)
                accepted = enabled and observer is None and observed_order is None and not certified
                self.assertEqual(probe.call_count, int(accepted))
                self.assertIs(finish.call_args.args[2], probe_result if accepted and not declined else generic_result)
                self.assertEqual(finish.call_args.args[3], [exclusion, exclusion])
                self.assertIs(finish.call_args.kwargs["parameters"], enc.parameters)
                self.assertIs(finish.call_args.kwargs["list_parameters"], enc.list_parameters)


if __name__ == "__main__":
    unittest.main()
