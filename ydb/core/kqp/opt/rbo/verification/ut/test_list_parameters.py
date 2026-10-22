"""Exact List membership, declaration isolation and complete finite witnesses."""
import copy
import os
import subprocess
import unittest
from unittest import mock

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import bundle, list_parameters, smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    Expr, SnapshotError, ValueType, parse_snapshot, query_parameters,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import Encoder, Value
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import (
    SolverQuery, VerificationError, build_logical_kernel_problem_for_tests,
    build_transformation_pair_problem, solve,
)

SOLVER = (yatest_common.binary_path("contrib/tools/z3/z3")
          if yatest_common is not None else os.environ.get("RBO_Z3"))


def literal(value, scalar_type="String"):
    return {"kind": "literal", "type": scalar_type, "value": value}


def membership(name="$p", optional=False, ansi=True, lookup=None, lookup_nullable=False):
    return {"kind": "in_parameter", "name": name,
            "parameter_type": "List<Optional<String>>" if optional else "List<String>",
            "lookup": literal("x") if lookup is None else lookup,
            "ansi": ansi, "type": "Bool", "nullable": lookup_nullable or (ansi and optional)}


def snapshot(expression, extra=None):
    projections = [{"output": "answer", "expression": expression}]
    if extra is not None:
        projections.append({"output": "unused", "expression": extra})
    return {"format": "ydb-rbo-semantic-snapshot", "version": 1,
            "schema": {"tables": [{"name": "T", "columns": [
                {"name": "k", "type": "Int32", "nullable": False}], "unique_keys": []}]},
            "plan": {"nodes": [
                {"id": "scan", "op": "scan", "table": "T",
                 "columns": [{"source": "k", "output": "key"}], "pushed_limit": None},
                {"id": "project", "op": "project", "input": "scan", "ordered": False,
                 "columns": projections}], "root": "project", "output": ["answer"], "subplans": []},
            "stage_graph": None}


def problem(before, after):
    return build_logical_kernel_problem_for_tests(parse_snapshot(before), parse_snapshot(after), 2, 2000)


class ListParameterAdmissionTest(unittest.TestCase):
    def test_modes_lookup_and_element_nullability(self):
        for optional in (False, True):
            for ansi in (False, True):
                for nullable in (False, True):
                    lookup = {"kind": "null", "type": "String"} if nullable else literal("x")
                    expr = membership(optional=optional, ansi=ansi, lookup=lookup, lookup_nullable=nullable)
                    parsed = parse_snapshot(snapshot(expr))
                    self.assertEqual(query_parameters(parsed), {"$p": ValueType(expr["parameter_type"], False)})

    def test_rejects_mutated_wire_contract(self):
        changes = ({"name": "$"}, {"name": "%tx_result"}, {"name": "p"}, {"name": 4},
                   {"parameter_type": "List<Utf8>"}, {"parameter_type": "Optional<List<String>>"},
                   {"parameter_type": "List<Optional<Optional<String>>>"},
                   {"parameter_type": "List<PgText>"}, {"parameter_type": "List<Struct<x:String>>"},
                   {"ansi": 1}, {"nullable": True}, {"type": "Int32"}, {"extra": 0},
                   {"lookup": literal("x", "Utf8")}, {"lookup": literal(1, "Int32")})
        for changed in changes:
            expr = membership()
            expr.update(changed)
            with self.subTest(changed=changed), self.assertRaises(SnapshotError):
                parse_snapshot(snapshot(expr))
        for expr in (
            membership(optional=True),
            membership(lookup={"kind": "null", "type": "String"}, lookup_nullable=True),
        ):
            expr["nullable"] = False
            with self.assertRaises(SnapshotError):
                parse_snapshot(snapshot(expr))

    def test_scalar_list_and_element_nullability_conflicts_even_when_pruned(self):
        scalar = {"kind": "parameter", "name": "$p", "type": "String", "nullable": False}
        for conflict in (scalar, membership(optional=True)):
            with self.assertRaisesRegex(SnapshotError, "inconsistent declared types"):
                parse_snapshot(snapshot(membership(), conflict))
            with self.assertRaisesRegex(VerificationError, "inconsistent declared types"):
                problem(snapshot(literal(True, "Bool"), membership()),
                        snapshot(literal(True, "Bool"), conflict))
        anchor = parse_snapshot(snapshot(literal(True, "Bool"), membership()))
        middle = parse_snapshot(snapshot(literal(True, "Bool")))
        after = parse_snapshot(snapshot(literal(True, "Bool"), membership(optional=True)))
        with self.assertRaisesRegex(VerificationError, "inconsistent declared types"):
            build_transformation_pair_problem(middle, after, 2, observation_snapshot=anchor)

    def test_all_other_list_value_uses_stay_closed(self):
        collection = {"kind": "parameter", "name": "$p", "type": "List<String>", "nullable": False}
        for expr in (collection, {"kind": "length", "arg": collection},
                     {"kind": "opaque", "type": "String", "nullable": False,
                      "fingerprint": "list-projection", "args": [collection]},
                     {"kind": "as_table", "arg": collection}):
            with self.assertRaises(SnapshotError):
                parse_snapshot(snapshot(expr))

    def test_scalar_and_list_encoder_registrations_share_one_type_contract(self):
        for first, second in (("String", "List<String>"), ("List<String>", "String"),
                              ("List<String>", "List<Optional<String>>")):
            encoder = Encoder(smt.Script())
            encoder.register_parameters({"$p": ValueType(first, False)})
            with self.assertRaisesRegex(smt.SmtError, "inconsistent declared types"):
                encoder.register_parameters({"$p": ValueType(second, False)})
        with self.assertRaises(smt.SmtError):
            Encoder(smt.Script()).register_parameters({"$p": ValueType("List<String>", True)})

    def test_pruned_parameter_keeps_shared_identity_and_bundle_contract(self):
        before = parse_snapshot(snapshot(literal(True, "Bool"), membership()))
        after = parse_snapshot(snapshot(literal(True, "Bool")))
        with mock.patch.object(bundle, "_check_boundary_roles"):
            result = bundle.build_bundle_problem(((before, after), (before, after)), 2)
            self.assertEqual(set(result.list_parameters), {"$p"})
            self.assertFalse(result.parameters)
            bad = parse_snapshot(snapshot(literal(True, "Bool"), membership(optional=True)))
            with self.assertRaisesRegex(VerificationError, "inconsistent declared types"):
                bundle.build_bundle_problem(((before, after), (bad, after)), 2)
        encoder = Encoder(smt.Script())
        encoder.register_parameters({"$p": ValueType("List<String>", False)})
        first = encoder.list_parameters["$p"]
        encoder.register_parameters({"$p": ValueType("List<String>", False)})
        self.assertIs(first, encoder.list_parameters["$p"])
        for conflict in (ValueType("String", False), ValueType("List<Optional<String>>", False)):
            with self.assertRaisesRegex(smt.SmtError, "inconsistent declared types"):
                encoder.register_parameters({"$p": conflict})


class ListParameterTruthTableTest(unittest.TestCase):
    def test_independent_native_truth_table_including_empty_and_null_only_lists(self):
        # Explicit cases from ExpandSqlIn; no solver or mirror of formula shape.
        cases = [
            (None, [], False, None), (None, ["other"], None, None),
            (None, [None], None, None), ("x", [], False, False),
            ("x", ["x"], True, True), ("x", [None], None, False),
            ("x", ["other", None], None, False), ("x", ["x", None], True, True),
            ("x", ["x", "x"], True, True), ("", [""], True, True),
        ]
        for lookup, values, ansi_expected, legacy_expected in cases:
            summary = list_parameters.ListParameter(
                "List<Optional<String>>", smt.bool_value(any(v is not None for v in values)),
                smt.bool_value(None in values), smt.ZERO,
                lambda rank: smt.bool_value(lookup is not None and lookup in values))
            for ansi, expected in ((True, ansi_expected), (False, legacy_expected)):
                null, truth = summary.contains(smt.bool_value(lookup is None), smt.ZERO, ansi)
                self.assertEqual(null, smt.bool_value(expected is None))
                if expected is not None:
                    self.assertEqual(truth, smt.bool_value(expected))


class ListParameterWitnessTest(unittest.TestCase):
    def make_model(self, optional=True):
        script = smt.Script()
        for value in ("", "x", "y", "\x00", "λ"):
            script.string_atom(value)
        parameter = list_parameters.ListParameter.create(
            script, "$p", "List<Optional<String>>" if optional else "List<String>",
        )
        terms = list_parameters.prepare_witnesses(script, {"$p": parameter})
        ranks = script.string_literals
        selected = {0, len(ranks) - 1}
        model = {parameter.has_non_null.atom: True, parameter.member.atom: 0}
        if optional:
            model[parameter.has_null.atom] = True
        model.update({term.atom: rank in selected for rank, term in enumerate(parameter.rank_values)})
        return script, parameter, terms, model, selected

    def test_complete_graph_materializes_positive_and_negative_membership(self):
        script, parameter, terms, model, selected = self.make_model()
        result = list_parameters.decode_lists({"$p": parameter}, model, script.string_literals)["$p"]
        self.assertEqual(result, [script.string_literals[i] for i in sorted(selected)] + [None])
        for rank, text in script.string_literals.items():
            self.assertEqual(text in result, rank in selected)
        self.assertEqual(terms, list_parameters.prepare_witnesses(script, {"$p": parameter}))
        self.assertEqual(len(parameter.rank_values), len(script.string_literals))

    def test_empty_and_null_only_lists_decode_exactly(self):
        for optional in (False, True):
            script, parameter, _, model, _ = self.make_model(optional)
            model[parameter.has_non_null.atom] = False
            model.update({term.atom: False for term in parameter.rank_values})
            result = list_parameters.decode_lists({"$p": parameter}, model, script.string_literals)
            self.assertEqual(result["$p"], [None] if optional else [])

    def test_dense_alias_hints_do_not_multiply_external_parameter_name_bytes(self):
        script = smt.Script()
        script.string_atom("x")
        name = "$" + "p" * 4096  # small regression, never an OOM stress test
        parameter = list_parameters.ListParameter.create(script, name, "List<String>")
        before = len(script.declarations)
        list_parameters.prepare_witnesses(script, {name: parameter})
        aliases = script.declarations[before:]
        self.assertEqual(len(aliases), len(script.string_literals))
        for declaration in aliases:
            self.assertLess(len(declaration.hint), 96)
            self.assertIn(parameter.membership.name, declaration.hint)
            self.assertNotIn(name, declaration.hint)

    def test_expired_preparation_deadline_refuses_before_alias_allocation(self):
        script = smt.Script()
        parameter = list_parameters.ListParameter.create(script, "$p", "List<String>")
        declarations = tuple(script.declarations)
        with self.assertRaisesRegex(list_parameters.ListWitnessError, "deadline expired"):
            list_parameters.prepare_witnesses(script, {"$p": parameter}, timeout_ms=0)
        self.assertEqual(tuple(script.declarations), declarations)
        self.assertIsNone(parameter.rank_values)

    def test_missing_or_contradictory_graph_is_never_a_partial_counterexample(self):
        script, parameter, _, model, _ = self.make_model()
        omitted = dict(model)
        del omitted[parameter.rank_values[-1].atom]
        bad_bool = dict(model)
        bad_bool[parameter.rank_values[0].atom] = 1
        bad_empty = dict(model)
        bad_empty[parameter.has_non_null.atom] = False
        bad_witness = dict(model)
        bad_witness[parameter.member.atom] = len(script.string_literals)
        for changed in (omitted, bad_bool, bad_empty, bad_witness):
            with self.assertRaises(list_parameters.ListWitnessError):
                list_parameters.decode_lists({"$p": parameter}, changed, script.string_literals)

    def test_lookup_bounds_cover_every_finite_selector_and_witness_covers_every_rank(self):
        script = smt.Script()
        encoder = Encoder(script)
        choice = script.fresh_constant("choice", smt.INT)
        script.register_quantified_choice(choice, 3)
        lookup_function = script.fresh_function("lookup_by_choice", (smt.INT,), smt.INT)
        lookup = lookup_function(choice)
        expr = Expr(kind="in_parameter", parameter="$p", parameter_type="List<String>",
                    args=(Expr(kind="column", column="text"),), ansi=True, result_type="Bool", nullable=False)
        encoder.evaluate(expr, {"text": Value("String", smt.FALSE, lookup)})
        parameter = encoder.list_parameters["$p"]
        list_parameters.prepare_witnesses(script, encoder.list_parameters)
        self.assertIn("(forall", script.render())
        self.assertEqual(len(parameter.rank_values), len(script.string_literals))
        # Membership aliases are database inputs for all choice valuations;
        # none can be captured as a per-execution quantified symbol.
        for alias in parameter.rank_values:
            self.assertEqual(script.quantified_choice_dependencies((alias,)), ())
        self.assertGreaterEqual(len(script.string_literals), 4)

    def test_unsat_does_not_allocate_dense_witness_or_use_witness_budget(self):
        built = problem(snapshot(membership()), snapshot(membership()))
        with mock.patch.object(list_parameters, "MAX_MEMBERSHIP_CELLS", 0), \
             mock.patch("ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify._run_solver",
                        return_value=subprocess.CompletedProcess([], 0, "unsat\n", "")):
            result = solve(built, "/never/run", 2, 2000)
        self.assertEqual(result.status, "VERIFIED_BOUNDED")
        self.assertIsNone(built.list_parameters["$p"].rank_values)

    def test_witness_budget_refusal_and_model_timeout_return_unknown(self):
        before = snapshot(membership())
        after = snapshot(membership("$q"))
        built = problem(before, after)
        with mock.patch.object(list_parameters, "MAX_MEMBERSHIP_CELLS", 0), \
             mock.patch("ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify._run_solver",
                        return_value=subprocess.CompletedProcess([], 0, "sat\n", "")) as query:
            result = solve(built, "/never/run", 2, 2000)
            self.assertEqual(result.status, "UNKNOWN")
            self.assertIsNone(result.parameters)
            self.assertEqual(query.call_count, 1)  # no partial model query
        built = problem(before, after)
        with mock.patch("ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify.query_solver",
                        return_value=SolverQuery("unknown", {}, "model deadline", "model")):
            self.assertEqual(solve(built, "/never/run", 2, 1).status, "UNKNOWN")


@unittest.skipUnless(SOLVER, "run through ya or set RBO_Z3")
class ListParameterProofTest(unittest.TestCase):
    def verify(self, before, after):
        return solve(problem(before, after), SOLVER, 2, 2000)

    def test_identity_is_universal_and_pruning_preserves_inputs(self):
        for optional in (False, True):
            for ansi in (False, True):
                expr = membership(optional=optional, ansi=ansi)
                self.assertEqual(self.verify(snapshot(expr), snapshot(copy.deepcopy(expr))).status, "VERIFIED_BOUNDED")
        self.assertEqual(self.verify(snapshot(literal(True, "Bool"), membership()),
                                     snapshot(literal(True, "Bool"))).status, "VERIFIED_BOUNDED")

    def test_independent_list_names_produce_complete_replayable_inputs(self):
        result = self.verify(snapshot(membership()), snapshot(membership("$q")))
        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertNotEqual("x" in result.parameters["$p"], "x" in result.parameters["$q"])
        self.assertEqual(result.parameter_types["$p"], {"type": "List<String>", "nullable": False})
        self.assertEqual(result.to_json()["parameter_types"], result.parameter_types)

    def test_null_lookup_empty_list_distinguishes_ansi_from_legacy(self):
        lookup = {"kind": "null", "type": "String"}
        before = membership(lookup=lookup, lookup_nullable=True, ansi=True)
        after = membership(lookup=lookup, lookup_nullable=True, ansi=False)
        result = self.verify(snapshot(before), snapshot(after))
        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertEqual(result.parameters["$p"], [])

    def test_ignoring_nullable_element_changes_projected_nullness(self):
        # Both projections are non-null Bool, so schema equality does not mask
        # the semantic mutation to ANSI NULL-vs-legacy false membership.
        before = {"kind": "exists", "arg": membership(optional=True, ansi=True)}
        after = {"kind": "exists", "arg": membership(optional=True, ansi=False)}
        result = self.verify(snapshot(before), snapshot(after))
        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertIn(None, result.parameters["$p"])
        self.assertNotIn("x", result.parameters["$p"])

    def test_different_lookup_ranks_can_have_different_membership(self):
        before = {"kind": "and", "args": [membership(lookup=literal("x")),
                  {"kind": "not", "arg": membership(lookup=literal("y"))}]}
        result = self.verify(snapshot(before), snapshot(literal(False, "Bool")))
        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertIn("x", result.parameters["$p"])
        self.assertNotIn("y", result.parameters["$p"])

    def test_same_input_and_equal_lookup_cannot_disagree(self):
        member = membership()
        both = {"kind": "and", "args": [member, {"kind": "not", "arg": copy.deepcopy(member)}]}
        self.assertEqual(self.verify(snapshot(both), snapshot(literal(False, "Bool"))).status,
                         "VERIFIED_BOUNDED")
