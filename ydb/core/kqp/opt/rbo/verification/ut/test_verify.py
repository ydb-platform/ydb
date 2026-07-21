import io
import os
import subprocess
import unittest
from contextlib import redirect_stdout
from itertools import product
from unittest import mock

from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import parse_snapshot
from ydb.core.kqp.opt.rbo.verification.rbo_verifier import cli
from ydb.core.kqp.opt.rbo.verification.rbo_verifier import smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier import verify as verifier
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import (
    Problem,
    SchemaMismatch,
    SolverError,
    build_problem,
    solve,
)


SOLVER = os.environ.get("RBO_Z3")


def schema():
    return {
        "tables": [
            {
                "name": "A",
                "columns": [
                    {"name": "k", "type": "int", "nullable": False},
                    {"name": "x", "type": "int", "nullable": False},
                ],
                "unique_keys": [],
            },
            {
                "name": "B",
                "columns": [
                    {"name": "k", "type": "int", "nullable": False},
                    {"name": "x", "type": "int", "nullable": False},
                ],
                "unique_keys": [],
            },
        ]
    }


SCAN_A = {
    "id": "a",
    "op": "scan",
    "table": "A",
    "columns": [
        {"source": "k", "output": "a.k"},
        {"source": "x", "output": "a.x"},
    ],
}
SCAN_B = {
    "id": "b",
    "op": "scan",
    "table": "B",
    "columns": [
        {"source": "k", "output": "b.k"},
        {"source": "x", "output": "b.x"},
    ],
}
KEY_EQUALITY = {
    "kind": "eq",
    "left": {"kind": "column", "column": "a.k"},
    "right": {"kind": "column", "column": "b.k"},
}
RESIDUAL = {
    "kind": "opaque",
    "fingerprint": "greater_than($0,$1)",
    "type": "bool",
    "nullable": False,
    "args": [
        {"kind": "column", "column": "a.x"},
        {"kind": "column", "column": "b.x"},
    ],
}


def right_join(predicate):
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": schema(),
            "plan": {
                "nodes": [
                    SCAN_A,
                    SCAN_B,
                    {
                        "id": "join",
                        "op": "join",
                        "left": "a",
                        "right": "b",
                        "kind": "right",
                        "predicate": predicate,
                    },
                ],
                "root": "join",
                "output": ["a.x", "b.x"],
            },
            "stage_graph": None,
        }
    )


def union_snapshot(duplicate):
    if duplicate:
        nodes = [
            SCAN_A,
            {
                "id": "union",
                "op": "union_all",
                "inputs": [
                    {"node": "a", "columns": ["a.k"]},
                    {"node": "a", "columns": ["a.k"]},
                ],
                "output": ["u.k"],
            },
        ]
    else:
        nodes = [
            {
                "id": "a",
                "op": "scan",
                "table": "A",
                "columns": [{"source": "k", "output": "u.k"}],
            }
        ]
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": schema(),
            "plan": {
                "nodes": nodes,
                "root": "union" if duplicate else "a",
                "output": ["u.k"],
            },
            "stage_graph": None,
        }
    )


def filtered_snapshot(predicate):
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": {
                "tables": [
                    {
                        "name": "T",
                        "columns": [{"name": "flag", "type": "bool", "nullable": True}],
                        "unique_keys": [],
                    }
                ]
            },
            "plan": {
                "nodes": [
                    {
                        "id": "scan",
                        "op": "scan",
                        "table": "T",
                        "columns": [{"source": "flag", "output": "t.flag"}],
                    },
                    {
                        "id": "filter",
                        "op": "filter",
                        "input": "scan",
                        "predicate": predicate,
                    },
                ],
                "root": "filter",
                "output": ["t.flag"],
            },
            "stage_graph": None,
        }
    )


def left_join_elimination_snapshot(with_join, right_key_is_unique):
    value = schema()
    value["tables"][1]["unique_keys"] = (
        [{"columns": ["k"], "nulls_distinct": False}] if right_key_is_unique else []
    )
    nodes = [SCAN_A]
    if with_join:
        nodes.extend(
            [
                SCAN_B,
                {
                    "id": "join",
                    "op": "join",
                    "left": "a",
                    "right": "b",
                    "kind": "left",
                    "predicate": KEY_EQUALITY,
                },
            ]
        )
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": value,
            "plan": {
                "nodes": nodes,
                "root": "join" if with_join else "a",
                "output": ["a.x"],
            },
            "stage_graph": None,
        }
    )


def constant_snapshot(value, output="result"):
    scalar_type = "string" if isinstance(value, str) else "int"
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": {"tables": []},
            "plan": {
                "nodes": [
                    {"id": "source", "op": "empty_source"},
                    {
                        "id": "project",
                        "op": "project",
                        "input": "source",
                        "columns": [
                            {
                                "output": output,
                                "expression": {"kind": "literal", "type": scalar_type, "value": value},
                            }
                        ],
                    },
                ],
                "root": "project",
                "output": [output],
            },
            "stage_graph": None,
        }
    )


class SolverProtocolTest(unittest.TestCase):
    def test_solver_error_cannot_be_reported_as_verified(self):
        process = subprocess.CompletedProcess(
            args=["z3"],
            returncode=1,
            stdout="unsat\n",
            stderr="parser error\n",
        )
        with mock.patch.object(verifier, "_run_solver", return_value=process):
            with self.assertRaisesRegex(SolverError, "parser error"):
                solve(Problem(smt.Script(), {}), "z3", 0)

    def test_extra_solver_responses_cannot_be_reported_as_verified(self):
        for output in (
            "unsat\nsat\n",
            "unsat\nsuccess\n",
            'unsat\n(error\n"late")\n',
        ):
            process = subprocess.CompletedProcess(
                args=["z3"],
                returncode=0,
                stdout=output,
                stderr="",
            )
            with self.subTest(output=output):
                with mock.patch.object(verifier, "_run_solver", return_value=process):
                    with self.assertRaisesRegex(SolverError, "exactly one solver status"):
                        solve(Problem(smt.Script(), {}), "z3", 0)

    def test_extra_witness_response_is_rejected(self):
        with self.assertRaisesRegex(SolverError, "SAT and one get-value response"):
            verifier._get_values("sat\n((v_0 true))\nsat\n")

    def test_schema_mismatch_is_a_correctness_verdict(self):
        output = io.StringIO()
        with (
            mock.patch.object(cli, "load_snapshot", return_value=mock.sentinel.snapshot),
            mock.patch.object(cli, "build_problem", side_effect=SchemaMismatch("changed root")),
            redirect_stdout(output),
        ):
            exit_code = cli.main(["before.json", "after.json", "--emit-smt", "formula.smt2"])
        self.assertEqual(exit_code, 1)
        self.assertIn('"status": "SCHEMA_MISMATCH"', output.getvalue())

    def test_root_nullability_is_part_of_the_contract(self):
        nullable = parse_snapshot(
            {
                "format": "ydb-rbo-semantic-snapshot",
                "version": 1,
                "schema": {"tables": []},
                "plan": {
                    "nodes": [
                        {"id": "source", "op": "empty_source"},
                        {
                            "id": "project",
                            "op": "project",
                            "input": "source",
                            "columns": [
                                {
                                    "output": "result",
                                    "expression": {
                                        "kind": "opaque",
                                        "fingerprint": "nullable_constant",
                                        "type": "int",
                                        "nullable": True,
                                        "args": [],
                                    },
                                }
                            ],
                        },
                    ],
                    "root": "project",
                    "output": ["result"],
                },
                "stage_graph": None,
            }
        )
        with self.assertRaisesRegex(SchemaMismatch, "root output nullability differs"):
            build_problem(constant_snapshot(1), nullable, 0)

    def test_root_output_name_is_part_of_the_contract(self):
        with self.assertRaisesRegex(SchemaMismatch, "root output names or order differ"):
            build_problem(constant_snapshot(1, "before_name"), constant_snapshot(1, "after_name"), 0)


class _MissingFunction(Exception):
    def __init__(self, key, sort):
        super().__init__(key)
        self.key = key
        self.sort = sort


def _restricted_domain_has_model(script):
    """Exhaust a tiny data domain and all Boolean/UF choices.

    This is deliberately test-only and independent of SMT-LIB rendering. It
    produces valid witnesses when true. False only means that this restricted
    domain has no witness; solver-backed tests establish actual UNSAT results.
    """

    root = smt.and_(*script.assertions)
    symbols = {}
    integer_literals = {0, 1}

    def collect(term):
        if term.operation == "symbol":
            symbols[term.atom] = term.sort
        elif term.operation == "int":
            integer_literals.add(term.atom)
        for argument in term.arguments:
            collect(argument)

    collect(root)
    names = tuple(sorted(symbols))
    domains = {smt.BOOL: (False, True), smt.INT: tuple(sorted(integer_literals))}

    def evaluate(term, constants, functions):
        if term.operation == "symbol":
            return constants[term.atom]
        if term.operation in {"bool", "int"}:
            return term.atom
        if term.operation == "not":
            return not evaluate(term.arguments[0], constants, functions)
        if term.operation == "and":
            return all(evaluate(argument, constants, functions) for argument in term.arguments)
        if term.operation == "or":
            return any(evaluate(argument, constants, functions) for argument in term.arguments)
        if term.operation == "=":
            return evaluate(term.arguments[0], constants, functions) == evaluate(
                term.arguments[1], constants, functions
            )
        if term.operation == "ite":
            branch = term.arguments[1] if evaluate(term.arguments[0], constants, functions) else term.arguments[2]
            return evaluate(branch, constants, functions)
        if term.operation == "+":
            return sum(evaluate(argument, constants, functions) for argument in term.arguments)
        if term.operation.startswith("f_"):
            key = (
                term.operation,
                tuple(evaluate(argument, constants, functions) for argument in term.arguments),
            )
            if key not in functions:
                raise _MissingFunction(key, term.sort)
            return functions[key]
        raise AssertionError(f"unsupported test SMT operation {term.operation!r}")

    def choose_functions(constants, functions):
        try:
            return evaluate(root, constants, functions) is True
        except _MissingFunction as missing:
            for value in domains[missing.sort]:
                functions[missing.key] = value
                if choose_functions(constants, functions):
                    return True
            del functions[missing.key]
            return False

    for values in product(*(domains[symbols[name]] for name in names)):
        if choose_functions(dict(zip(names, values)), {}):
            return True
    return False


class RestrictedModelSmokeTest(unittest.TestCase):
    def test_identical_join_has_no_restricted_model(self):
        snapshot = right_join({"kind": "and", "args": [KEY_EQUALITY, RESIDUAL]})
        self.assertFalse(_restricted_domain_has_model(build_problem(snapshot, snapshot, 1).script))

    def test_dropped_right_join_filter_has_a_restricted_model(self):
        before = right_join({"kind": "and", "args": [KEY_EQUALITY, RESIDUAL]})
        self.assertTrue(_restricted_domain_has_model(build_problem(before, right_join(KEY_EQUALITY), 1).script))

    def test_union_multiplicity_mutation_has_a_restricted_model(self):
        self.assertTrue(
            _restricted_domain_has_model(build_problem(union_snapshot(True), union_snapshot(False), 1).script)
        )

    def test_null_and_false_filters_have_no_restricted_model(self):
        before = filtered_snapshot({"kind": "null", "type": "bool"})
        after = filtered_snapshot({"kind": "literal", "type": "bool", "value": False})
        self.assertFalse(_restricted_domain_has_model(build_problem(before, after, 1).script))

    def test_catalog_key_controls_left_join_elimination(self):
        with_key = build_problem(
            left_join_elimination_snapshot(True, True),
            left_join_elimination_snapshot(False, True),
            2,
        )
        without_key = build_problem(
            left_join_elimination_snapshot(True, False),
            left_join_elimination_snapshot(False, False),
            2,
        )
        self.assertFalse(_restricted_domain_has_model(with_key.script))
        self.assertTrue(_restricted_domain_has_model(without_key.script))


@unittest.skipUnless(SOLVER, "set RBO_Z3 to run solver integration tests")
class VerificationTest(unittest.TestCase):
    def test_identical_plans_are_bounded_equivalent(self):
        snapshot = right_join({"kind": "and", "args": [KEY_EQUALITY, RESIDUAL]})
        result = solve(build_problem(snapshot, snapshot, 1, 10_000), SOLVER, 1, 10_000)
        self.assertEqual(result.status, "VERIFIED_BOUNDED")

    def test_dropped_right_join_filter_has_a_witness(self):
        before = right_join({"kind": "and", "args": [KEY_EQUALITY, RESIDUAL]})
        after = right_join(KEY_EQUALITY)
        result = solve(build_problem(before, after, 1, 10_000), SOLVER, 1, 10_000)
        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertEqual(len(result.witness["A"]), 1)
        self.assertEqual(len(result.witness["B"]), 1)
        self.assertEqual(result.witness["A"][0]["k"], result.witness["B"][0]["k"])

    def test_dropping_union_all_branch_changes_multiplicity(self):
        result = solve(
            build_problem(union_snapshot(True), union_snapshot(False), 1, 10_000),
            SOLVER,
            1,
            10_000,
        )
        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertEqual(len(result.witness["A"]), 1)

    def test_null_and_false_filters_are_equivalent(self):
        null_filter = filtered_snapshot({"kind": "null", "type": "bool"})
        false_filter = filtered_snapshot({"kind": "literal", "type": "bool", "value": False})
        result = solve(build_problem(null_filter, false_filter, 1, 10_000), SOLVER, 1, 10_000)
        self.assertEqual(result.status, "VERIFIED_BOUNDED")

    def test_unique_key_justifies_left_join_elimination(self):
        before = left_join_elimination_snapshot(True, True)
        after = left_join_elimination_snapshot(False, True)
        result = solve(build_problem(before, after, 2, 10_000), SOLVER, 2, 10_000)
        self.assertEqual(result.status, "VERIFIED_BOUNDED")

    def test_left_join_elimination_without_unique_key_has_a_witness(self):
        before = left_join_elimination_snapshot(True, False)
        after = left_join_elimination_snapshot(False, False)
        result = solve(build_problem(before, after, 2, 10_000), SOLVER, 2, 10_000)
        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertEqual(len(result.witness["B"]), 2)

    def test_constant_query_uses_one_empty_source_row(self):
        result = solve(build_problem(constant_snapshot(1), constant_snapshot(2), 0, 10_000), SOLVER, 0, 10_000)
        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertEqual(result.witness, {})

    def test_string_escape_forms_remain_distinct(self):
        result = solve(
            build_problem(constant_snapshot("\\n"), constant_snapshot("\n"), 0, 10_000),
            SOLVER,
            0,
            10_000,
        )
        self.assertEqual(result.status, "COUNTEREXAMPLE")

    def test_unicode_literal_round_trips_through_z3(self):
        result = solve(
            build_problem(constant_snapshot("é"), constant_snapshot("u{e9}"), 0, 10_000),
            SOLVER,
            0,
            10_000,
        )
        self.assertEqual(result.status, "COUNTEREXAMPLE")


if __name__ == "__main__":
    unittest.main()
