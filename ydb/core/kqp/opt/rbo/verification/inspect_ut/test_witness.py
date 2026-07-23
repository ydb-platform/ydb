import os
import unittest

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from ydb.core.kqp.opt.rbo.verification.inspector.witness import (
    InvalidWitness,
    bind_witness,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier import decimal, smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import parse_snapshot
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import Database
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import (
    Problem,
    decode_witness,
    query_solver,
)


SOLVER = (
    yatest_common.binary_path("contrib/tools/z3/z3")
    if yatest_common is not None
    else os.environ.get("RBO_Z3")
)


def _snapshot():
    columns = [
        {"name": "b", "type": "Bool", "nullable": False},
        {"name": "i", "type": "Int8", "nullable": False},
        {"name": "u", "type": "Uint64", "nullable": False},
        {"name": "d", "type": "Date", "nullable": False},
        {"name": "s", "type": "String", "nullable": False},
        {"name": "t", "type": "Utf8", "nullable": True},
        {"name": "x", "type": "Decimal(3,1)", "nullable": True},
    ]
    return parse_snapshot({
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": {
            "tables": [{"name": "A", "columns": columns, "unique_keys": []}]
        },
        "plan": {
            "nodes": [{
                "id": "scan",
                "op": "scan",
                "table": "A",
                "columns": [
                    {"source": column["name"], "output": column["name"]}
                    for column in columns
                ],
                "predicate": None,
                "pushed_limit": None,
            }],
            "root": "scan",
            "output": [column["name"] for column in columns],
            "subplans": [],
        },
        "stage_graph": None,
    })


@unittest.skipUnless(SOLVER, "run through ya or set RBO_Z3 for solver tests")
class WitnessBindingTest(unittest.TestCase):
    def problem(self, rows=2):
        script = smt.Script(10_000)
        database = Database(_snapshot(), rows, script)
        return Problem(script, database.witness)

    @staticmethod
    def witness():
        return {
            "A": [{
                "b": True,
                "i": -128,
                "u": (1 << 64) - 1,
                "d": 49_672,
                "s": "__ydb_rbo_string_atom_7",
                "t": None,
                "x": decimal.INF,
            }]
        }

    def test_decoded_database_round_trips_with_absent_slots_and_nulls(self):
        problem = self.problem()
        witness = self.witness()
        self.assertEqual(bind_witness(problem, witness), witness)
        query = query_solver(problem, SOLVER, problem.witness_values(), 10_000)
        self.assertEqual(query.status, "sat")
        self.assertEqual(
            decode_witness(
                problem.witness,
                query.values,
                problem.script.string_literals,
            ),
            witness,
        )

    def test_shape_and_scalar_domains_fail_closed(self):
        mutations = []
        missing_table = self.witness()
        missing_table.clear()
        mutations.append((missing_table, "table identities"))
        wrong_column = self.witness()
        wrong_column["A"][0].pop("i")
        mutations.append((wrong_column, "do not match its schema"))
        too_many = self.witness()
        too_many["A"] *= 3
        mutations.append((too_many, "exceed the declared row bound"))
        null_non_nullable = self.witness()
        null_non_nullable["A"][0]["b"] = None
        mutations.append((null_non_nullable, "non-nullable"))
        bool_as_integer = self.witness()
        bool_as_integer["A"][0]["i"] = True
        mutations.append((bool_as_integer, "outside Int8"))
        bad_date = self.witness()
        bad_date["A"][0]["d"] = 49_673
        mutations.append((bad_date, "outside Date"))
        bad_decimal = self.witness()
        bad_decimal["A"][0]["x"] = decimal.NAN + 1
        mutations.append((bad_decimal, "outside Decimal"))

        for witness, reason in mutations:
            with self.subTest(reason=reason):
                with self.assertRaisesRegex(InvalidWitness, reason):
                    bind_witness(self.problem(), witness)

    def test_rejection_is_atomic(self):
        problem = self.problem()
        before = problem.script.assertions
        witness = self.witness()
        witness["A"][0]["x"] = decimal.NAN + 1
        with self.assertRaises(InvalidWitness):
            bind_witness(problem, witness)
        self.assertEqual(problem.script.assertions, before)

    def test_new_string_binding_after_formula_sealing_fails_atomically(self):
        problem = self.problem()
        problem.formula()
        before = problem.script.assertions

        with self.assertRaisesRegex(InvalidWitness, "after.*sealed"):
            bind_witness(problem, self.witness())

        self.assertEqual(problem.script.assertions, before)

    def test_witness_strings_round_trip_independently_of_registration_order(self):
        problem = self.problem()
        witness = self.witness()
        second = dict(witness["A"][0])
        witness["A"][0]["s"] = "z"
        witness["A"][0]["t"] = "e\u0301"
        second["s"] = "a"
        second["t"] = "é"
        witness["A"].append(second)

        self.assertEqual(bind_witness(problem, witness), witness)
        query = query_solver(problem, SOLVER, problem.witness_values(), 10_000)
        self.assertEqual(query.status, "sat")
        self.assertEqual(
            decode_witness(
                problem.witness,
                query.values,
                problem.script.string_literals,
            ),
            witness,
        )


if __name__ == "__main__":
    unittest.main()
