"""Grounded uniqueness laws and strict physical-key metadata, without a solver."""
import copy
import unittest
from dataclasses import replace
from itertools import product

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import decimal, smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.bundle import _union_catalog
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    SnapshotError, UniqueKey, parse_snapshot, validate_snapshot)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import Database
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import _check_catalogs, VerificationError
from ydb.core.kqp.opt.rbo.verification.replay.model import primary_key, ReplayError
if __package__:
    from .test_point_lookup import document as lookup_document, bind
else:
    from test_point_lookup import document as lookup_document, bind


def document():
    return {
        "format": "ydb-rbo-semantic-snapshot", "version": 1,
        "schema": {"tables": [{"name": "T", "columns": [
            {"name": "pk", "type": "Int32", "nullable": False},
            {"name": "u", "type": "Decimal(10,0)", "nullable": True},
            {"name": "v", "type": "Int32", "nullable": True},
        ], "primary_key": ["pk"], "unique_keys": [
            {"columns": ["pk"], "nulls_distinct": False},
            {"columns": ["u", "v"], "nulls_distinct": True},
        ]}]},
        "plan": {"nodes": [
            {"id": "s", "op": "scan", "table": "T", "columns": [{"source": "pk", "output": "pk"}]},
        ], "root": "s", "output": ["pk"]},
        "stage_graph": None,
    }


def ground(term, bindings):
    if term.operation == "symbol":
        return bindings[term.atom]
    if term.operation in {"bool", "int"}:
        return term.atom
    args = [ground(item, bindings) for item in term.arguments]
    if term.operation == "and":
        return all(args)
    if term.operation == "or":
        return any(args)
    if term.operation == "not":
        return not args[0]
    if term.operation == "=":
        return args[0] == args[1]
    if term.operation == "<":
        return args[0] < args[1]
    if term.operation == "ite":
        return args[1] if args[0] else args[2]
    raise AssertionError("Unexpected term in independent ground check: " + term.operation)


class UniqueConstraintTest(unittest.TestCase):
    def test_original_composite_prefix_null_and_decimal_storage_identity(self):
        snap = parse_snapshot(document())
        script = smt.Script()
        db = Database(snap, 2, script)
        domain = (None, -decimal.INF, -1, 0, decimal.INF, decimal.NAN)
        # 6*6*3*3 = 324 concrete database pairs; compare constraints with an
        # independent ordinary Python tuple law, never SQL NaN comparison.
        for left_u, right_u, left_v, right_v in product(domain, domain, (None, 0, 1), (None, 0, 1)):
            rows = [{"pk": 1, "u": left_u, "v": left_v}, {"pk": 2, "u": right_u, "v": right_v}]
            bindings = bind(db, {"T": rows})
            actual = all(ground(term, bindings) for term in script.assertions)
            expected = (None in (left_u, right_u, left_v, right_v)
                        or (left_u, left_v) != (right_u, right_v))
            self.assertEqual(actual, expected, rows)

    def test_complete_physical_key_constraint_is_still_null_safe(self):
        data = document()
        data["schema"]["tables"][0]["columns"][0]["nullable"] = True
        snap = parse_snapshot(data)
        script = smt.Script()
        db = Database(snap, 2, script)
        rows = [{"pk": None, "u": None, "v": 0}, {"pk": None, "u": None, "v": 1}]
        self.assertFalse(all(ground(term, bind(db, {"T": rows})) for term in script.assertions))

    def test_absent_row_and_null_tuple_do_not_conflict(self):
        snap = parse_snapshot(document())
        script = smt.Script()
        db = Database(snap, 2, script)
        for rows in ([{"pk": 1, "u": decimal.NAN, "v": 0}, None],
                     [{"pk": 1, "u": None, "v": 0}, {"pk": 2, "u": None, "v": 0}]):
            bindings = bind(db, {"T": rows})
            self.assertTrue(all(ground(term, bindings) for term in script.assertions))

    def test_all_unselected_constraints_apply_even_when_hidden(self):
        data = document()
        data["schema"]["tables"][0]["unique_keys"].append({"columns": ["v"], "nulls_distinct": True})
        snap = parse_snapshot(data)
        self.assertEqual(snap.plan.output, ("pk",))
        script = smt.Script()
        db = Database(snap, 2, script)
        rows = [{"pk": 1, "u": 0, "v": 7}, {"pk": 2, "u": 1, "v": 7}]
        bindings = bind(db, {"T": rows})
        self.assertFalse(all(ground(term, bindings) for term in script.assertions))

    def test_wrong_null_policy_or_full_pk_suffix_changes_allowed_states(self):
        rows = [{"pk": 1, "u": None, "v": 0}, {"pk": 2, "u": None, "v": 0}]
        wrong_nulls = document()
        wrong_nulls["schema"]["tables"][0]["unique_keys"][1]["nulls_distinct"] = False
        script = smt.Script()
        db = Database(parse_snapshot(wrong_nulls), 2, script)
        self.assertFalse(all(ground(term, bind(db, {"T": rows})) for term in script.assertions))
        wrong_prefix = document()
        wrong_prefix["schema"]["tables"][0]["unique_keys"][1]["columns"].append("pk")
        script = smt.Script()
        db = Database(parse_snapshot(wrong_prefix), 2, script)
        rows = [{"pk": 1, "u": decimal.NAN, "v": 0}, {"pk": 2, "u": decimal.NAN, "v": 0}]
        self.assertTrue(all(ground(term, bind(db, {"T": rows})) for term in script.assertions))

    def test_primary_key_requires_matching_exact_constraint_and_fields(self):
        for key in ([], ["missing"], ["pk", "pk"], ["u"], None, "pk"):
            data = document()
            data["schema"]["tables"][0]["primary_key"] = key
            with self.assertRaises(SnapshotError):
                parse_snapshot(data)
        data = document()
        data["schema"]["tables"][0]["unique_keys"][0]["nulls_distinct"] = True
        with self.assertRaises(SnapshotError):
            parse_snapshot(data)

    def test_direct_api_cannot_bypass_physical_key_or_constraint_validation(self):
        snap = parse_snapshot(document())
        table = snap.tables[0]
        for replacement in (replace(table, primary_key=("u",)), replace(table, primary_key=()),
                            replace(table, primary_key=([],)),
                            replace(table, unique_keys=(UniqueKey((), True),)),
                            replace(table, unique_keys=(UniqueKey(("pk", "pk"), False),)),
                            replace(table, unique_keys=(UniqueKey(([],), False),)),
                            replace(table, unique_keys=(UniqueKey(("pk",), 1),)),
                            replace(table, unique_keys=(UniqueKey(["pk"], False),)),
                            replace(table, unique_keys=(object(),))):
            with self.assertRaises(SnapshotError):
                validate_snapshot(replace(snap, tables=(replacement,)))

    def test_lookup_uses_explicit_physical_key_with_additional_constraint(self):
        data = lookup_document(index=True, staged=True)
        data["schema"]["tables"][0]["unique_keys"].insert(0, {"columns": ["value"], "nulls_distinct": True})
        snap = parse_snapshot(data)
        self.assertEqual(snap.tables[0].primary_key, ("k",))
        script = smt.Script()
        db = Database(snap, 2, script)
        self.assertEqual(set(db.witness), {"T"})
        self.assertIs(db.relations["I"].rows[0].values["k"], db.relations["T"].rows[0].values["k"])
        wrong = copy.deepcopy(data)
        wrong["plan"]["nodes"][-1]["keys"][0]["source"] = "value"
        with self.assertRaises(SnapshotError):
            parse_snapshot(wrong)
        legacy = copy.deepcopy(data)
        del legacy["schema"]["tables"][0]["primary_key"]
        with self.assertRaises(SnapshotError):
            parse_snapshot(legacy)

    def test_legacy_scan_keeps_supplied_constraint_semantics_but_no_inferred_pk(self):
        data = document()
        del data["schema"]["tables"][0]["primary_key"]
        snap = parse_snapshot(data)
        self.assertIsNone(snap.tables[0].primary_key)
        with self.assertRaisesRegex(ReplayError, "explicit captured physical primary key"):
            primary_key(snap.tables[0])

    def test_new_secondary_constraints_replay_is_explicitly_unsupported(self):
        table = parse_snapshot(document()).tables[0]
        with self.assertRaisesRegex(ReplayError, "UNSUPPORTED_UNIQUE_CONSTRAINT_REPLAY"):
            primary_key(table)
        self.assertEqual(primary_key(replace(table, unique_keys=(UniqueKey(("pk",), False),))), ("pk",))

    def test_bundle_and_pair_check_physical_identity_as_well_as_constraints(self):
        snap = parse_snapshot(document())
        legacy = replace(snap, tables=(replace(snap.tables[0], primary_key=None),))
        with self.assertRaises(VerificationError):
            _check_catalogs(snap, legacy)
        with self.assertRaises(VerificationError):
            _union_catalog(((snap, snap), (legacy, legacy)))


if __name__ == '__main__':
    unittest.main()
