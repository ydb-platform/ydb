"""Repeated DISTINCT aliases retain explicit bindings and SQL bag multiplicity.

These are parser and grounded reference-model tests, without a solver. Native
exporter/solver controls live in native_cpp_ut/native_snapshot_ut.cpp.
"""

import unittest
from collections import Counter
from itertools import product

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import SnapshotError, parse_snapshot
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import (
    Database, Evaluator, _aggregate_partition_key, _aggregate_unique_key,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import Encoder


def document():
    return {
        "format": "ydb-rbo-semantic-snapshot", "version": 1,
        "schema": {"tables": [{"name": "T", "columns": [
            {"name": name, "type": "Int32", "nullable": True} for name in ("k", "x")
        ], "unique_keys": []}]},
        "plan": {"root": "distinct", "output": ["first", "second", "third"], "nodes": [
            {"id": "scan", "op": "scan", "table": "T", "columns": [
                {"source": name, "output": name} for name in ("k", "x")
            ]},
            {
                "id": "distinct", "op": "aggregate", "input": "scan", "keys": ["k", "x"],
                "phase": "undefined", "distinct_all": True, "aggregates": [
                    {"input": source, "output": output, "function": "distinct", "type": "Int32",
                     "nullable": True, "distinct": False, "unwrap": False}
                    for source, output in (("x", "third"), ("x", "first"), ("k", "second"))
                ],
            },
        ]}, "stage_graph": None,
    }


def ground(term, bindings):
    if term.operation == "symbol":
        return bindings[term.atom]
    if term.operation in {"bool", "int"}:
        return term.atom
    args = [ground(arg, bindings) for arg in term.arguments]
    if term.operation == "and":
        return all(args)
    if term.operation == "or":
        return any(args)
    if term.operation == "not":
        return not args[0]
    if term.operation == "=":
        return args[0] == args[1]
    if term.operation == "ite":
        return args[1] if args[0] else args[2]
    raise AssertionError(f"unreviewed concrete DISTINCT term {term.operation}")


def model(value):
    snapshot = parse_snapshot(value)
    script = smt.Script()
    database = Database(snapshot, 2, script)
    return database, Evaluator(snapshot, database, Encoder(script)).root()


def bag(model_value, concrete_rows):
    database, family = model_value
    bindings = {}
    for row, concrete in zip(database.witness["T"], concrete_rows):
        bindings[row.present.atom] = concrete is not None
        for index, name in enumerate(("k", "x")):
            cell = row.cells[name]
            value = None if concrete is None else concrete[index]
            bindings[cell.is_null.atom] = value is None
            bindings[cell.value.atom] = 0 if value is None else value
    outcomes = [item for item in family.outcomes if ground(item.enabled, bindings)]
    assert len(outcomes) == 1 and not ground(outcomes[0].error, bindings)
    return Counter(
        tuple(None if ground(row.values[column.name].is_null, bindings)
              else ground(row.values[column.name].value, bindings)
              for column in outcomes[0].relation.columns)
        for row in outcomes[0].relation.rows if ground(row.present, bindings)
    )


class RepeatedDistinctAliasesTest(unittest.TestCase):
    def test_alias_order_and_group_key_order_are_independent(self):
        for phase, reverse_keys, reverse_traits in product(
            ("undefined", "intermediate", "final"), (False, True), (False, True)
        ):
            value = document()
            aggregate = value["plan"]["nodes"][-1]
            aggregate["phase"] = phase
            if reverse_keys:
                aggregate["keys"].reverse()
            if reverse_traits:
                aggregate["aggregates"].reverse()
            parsed = parse_snapshot(value)
            self.assertEqual([column.name for column in parsed.output_schema()], ["first", "second", "third"])
            self.assertEqual(len(parsed.plan.nodes[-1].aggregates), 3)

    def test_rejects_missing_foreign_or_duplicate_grouping_keys(self):
        for mutation in ("missing_alias", "foreign_alias", "duplicate_key", "empty_keys", "empty_traits"):
            value = document()
            node = value["plan"]["nodes"][-1]
            if mutation == "missing_alias":
                node["aggregates"][-1]["input"] = "x"
            elif mutation == "foreign_alias":
                node["keys"] = ["x"]
            elif mutation == "duplicate_key":
                node["keys"].append("x")
            elif mutation == "empty_keys":
                node["keys"] = []
            else:
                node["aggregates"] = []
            with self.subTest(mutation=mutation), self.assertRaises(SnapshotError):
                parse_snapshot(value)

    def test_rejects_alias_schema_and_trait_mutations(self):
        for field, changed in (("type", "Uint32"), ("nullable", False), ("distinct", True),
                               ("unwrap", True), ("function", "min"), ("output", "first")):
            value = document()
            value["plan"]["nodes"][-1]["aggregates"][0][field] = changed
            with self.subTest(field=field), self.assertRaises(SnapshotError):
                parse_snapshot(value)

    def test_ordinary_aggregate_still_rejects_duplicate_grouping_keys(self):
        value = document()
        value["plan"]["nodes"][-1].update(distinct_all=False, aggregates=[], keys=["k", "x", "x"])
        value["plan"]["output"] = ["k", "x"]
        with self.assertRaises(SnapshotError):
            parse_snapshot(value)

    def test_grounded_nullable_bags_match_independent_tuple_set(self):
        states = (None,) + tuple(product((None, 0, 1), repeat=2))
        for reverse_keys, reverse_traits in product((False, True), repeat=2):
            value = document()
            if reverse_keys:
                value["plan"]["nodes"][-1]["keys"].reverse()
            if reverse_traits:
                value["plan"]["nodes"][-1]["aggregates"].reverse()
            evaluated = model(value)
            for rows in product(states, repeat=2):
                expected = Counter((row[1], row[0], row[1]) for row in set(rows) if row is not None)
                with self.subTest(keys=reverse_keys, traits=reverse_traits, rows=rows):
                    self.assertEqual(bag(evaluated, rows), expected)

    def test_changed_alias_source_has_a_concrete_witness(self):
        changed = document()
        changed["plan"]["nodes"][-1]["aggregates"][0]["input"] = "k"
        rows = ((1, 0), None)
        self.assertEqual(bag(model(document()), rows), Counter({(0, 1, 0): 1}))
        self.assertEqual(bag(model(changed), rows), Counter({(0, 1, 1): 1}))

    def test_removing_distinct_preserves_duplicate_witness_not_proof(self):
        changed = document()
        changed["plan"]["nodes"][-1] = {
            "id": "distinct", "op": "project", "input": "scan", "ordered": False, "columns": [
                {"output": output, "expression": {"kind": "column", "column": source}}
                for source, output in (("x", "first"), ("k", "second"), ("x", "third"))
            ],
        }
        rows = ((1, None), (1, None))
        self.assertEqual(bag(model(document()), rows), Counter({(None, 1, None): 1}))
        self.assertEqual(bag(model(changed), rows), Counter({(None, 1, None): 2}))

    def test_partition_and_unique_certificates_keep_source_bindings(self):
        for reverse_traits in (False, True):
            value = document()
            if reverse_traits:
                value["plan"]["nodes"][-1]["aggregates"].reverse()
            node = parse_snapshot(value).plan.nodes[-1]
            sources = {trait.output: trait.input for trait in node.aggregates}
            self.assertEqual(_aggregate_unique_key(node), frozenset(("first", "second", "third")))
            for key in (frozenset(("k",)), frozenset(("x",)), frozenset(("k", "x"))):
                projected = _aggregate_partition_key(node, key)
                self.assertEqual({sources[output] for output in projected}, set(key))
                self.assertEqual(len(projected), len(key))
            self.assertIsNone(_aggregate_partition_key(node, frozenset(("missing",))))
            self.assertIsNone(_aggregate_partition_key(node, None))
