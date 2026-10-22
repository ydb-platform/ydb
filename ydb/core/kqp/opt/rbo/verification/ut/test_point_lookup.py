"""Complete-key lookup and synchronous-index invariants; no solver needed here.

Native end-to-end solver controls live in native_cpp_ut. These tests ground the
production relation model against independent bag lookup and task ownership.
"""
import copy
import unittest
from collections import Counter
from itertools import product

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import SnapshotError, parse_snapshot, stage_task_counts
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import Database, Evaluator
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import Encoder
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.stages import Evaluator as StageEvaluator, Router
from ydb.core.kqp.opt.rbo.verification.inspector.plan import render_snapshot


def column(name, type="Int32", nullable=False):
    return {"name": name, "type": type, "nullable": nullable}


def table(name, columns, keys):
    return {"name": name, "columns": columns,
            "unique_keys": [{"columns": keys, "nulls_distinct": False}] if keys else [],
            **({"primary_key": list(keys)} if keys else {})}


def scan(id, table, names):
    return {"id": id, "op": "scan", "table": table,
            "columns": [{"source": name, "output": name} for name in names]}


def stage(id, nodes, inputs, storage=None):
    return {"id": id, "nodes": nodes, "inputs": inputs, "source_storage": storage,
            "outputs": [{"index": 0, "node": nodes[-1]}]}


def document(index=False, staged=False, nullable_key=False, composite=False):
    keys = ["k", "j"] if composite else ["k"]
    base_columns = [column(name, nullable=nullable_key) for name in keys]
    base_columns += [column("flag", "Bool", True), column("value", nullable=True)]
    target = table("T", base_columns, keys)
    probe_columns = [column(name, nullable=nullable_key) for name in keys]
    if index:
        probe = table("I", copy.deepcopy(probe_columns) + [column("flag", "Bool", True)], ["flag", *keys])
        probe["derived_from"] = "T"
        source = scan("probe", "I", keys + ["flag"])
        source["predicate"] = {
            "kind": "eq",
            "left": {"kind": "column", "column": "flag"},
            "right": {"kind": "literal", "type": "Bool", "value": True},
        }
    else:
        probe = table("P", probe_columns, [])
        source = scan("probe", "P", keys)
    lookup = {"id": "lookup", "op": "point_lookup", "input": "probe", "table": "T",
              "keys": [{"input": name, "source": name} for name in keys],
              "columns": [{"source": "value", "output": "value"}]}
    return {"format": "ydb-rbo-semantic-snapshot", "version": 1,
            "schema": {"tables": [target, probe]},
            "plan": {"nodes": [source, lookup], "root": "lookup", "output": ["value"]},
            "stage_graph": ({"root_stage": "s1", "stages": [
                stage("s0", ["probe"], [], "row"), stage("s1", ["lookup"], ["probe"])],
                "edges": [{"id": "edge", "kind": "stream_lookup", "producer": "s0", "consumer": "s1",
                           "producer_output": 0, "consumer_input": 0, "occurrence": 0}], "assumptions": []}
                if staged else None)}


def ground(term, bindings):
    if term.operation == "symbol":
        return bindings[term.atom]
    if term.operation in {"bool", "int"}:
        return term.atom
    values = [ground(arg, bindings) for arg in term.arguments]
    if term.operation == "and":
        return all(values)
    if term.operation == "or":
        return any(values)
    if term.operation == "not":
        return not values[0]
    if term.operation == "=":
        return values[0] == values[1]
    if term.operation == "ite":
        return values[1] if values[0] else values[2]
    raise AssertionError(f"unreviewed term in concrete lookup test: {term.operation}")


def bind(database, values):
    bindings = {}
    for name, witnesses in database.witness.items():
        for symbolic, concrete in zip(witnesses, values[name]):
            bindings[symbolic.present.atom] = concrete is not None
            for column, cell in symbolic.cells.items():
                value = None if concrete is None else concrete[column]
                if cell.is_null.operation == "symbol":
                    bindings[cell.is_null.atom] = value is None
                bindings[cell.value.atom] = (False if cell.type == "Bool" else 0) if value is None else value
    return bindings


def bag(family, bindings):
    outcomes = [item for item in family.outcomes if ground(item.enabled, bindings)]
    assert len(outcomes) == 1 and not ground(outcomes[0].error, bindings)
    result = Counter()
    for row in outcomes[0].relation.rows:
        if ground(row.present, bindings):
            for fact in row.partition_facts:
                assert ground(fact.term, bindings) == fact.value
            result[tuple(None if ground(row.values[col.name].is_null, bindings)
                         else ground(row.values[col.name].value, bindings)
                         for col in outcomes[0].relation.columns)] += 1
    return result


def concrete_lookup(probes, targets, keys):
    return Counter((target["value"],) for probe in probes if probe is not None
                   for target in targets if target is not None
                   if all(probe[name] == target[name] for name in keys))


class PointLookupAdmissionTest(unittest.TestCase):
    def test_valid_derived_catalog_and_stream_mapping(self):
        parsed = parse_snapshot(document(index=True, staged=True))
        self.assertEqual(stage_task_counts(parsed), {"s0": 2, "s1": 2})
        rendered = render_snapshot(parsed)
        self.assertIn('derived_from="T"', rendered)
        self.assertIn('kind=stream_lookup', rendered)

    def test_rejects_inconsistent_index_catalog(self):
        mutations = [
            lambda t: t.update(derived_from="missing"),
            lambda t: t.update(derived_from="I"),
            lambda t: t["columns"][0].update(type="Int64"),
            lambda t: t["columns"][0].update(nullable=True),
            lambda t: t["columns"].append(column("absent")),
            lambda t: t["unique_keys"][0].update(columns=["flag"]),
            lambda t: t["unique_keys"][0].update(nulls_distinct=True),
        ]
        for mutate in mutations:
            changed = document(index=True)
            mutate(changed["schema"]["tables"][1])
            with self.subTest(mutation=mutations.index(mutate)), self.assertRaises(SnapshotError):
                parse_snapshot(changed)

    def test_rejects_derived_chains(self):
        changed = document(index=True)
        changed["schema"]["tables"][0]["derived_from"] = "I"
        with self.assertRaises(SnapshotError):
            parse_snapshot(changed)

    def test_rejects_incomplete_wrong_or_reordered_physical_keys(self):
        mutations = [[], [{"input": "k", "source": "k"}],
                     [{"input": "j", "source": "j"}, {"input": "k", "source": "k"}],
                     [{"input": "unknown", "source": "k"}, {"input": "j", "source": "j"}],
                     [{"input": "k", "source": "k"}, {"input": "k", "source": "j"}]]
        for keys in mutations:
            changed = document(composite=True)
            changed["plan"]["nodes"][-1]["keys"] = keys
            with self.subTest(keys=keys), self.assertRaises(SnapshotError):
                parse_snapshot(changed)

    def test_rejects_missing_fetched_columns_wrong_types_and_modes(self):
        for mode in ("column", "type", "left", "prefix", "nulls_distinct"):
            changed = document()
            lookup = changed["plan"]["nodes"][-1]
            if mode == "column":
                lookup["columns"][0]["source"] = "absent"
            elif mode == "type":
                changed["schema"]["tables"][1]["columns"][0]["type"] = "Int64"
            elif mode == "left":
                lookup["join_kind"] = "left"
            elif mode == "prefix":
                lookup["prefix"] = []
            else:
                changed["schema"]["tables"][0]["unique_keys"][0]["nulls_distinct"] = True
            with self.subTest(mode=mode), self.assertRaises(SnapshotError):
                parse_snapshot(changed)

    def test_stream_connection_cannot_be_replaced_with_map_or_broadcast(self):
        for kind in ("map", "broadcast"):
            changed = document(staged=True)
            changed["stage_graph"]["edges"][0]["kind"] = kind
            with self.subTest(kind=kind), self.assertRaises(SnapshotError):
                parse_snapshot(changed)

    def test_stream_connection_cannot_feed_an_unrelated_operator(self):
        changed = document(staged=True)
        changed["plan"]["nodes"][-1] = {
            "id": "lookup", "op": "project", "input": "probe",
            "ordered": True, "columns": [{"output": "value", "expression": {"kind": "column", "column": "k"}}],
        }
        with self.assertRaises(SnapshotError):
            parse_snapshot(changed)

    def test_nullable_or_unrelated_index_prefix_boundaries_rejected(self):
        for literal in ({"kind": "null", "type": "Bool"},
                        {"kind": "parameter", "name": "$p", "type": "Bool", "nullable": True},
                        {"kind": "literal", "type": "Int32", "value": 1}):
            changed = document(index=True, staged=True)
            changed["plan"]["nodes"][0]["predicate"]["right"] = literal
            with self.subTest(literal=literal), self.assertRaises(SnapshotError):
                parse_snapshot(changed)
        changed = document(index=True, staged=True)
        changed["plan"]["nodes"][0]["predicate"]["null_safe"] = True
        with self.assertRaises(SnapshotError):
            parse_snapshot(changed)


class PointLookupConcreteTest(unittest.TestCase):
    def test_index_uses_base_symbols_and_has_no_independent_witness(self):
        snapshot = parse_snapshot(document(index=True))
        database = Database(snapshot, 2, smt.Script())
        self.assertEqual(set(database.witness), {"T"})
        for base, index in zip(database.relations["T"].rows, database.relations["I"].rows):
            self.assertIs(base.present, index.present)
            for name in ("k", "flag"):
                self.assertIs(base.values[name], index.values[name])

    def test_duplicates_missing_null_keys_and_null_payload_match_reference(self):
        snapshot = parse_snapshot(document(nullable_key=True))
        script = smt.Script()
        database = Database(snapshot, 2, script)
        actual = Evaluator(snapshot, database, Encoder(script)).root()
        targets = [{"k": None, "flag": None, "value": 11}, {"k": 1, "flag": True, "value": None}]
        for first, second in product((None, 1, 2, "absent"), repeat=2):
            probes = [None if key == "absent" else {"k": key} for key in (first, second)]
            with self.subTest(probes=probes):
                self.assertEqual(bag(actual, bind(database, {"T": targets, "P": probes})),
                                 concrete_lookup(probes, targets, ("k",)))
        # The duplicate-probe control would fail if LookupRows deduplicated keys.
        probes = [{"k": 1}, {"k": 1}]
        self.assertEqual(bag(actual, bind(database, {"T": targets, "P": probes})), Counter({(None,): 2}))

    def test_composite_keys_match_all_components_and_drop_missing_rows(self):
        snapshot = parse_snapshot(document(composite=True, nullable_key=True))
        script = smt.Script()
        database = Database(snapshot, 2, script)
        actual = Evaluator(snapshot, database, Encoder(script)).root()
        targets = [{"k": 1, "j": None, "flag": False, "value": 10},
                   {"k": 1, "j": 2, "flag": True, "value": 20}]
        for second in (None, 2, 3):
            probes = [{"k": 1, "j": second}, {"k": 1, "j": second}]
            self.assertEqual(bag(actual, bind(database, {"T": targets, "P": probes})),
                             concrete_lookup(probes, targets, ("k", "j")))

    def test_bool_index_prefix_preserves_null_filter_and_duplicate_index_values(self):
        snapshot = parse_snapshot(document(index=True, staged=True))
        for flags in product((None, False, True), repeat=2):
            script = smt.Script()
            database = Database(snapshot, 2, script)

            class FixedRouter(Router):
                def source_task(self, table, slot):
                    assert table == "I"
                    return smt.TRUE if slot else smt.FALSE
            actual = StageEvaluator(snapshot, database, Encoder(script), FixedRouter(script)).root()
            targets = [{"k": i, "flag": flag, "value": 7} for i, flag in enumerate(flags)]
            self.assertEqual(bag(actual, bind(database, {"T": targets})),
                             Counter({(7,): sum(flag is True for flag in flags)}) if True in flags else Counter())

    def test_fetched_rows_return_to_probe_task_even_when_target_is_elsewhere(self):
        snapshot = parse_snapshot(document(staged=True))
        script = smt.Script()
        database = Database(snapshot, 2, script)

        class FixedRouter(Router):
            def source_task(self, table, slot):
                # A target-table source partition must never be consulted.
                assert table == "P"
                return smt.TRUE if slot else smt.FALSE
        evaluator = StageEvaluator(snapshot, database, Encoder(script), FixedRouter(script))
        evaluator.root()
        targets = [{"k": 1, "flag": None, "value": 10}, {"k": 2, "flag": None, "value": 20}]
        probes = [{"k": 2}, {"k": 1}]
        bindings = bind(database, {"T": targets, "P": probes})
        partitions = evaluator.outputs[("s1", 0)].relations
        self.assertEqual(bag(partitions[0], bindings), Counter({(20,): 1}))
        self.assertEqual(bag(partitions[1], bindings), Counter({(10,): 1}))
