"""Concrete ground controls for the composed List/index/lookup model.

No solver, proof verdict, SQL, or native backend execution. The membership UF
is assigned from a finite concrete Python list; model bags are checked against
independent source-row filtering and physical probe task ownership.
"""
from collections import Counter
import unittest

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import parse_snapshot, query_parameters
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import Database
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import Encoder
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.stages import Evaluator, Router
if __package__:
    from .test_point_lookup import document, column, bind, stage
else:
    from test_point_lookup import document, column, bind, stage


def membership(name):
    return {"kind": "in_parameter", "name": name,
            "parameter_type": "List<Optional<String>>", "ansi": True,
            "lookup": {"kind": "column", "column": "text"}, "type": "Bool", "nullable": True}


def joint_document(post_name=None):
    result = document(index=True, staged=True, nullable_key=True)
    for table in result["schema"]["tables"]:
        table["columns"].append(column("text", "String", True))
    scan, lookup = result["plan"]["nodes"]
    scan["columns"].append({"source": "text", "output": "text"})
    filtered = {"id": "filtered", "op": "filter", "input": "probe", "predicate": membership("$p")}
    lookup["input"] = "filtered"
    lookup["columns"].append({"source": "text", "output": "text"})
    result["plan"]["nodes"] = [scan, filtered, lookup]
    producer, consumer = result["stage_graph"]["stages"]
    # Row-storage source stages admit only the scan/passive projections.
    # Keep the evaluated List filter in a separate computation stage.
    middle = stage("sf", ["filtered"], ["probe"])
    result["stage_graph"]["stages"] = [producer, middle, consumer]
    lookup_edge = result["stage_graph"]["edges"][0]
    lookup_edge["producer"] = "sf"
    result["stage_graph"]["edges"] = [
        {"id": "to_filter", "kind": "map", "producer": "s0", "consumer": "sf",
         "producer_output": 0, "consumer_input": 0, "occurrence": 0},
        lookup_edge,
    ]
    consumer["inputs"] = ["filtered"]
    if post_name is not None:
        post = {"id": "post", "op": "filter", "input": "lookup", "predicate": membership(post_name)}
        result["plan"]["nodes"].append(post)
        result["plan"]["root"] = "post"
        consumer["nodes"].append("post")
        consumer["outputs"][0]["node"] = "post"
    return result


def ground(term, symbols, functions):
    if term.operation == "symbol":
        return symbols[term.atom]
    if term.operation in {"int", "bool"}:
        return term.atom
    values = [ground(child, symbols, functions) for child in term.arguments]
    if term.operation in functions:
        return functions[term.operation](*values)
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
    if term.operation == "<":
        return values[0] < values[1]
    if term.operation == "<=":
        return values[0] <= values[1]
    if term.operation == ">":
        return values[0] > values[1]
    if term.operation == ">=":
        return values[0] >= values[1]
    if term.operation == "=>":
        return not values[0] or values[1]
    raise AssertionError('Unreviewed operation in concrete joint model: ' + term.operation)


def value_bag(family, symbols, functions):
    outcomes = [outcome for outcome in family.outcomes if ground(outcome.enabled, symbols, functions)]
    assert len(outcomes) == 1
    assert not ground(outcomes[0].error, symbols, functions)
    result = Counter()
    for row in outcomes[0].relation.rows:
        if not ground(row.present, symbols, functions):
            continue
        for fact in row.partition_facts:
            assert ground(fact.term, symbols, functions) == fact.value
        value = row.values["value"]
        result[(None if ground(value.is_null, symbols, functions) else ground(value.value, symbols, functions),)] += 1
    return result


class JointReferenceTest(unittest.TestCase):
    def model(self, post_name=None):
        snapshot = parse_snapshot(joint_document(post_name))
        script = smt.Script()
        literals = {value: script.string_atom(value) for value in ("red", "blue", "missing")}
        database = Database(snapshot, 2, script)
        scalar = Encoder(script)
        scalar.register_parameters(query_parameters(snapshot))

        class FixedRouter(Router):
            def source_task(self, table, slot):
                assert table == "I", "base-table source ownership must not gate a lookup"
                return smt.bool_value(bool(slot))
        evaluator = Evaluator(snapshot, database, scalar, FixedRouter(script))
        family = evaluator.root()
        script.seal_string_order()
        self.assertEqual(set(database.witness), {"T"})
        for base, index in zip(database.relations["T"].rows, database.relations["I"].rows):
            self.assertIs(base.present, index.present)
            self.assertIs(base.values["text"], index.values["text"])
        return script, literals, database, scalar, evaluator, family

    def bindings(self, model, rows, lists):
        script, literals, database, scalar, _, _ = model
        ranks = {value: rank for rank, value in script.string_literals.items()}
        encoded = [None if row is None else dict(row, text=None if row["text"] is None else ranks[row["text"]])
                   for row in rows]
        symbols = bind(database, {"T": encoded})
        symbols.update({term.atom: ranks[text] for text, term in literals.items()})
        functions = {}
        self.assertEqual(set(lists), set(scalar.list_parameters))
        for name, values in lists.items():
            parameter = scalar.list_parameters[name]
            selected = {ranks[value] for value in values if value is not None}
            symbols[parameter.has_non_null.atom] = bool(selected)
            symbols[parameter.has_null.atom] = None in values
            symbols[parameter.member.atom] = min(selected) if selected else ranks["red"]
            functions[parameter.membership.name] = lambda rank, selected=selected: rank in selected
        # This is a valid concrete model of all catalog/parameter/domain
        # constraints, not an assignment that ignores those constraints.
        for assertion in script.assertions:
            self.assertTrue(ground(assertion, symbols, functions))
        return symbols, functions

    def test_list_filter_derived_lookup_matches_bag_nulls_and_probe_ownership(self):
        model = self.model()
        datasets = [
            [{"k": None, "flag": True, "text": "red", "value": 7},
             {"k": 1, "flag": True, "text": "blue", "value": 7}],
            [{"k": None, "flag": True, "text": None, "value": 11},
             {"k": 1, "flag": True, "text": "blue", "value": None}],
            [{"k": None, "flag": None, "text": "red", "value": 7},
             {"k": 1, "flag": False, "text": "blue", "value": 7}],
            [None, {"k": 1, "flag": True, "text": "blue", "value": None}],
        ]
        lists = ([], [None], ["red"], ["blue"], ["red", "red"], ["red", "blue"],
                 ["red", None], ["missing"])
        for rows in datasets:
            for values in lists:
                with self.subTest(rows=rows, values=values):
                    symbols, functions = self.bindings(model, rows, {"$p": values})
                    selected = [row is not None and row["flag"] is True and row["text"] is not None
                                and row["text"] in values for row in rows]
                    expected = Counter((row["value"],) for row, keep in zip(rows, selected) if keep)
                    self.assertEqual(value_bag(model[-1], symbols, functions), expected)
                    partitions = model[-2].outputs[("s1", 0)].relations
                    for task in range(2):
                        expected_task = Counter({(rows[task]["value"],): 1}) if selected[task] else Counter()
                        self.assertEqual(value_bag(partitions[task], symbols, functions), expected_task)

    def test_pre_and_post_lookup_filters_share_same_list_identity_and_name_mutation_matters(self):
        rows = [{"k": None, "flag": True, "text": "red", "value": 7},
                {"k": 1, "flag": True, "text": "blue", "value": None}]
        for post_name in ("$p", "$q"):
            with self.subTest(post_name=post_name):
                model = self.model(post_name)
                parameters = {"$p": ["red", "red", None]}
                if post_name == "$q":
                    parameters["$q"] = ["blue"]
                symbols, functions = self.bindings(model, rows, parameters)
                self.assertEqual(len(model[3].list_parameters), 1 if post_name == "$p" else 2)
                expected = Counter({(7,): 1}) if post_name == "$p" else Counter()
                self.assertEqual(value_bag(model[-1], symbols, functions), expected)
