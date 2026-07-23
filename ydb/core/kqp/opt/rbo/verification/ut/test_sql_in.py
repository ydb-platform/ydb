import copy
import itertools
import unittest

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    Expr,
    MAX_STATIC_IN_ITEMS,
    SnapshotError,
    parse_snapshot,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import Encoder, Value


def _literal(scalar_type, value):
    return {"kind": "literal", "type": scalar_type, "value": value}


def _in(lookup, items):
    return {"kind": "in", "lookup": lookup, "items": items}


def _snapshot(expression, scalar_type="Int64", nullable=False):
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": {
            "tables": [{
                "name": "A",
                "columns": [{"name": "x", "type": scalar_type, "nullable": nullable}],
                "unique_keys": [],
            }]
        },
        "plan": {
            "nodes": [
                {
                    "id": "scan",
                    "op": "scan",
                    "table": "A",
                    "columns": [{"source": "x", "output": "x"}],
                    "predicate": None,
                    "pushed_limit": None,
                },
                {
                    "id": "project",
                    "op": "project",
                    "input": "scan",
                    "ordered": False,
                    "columns": [{"output": "result", "expression": expression}],
                },
            ],
            "root": "project",
            "output": ["result"],
            "subplans": [],
        },
        "stage_graph": None,
    }


def _expression(value):
    snapshot = parse_snapshot(value)
    return snapshot, snapshot.plan.nodes[1].columns[0].expression


def _ground(term, constants=None):
    constants = constants or {}
    if term.operation in {"bool", "int"}:
        return term.atom
    if term.operation == "symbol":
        return constants[term.atom]
    arguments = tuple(_ground(argument, constants) for argument in term.arguments)
    if term.operation == "not":
        return not arguments[0]
    if term.operation == "and":
        return all(arguments)
    if term.operation == "or":
        return any(arguments)
    if term.operation == "=":
        return arguments[0] == arguments[1]
    raise AssertionError(f"non-ground operation {term.operation!r}")


def _sealed_literal_constants(script):
    script.seal_string_order()
    result = {}
    for assertion in script.assertions:
        if (
            assertion.operation == "="
            and assertion.arguments[0].operation == "symbol"
            and assertion.arguments[1].operation == "int"
        ):
            result[assertion.arguments[0].atom] = assertion.arguments[1].atom
    return result


def _reference(lookup, items):
    return None if lookup is None else any(lookup == item for item in items)


def _encoded_atom(script, scalar_type, value):
    if scalar_type == "String":
        return script.string_atom(value)
    return smt.int_value(value)


class StaticSqlInTest(unittest.TestCase):
    def test_parser_preserves_lookup_then_items_and_exact_nullability(self):
        expression = _in(
            {"kind": "column", "column": "x"},
            [_literal("Int64", 1), _literal("Int64", 1)],
        )
        for nullable in (False, True):
            snapshot, parsed = _expression(_snapshot(expression, nullable=nullable))
            with self.subTest(nullable=nullable):
                self.assertEqual(parsed.kind, "in")
                self.assertEqual(parsed.args[0].column, "x")
                self.assertEqual([item.value for item in parsed.args[1:]], [1, 1])
                self.assertEqual(snapshot.output_schema()[0].value_type.name, "Bool")
                self.assertEqual(snapshot.output_schema()[0].nullable, nullable)

        string_snapshot, _ = _expression(_snapshot(
            _in(
                {"kind": "column", "column": "x"},
                [_literal("String", "a"), _literal("String", "b")],
            ),
            scalar_type="String",
        ))
        self.assertEqual(string_snapshot.output_schema()[0].type, "Bool")

    def test_small_int_and_string_domains_match_an_independent_3vl_reference(self):
        domains = (
            ("Int64", (-1, 0, 1), (None, -1, 0, 1, 2)),
            ("String", ("a", "b"), (None, "a", "b", "c")),
        )
        for scalar_type, item_domain, lookup_domain in domains:
            for length in range(1, 4):
                for items in itertools.product(item_domain, repeat=length):
                    expression = Expr(
                        kind="in",
                        args=(
                            Expr(kind="column", column="x"),
                            *(
                                Expr(
                                    kind="literal",
                                    value=item,
                                    result_type=scalar_type,
                                    nullable=False,
                                )
                                for item in items
                            ),
                        ),
                    )
                    for lookup in lookup_domain:
                        script = smt.Script()
                        encoder = Encoder(script)
                        row_value = (
                            encoder.null(scalar_type)
                            if lookup is None
                            else Value(
                                scalar_type,
                                smt.FALSE,
                                _encoded_atom(script, scalar_type, lookup),
                            )
                        )
                        actual = encoder.evaluate(expression, {"x": row_value})
                        expected = _reference(lookup, items)
                        constants = _sealed_literal_constants(script)
                        with self.subTest(
                            scalar_type=scalar_type,
                            lookup=lookup,
                            items=items,
                        ):
                            self.assertEqual(
                                _ground(actual.is_null, constants),
                                expected is None,
                            )
                            if expected is not None:
                                self.assertEqual(
                                    _ground(actual.value, constants),
                                    expected,
                                )

    def test_lossless_mixed_integer_items_match_the_equality_reference(self):
        cases = (
            ("Int64", "Int32", (-2, 0, 2), (None, -2, -1, 0, 2)),
            ("Int32", "Int64", (-2, 0, 2), (None, -2, -1, 0, 2)),
            (
                "Uint64",
                "Uint32",
                (0, 2, 4_294_967_295),
                (None, 0, 1, 2, 4_294_967_295),
            ),
            (
                "Int64",
                "Uint32",
                (0, 2, 4_294_967_295),
                (None, -1, 0, 2, 4_294_967_295),
            ),
        )
        for lookup_type, item_type, items, lookup_values in cases:
            snapshot, expression = _expression(_snapshot(
                _in(
                    {"kind": "column", "column": "x"},
                    [_literal(item_type, item) for item in items],
                ),
                scalar_type=lookup_type,
                nullable=True,
            ))
            self.assertTrue(snapshot.output_schema()[0].nullable)
            for lookup in lookup_values:
                encoder = Encoder(smt.Script())
                row_value = (
                    encoder.null(lookup_type)
                    if lookup is None
                    else Value(lookup_type, smt.FALSE, smt.int_value(lookup))
                )
                actual = encoder.evaluate(expression, {"x": row_value})
                expected = _reference(lookup, items)
                with self.subTest(
                    lookup_type=lookup_type,
                    item_type=item_type,
                    lookup=lookup,
                ):
                    self.assertEqual(_ground(actual.is_null), expected is None)
                    if expected is not None:
                        self.assertEqual(_ground(actual.value), expected)

    def test_argument_position_and_literal_mutations_are_observable(self):
        column = {"kind": "column", "column": "x"}
        one = _literal("Int64", 1)
        two = _literal("Int64", 2)
        three = _literal("Int64", 3)
        raw_expressions = (
            _in(column, [one, two]),
            _in(one, [column, two]),
            _in(column, [one, three]),
        )
        outcomes = []
        for raw_expression in raw_expressions:
            _, expression = _expression(_snapshot(raw_expression))
            result = Encoder(smt.Script()).evaluate(
                expression,
                {"x": Value("Int64", smt.FALSE, smt.int_value(2))},
            )
            outcomes.append((_ground(result.is_null), _ground(result.value)))
        self.assertEqual(outcomes, [(False, True), (False, False), (False, False)])

    def test_schema_and_bounds_fail_closed(self):
        base = _snapshot(_in(
            {"kind": "column", "column": "x"},
            [_literal("Int64", 1)],
        ))
        accepted = copy.deepcopy(base)
        accepted["plan"]["nodes"][1]["columns"][0]["expression"]["items"] = [
            _literal("Int64", item) for item in range(MAX_STATIC_IN_ITEMS)
        ]
        parse_snapshot(accepted)

        mutations = []
        empty = copy.deepcopy(base)
        empty["plan"]["nodes"][1]["columns"][0]["expression"]["items"] = []
        mutations.append((empty, "between 1 and 512"))

        oversized = copy.deepcopy(base)
        oversized["plan"]["nodes"][1]["columns"][0]["expression"]["items"] = [
            _literal("Int64", item) for item in range(MAX_STATIC_IN_ITEMS + 1)
        ]
        mutations.append((oversized, "between 1 and 512"))

        null_item = copy.deepcopy(base)
        null_item["plan"]["nodes"][1]["columns"][0]["expression"]["items"] = [
            {"kind": "null", "type": "Int64"}
        ]
        mutations.append((null_item, "IN items must be non-nullable"))

        mismatched = copy.deepcopy(base)
        mismatched["plan"]["nodes"][1]["columns"][0]["expression"]["items"] = [
            _literal("String", "1")
        ]
        mutations.append((mismatched, "not equality-compatible"))

        heterogeneous = copy.deepcopy(base)
        heterogeneous["plan"]["nodes"][1]["columns"][0]["expression"]["items"] = [
            _literal("Int32", 1),
            _literal("Uint32", 2),
        ]
        mutations.append((heterogeneous, "IN items must have one type"))

        lossy_integer = copy.deepcopy(base)
        lossy_integer["plan"]["nodes"][1]["columns"][0]["expression"]["items"] = [
            _literal("Uint64", 1)
        ]
        mutations.append((lossy_integer, "not equality-compatible"))

        cross_string = _snapshot(
            _in(
                {"kind": "column", "column": "x"},
                [_literal("Utf8", "same bytes")],
            ),
            scalar_type="String",
        )
        mutations.append((cross_string, "not equality-compatible"))

        unknown = copy.deepcopy(base)
        unknown["plan"]["nodes"][1]["columns"][0]["expression"]["null_safe"] = False
        mutations.append((unknown, "unknown fields: null_safe"))

        non_array = copy.deepcopy(base)
        non_array["plan"]["nodes"][1]["columns"][0]["expression"]["items"] = {}
        mutations.append((non_array, "expected an array"))

        for value, message in mutations:
            with self.subTest(message=message):
                with self.assertRaisesRegex(SnapshotError, message):
                    parse_snapshot(value)


if __name__ == "__main__":
    unittest.main()
