import copy
import unittest
from fractions import Fraction
from itertools import product

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import decimal, smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    Expr,
    SnapshotError,
    parse_snapshot,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import Database
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import Encoder, Value


_REF_RANK = {
    decimal.NEG_INF: 0,
    decimal.FINITE: 1,
    decimal.POS_INF: 2,
}


def _ground(term, symbols=None):
    symbols = symbols or {}
    if term.operation in {"bool", "int"}:
        return term.atom
    if term.operation == "symbol":
        return symbols[term.render()]
    values = tuple(_ground(argument, symbols) for argument in term.arguments)
    if term.operation == "not":
        return not values[0]
    if term.operation == "and":
        return all(values)
    if term.operation == "or":
        return any(values)
    if term.operation == "=":
        return values[0] == values[1]
    if term.operation == "<":
        return values[0] < values[1]
    if term.operation == "ite":
        return values[1] if values[0] else values[2]
    if term.operation == "+":
        return sum(values)
    if term.operation == "-":
        return values[0] - values[1]
    if term.operation == "*":
        result = 1
        for value in values:
            result *= value
        return result
    raise AssertionError(f"unsupported ground operation {term.operation!r}")


def _decimal_literal(scalar_type, code):
    if code == decimal.INF:
        value = decimal.Literal(decimal.POS_INF)
    elif code == -decimal.INF:
        value = decimal.Literal(decimal.NEG_INF)
    elif code == decimal.NAN:
        value = decimal.Literal(decimal.NAN_KIND)
    else:
        value = decimal.Literal(decimal.FINITE, code)
    return Expr(kind="literal", result_type=scalar_type, nullable=False, value=value)


def _literal(scalar_type, value):
    return Expr(kind="literal", result_type=scalar_type, nullable=False, value=value)


def _comparison(kind, left, right, null_safe=False):
    return Expr(kind=kind, args=(left, right), null_safe=null_safe)


def _snapshot(predicate):
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": {
            "tables": [
                {
                    "name": "A",
                    "columns": [
                        {"name": "d", "type": "Decimal(7,2)", "nullable": True},
                        {"name": "wide", "type": "Decimal(12,2)", "nullable": False},
                        {"name": "i", "type": "Int32", "nullable": False},
                    ],
                    "unique_keys": [],
                }
            ]
        },
        "plan": {
            "nodes": [
                {
                    "id": "scan",
                    "op": "scan",
                    "table": "A",
                    "columns": [
                        {"source": "d", "output": "a.d"},
                        {"source": "wide", "output": "a.wide"},
                        {"source": "i", "output": "a.i"},
                    ],
                    "predicate": None,
                    "pushed_limit": None,
                },
                {"id": "filter", "op": "filter", "input": "scan", "predicate": predicate},
            ],
            "root": "filter",
            "output": ["a.d"],
        },
        "stage_graph": None,
    }


def _ref_value(code, scalar_type):
    """Decode a small-domain code without reproducing YDB's scale alignment."""

    if code == -decimal.INF:
        return (decimal.NEG_INF, None)
    if code == decimal.INF:
        return (decimal.POS_INF, None)
    if code == decimal.NAN:
        return (decimal.NAN_KIND, None)
    return (decimal.FINITE, Fraction(code, 10**scalar_type.scale))


def _ref_compare(kind, left, right):
    if decimal.NAN_KIND in {left[0], right[0]}:
        return False
    if kind == "eq":
        return left == right

    if left[0] == right[0] == decimal.FINITE:
        order = (left[1] > right[1]) - (left[1] < right[1])
    else:
        order = (_REF_RANK[left[0]] > _REF_RANK[right[0]]) - (
            _REF_RANK[left[0]] < _REF_RANK[right[0]]
        )
    if kind == "lt":
        return order < 0
    if kind == "lte":
        return order <= 0
    if kind == "gt":
        return order > 0
    assert kind == "gte"
    return order >= 0


def _small_domain(scalar_type):
    bound = 10**scalar_type.precision
    codes = (*range(-bound + 1, bound), -decimal.INF, decimal.INF, decimal.NAN)
    return tuple((code, _ref_value(code, scalar_type)) for code in codes)


def _type_name(scalar_type):
    return f"Decimal({scalar_type.precision},{scalar_type.scale})"


class DecimalKernelTest(unittest.TestCase):
    def test_typed_domain_matches_ydb_import_boundaries_and_specials(self):
        accepted = (-decimal.INF, -999, -1, 0, 1, 999, decimal.INF, decimal.NAN)
        rejected = (-decimal.INF - 1, -1000, 1000, decimal.NAN + 1)
        for code, expected in (
            *((code, True) for code in accepted),
            *((code, False) for code in rejected),
        ):
            with self.subTest(code=code):
                self.assertEqual(
                    _ground(decimal.domain(smt.int_value(code), "Decimal(3,2)")),
                    expected,
                )

    def test_alignment_matches_exhaustive_fraction_reference(self):
        types = tuple(
            decimal.Type(precision, scale)
            for precision in range(1, 3)
            for scale in range(precision + 1)
        )
        domains = {scalar_type: _small_domain(scalar_type) for scalar_type in types}

        # Every finite code of every Decimal(p,s), p <= 2, plus all three
        # specials.  Ordered type pairs cover both alignment orientations.
        for left_type, right_type in product(types, repeat=2):
            aligned_type = decimal.Type(2, max(left_type.scale, right_type.scale))
            for left, expected in domains[left_type]:
                actual, _ = decimal.align(
                    smt.int_value(left),
                    _type_name(left_type),
                    smt.ZERO,
                    _type_name(right_type),
                )
                self.assertEqual(
                    _ref_value(_ground(actual), aligned_type),
                    expected,
                    (_type_name(left_type), _type_name(right_type), left),
                )
            for right, expected in domains[right_type]:
                _, actual = decimal.align(
                    smt.ZERO,
                    _type_name(left_type),
                    smt.int_value(right),
                    _type_name(right_type),
                )
                self.assertEqual(
                    _ref_value(_ground(actual), aligned_type),
                    expected,
                    (_type_name(left_type), _type_name(right_type), right),
                )

    def test_all_comparisons_match_exhaustive_value_reference(self):
        # Comparison itself is scale-independent after alignment.  Exhausting
        # the largest p<=2 code domain keeps this independent matrix compact.
        domain = _small_domain(decimal.Type(2, 0))
        kinds = ("eq", "lt", "lte", "gt", "gte")
        for (left, left_ref), (right, right_ref) in product(domain, repeat=2):
            for kind in kinds:
                actual = decimal.compare(kind, smt.int_value(left), smt.int_value(right))
                expected = _ref_compare(kind, left_ref, right_ref)
                if _ground(actual) != expected:
                    self.fail(
                        f"Decimal {kind} mismatch for {left} and {right}: "
                        f"actual={_ground(actual)}, expected={expected}"
                    )

    def test_decimal_integer_alignment_uses_integer_decimal_width(self):
        encoder = Encoder(smt.Script())
        cases = (
            ("eq", "Decimal(7,2)", 12_300, "Int8", 123, True),
            ("lt", "Decimal(7,2)", 12_299, "Int32", 123, True),
            ("gt", "Uint64", 124, "Decimal(7,2)", 12_300, True),
            ("eq", "Decimal(7,2)", decimal.NAN, "Int32", 0, False),
        )
        for kind, left_type, left, right_type, right, expected in cases:
            left_expr = (
                _decimal_literal(left_type, left)
                if decimal.is_type(left_type)
                else _literal(left_type, left)
            )
            right_expr = (
                _decimal_literal(right_type, right)
                if decimal.is_type(right_type)
                else _literal(right_type, right)
            )
            result = encoder.evaluate(_comparison(kind, left_expr, right_expr), {})
            with self.subTest(kind=kind, types=(left_type, right_type)):
                self.assertEqual(_ground(result.value), expected)

    def test_precision_cap_saturates_before_scale_up(self):
        cases = (
            ("Decimal(35,0)", -10, "Decimal(35,34)", -decimal.INF),
            ("Decimal(35,0)", -9, "Decimal(35,34)", -9 * 10**34),
            ("Decimal(35,0)", 9, "Decimal(35,34)", 9 * 10**34),
            ("Decimal(35,0)", 10, "Decimal(35,34)", decimal.INF),
            ("Int8", -100, "Decimal(35,33)", -decimal.INF),
            ("Int8", -99, "Decimal(35,33)", -99 * 10**33),
            ("Int8", 99, "Decimal(35,33)", 99 * 10**33),
            ("Int8", 100, "Decimal(35,33)", decimal.INF),
        )
        for left_type, left, right_type, expected in cases:
            actual, _ = decimal.align(
                smt.int_value(left),
                left_type,
                smt.ZERO,
                right_type,
            )
            with self.subTest(left_type=left_type, left=left, right_type=right_type):
                self.assertEqual(_ground(actual), expected)

    def test_nan_null_and_null_safe_equality_follow_ydb(self):
        nan = _decimal_literal("Decimal(7,2)", decimal.NAN)
        ordinary = Encoder(smt.Script()).evaluate(_comparison("eq", nan, nan), {})
        distinct = Encoder(smt.Script()).evaluate(
            _comparison("eq", nan, nan, null_safe=True),
            {},
        )
        nullable = Encoder(smt.Script()).evaluate(
            _comparison("lt", Expr(kind="column", column="d"), _decimal_literal("Decimal(7,2)", 0)),
            {"d": Value("Decimal(7,2)", smt.TRUE, smt.int_value(decimal.NAN))},
        )
        self.assertEqual(ordinary.value, smt.FALSE)
        self.assertEqual(distinct.value, smt.TRUE)
        self.assertEqual(nullable.is_null, smt.TRUE)
        self.assertEqual(Encoder.is_true(nullable), smt.FALSE)


class DecimalIrTest(unittest.TestCase):
    def test_tagged_literals_are_strict_and_json_safe(self):
        values = (
            ({"kind": "finite", "scaled": "-123"}, decimal.Literal(decimal.FINITE, -123)),
            ({"kind": "pos_inf"}, decimal.Literal(decimal.POS_INF)),
            ({"kind": "neg_inf"}, decimal.Literal(decimal.NEG_INF)),
            ({"kind": "nan"}, decimal.Literal(decimal.NAN_KIND)),
        )
        for raw, expected in values:
            predicate = {
                "kind": "eq",
                "left": {"kind": "column", "column": "a.d"},
                "right": {"kind": "literal", "type": "Decimal(7,2)", "value": raw},
            }
            expression = parse_snapshot(_snapshot(predicate)).plan.nodes[1].predicate.args[1]
            with self.subTest(value=raw):
                self.assertEqual(expression.value, expected)
                self.assertEqual(decimal.literal_json(expression.value), raw)

        malformed = (
            {"kind": "finite", "scaled": 1},
            {"kind": "finite", "scaled": "+1"},
            {"kind": "finite", "scaled": "01"},
            {"kind": "finite", "scaled": "-0"},
            {"kind": "finite", "scaled": "10000000"},
            {"kind": "pos_inf", "scaled": "0"},
            {"kind": "future"},
            "123",
        )
        for raw in malformed:
            value = _snapshot(
                {
                    "kind": "eq",
                    "left": {"kind": "column", "column": "a.d"},
                    "right": {"kind": "literal", "type": "Decimal(7,2)", "value": raw},
                }
            )
            with self.subTest(value=raw):
                with self.assertRaises(SnapshotError):
                    parse_snapshot(value)

    def test_cross_scale_and_integer_comparisons_are_admitted(self):
        for right in (
            {"kind": "column", "column": "a.wide"},
            {"kind": "column", "column": "a.i"},
        ):
            for kind in ("eq", "lt", "lte", "gt", "gte"):
                predicate = {
                    "kind": kind,
                    "left": {"kind": "column", "column": "a.d"},
                    "right": right,
                }
                with self.subTest(kind=kind, right=right["column"]):
                    parse_snapshot(_snapshot(predicate))

    def test_unproven_decimal_forms_fail_closed(self):
        decimal_in = {
            "kind": "in",
            "lookup": {"kind": "column", "column": "a.d"},
            "items": [
                {
                    "kind": "literal",
                    "type": "Decimal(7,2)",
                    "value": {"kind": "finite", "scaled": "0"},
                }
            ],
        }
        null_safe_cross_type = {
            "kind": "eq",
            "left": {"kind": "column", "column": "a.d"},
            "right": {"kind": "column", "column": "a.wide"},
            "null_safe": True,
        }
        for predicate, message in (
            (decimal_in, "Decimal IN"),
            (null_safe_cross_type, "exactly matching types"),
        ):
            with self.subTest(message=message):
                with self.assertRaisesRegex(SnapshotError, message):
                    parse_snapshot(_snapshot(predicate))

        extreme = _snapshot({"kind": "column", "column": "a.d"})
        extreme["schema"]["tables"][0]["columns"][0]["type"] = "Decimal(35,35)"
        extreme["plan"]["nodes"][1]["predicate"] = {
            "kind": "eq",
            "left": {"kind": "column", "column": "a.d"},
            "right": {"kind": "column", "column": "a.i"},
        }
        with self.assertRaisesRegex(SnapshotError, "equality type mismatch"):
            parse_snapshot(extreme)

        extreme_decimals = _snapshot({"kind": "column", "column": "a.d"})
        extreme_decimals["schema"]["tables"][0]["columns"][0]["type"] = "Decimal(35,0)"
        extreme_decimals["schema"]["tables"][0]["columns"][1]["type"] = "Decimal(35,35)"
        for left, right in (("a.d", "a.wide"), ("a.wide", "a.d")):
            value = copy.deepcopy(extreme_decimals)
            value["plan"]["nodes"][1]["predicate"] = {
                "kind": "eq",
                "left": {"kind": "column", "column": left},
                "right": {"kind": "column", "column": right},
            }
            with self.subTest(left=left, right=right):
                with self.assertRaisesRegex(SnapshotError, "equality type mismatch"):
                    parse_snapshot(value)

    def test_source_and_opaque_decimal_values_receive_typed_domains(self):
        value = _snapshot({"kind": "literal", "type": "Bool", "value": True})
        snapshot = parse_snapshot(value)
        script = smt.Script()
        database = Database(snapshot, 1, script)
        cell = database.witness["A"][0].cells["d"]
        self.assertIn(decimal.domain(cell.value, cell.type), script.assertions)

        opaque_script = smt.Script()
        opaque = Encoder(opaque_script).evaluate(
            Expr(
                kind="opaque",
                result_type="Decimal(7,2)",
                nullable=True,
                fingerprint="decimal-result",
            ),
            {},
        )
        self.assertIn(
            smt.or_(opaque.is_null, decimal.domain(opaque.value, opaque.type)),
            opaque_script.assertions,
        )


if __name__ == "__main__":
    unittest.main()
