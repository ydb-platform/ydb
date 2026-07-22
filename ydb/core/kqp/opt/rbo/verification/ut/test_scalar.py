import unittest
from unittest import mock

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import scalar as scalar_module
from ydb.core.kqp.opt.rbo.verification.rbo_verifier import smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import Expr, parse_snapshot
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import Database
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import (
    Encoder,
    Value,
    date_domain,
    integer_domain,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.types import (
    DATE,
    INTEGER_TYPES,
    MAX_DATE,
    integer_bounds,
)


def _literal(scalar_type, value):
    return Expr(
        kind="literal",
        value=value,
        result_type=scalar_type,
        nullable=False,
    )


def _arithmetic(kind, scalar_type, left, right, nullable=False):
    return Expr(
        kind=kind,
        args=(left, right),
        result_type=scalar_type,
        nullable=nullable,
    )


def _ground(term):
    if term.operation in {"bool", "int"}:
        return term.atom
    values = tuple(_ground(argument) for argument in term.arguments)
    if term.operation == "not":
        return not values[0]
    if term.operation == "and":
        return all(values)
    if term.operation == "<":
        return values[0] < values[1]
    if term.operation == "+":
        return sum(values)
    if term.operation == "-":
        return values[0] - values[1]
    if term.operation == "*":
        return values[0] * values[1]
    if term.operation == "mod":
        return values[0] % values[1]
    raise AssertionError(f"non-ground operation {term.operation!r}")


class IntegerArithmeticTest(unittest.TestCase):
    def test_every_integer_width_uses_twos_complement_modular_wrap(self):
        cases = (
            ("add", "Int8", 127, 1, -128),
            ("sub", "Int16", -(1 << 15), 1, (1 << 15) - 1),
            ("mul", "Int32", (1 << 31) - 1, 2, -2),
            ("add", "Int64", (1 << 63) - 1, 1, -(1 << 63)),
            ("add", "Uint8", (1 << 8) - 1, 1, 0),
            ("sub", "Uint16", 0, 1, (1 << 16) - 1),
            ("mul", "Uint32", (1 << 32) - 1, 2, (1 << 32) - 2),
            ("add", "Uint64", (1 << 64) - 1, 1, 0),
        )
        for kind, scalar_type, left, right, expected in cases:
            with self.subTest(kind=kind, scalar_type=scalar_type):
                expression = _arithmetic(
                    kind,
                    scalar_type,
                    _literal(scalar_type, left),
                    _literal(scalar_type, right),
                )
                actual = Encoder(smt.Script()).evaluate(expression, {})
                self.assertEqual(actual.type, scalar_type)
                self.assertEqual(actual.is_null, smt.FALSE)
                self.assertEqual(_ground(actual.value), expected)

    def test_nullability_is_the_or_of_operand_nullability(self):
        expression = _arithmetic(
            "mul",
            "Int8",
            Expr(kind="column", column="left"),
            Expr(kind="column", column="right"),
            nullable=True,
        )
        actual = Encoder(smt.Script()).evaluate(
            expression,
            {
                "left": Value("Int8", smt.TRUE, smt.int_value(7)),
                "right": Value("Int8", smt.FALSE, smt.int_value(3)),
            },
        )
        self.assertEqual(actual.is_null, smt.TRUE)
        self.assertEqual(_ground(actual.value), 21)


class DecimalDivisionDispatchTest(unittest.TestCase):
    def test_same_decimal_and_every_integer_right_type_are_forwarded_exactly(self):
        for right_type in ("Decimal(7,2)", *sorted(INTEGER_TYPES)):
            left_value = smt.int_value(1_250)
            right_value = smt.int_value(5)
            divided_value = smt.int_value(250)
            expression = _arithmetic(
                "div",
                "Decimal(7,2)",
                Expr(kind="column", column="left"),
                Expr(kind="column", column="right"),
            )
            with self.subTest(right_type=right_type):
                with mock.patch.object(
                    scalar_module.decimal,
                    "divide",
                    return_value=divided_value,
                ) as divide:
                    actual = Encoder(smt.Script()).evaluate(
                        expression,
                        {
                            "left": Value("Decimal(7,2)", smt.FALSE, left_value),
                            "right": Value(right_type, smt.FALSE, right_value),
                        },
                    )

                divide.assert_called_once_with(
                    left_value,
                    right_value,
                    "Decimal(7,2)",
                    right_type,
                )
                self.assertEqual(actual.type, "Decimal(7,2)")
                self.assertEqual(actual.is_null, smt.FALSE)
                self.assertEqual(actual.value, divided_value)

    def test_result_is_null_exactly_when_either_operand_is_null(self):
        expression = _arithmetic(
            "div",
            "Decimal(7,2)",
            Expr(kind="column", column="left"),
            Expr(kind="column", column="right"),
            nullable=True,
        )
        for left_is_null in (False, True):
            for right_is_null in (False, True):
                with self.subTest(
                    left_is_null=left_is_null,
                    right_is_null=right_is_null,
                ):
                    with mock.patch.object(
                        scalar_module.decimal,
                        "divide",
                        return_value=smt.int_value(250),
                    ):
                        actual = Encoder(smt.Script()).evaluate(
                            expression,
                            {
                                "left": Value(
                                    "Decimal(7,2)",
                                    smt.bool_value(left_is_null),
                                    smt.int_value(1_250),
                                ),
                                "right": Value(
                                    "Int32",
                                    smt.bool_value(right_is_null),
                                    smt.int_value(5),
                                ),
                            },
                        )
                    self.assertEqual(
                        actual.is_null,
                        smt.bool_value(left_is_null or right_is_null),
                    )

    def test_integer_division_cannot_enter_fixed_width_arithmetic(self):
        expression = _arithmetic(
            "div",
            "Int64",
            _literal("Int64", 10),
            _literal("Int64", 2),
        )
        with self.assertRaisesRegex(AssertionError, "integer division is not part"):
            Encoder(smt.Script()).evaluate(expression, {})


class IntegerDomainTest(unittest.TestCase):
    def test_every_integer_width_has_its_exact_typed_domain(self):
        for scalar_type in sorted(INTEGER_TYPES):
            bounds = integer_bounds(scalar_type)
            assert bounds is not None
            lower, upper = bounds
            for value, expected in (
                (lower - 1, False),
                (lower, True),
                (upper - 1, True),
                (upper, False),
            ):
                with self.subTest(scalar_type=scalar_type, value=value):
                    self.assertEqual(
                        _ground(integer_domain(smt.int_value(value), scalar_type)),
                        expected,
                    )

    def test_source_and_opaque_integer_values_are_range_constrained(self):
        for scalar_type in sorted(INTEGER_TYPES):
            snapshot = parse_snapshot(
                {
                    "format": "ydb-rbo-semantic-snapshot",
                    "version": 1,
                    "schema": {
                        "tables": [
                            {
                                "name": "A",
                                "columns": [
                                    {"name": "i", "type": scalar_type, "nullable": True}
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
                                "columns": [{"source": "i", "output": "a.i"}],
                                "predicate": None,
                                "pushed_limit": None,
                            }
                        ],
                        "root": "scan",
                        "output": ["a.i"],
                    },
                    "stage_graph": None,
                }
            )
            source_script = smt.Script()
            database = Database(snapshot, 1, source_script)
            cell = database.witness["A"][0].cells["i"]
            self.assertIn(integer_domain(cell.value, scalar_type), source_script.assertions)

            opaque_script = smt.Script()
            opaque = Encoder(opaque_script).evaluate(
                Expr(
                    kind="opaque",
                    result_type=scalar_type,
                    nullable=True,
                    fingerprint=f"{scalar_type}-result",
                ),
                {},
            )
            self.assertIn(
                smt.or_(opaque.is_null, integer_domain(opaque.value, scalar_type)),
                opaque_script.assertions,
            )


class DateScalarTest(unittest.TestCase):
    def test_every_non_null_opaque_date_result_is_range_constrained(self):
        for nullable in (False, True):
            script = smt.Script()
            result = Encoder(script).evaluate(
                Expr(
                    kind="opaque",
                    result_type=DATE,
                    nullable=nullable,
                    fingerprint="date-result",
                ),
                {},
            )
            expected = smt.or_(result.is_null, date_domain(result.value))
            with self.subTest(nullable=nullable):
                self.assertIn(expected, script.assertions)

    def test_date_literals_and_every_ordering_operator_use_smt_integers(self):
        cases = (
            ("lt", 0, MAX_DATE - 1),
            ("lte", MAX_DATE - 1, MAX_DATE - 1),
            ("gt", MAX_DATE - 1, 0),
            ("gte", MAX_DATE - 1, MAX_DATE - 1),
        )
        encoder = Encoder(smt.Script())
        for kind, left, right in cases:
            expression = Expr(
                kind=kind,
                args=(_literal(DATE, left), _literal(DATE, right)),
            )
            with self.subTest(kind=kind):
                result = encoder.evaluate(expression, {})
                self.assertEqual(result.type, "Bool")
                self.assertEqual(result.is_null, smt.FALSE)
                self.assertEqual(result.value, smt.TRUE)

        literal = encoder.evaluate(_literal(DATE, MAX_DATE - 1), {})
        self.assertEqual(literal.type, DATE)
        self.assertEqual(literal.value, smt.int_value(MAX_DATE - 1))

    def test_null_date_comparison_is_sql_unknown(self):
        expression = Expr(
            kind="lt",
            args=(Expr(kind="column", column="d"), _literal(DATE, 1)),
        )
        result = Encoder(smt.Script()).evaluate(
            expression,
            {"d": Value(DATE, smt.TRUE, smt.ZERO)},
        )
        self.assertEqual(result.is_null, smt.TRUE)
        self.assertEqual(Encoder.is_true(result), smt.FALSE)


if __name__ == "__main__":
    unittest.main()
