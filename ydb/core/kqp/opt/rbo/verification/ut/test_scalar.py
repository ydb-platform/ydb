import unittest
from unittest import mock

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import decimal, smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier import scalar as scalar_module
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    OPAQUE_DOUBLE_FINGERPRINT_PREFIX,
    Expr,
    parse_snapshot,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import Database
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import (
    Encoder,
    Value,
    date_domain,
    integer_domain,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.string_order import (
    StringOrderUniverse,
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


def _integral_cast(source, target_type):
    return Expr(
        kind="cast_integral",
        args=(source,),
        result_type=target_type,
        nullable=True,
    )


def _integral_conversion_may_fail(source_type, target_type):
    source_lower, source_upper = integer_bounds(source_type)
    target_lower, target_upper = integer_bounds(target_type)
    return not (
        target_lower <= source_lower and source_upper <= target_upper
    )


def _if_present(optional, present, missing, scalar_type, nullable=False):
    return Expr(
        kind="if_present",
        args=(optional, present, missing),
        result_type=scalar_type,
        nullable=nullable,
    )


def _if(condition, then, otherwise, scalar_type, nullable=False):
    return Expr(
        kind="if",
        args=(condition, then, otherwise),
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
    if term.operation == "div":
        return values[0] // values[1]
    if term.operation == "mod":
        return values[0] % values[1]
    if term.operation == "ite":
        return values[1] if values[0] else values[2]
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


def _integral_division_reference(scalar_type, left, right):
    bounds = integer_bounds(scalar_type)
    assert bounds is not None
    if right == 0:
        return None
    if scalar_type.startswith("Int") and left == bounds[0] and right == -1:
        return None
    magnitude = abs(left) // abs(right)
    return -magnitude if (left < 0) != (right < 0) else magnitude


class IntegralDivisionTest(unittest.TestCase):
    @staticmethod
    def _evaluate(
        scalar_type,
        left,
        right,
        *,
        left_is_null=False,
        right_is_null=False,
    ):
        return Encoder(smt.Script()).evaluate(
            _arithmetic(
                "div",
                scalar_type,
                Expr(kind="column", column="left"),
                Expr(kind="column", column="right"),
                nullable=True,
            ),
            {
                "left": Value(
                    scalar_type,
                    smt.bool_value(left_is_null),
                    smt.int_value(left),
                ),
                "right": Value(
                    scalar_type,
                    smt.bool_value(right_is_null),
                    smt.int_value(right),
                ),
            },
        )

    def test_int8_and_uint8_match_independent_exhaustive_reference(self):
        for scalar_type in ("Int8", "Uint8"):
            lower, upper = integer_bounds(scalar_type)
            for left in range(lower, upper):
                for right in range(lower, upper):
                    expected = _integral_division_reference(
                        scalar_type,
                        left,
                        right,
                    )
                    actual = self._evaluate(scalar_type, left, right)
                    if expected is None:
                        self.assertIs(actual.is_null, smt.TRUE)
                        self.assertEqual(_ground(actual.value), 0)
                    else:
                        self.assertIs(actual.is_null, smt.FALSE)
                        self.assertEqual(_ground(actual.value), expected)

    def test_every_width_covers_zero_overflow_and_truncation_toward_zero(self):
        for scalar_type in sorted(INTEGER_TYPES):
            lower, upper = integer_bounds(scalar_type)
            cases = [
                (upper - 1, 3),
                (upper - 1, 0),
            ]
            if scalar_type.startswith("Int"):
                cases.extend(
                    [
                        (-7, 3),
                        (7, -3),
                        (-7, -3),
                        (lower, -1),
                        (lower, 1),
                    ]
                )
            for left, right in cases:
                with self.subTest(
                    scalar_type=scalar_type,
                    left=left,
                    right=right,
                ):
                    expected = _integral_division_reference(
                        scalar_type,
                        left,
                        right,
                    )
                    actual = self._evaluate(scalar_type, left, right)
                    self.assertEqual(_ground(actual.is_null), expected is None)
                    self.assertEqual(
                        _ground(actual.value),
                        0 if expected is None else expected,
                    )

    def test_operand_nulls_are_absorbing_and_canonicalize_the_payload(self):
        for left_is_null in (False, True):
            for right_is_null in (False, True):
                actual = self._evaluate(
                    "Int64",
                    -7,
                    3,
                    left_is_null=left_is_null,
                    right_is_null=right_is_null,
                )
                with self.subTest(
                    left_is_null=left_is_null,
                    right_is_null=right_is_null,
                ):
                    expected_null = left_is_null or right_is_null
                    self.assertEqual(_ground(actual.is_null), expected_null)
                    self.assertEqual(
                        _ground(actual.value),
                        0 if expected_null else -2,
                    )

    def test_null_unsigned_payloads_do_not_enter_nonnegative_division(self):
        trusted_division = smt.div_nonnegative_by_positive
        with mock.patch.object(
            smt,
            "div_nonnegative_by_positive",
            wraps=trusted_division,
        ) as division:
            actual = self._evaluate(
                "Uint64",
                -7,
                -3,
                left_is_null=True,
                right_is_null=True,
            )

        dividend, divisor = division.call_args.args
        self.assertGreaterEqual(_ground(dividend), 0)
        self.assertGreater(_ground(divisor), 0)
        self.assertIs(actual.is_null, smt.TRUE)
        self.assertEqual(_ground(actual.value), 0)

    def test_symbolic_divisor_is_replaced_before_smt_division(self):
        script = smt.Script()
        left = script.fresh_constant("left", smt.INT)
        right = script.fresh_constant("right", smt.INT)
        actual = Encoder(script).evaluate(
            _arithmetic(
                "div",
                "Int64",
                Expr(kind="column", column="left"),
                Expr(kind="column", column="right"),
                nullable=True,
            ),
            {
                "left": Value("Int64", smt.FALSE, left),
                "right": Value("Int64", smt.FALSE, right),
            },
        )
        rendered = actual.value.render()
        self.assertIn("(div ", rendered)
        self.assertIn(f"(ite (= {right.render()} 0) 1", rendered)
        self.assertIn("(- 9223372036854775808)", actual.is_null.render())
        self.assertIn("(- 1)", actual.is_null.render())


class IntegralSafeCastTest(unittest.TestCase):
    def test_symbolic_nullness_and_canonical_payload_are_exact(self):
        script = smt.Script()
        source_is_null = script.fresh_constant("source_is_null", smt.BOOL)
        source_value = script.fresh_constant("source_value", smt.INT)
        actual = Encoder(script).evaluate(
            _integral_cast(Expr(kind="column", column="source"), "Int32"),
            {"source": Value("Int64", source_is_null, source_value)},
        )
        expected_is_null = smt.or_(
            source_is_null,
            smt.not_(integer_domain(source_value, "Int32")),
        )
        self.assertEqual(
            actual,
            Value(
                "Int32",
                expected_is_null,
                smt.ite(expected_is_null, smt.ZERO, source_value),
            ),
        )

    def test_every_partial_integer_pair_preserves_present_in_range_values_and_nulls(self):
        for source_type in sorted(INTEGER_TYPES):
            for target_type in sorted(INTEGER_TYPES):
                if not _integral_conversion_may_fail(source_type, target_type):
                    continue
                expression = _integral_cast(
                    Expr(kind="column", column="source"),
                    target_type,
                )
                for source_is_null, expected_value in ((False, 17), (True, 0)):
                    with self.subTest(
                        source_type=source_type,
                        target_type=target_type,
                        source_is_null=source_is_null,
                    ):
                        actual = Encoder(smt.Script()).evaluate(
                            expression,
                            {
                                "source": Value(
                                    source_type,
                                    smt.bool_value(source_is_null),
                                    smt.int_value(17),
                                )
                            },
                        )
                        self.assertEqual(actual.type, target_type)
                        self.assertEqual(_ground(actual.is_null), source_is_null)
                        self.assertEqual(_ground(actual.value), expected_value)

    def test_every_target_boundary_has_exact_success_or_canonical_null(self):
        cases = []
        for target_type in ("Int8", "Int16", "Int32"):
            lower, upper = integer_bounds(target_type)
            cases.extend(
                ("Int64", target_type, value, expected_is_null)
                for value, expected_is_null in (
                    (lower - 1, True),
                    (lower, False),
                    (upper - 1, False),
                    (upper, True),
                )
            )
        cases.extend(
            (
                ("Int64", "Int64", -(1 << 63), False),
                ("Uint64", "Int64", (1 << 63) - 1, False),
                ("Uint64", "Int64", 1 << 63, True),
            )
        )
        for target_type in ("Uint8", "Uint16", "Uint32"):
            _, upper = integer_bounds(target_type)
            cases.extend(
                ("Int64", target_type, value, expected_is_null)
                for value, expected_is_null in (
                    (-1, True),
                    (0, False),
                    (upper - 1, False),
                    (upper, True),
                )
            )
        cases.extend(
            (
                ("Int64", "Uint64", -1, True),
                ("Int64", "Uint64", 0, False),
                ("Int64", "Uint64", (1 << 63) - 1, False),
            )
        )

        for source_type, target_type, source_value, expected_is_null in cases:
            with self.subTest(
                source_type=source_type,
                target_type=target_type,
                source_value=source_value,
            ):
                actual = Encoder(smt.Script()).evaluate(
                    _integral_cast(Expr(kind="column", column="source"), target_type),
                    {
                        "source": Value(
                            source_type,
                            smt.FALSE,
                            smt.int_value(source_value),
                        )
                    },
                )
                self.assertEqual(_ground(actual.is_null), expected_is_null)
                self.assertEqual(
                    _ground(actual.value),
                    0 if expected_is_null else source_value,
                )


class IntegerComparisonTest(unittest.TestCase):
    @staticmethod
    def _evaluate(
        kind,
        left_type,
        left_value,
        right_type,
        right_value,
        *,
        left_is_null=False,
        right_is_null=False,
        null_safe=False,
    ):
        expression = Expr(
            kind=kind,
            args=(
                Expr(kind="column", column="left"),
                Expr(kind="column", column="right"),
            ),
            null_safe=null_safe,
        )
        return Encoder(smt.Script()).evaluate(
            expression,
            {
                "left": Value(
                    left_type,
                    smt.bool_value(left_is_null),
                    smt.int_value(left_value),
                ),
                "right": Value(
                    right_type,
                    smt.bool_value(right_is_null),
                    smt.int_value(right_value),
                ),
            },
        )

    def test_signed_unsigned_endpoints_use_mathematical_integer_order(self):
        cases = (
            ("Int8", -(1 << 7), "Uint64", 0),
            ("Int64", -1, "Uint64", (1 << 64) - 1),
            ("Int64", (1 << 63) - 1, "Uint64", (1 << 63) - 1),
            ("Uint8", (1 << 8) - 1, "Int8", (1 << 7) - 1),
            ("Uint64", (1 << 64) - 1, "Int64", (1 << 63) - 1),
            ("Uint64", 0, "Int64", -(1 << 63)),
        )
        operations = {
            "eq": lambda left, right: left == right,
            "lt": lambda left, right: left < right,
            "lte": lambda left, right: left <= right,
            "gt": lambda left, right: left > right,
            "gte": lambda left, right: left >= right,
        }
        for left_type, left, right_type, right in cases:
            for kind, reference in operations.items():
                actual = self._evaluate(
                    kind,
                    left_type,
                    left,
                    right_type,
                    right,
                )
                with self.subTest(
                    left_type=left_type,
                    left=left,
                    right_type=right_type,
                    right=right,
                    kind=kind,
                ):
                    self.assertEqual(actual.type, "Bool")
                    self.assertEqual(actual.is_null, smt.FALSE)
                    self.assertEqual(
                        _ground(actual.value),
                        reference(left, right),
                    )

    def test_cross_type_comparisons_have_exact_sql_null_semantics(self):
        for kind in ("eq", "lt", "lte", "gt", "gte"):
            for left_is_null in (False, True):
                for right_is_null in (False, True):
                    actual = self._evaluate(
                        kind,
                        "Int8",
                        -1,
                        "Uint64",
                        (1 << 64) - 1,
                        left_is_null=left_is_null,
                        right_is_null=right_is_null,
                    )
                    expected_is_null = left_is_null or right_is_null
                    with self.subTest(
                        kind=kind,
                        left_is_null=left_is_null,
                        right_is_null=right_is_null,
                    ):
                        self.assertEqual(_ground(actual.is_null), expected_is_null)
                        self.assertEqual(
                            _ground(Encoder.is_true(actual)),
                            not expected_is_null and kind in {"lt", "lte"},
                        )

    def test_cross_type_null_safe_equality_is_exactly_two_valued(self):
        cases = (
            (True, True, 0, 1, True),
            (True, False, 0, 0, False),
            (False, True, 0, 0, False),
            (False, False, (1 << 7) - 1, (1 << 7) - 1, True),
            (False, False, (1 << 7) - 1, (1 << 64) - 1, False),
        )
        for left_is_null, right_is_null, left, right, expected in cases:
            actual = self._evaluate(
                "eq",
                "Int8",
                left,
                "Uint64",
                right,
                left_is_null=left_is_null,
                right_is_null=right_is_null,
                null_safe=True,
            )
            with self.subTest(
                left_is_null=left_is_null,
                right_is_null=right_is_null,
                left=left,
                right=right,
            ):
                self.assertEqual(actual.is_null, smt.FALSE)
                self.assertEqual(_ground(actual.value), expected)


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

    def test_integral_division_does_not_dispatch_to_decimal(self):
        expression = _arithmetic(
            "div",
            "Int64",
            _literal("Int64", 10),
            _literal("Int64", 2),
            nullable=True,
        )
        with mock.patch.object(
            scalar_module.decimal,
            "divide",
        ) as decimal_divide:
            actual = Encoder(smt.Script()).evaluate(expression, {})
        decimal_divide.assert_not_called()
        self.assertIs(actual.is_null, smt.FALSE)
        self.assertEqual(_ground(actual.value), 5)


class DecimalCastDispatchTest(unittest.TestCase):
    def test_exact_integral_cast_is_forwarded_and_cannot_be_null(self):
        source_value = smt.int_value(-10)
        cast_value = smt.int_value(-1_000)
        expression = Expr(
            kind="cast_decimal",
            args=(Expr(kind="column", column="source"),),
            result_type="Decimal(3,2)",
            nullable=False,
        )
        with mock.patch.object(
            scalar_module.decimal,
            "cast_integral",
            return_value=cast_value,
        ) as cast_integral:
            actual = Encoder(smt.Script()).evaluate(
                expression,
                {"source": Value("Int8", smt.FALSE, source_value)},
            )

        cast_integral.assert_called_once_with(
            source_value,
            "Int8",
            "Decimal(3,2)",
        )
        self.assertEqual(
            actual,
            Value("Decimal(3,2)", smt.FALSE, cast_value, 900),
        )

    def test_nullable_integral_cast_propagates_null_and_saturates_overflow(self):
        expression = Expr(
            kind="cast_decimal",
            args=(Expr(kind="column", column="source"),),
            result_type="Decimal(12,2)",
            nullable=True,
        )
        cases = (
            (-10_000_000_000, -decimal.INF),
            (-9_999_999_999, -999_999_999_900),
            (9_999_999_999, 999_999_999_900),
            (10_000_000_000, decimal.INF),
        )
        for source, expected in cases:
            actual = Encoder(smt.Script()).evaluate(
                expression,
                {
                    "source": Value(
                        "Int64",
                        smt.FALSE,
                        smt.int_value(source),
                    )
                },
            )
            with self.subTest(source=source):
                self.assertEqual(actual.type, "Decimal(12,2)")
                self.assertEqual(actual.is_null, smt.FALSE)
                self.assertEqual(_ground(actual.value), expected)
                self.assertEqual(
                    actual.decimal_finite_abs_bound,
                    999_999_999_900,
                )

        null = Encoder(smt.Script()).evaluate(
            expression,
            {
                "source": Value(
                    "Int64",
                    smt.TRUE,
                    smt.int_value(10_000_000_000),
                )
            },
        )
        self.assertEqual(null.is_null, smt.TRUE)

    def test_nullable_same_scale_decimal_widening_preserves_codes_and_bounds(self):
        expression = Expr(
            kind="cast_decimal",
            args=(Expr(kind="column", column="source"),),
            result_type="Decimal(12,2)",
            nullable=True,
        )
        cases = (
            (smt.FALSE, -321, 321),
            (smt.FALSE, decimal.INF, 0),
            (smt.FALSE, decimal.NAN, 0),
            (smt.TRUE, 0, 0),
        )
        for is_null, source, finite_bound in cases:
            actual = Encoder(smt.Script()).evaluate(
                expression,
                {
                    "source": Value(
                        "Decimal(7,2)",
                        is_null,
                        smt.int_value(source),
                        finite_bound,
                    )
                },
            )
            with self.subTest(is_null=is_null, source=source):
                self.assertEqual(actual.type, "Decimal(12,2)")
                self.assertEqual(actual.is_null, is_null)
                self.assertEqual(actual.value, smt.int_value(source))
                self.assertEqual(
                    actual.decimal_finite_abs_bound,
                    finite_bound,
                )

        unbounded = Encoder(smt.Script()).evaluate(
            expression,
            {
                "source": Value(
                    "Decimal(7,2)",
                    smt.FALSE,
                    smt.ZERO,
                )
            },
        )
        self.assertEqual(
            unbounded.decimal_finite_abs_bound,
            9_999_999,
        )


class DecimalFiniteAbsBoundTest(unittest.TestCase):
    def test_finite_null_and_special_literals_have_exact_bounds(self):
        cases = (
            (decimal.Literal(decimal.FINITE, -125), 125),
            (decimal.Literal(decimal.POS_INF), 0),
            (decimal.Literal(decimal.NEG_INF), 0),
            (decimal.Literal(decimal.NAN_KIND), 0),
        )
        encoder = Encoder(smt.Script())
        for literal, expected in cases:
            with self.subTest(kind=literal.kind):
                result = encoder.evaluate(_literal("Decimal(5,2)", literal), {})
                self.assertEqual(result.decimal_finite_abs_bound, expected)

        result = encoder.evaluate(
            Expr(kind="null", result_type="Decimal(5,2)", nullable=True),
            {},
        )
        self.assertEqual(result.decimal_finite_abs_bound, 0)

    def test_add_and_sub_sum_known_operand_bounds(self):
        row = {
            "left": Value("Decimal(5,2)", smt.FALSE, smt.int_value(20), 20),
            "right": Value("Decimal(5,2)", smt.FALSE, smt.int_value(35), 35),
        }
        for kind in ("add", "sub"):
            expression = _arithmetic(
                kind,
                "Decimal(5,2)",
                Expr(kind="column", column="left"),
                Expr(kind="column", column="right"),
            )
            with self.subTest(kind=kind):
                result = Encoder(smt.Script()).evaluate(expression, row)
                self.assertEqual(result.decimal_finite_abs_bound, 55)

    def test_additive_bound_is_capped_at_largest_finite_coefficient(self):
        expression = _arithmetic(
            "add",
            "Decimal(3,0)",
            Expr(kind="column", column="left"),
            Expr(kind="column", column="right"),
        )
        result = Encoder(smt.Script()).evaluate(
            expression,
            {
                "left": Value("Decimal(3,0)", smt.FALSE, smt.int_value(700), 700),
                "right": Value("Decimal(3,0)", smt.FALSE, smt.int_value(600), 600),
            },
        )
        self.assertEqual(result.decimal_finite_abs_bound, 999)

    def test_unknown_additive_operand_bound_stays_unknown(self):
        expression = _arithmetic(
            "sub",
            "Decimal(5,2)",
            Expr(kind="column", column="left"),
            Expr(kind="column", column="right"),
        )
        result = Encoder(smt.Script()).evaluate(
            expression,
            {
                "left": Value("Decimal(5,2)", smt.FALSE, smt.int_value(20), 20),
                "right": Value("Decimal(5,2)", smt.FALSE, smt.int_value(35)),
            },
        )
        self.assertIsNone(result.decimal_finite_abs_bound)

    def test_integral_multiply_uses_full_type_domain_and_decimal_saturation(self):
        cases = (
            ("Int8", "Decimal(5,2)", 20, 2_560),
            ("Uint8", "Decimal(5,2)", 20, 5_100),
            ("Int64", "Decimal(35,2)", 20, 20 * (1 << 63)),
            ("Uint64", "Decimal(35,2)", 20, 20 * ((1 << 64) - 1)),
            ("Int8", "Decimal(3,0)", 20, 999),
        )
        for right_type, result_type, left_bound, expected in cases:
            expression = _arithmetic(
                "mul",
                result_type,
                Expr(kind="column", column="left"),
                Expr(kind="column", column="right"),
            )
            with self.subTest(right_type=right_type, result_type=result_type):
                result = Encoder(smt.Script()).evaluate(
                    expression,
                    {
                        "left": Value(
                            result_type,
                            smt.FALSE,
                            smt.ONE,
                            left_bound,
                        ),
                        "right": Value(right_type, smt.FALSE, smt.ONE),
                    },
                )
                self.assertEqual(result.decimal_finite_abs_bound, expected)

    def test_integral_division_preserves_known_left_bound(self):
        expression = _arithmetic(
            "div",
            "Decimal(35,2)",
            Expr(kind="column", column="left"),
            Expr(kind="column", column="right"),
        )
        for right_type in sorted(INTEGER_TYPES):
            with self.subTest(right_type=right_type):
                result = Encoder(smt.Script()).evaluate(
                    expression,
                    {
                        "left": Value(
                            "Decimal(35,2)",
                            smt.FALSE,
                            smt.int_value(-125),
                            125,
                        ),
                        "right": Value(
                            right_type,
                            smt.FALSE,
                            smt.int_value(-1 if right_type.startswith("Int") else 1),
                        ),
                    },
                )
                self.assertEqual(result.decimal_finite_abs_bound, 125)

    def test_integral_arithmetic_bounds_cover_boundary_and_special_results(self):
        scalar_type = "Decimal(5,0)"
        left_values = (
            -decimal.INF,
            -17,
            -1,
            0,
            1,
            17,
            decimal.INF,
            decimal.NAN,
        )
        right_values = (-128, -2, -1, 0, 1, 2, 127)
        expected_bounds = {"mul": 17 * 128, "div": 17}
        for kind, expected_bound in expected_bounds.items():
            expression = _arithmetic(
                kind,
                scalar_type,
                Expr(kind="column", column="left"),
                Expr(kind="column", column="right"),
            )
            for left in left_values:
                for right in right_values:
                    with self.subTest(kind=kind, left=left, right=right):
                        result = Encoder(smt.Script()).evaluate(
                            expression,
                            {
                                "left": Value(
                                    scalar_type,
                                    smt.FALSE,
                                    smt.int_value(left),
                                    17,
                                ),
                                "right": Value(
                                    "Int8",
                                    smt.FALSE,
                                    smt.int_value(right),
                                ),
                            },
                        )
                        self.assertEqual(
                            result.decimal_finite_abs_bound,
                            expected_bound,
                        )
                        value = _ground(result.value)
                        if abs(value) < 10**5:
                            self.assertLessEqual(abs(value), expected_bound)

    def test_unknown_and_same_decimal_multiplicative_bounds_stay_unknown(self):
        for kind in ("mul", "div"):
            expression = _arithmetic(
                kind,
                "Decimal(5,2)",
                Expr(kind="column", column="left"),
                Expr(kind="column", column="right"),
            )
            cases = (
                (
                    Value("Decimal(5,2)", smt.FALSE, smt.ONE),
                    Value("Int8", smt.FALSE, smt.ONE),
                ),
                (
                    Value("Decimal(5,2)", smt.FALSE, smt.ONE, 1),
                    Value("Decimal(5,2)", smt.FALSE, smt.ONE, 1),
                ),
            )
            for left, right in cases:
                with self.subTest(kind=kind, right_type=right.type):
                    result = Encoder(smt.Script()).evaluate(
                        expression,
                        {"left": left, "right": right},
                    )
                    self.assertIsNone(result.decimal_finite_abs_bound)

    def test_integral_cast_uses_source_domain_scale_and_saturation(self):
        cases = (
            ("Int8", "Decimal(5,2)", 12_800),
            ("Uint8", "Decimal(5,2)", 25_500),
            ("Int64", "Decimal(35,2)", (1 << 63) * 100),
            ("Uint64", "Decimal(35,2)", ((1 << 64) - 1) * 100),
            ("Int8", "Decimal(3,2)", 900),
            ("Uint8", "Decimal(3,2)", 900),
        )
        for source_type, target_type, expected in cases:
            expression = Expr(
                kind="cast_decimal",
                args=(Expr(kind="column", column="source"),),
                result_type=target_type,
                nullable=False,
            )
            source_value = integer_bounds(source_type)[0]
            with self.subTest(source_type=source_type, target_type=target_type):
                result = Encoder(smt.Script()).evaluate(
                    expression,
                    {
                        "source": Value(
                            source_type,
                            smt.FALSE,
                            smt.int_value(source_value),
                        )
                    },
                )
                self.assertEqual(result.decimal_finite_abs_bound, expected)

    def test_if_with_decimal_value_and_typed_null_preserves_bound(self):
        scalar_type = "Decimal(5,2)"
        cases = (
            (
                "literal",
                _literal(
                    scalar_type,
                    decimal.Literal(decimal.FINITE, -125),
                ),
                {},
                125,
                -125,
            ),
            (
                "integral cast",
                Expr(
                    kind="cast_decimal",
                    args=(Expr(kind="column", column="source"),),
                    result_type=scalar_type,
                    nullable=False,
                ),
                {
                    "source": Value(
                        "Int8",
                        smt.FALSE,
                        smt.int_value(-7),
                    )
                },
                12_800,
                -700,
            ),
        )
        for name, present, row, expected_bound, expected_value in cases:
            expression = _if(
                _literal("Bool", True),
                present,
                Expr(
                    kind="null",
                    result_type=scalar_type,
                    nullable=True,
                ),
                scalar_type,
                nullable=True,
            )
            with self.subTest(name=name):
                result = Encoder(smt.Script()).evaluate(expression, row)
                self.assertFalse(_ground(result.is_null))
                self.assertEqual(_ground(result.value), expected_value)
                self.assertEqual(
                    result.decimal_finite_abs_bound,
                    expected_bound,
                )


class ConditionalScalarTest(unittest.TestCase):
    def test_exists_is_the_exact_non_null_optional_presence_test(self):
        expression = Expr(
            kind="exists",
            args=(Expr(kind="column", column="optional"),),
        )
        for is_null, expected in ((False, True), (True, False)):
            with self.subTest(is_null=is_null):
                actual = Encoder(smt.Script()).evaluate(
                    expression,
                    {
                        "optional": Value(
                            "Int64",
                            smt.bool_value(is_null),
                            smt.int_value(17),
                        )
                    },
                )
                self.assertEqual(actual.type, "Bool")
                self.assertFalse(_ground(actual.is_null))
                self.assertEqual(_ground(actual.value), expected)

    def test_if_propagates_optional_condition_and_selects_one_branch(self):
        expression = _if(
            Expr(kind="column", column="condition"),
            Expr(kind="column", column="then"),
            Expr(kind="column", column="else"),
            "Int32",
            nullable=True,
        )
        for condition_is_null in (False, True):
            for condition_value in (False, True):
                for then_is_null in (False, True):
                    for else_is_null in (False, True):
                        with self.subTest(
                            condition_is_null=condition_is_null,
                            condition_value=condition_value,
                            then_is_null=then_is_null,
                            else_is_null=else_is_null,
                        ):
                            actual = Encoder(smt.Script()).evaluate(
                                expression,
                                {
                                    "condition": Value(
                                        "Bool",
                                        smt.bool_value(condition_is_null),
                                        smt.bool_value(condition_value),
                                    ),
                                    "then": Value(
                                        "Int32",
                                        smt.bool_value(then_is_null),
                                        smt.int_value(11),
                                    ),
                                    "else": Value(
                                        "Int32",
                                        smt.bool_value(else_is_null),
                                        smt.int_value(22),
                                    ),
                                },
                            )
                            selected_null = then_is_null if condition_value else else_is_null
                            selected_value = 11 if condition_value else 22
                            self.assertEqual(
                                _ground(actual.is_null),
                                condition_is_null or selected_null,
                            )
                            self.assertEqual(_ground(actual.value), selected_value)

    def test_lowered_optional_membership_has_the_same_filter_truth(self):
        lookup = Expr(kind="column", column="optional")
        items = (_literal("Int64", 1), _literal("Int64", 2))
        initial = Expr(kind="in", args=(lookup, *items))
        lowered = _if(
            Expr(kind="exists", args=(lookup,)),
            _if_present(
                lookup,
                Expr(
                    kind="in",
                    args=(Expr(kind="bound", depth=0), *items),
                ),
                _literal("Bool", False),
                "Bool",
            ),
            _literal("Bool", False),
            "Bool",
        )

        for is_null, value in (
            (True, 1),
            (False, 1),
            (False, 2),
            (False, 3),
        ):
            row = {
                "optional": Value(
                    "Int64",
                    smt.bool_value(is_null),
                    smt.int_value(value),
                )
            }
            encoder = Encoder(smt.Script())
            before = encoder.evaluate(initial, row)
            after = encoder.evaluate(lowered, row)
            before_is_true = smt.and_(smt.not_(before.is_null), before.value)
            after_is_true = smt.and_(smt.not_(after.is_null), after.value)
            with self.subTest(is_null=is_null, value=value):
                self.assertEqual(_ground(before_is_true), _ground(after_is_true))


class IfPresentScalarTest(unittest.TestCase):
    def test_optional_payload_is_bound_non_null_and_selected_exactly(self):
        expression = _if_present(
            Expr(kind="column", column="optional"),
            Expr(kind="bound", depth=0),
            _literal("Int64", -1),
            "Int64",
        )
        for optional_is_null, expected in ((False, 17), (True, -1)):
            with self.subTest(optional_is_null=optional_is_null):
                actual = Encoder(smt.Script()).evaluate(
                    expression,
                    {
                        "optional": Value(
                            "Int64",
                            smt.bool_value(optional_is_null),
                            smt.int_value(17),
                        )
                    },
                )
                self.assertEqual(actual.type, "Int64")
                self.assertFalse(_ground(actual.is_null))
                self.assertEqual(_ground(actual.value), expected)

    def test_bound_payload_flows_through_opaque_arguments(self):
        expression = _if_present(
            Expr(kind="column", column="optional"),
            Expr(
                kind="opaque",
                args=(Expr(kind="bound", depth=0),),
                result_type="Int64",
                nullable=False,
                fingerprint="use-bound",
            ),
            _literal("Int64", 0),
            "Int64",
        )
        actual = Encoder(smt.Script()).evaluate(
            expression,
            {"optional": Value("Int64", smt.FALSE, smt.int_value(17))},
        )
        self.assertEqual(actual.value.operation, "f_0")
        self.assertEqual(actual.value.arguments, (smt.FALSE, smt.int_value(17)))

    def test_nested_handlers_use_nearest_first_de_bruijn_bindings(self):
        inner = _if_present(
            Expr(kind="column", column="inner"),
            _arithmetic(
                "add",
                "Int64",
                Expr(kind="bound", depth=1),
                Expr(kind="bound", depth=0),
            ),
            Expr(kind="bound", depth=0),
            "Int64",
        )
        expression = _if_present(
            Expr(kind="column", column="outer"),
            inner,
            _literal("Int64", 0),
            "Int64",
        )
        for outer_is_null, inner_is_null, expected in (
            (True, False, 0),
            (True, True, 0),
            (False, True, 10),
            (False, False, 30),
        ):
            with self.subTest(
                outer_is_null=outer_is_null,
                inner_is_null=inner_is_null,
            ):
                actual = Encoder(smt.Script()).evaluate(
                    expression,
                    {
                        "outer": Value(
                            "Int64",
                            smt.bool_value(outer_is_null),
                            smt.int_value(10),
                        ),
                        "inner": Value(
                            "Int64",
                            smt.bool_value(inner_is_null),
                            smt.int_value(20),
                        ),
                    },
                )
                self.assertFalse(_ground(actual.is_null))
                self.assertEqual(_ground(actual.value), expected)

    def test_selected_branch_controls_both_nullness_and_value(self):
        expression = _if_present(
            Expr(kind="column", column="optional"),
            Expr(kind="column", column="present"),
            Expr(kind="column", column="missing"),
            "Int32",
            nullable=True,
        )
        for optional_is_null in (False, True):
            for present_is_null in (False, True):
                for missing_is_null in (False, True):
                    with self.subTest(
                        optional_is_null=optional_is_null,
                        present_is_null=present_is_null,
                        missing_is_null=missing_is_null,
                    ):
                        actual = Encoder(smt.Script()).evaluate(
                            expression,
                            {
                                "optional": Value(
                                    "Uint8",
                                    smt.bool_value(optional_is_null),
                                    smt.int_value(7),
                                ),
                                "present": Value(
                                    "Int32",
                                    smt.bool_value(present_is_null),
                                    smt.int_value(11),
                                ),
                                "missing": Value(
                                    "Int32",
                                    smt.bool_value(missing_is_null),
                                    smt.int_value(22),
                                ),
                            },
                        )
                        selected_null = (
                            missing_is_null if optional_is_null else present_is_null
                        )
                        selected_value = 22 if optional_is_null else 11
                        self.assertEqual(_ground(actual.is_null), selected_null)
                        self.assertEqual(_ground(actual.value), selected_value)

    def test_decimal_headroom_is_the_conservative_branch_maximum(self):
        expression = _if_present(
            Expr(kind="column", column="optional"),
            Expr(kind="column", column="present"),
            Expr(kind="column", column="missing"),
            "Decimal(5,2)",
        )
        row = {
            "optional": Value("Int64", smt.FALSE, smt.ONE),
            "present": Value("Decimal(5,2)", smt.FALSE, smt.int_value(25), 25),
            "missing": Value("Decimal(5,2)", smt.FALSE, smt.int_value(40), 40),
        }
        actual = Encoder(smt.Script()).evaluate(expression, row)
        self.assertEqual(actual.decimal_finite_abs_bound, 40)

        unknown = dict(row)
        unknown["missing"] = Value(
            "Decimal(5,2)",
            smt.FALSE,
            smt.int_value(40),
        )
        actual = Encoder(smt.Script()).evaluate(expression, unknown)
        self.assertIsNone(actual.decimal_finite_abs_bound)

    def test_string_selection_does_not_allocate_an_independent_rank(self):
        script = smt.Script()
        expression = _if_present(
            Expr(kind="column", column="optional"),
            Expr(
                kind="opaque",
                result_type="String",
                nullable=False,
                fingerprint="present-string",
            ),
            Expr(
                kind="opaque",
                result_type="String",
                nullable=False,
                fingerprint="missing-string",
            ),
            "String",
        )
        result = Encoder(script).evaluate(
            expression,
            {
                "optional": Value(
                    "Int64",
                    script.fresh_constant("optional_is_null", smt.BOOL),
                    smt.ONE,
                )
            },
        )
        self.assertEqual(result.value.operation, "ite")

        script.render()
        self.assertEqual(script.string_literals, {0: "", 1: "\0"})

    def test_unscoped_bound_fails_closed_in_the_encoder(self):
        with self.assertRaises(AssertionError):
            Encoder(smt.Script()).evaluate(Expr(kind="bound", depth=0), {})


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
                        "subplans": [],
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
    def test_if_present_date_payload_or_zero_is_exact_and_non_null(self):
        expression = _if_present(
            Expr(kind="column", column="optional"),
            Expr(kind="bound", depth=0),
            _literal(DATE, 0),
            DATE,
        )
        for optional_is_null, expected in (
            (False, MAX_DATE - 1),
            (True, 0),
        ):
            with self.subTest(optional_is_null=optional_is_null):
                result = Encoder(smt.Script()).evaluate(
                    expression,
                    {
                        "optional": Value(
                            DATE,
                            smt.bool_value(optional_is_null),
                            smt.int_value(MAX_DATE - 1),
                        )
                    },
                )
                self.assertEqual(result.type, DATE)
                self.assertFalse(_ground(result.is_null))
                self.assertEqual(_ground(result.value), expected)

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


class StringScalarTest(unittest.TestCase):
    def test_opaque_string_results_are_bounded_to_the_finite_universe(self):
        script = smt.Script()
        result = Encoder(script).evaluate(
            Expr(
                kind="opaque",
                result_type="Utf8",
                nullable=False,
                fingerprint="string-result",
            ),
            {},
        )

        formula = script.render()

        self.assertEqual(result.value.operation, "f_0")
        self.assertIn("(assert (and (not (< f_0 0)) (< f_0 1)))", formula)
        self.assertEqual(script.string_literals, {0: ""})

    def test_all_comparisons_use_raw_byte_ranks_across_string_types(self):
        values = ("", "\0", "a", "a\0", "e\u0301", "é", "😀")
        universe = StringOrderUniverse(values, 0)
        operations = {
            "eq": lambda left, right: left == right,
            "lt": lambda left, right: left < right,
            "lte": lambda left, right: left <= right,
            "gt": lambda left, right: left > right,
            "gte": lambda left, right: left >= right,
        }
        for left_type, right_type in (
            ("String", "String"),
            ("Utf8", "Utf8"),
            ("String", "Utf8"),
            ("Utf8", "String"),
        ):
            for kind, reference in operations.items():
                expression = Expr(
                    kind=kind,
                    args=(
                        Expr(kind="column", column="left"),
                        Expr(kind="column", column="right"),
                    ),
                )
                for left in values:
                    for right in values:
                        result = Encoder(smt.Script()).evaluate(
                            expression,
                            {
                                "left": Value(
                                    left_type,
                                    smt.FALSE,
                                    smt.int_value(universe.rank(left)),
                                ),
                                "right": Value(
                                    right_type,
                                    smt.FALSE,
                                    smt.int_value(universe.rank(right)),
                                ),
                            },
                        )
                        left_bytes = left.encode("utf-8")
                        right_bytes = right.encode("utf-8")
                        with self.subTest(
                            left_type=left_type,
                            right_type=right_type,
                            kind=kind,
                            left=left,
                            right=right,
                        ):
                            self.assertEqual(result.is_null, smt.FALSE)
                            self.assertEqual(
                                result.value,
                                smt.bool_value(reference(left_bytes, right_bytes)),
                            )

    def test_nullable_string_comparison_is_sql_unknown(self):
        expression = Expr(
            kind="lt",
            args=(
                Expr(kind="column", column="left"),
                Expr(kind="column", column="right"),
            ),
        )
        result = Encoder(smt.Script()).evaluate(
            expression,
            {
                "left": Value("String", smt.TRUE, smt.ZERO),
                "right": Value("Utf8", smt.FALSE, smt.ZERO),
            },
        )
        self.assertEqual(result.is_null, smt.TRUE)
        self.assertEqual(Encoder.is_true(result), smt.FALSE)


class PassiveDoubleScalarTest(unittest.TestCase):
    @staticmethod
    def _expression(fingerprint, columns=("a", "b", "c")):
        return Expr(
            kind="opaque_double",
            args=tuple(Expr(kind="column", column=column) for column in columns),
            result_type="Double",
            nullable=True,
            fingerprint=fingerprint,
        )

    def test_carrier_uses_one_nullable_uninterpreted_int_function(self):
        script = smt.Script()
        encoder = Encoder(script)
        row = {
            name: Value(
                "Int64",
                script.fresh_constant(f"{name}_null", smt.BOOL),
                script.fresh_constant(f"{name}_value", smt.INT),
            )
            for name in ("a", "b", "c")
        }
        fingerprint = OPAQUE_DOUBLE_FINGERPRINT_PREFIX + "identity"

        first = encoder.evaluate(self._expression(fingerprint), row)
        second = encoder.evaluate(self._expression(fingerprint), row)

        self.assertEqual(
            (first.type, first.is_null.sort, first.value.sort),
            ("Double", smt.BOOL, smt.INT),
        )
        self.assertEqual(first, second)
        self.assertEqual(script.assertions, ())

    def test_fingerprint_and_argument_values_remain_semantic(self):
        script = smt.Script()
        encoder = Encoder(script)
        row = {
            name: Value("Int64", smt.FALSE, smt.int_value(value))
            for name, value in (("a", 1), ("b", 2), ("c", 3))
        }
        fingerprint = OPAQUE_DOUBLE_FINGERPRINT_PREFIX + "identity"

        original = encoder.evaluate(self._expression(fingerprint), row)
        changed_fingerprint = encoder.evaluate(
            self._expression(fingerprint + ":changed"),
            row,
        )
        changed_argument = encoder.evaluate(
            self._expression(fingerprint, ("b", "a", "c")),
            row,
        )

        self.assertNotEqual(
            original.value.operation,
            changed_fingerprint.value.operation,
        )
        self.assertEqual(
            original.value.operation,
            changed_argument.value.operation,
        )
        self.assertNotEqual(
            original.value.arguments,
            changed_argument.value.arguments,
        )


if __name__ == "__main__":
    unittest.main()
