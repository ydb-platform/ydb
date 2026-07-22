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
        self.assertEqual(actual, Value("Decimal(3,2)", smt.FALSE, cast_value))


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


if __name__ == "__main__":
    unittest.main()
