import copy
import os
import unittest
from fractions import Fraction
from itertools import product

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import decimal, smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    Expr,
    SnapshotError,
    parse_snapshot,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import Database
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import Encoder, Value
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.types import (
    INTEGER_TYPES,
    integer_bounds,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import build_problem, solve


SOLVER = (
    yatest_common.binary_path("contrib/tools/z3/z3")
    if yatest_common is not None
    else os.environ.get("RBO_Z3")
)


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
    if term.operation == "div":
        return values[0] // values[1]
    if term.operation == "mod":
        return values[0] % values[1]
    raise AssertionError(f"unsupported ground operation {term.operation!r}")


def _compile_ground(term):
    """Compile one symbolic SMT DAG for fast exhaustive concrete evaluation."""

    instructions = []
    indices = {}

    def visit(node):
        key = id(node)
        if key in indices:
            return indices[key]
        arguments = tuple(visit(argument) for argument in node.arguments)
        index = len(instructions)
        indices[key] = index
        instructions.append((node.operation, arguments, node.atom))
        return index

    result_index = visit(term)

    def evaluate(left, right):
        values = [None] * len(instructions)
        for index, (operation, arguments, atom) in enumerate(instructions):
            if operation in {"bool", "int"}:
                value = atom
            elif operation == "symbol":
                if atom == "left":
                    value = left
                elif atom == "right":
                    value = right
                else:
                    raise AssertionError(f"unsupported compiled symbol {atom!r}")
            elif operation == "not":
                value = not values[arguments[0]]
            elif operation == "and":
                value = all(values[argument] for argument in arguments)
            elif operation == "or":
                value = any(values[argument] for argument in arguments)
            elif operation == "=":
                value = values[arguments[0]] == values[arguments[1]]
            elif operation == "<":
                value = values[arguments[0]] < values[arguments[1]]
            elif operation == "ite":
                value = values[arguments[1] if values[arguments[0]] else arguments[2]]
            elif operation == "+":
                value = sum(values[argument] for argument in arguments)
            elif operation == "-":
                value = values[arguments[0]] - values[arguments[1]]
            elif operation == "*":
                value = values[arguments[0]] * values[arguments[1]]
            elif operation == "div":
                value = values[arguments[0]] // values[arguments[1]]
            elif operation == "mod":
                value = values[arguments[0]] % values[arguments[1]]
            else:
                raise AssertionError(f"unsupported compiled operation {operation!r}")
            values[index] = value
        return values[result_index]

    return evaluate


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


def _arithmetic(kind, result_type, left, right, nullable=False):
    return Expr(
        kind=kind,
        args=(left, right),
        result_type=result_type,
        nullable=nullable,
    )


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


def _arithmetic_snapshot(kind, staged, right=None):
    value = {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": {
            "tables": [
                {
                    "name": "A",
                    "columns": [
                        {"name": "d", "type": "Decimal(7,2)", "nullable": False},
                        {"name": "i", "type": "Int8", "nullable": False},
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
                        {"source": "i", "output": "a.i"},
                    ],
                    "predicate": None,
                    "pushed_limit": None,
                },
                {
                    "id": "project",
                    "op": "project",
                    "input": "scan",
                    "ordered": False,
                    "columns": [
                        {
                            "output": "result",
                            "expression": {
                                "kind": kind,
                                "left": {"kind": "column", "column": "a.d"},
                                "right": (
                                    right
                                    if right is not None
                                    else {"kind": "column", "column": "a.d"}
                                ),
                                "type": "Decimal(7,2)",
                                "nullable": False,
                            },
                        }
                    ],
                },
            ],
            "root": "project",
            "output": ["result"],
        },
        "stage_graph": None,
    }
    if staged:
        value["stage_graph"] = {
            "root_stage": "project_stage",
            "stages": [
                {
                    "id": "source",
                    "nodes": ["scan"],
                    "inputs": [],
                    "outputs": [{"index": 0, "node": "scan"}],
                    "source_storage": "row",
                },
                {
                    "id": "project_stage",
                    "nodes": ["project"],
                    "inputs": ["scan"],
                    "outputs": [{"index": 0, "node": "project"}],
                    "source_storage": None,
                },
            ],
            "edges": [
                {
                    "id": "map",
                    "producer": "source",
                    "consumer": "project_stage",
                    "occurrence": 0,
                    "producer_output": 0,
                    "consumer_input": 0,
                    "kind": "map",
                }
            ],
            "assumptions": [],
        }
    return parse_snapshot(value)


def _cast_snapshot(staged, argument=None):
    argument = argument or {"kind": "column", "column": "a.i"}
    value = {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": {
            "tables": [
                {
                    "name": "A",
                    "columns": [
                        {"name": "i", "type": "Int8", "nullable": False},
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
                },
                {
                    "id": "project",
                    "op": "project",
                    "input": "scan",
                    "ordered": False,
                    "columns": [
                        {
                            "output": "result",
                            "expression": {
                                "kind": "cast_decimal",
                                "arg": argument,
                                "type": "Decimal(3,2)",
                                "nullable": False,
                            },
                        }
                    ],
                },
            ],
            "root": "project",
            "output": ["result"],
        },
        "stage_graph": None,
    }
    if staged:
        value["stage_graph"] = {
            "root_stage": "project_stage",
            "stages": [
                {
                    "id": "source",
                    "nodes": ["scan"],
                    "inputs": [],
                    "outputs": [{"index": 0, "node": "scan"}],
                    "source_storage": "row",
                },
                {
                    "id": "project_stage",
                    "nodes": ["project"],
                    "inputs": ["scan"],
                    "outputs": [{"index": 0, "node": "project"}],
                    "source_storage": None,
                },
            ],
            "edges": [
                {
                    "id": "map",
                    "producer": "source",
                    "consumer": "project_stage",
                    "occurrence": 0,
                    "producer_output": 0,
                    "consumer_input": 0,
                    "kind": "map",
                }
            ],
            "assumptions": [],
        }
    return parse_snapshot(value)


def _ref_integral_cast(value, precision, scale):
    """Independent integer oracle for the audited YDB cast subset."""

    coefficient = value * 10**scale
    bound = 10**precision
    if -bound < coefficient < bound:
        return coefficient
    return decimal.INF if coefficient > 0 else -decimal.INF


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


def _ref_negate(value):
    kind, finite = value
    if kind == decimal.POS_INF:
        return (decimal.NEG_INF, None)
    if kind == decimal.NEG_INF:
        return (decimal.POS_INF, None)
    return (kind, -finite if kind == decimal.FINITE else None)


def _ref_sign(value):
    kind, finite = value
    if kind == decimal.POS_INF:
        return 1
    if kind == decimal.NEG_INF:
        return -1
    assert kind == decimal.FINITE and finite is not None
    return (finite > 0) - (finite < 0)


def _ref_bounded_code(code, scalar_type):
    bound = 10**scalar_type.precision
    if -bound < code < bound:
        return code
    return decimal.INF if code > 0 else -decimal.INF


def _trunc_fraction(value):
    magnitude = abs(value.numerator) // value.denominator
    return -magnitude if value < 0 else magnitude


def _ref_ndecimal_divide(numerator, denominator):
    """Literal concrete transcription of ``NDecimal::Div`` rounding.

    This intentionally follows the C++ signed quotient/remainder control flow,
    rather than the magnitude formulation used by the SMT kernel.
    """

    assert denominator
    working_numerator = numerator
    working_denominator = denominator
    if working_denominator & 1:
        working_numerator *= 2
    else:
        working_denominator //= 2

    doubled = _trunc_fraction(Fraction(working_numerator, working_denominator))
    remainder = working_numerator - doubled * working_denominator
    if doubled & 1:
        if remainder:
            if remainder > 0:
                doubled += 1
        elif doubled & 2:
            doubled += 1
    return doubled // 2


def _ref_add(left, right, scalar_type):
    if decimal.NAN_KIND in {left[0], right[0]}:
        return decimal.NAN
    if left[0] != decimal.FINITE or right[0] != decimal.FINITE:
        if left[0] != decimal.FINITE and right[0] != decimal.FINITE:
            return decimal.NAN if left[0] != right[0] else (
                decimal.INF if left[0] == decimal.POS_INF else -decimal.INF
            )
        infinite = left if left[0] != decimal.FINITE else right
        return decimal.INF if infinite[0] == decimal.POS_INF else -decimal.INF
    exact = left[1] + right[1]
    scaled = exact * 10**scalar_type.scale
    assert scaled.denominator == 1
    return _ref_bounded_code(scaled.numerator, scalar_type)


def _ref_arithmetic(kind, left, right, scalar_type):
    if kind == "add":
        return _ref_add(left, right, scalar_type)
    if kind == "sub":
        return _ref_add(left, _ref_negate(right), scalar_type)
    if kind == "div":
        if decimal.NAN_KIND in {left[0], right[0]}:
            return decimal.NAN
        if right[0] == decimal.FINITE and right[1] == 0:
            sign = _ref_sign(left)
            if sign == 0:
                return decimal.NAN
            return decimal.INF if sign > 0 else -decimal.INF
        if right[0] != decimal.FINITE:
            return decimal.NAN if left[0] != decimal.FINITE else 0
        if left[0] != decimal.FINITE:
            sign = _ref_sign(left) * _ref_sign(right)
            return decimal.INF if sign > 0 else -decimal.INF
        factor = 10**scalar_type.scale
        left_code = left[1] * factor
        right_code = right[1] * factor
        assert left_code.denominator == right_code.denominator == 1
        scaled = _ref_ndecimal_divide(
            left_code.numerator * factor,
            right_code.numerator,
        )
        return _ref_bounded_code(scaled, scalar_type)

    assert kind == "mul"
    if decimal.NAN_KIND in {left[0], right[0]}:
        return decimal.NAN
    if left[0] != decimal.FINITE or right[0] != decimal.FINITE:
        finite = right if left[0] != decimal.FINITE else left
        if finite[0] == decimal.FINITE and finite[1] == 0:
            return decimal.NAN
        sign = _ref_sign(left) * _ref_sign(right)
        return decimal.INF if sign > 0 else -decimal.INF
    exact = left[1] * right[1]
    scaled = round(exact * 10**scalar_type.scale)
    return _ref_bounded_code(scaled, scalar_type)


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

    def test_aggregate_max_uses_guarded_raw_code_order(self):
        values = (-decimal.INF, -1, 0, 1, decimal.INF, decimal.NAN)
        for left, right in product(values, repeat=2):
            actual = decimal.aggregate_max(
                (
                    (smt.TRUE, smt.int_value(left)),
                    (smt.TRUE, smt.int_value(right)),
                )
            )
            with self.subTest(left=left, right=right):
                self.assertEqual(_ground(actual), max(left, right))

        guarded = decimal.aggregate_max(
            (
                (smt.TRUE, smt.int_value(1)),
                (smt.FALSE, smt.int_value(decimal.NAN)),
                (smt.TRUE, smt.int_value(decimal.INF)),
            )
        )
        self.assertEqual(_ground(guarded), decimal.INF)
        self.assertEqual(_ground(decimal.aggregate_max(())), -decimal.INF)

    def test_all_arithmetic_matches_exhaustive_fraction_reference(self):
        types = tuple(
            decimal.Type(precision, scale)
            for precision in range(1, 3)
            for scale in range(precision + 1)
        )
        for scalar_type in types:
            type_name = _type_name(scalar_type)
            domain = _small_domain(scalar_type)
            left_term = smt.symbol("left", smt.INT)
            right_term = smt.symbol("right", smt.INT)
            evaluators = {
                "add": _compile_ground(decimal.add(left_term, right_term, type_name)),
                "sub": _compile_ground(decimal.subtract(left_term, right_term, type_name)),
                "mul": _compile_ground(
                    decimal.multiply(left_term, right_term, type_name, type_name)
                ),
                "div": _compile_ground(
                    decimal.divide(left_term, right_term, type_name, type_name)
                ),
            }
            # Multiplication and division depend on scale and are exhausted for
            # every type.  Add/sub operate directly on scaled coefficients, so
            # their SMT kernels depend only on precision; scale zero exhausts
            # the same code domain once per precision without tripling them.
            kinds = (
                ("add", "sub", "mul", "div")
                if scalar_type.scale == 0
                else ("mul", "div")
            )
            for (left, left_ref), (right, right_ref) in product(domain, repeat=2):
                for kind in kinds:
                    actual = evaluators[kind](left, right)
                    expected = _ref_arithmetic(kind, left_ref, right_ref, scalar_type)
                    if actual != expected:
                        self.fail(
                            f"Decimal {kind} mismatch for {type_name} values "
                            f"{left} and {right}: actual={actual}, "
                            f"expected={expected}"
                        )

        left = smt.symbol("left", smt.INT)
        right = smt.symbol("right", smt.INT)
        for precision in (1, 2):
            baseline = f"Decimal({precision},0)"
            for scale in range(1, precision + 1):
                scaled = f"Decimal({precision},{scale})"
                self.assertEqual(
                    decimal.add(left, right, scaled),
                    decimal.add(left, right, baseline),
                )
                self.assertEqual(
                    decimal.subtract(left, right, scaled),
                    decimal.subtract(left, right, baseline),
                )

    def test_decimal_multiply_integral_preserves_scale_for_every_integer_type(self):
        decimal_type = "Decimal(35,2)"
        cases = (
            ("Int8", -2),
            ("Int16", 3),
            ("Int32", -4),
            ("Int64", 5),
            ("Uint8", 6),
            ("Uint16", 7),
            ("Uint32", 8),
            ("Uint64", (1 << 64) - 1),
        )
        for integer_type, integer in cases:
            expression = _arithmetic(
                "mul",
                decimal_type,
                _decimal_literal(decimal_type, 100),
                _literal(integer_type, integer),
            )
            result = Encoder(smt.Script()).evaluate(expression, {})
            with self.subTest(integer_type=integer_type):
                self.assertEqual(result.type, decimal_type)
                self.assertEqual(result.is_null, smt.FALSE)
                self.assertEqual(_ground(result.value), 100 * integer)

    def test_decimal_divide_integral_preserves_scale_for_every_integer_type(self):
        scalar_type = "Decimal(35,2)"
        cases = (
            ("Int8", -2, -250),
            ("Int16", 4, 125),
            ("Int32", -4, -125),
            ("Int64", 8, 62),
            ("Uint8", 3, 167),
            ("Uint16", 4, 125),
            ("Uint32", 8, 62),
            ("Uint64", (1 << 64) - 1, 0),
        )
        for integer_type, integer, expected in cases:
            actual = decimal.divide(
                smt.int_value(500),
                smt.int_value(integer),
                scalar_type,
                integer_type,
            )
            with self.subTest(integer_type=integer_type):
                self.assertEqual(_ground(actual), expected)

    def test_decimal_divide_rejects_unaudited_operand_shapes(self):
        for result_type, right_type, message in (
            ("Int32", "Int32", "not a Decimal type"),
            ("Decimal(5,2)", "Decimal(6,2)", "same-type Decimal or integral"),
            ("Decimal(5,2)", "Bool", "same-type Decimal or integral"),
        ):
            with self.subTest(result_type=result_type, right_type=right_type):
                with self.assertRaisesRegex(ValueError, message):
                    decimal.divide(
                        smt.ONE,
                        smt.ONE,
                        result_type,
                        right_type,
                    )

    def test_integral_cast_matches_independent_boundary_and_overflow_oracle(self):
        result_types = (
            "Decimal(1,0)",
            "Decimal(3,2)",
            "Decimal(19,0)",
            "Decimal(20,0)",
            "Decimal(35,34)",
        )
        for source_type in sorted(INTEGER_TYPES):
            lower, upper = integer_bounds(source_type)
            source_values = {
                lower,
                lower + 1,
                0,
                1,
                upper - 2,
                upper - 1,
                -10,
                -9,
                9,
                10,
            }
            source_values = tuple(value for value in source_values if lower <= value < upper)
            for result_type in result_types:
                result = decimal.parse_type(result_type)
                assert result is not None
                for value in source_values:
                    with self.subTest(
                        source_type=source_type,
                        result_type=result_type,
                        value=value,
                    ):
                        actual = decimal.cast_integral(
                            smt.int_value(value),
                            source_type,
                            result_type,
                        )
                        self.assertEqual(
                            _ground(actual),
                            _ref_integral_cast(value, result.precision, result.scale),
                        )

        # Exhausting both 8-bit domains makes the strict +/-10 boundary for
        # Decimal(3,2), zero, and both overflow signs independent of samples.
        for source_type in ("Int8", "Uint8"):
            lower, upper = integer_bounds(source_type)
            for value in range(lower, upper):
                actual = decimal.cast_integral(
                    smt.int_value(value),
                    source_type,
                    "Decimal(3,2)",
                )
                self.assertEqual(_ground(actual), _ref_integral_cast(value, 3, 2))

    def test_integral_cast_kernel_rejects_broadened_shapes(self):
        for source_type, result_type, message in (
            ("Bool", "Decimal(3,2)", "source is not integral"),
            ("Date", "Decimal(3,2)", "source is not integral"),
            ("Int8", "Int64", "result is not Decimal"),
            ("Int8", "Decimal(3,3)", "at least one integral digit"),
        ):
            with self.subTest(source_type=source_type, result_type=result_type):
                with self.assertRaisesRegex(ValueError, message):
                    decimal.cast_integral(
                        smt.ONE,
                        source_type,
                        result_type,
                    )

    def test_arithmetic_specials_zero_and_precision_overflow_are_explicit(self):
        scalar_type = "Decimal(5,2)"
        cases = (
            ("add", decimal.INF, -decimal.INF, decimal.NAN),
            ("add", -decimal.INF, -decimal.INF, -decimal.INF),
            ("sub", decimal.INF, decimal.INF, decimal.NAN),
            ("sub", -decimal.INF, decimal.INF, -decimal.INF),
            ("mul", decimal.NAN, 0, decimal.NAN),
            ("mul", decimal.INF, 0, decimal.NAN),
            ("mul", decimal.INF, -1, -decimal.INF),
            ("mul", -decimal.INF, -decimal.INF, decimal.INF),
            ("mul", 0, -99999, 0),
            ("add", 99999, 1, decimal.INF),
            ("sub", -99999, 1, -decimal.INF),
            ("mul", 99999, 200, decimal.INF),
            ("mul", -99999, 200, -decimal.INF),
        )
        for kind, left, right, expected in cases:
            if kind == "add":
                actual = decimal.add(
                    smt.int_value(left), smt.int_value(right), scalar_type
                )
            elif kind == "sub":
                actual = decimal.subtract(
                    smt.int_value(left), smt.int_value(right), scalar_type
                )
            else:
                actual = decimal.multiply(
                    smt.int_value(left), smt.int_value(right), scalar_type, scalar_type
                )
            with self.subTest(kind=kind, left=left, right=right):
                self.assertEqual(_ground(actual), expected)

    def test_decimal_multiply_integral_specials_and_overflow(self):
        scalar_type = "Decimal(5,2)"
        cases = (
            (decimal.NAN, "Int8", 0, decimal.NAN),
            (decimal.INF, "Int8", 0, decimal.NAN),
            (decimal.INF, "Int8", -2, -decimal.INF),
            (-decimal.INF, "Int8", -2, decimal.INF),
            (99999, "Uint8", 2, decimal.INF),
            (-99999, "Uint8", 2, -decimal.INF),
        )
        for left, right_type, right, expected in cases:
            actual = decimal.multiply(
                smt.int_value(left),
                smt.int_value(right),
                scalar_type,
                right_type,
            )
            with self.subTest(left=left, right=right):
                self.assertEqual(_ground(actual), expected)

    def test_finite_product_colliding_with_nan_code_saturates_to_infinity(self):
        # 10^35 + 1 is the in-band NaN code and is divisible by 11.  Both
        # factors are legal Decimal(35,0) finite values, so their arithmetic
        # result must first normalize to +Inf rather than be decoded as NaN.
        factor = decimal.NAN // 11
        self.assertEqual(11 * factor, decimal.NAN)
        self.assertLess(factor, decimal.INF)
        for left, expected in ((11, decimal.INF), (-11, -decimal.INF)):
            actual = decimal.multiply(
                smt.int_value(left),
                smt.int_value(factor),
                "Decimal(35,0)",
                "Decimal(35,0)",
            )
            with self.subTest(left=left):
                self.assertEqual(_ground(actual), expected)

        # The Decimal-by-integer kernel must normalize the same finite
        # collision before interpreting any in-band special code.
        for left, right, expected in (
            (factor, 11, decimal.INF),
            (-factor, 11, -decimal.INF),
        ):
            actual = decimal.multiply(
                smt.int_value(left),
                smt.int_value(right),
                "Decimal(35,0)",
                "Int8",
            )
            with self.subTest(left=left, right_type="Int8"):
                self.assertEqual(_ground(actual), expected)

    def test_decimal_multiply_rounds_half_to_even_for_both_signs(self):
        scalar_type = "Decimal(5,2)"
        # Scaled products 2.5 and 3.5 distinguish ties-to-even from truncation
        # and ties-away; signs must be applied after rounding the magnitude.
        for left, right, expected in (
            (1, 250, 2),
            (1, 350, 4),
            (-1, 250, -2),
            (-1, 350, -4),
        ):
            actual = decimal.multiply(
                smt.int_value(left),
                smt.int_value(right),
                scalar_type,
                scalar_type,
            )
            with self.subTest(left=left, right=right):
                self.assertEqual(_ground(actual), expected)

    def test_decimal_divide_ties_to_even_for_all_signs(self):
        scalar_type = "Decimal(5,2)"
        for left, right, expected in (
            (1, 40, 2),
            (7, 200, 4),
            (-1, 40, -2),
            (-7, 200, -4),
            (1, -40, -2),
            (7, -200, -4),
        ):
            actual = decimal.divide(
                smt.int_value(left),
                smt.int_value(right),
                scalar_type,
                scalar_type,
            )
            with self.subTest(left=left, right=right):
                self.assertEqual(_ground(actual), expected)

        for left, right, expected in (
            (5, 2, 2),
            (7, 2, 4),
            (-5, 2, -2),
            (-7, 2, -4),
            (5, -2, -2),
            (7, -2, -4),
            (-5, -2, 2),
            (-7, -2, 4),
        ):
            actual = decimal.divide(
                smt.int_value(left),
                smt.int_value(right),
                scalar_type,
                "Int8",
            )
            with self.subTest(left=left, integer_right=right):
                self.assertEqual(_ground(actual), expected)

    def test_decimal_divide_matches_negative_divisor_runtime_asymmetry(self):
        scalar_type = "Decimal(10,0)"
        for left, right, expected in (
            (-238_973, -128, 1_866),
            (-238_973, -19, 12_577),
            (238_973, -128, -1_866),
            (238_973, -19, -12_577),
        ):
            self.assertEqual(_ref_ndecimal_divide(left, right), expected)
            self.assertNotEqual(round(Fraction(left, right)), expected)
            for right_type in (scalar_type, "Int16"):
                actual = decimal.divide(
                    smt.int_value(left),
                    smt.int_value(right),
                    scalar_type,
                    right_type,
                )
                with self.subTest(
                    left=left,
                    right=right,
                    right_type=right_type,
                ):
                    self.assertEqual(_ground(actual), expected)

    def test_decimal_divide_specials_zero_overflow_and_nan_collision(self):
        scalar_type = "Decimal(5,2)"
        cases = (
            (decimal.NAN, 1, decimal.NAN),
            (1, decimal.NAN, decimal.NAN),
            (0, 0, decimal.NAN),
            (1, 0, decimal.INF),
            (-1, 0, -decimal.INF),
            (decimal.INF, 0, decimal.INF),
            (-decimal.INF, 0, -decimal.INF),
            (1, decimal.INF, 0),
            (-1, -decimal.INF, 0),
            (decimal.INF, decimal.INF, decimal.NAN),
            (-decimal.INF, decimal.INF, decimal.NAN),
            (decimal.INF, -1, -decimal.INF),
            (-decimal.INF, -1, decimal.INF),
            (99_999, 1, decimal.INF),
            (-99_999, 1, -decimal.INF),
        )
        for left, right, expected in cases:
            actual = decimal.divide(
                smt.int_value(left),
                smt.int_value(right),
                scalar_type,
                scalar_type,
            )
            with self.subTest(left=left, right=right):
                self.assertEqual(_ground(actual), expected)

        for left, right, expected in (
            (decimal.NAN, 0, decimal.NAN),
            (0, 0, decimal.NAN),
            (1, 0, decimal.INF),
            (-1, 0, -decimal.INF),
            (decimal.INF, -2, -decimal.INF),
            (-decimal.INF, -2, decimal.INF),
        ):
            actual = decimal.divide(
                smt.int_value(left),
                smt.int_value(right),
                scalar_type,
                "Int8",
            )
            with self.subTest(left=left, integer_right=right):
                self.assertEqual(_ground(actual), expected)

        # At scale 35 this finite ratio rounds to 10^35 + 1, the in-band
        # NaN code.  NDecimal first normalizes the widened finite quotient to
        # the global +Inf bound; the wrapper must not reinterpret it as NaN.
        denominator = 8 * 10**34
        exact = Fraction(denominator + 1, denominator) * decimal.INF
        self.assertEqual(round(exact), decimal.NAN)
        for left, expected in (
            (denominator + 1, decimal.INF),
            (-denominator - 1, -decimal.INF),
        ):
            actual = decimal.divide(
                smt.int_value(left),
                smt.int_value(denominator),
                "Decimal(35,35)",
                "Decimal(35,35)",
            )
            with self.subTest(nan_collision_left=left):
                self.assertEqual(_ground(actual), expected)

    def test_decimal_arithmetic_is_strictly_null_propagating(self):
        scalar_type = "Decimal(7,2)"
        for kind in ("add", "sub", "mul", "div"):
            expression = _arithmetic(
                kind,
                scalar_type,
                Expr(kind="column", column="left"),
                Expr(kind="column", column="right"),
                nullable=True,
            )
            result = Encoder(smt.Script()).evaluate(
                expression,
                {
                    "left": Value(scalar_type, smt.TRUE, smt.int_value(125)),
                    "right": Value(scalar_type, smt.FALSE, smt.int_value(200)),
                },
            )
            with self.subTest(kind=kind):
                self.assertEqual(result.is_null, smt.TRUE)

    def test_symbolic_decimal_rescale_renders_only_exact_integer_operations(self):
        result = decimal.multiply(
            smt.symbol("left", smt.INT),
            smt.symbol("right", smt.INT),
            "Decimal(5,2)",
            "Decimal(5,2)",
        ).render()
        self.assertIn("(div ", result)
        self.assertIn("(mod ", result)
        self.assertNotIn("Real", result)
        self.assertNotIn("to_int", result)

        division = decimal.divide(
            smt.symbol("left", smt.INT),
            smt.symbol("right", smt.INT),
            "Decimal(5,2)",
            "Decimal(5,2)",
        ).render()
        self.assertIn("(div ", division)
        self.assertIn("(mod ", division)
        self.assertIn("(ite (= (ite (< right 0) (- 0 right) right) 0) 1", division)
        self.assertNotIn("Real", division)
        self.assertNotIn("to_int", division)

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

    def test_exact_decimal_arithmetic_gate_is_admitted(self):
        cases = (
            ("add", "a.d", "a.d", "Decimal(7,2)", True),
            ("sub", "a.wide", "a.wide", "Decimal(12,2)", False),
            ("mul", "a.d", "a.d", "Decimal(7,2)", True),
            ("mul", "a.d", "a.i", "Decimal(7,2)", True),
        )
        for kind, left, right, result_type, nullable in cases:
            arithmetic = {
                "kind": kind,
                "left": {"kind": "column", "column": left},
                "right": {"kind": "column", "column": right},
                "type": result_type,
                "nullable": nullable,
            }
            predicate = {
                "kind": "eq",
                "left": arithmetic,
                "right": {
                    "kind": "literal",
                    "type": result_type,
                    "value": {"kind": "finite", "scaled": "0"},
                },
            }
            with self.subTest(kind=kind, left=left, right=right):
                expression = parse_snapshot(_snapshot(predicate)).plan.nodes[1].predicate.args[0]
                self.assertEqual(
                    (expression.kind, expression.result_type, expression.nullable),
                    (kind, result_type, nullable),
                )

    def test_exact_non_null_integral_decimal_cast_gate_is_admitted(self):
        for source_type in sorted(INTEGER_TYPES):
            value = _snapshot({"kind": "literal", "type": "Bool", "value": True})
            value["schema"]["tables"][0]["columns"][2]["type"] = source_type
            cast = {
                "kind": "cast_decimal",
                "arg": {"kind": "column", "column": "a.i"},
                "type": "Decimal(3,2)",
                "nullable": False,
            }
            value["plan"]["nodes"][1]["predicate"] = {
                "kind": "eq",
                "left": cast,
                "right": {
                    "kind": "literal",
                    "type": "Decimal(3,2)",
                    "value": {"kind": "finite", "scaled": "0"},
                },
            }
            with self.subTest(source_type=source_type):
                expression = parse_snapshot(value).plan.nodes[1].predicate.args[0]
                self.assertEqual(
                    (expression.kind, expression.result_type, expression.nullable),
                    ("cast_decimal", "Decimal(3,2)", False),
                )

    def test_integral_decimal_cast_ir_fails_closed(self):
        base = {
            "kind": "cast_decimal",
            "arg": {"kind": "column", "column": "a.i"},
            "type": "Decimal(3,2)",
            "nullable": False,
        }

        def snapshot(cast):
            return _snapshot(
                {
                    "kind": "eq",
                    "left": cast,
                    "right": {
                        "kind": "literal",
                        "type": "Decimal(3,2)",
                        "value": {"kind": "finite", "scaled": "0"},
                    },
                }
            )

        cases = []
        decimal_source = copy.deepcopy(base)
        decimal_source["arg"]["column"] = "a.d"
        cases.append(
            ("Decimal source", snapshot(decimal_source), "source must be integral")
        )

        for source_type in ("Bool", "Date", "String"):
            non_integral_source = snapshot(copy.deepcopy(base))
            non_integral_source["schema"]["tables"][0]["columns"][2]["type"] = source_type
            cases.append(
                (
                    f"{source_type} source",
                    non_integral_source,
                    "source must be integral",
                )
            )

        nullable_source = snapshot(copy.deepcopy(base))
        nullable_source["schema"]["tables"][0]["columns"][2]["nullable"] = True
        cases.append(("nullable source", nullable_source, "source must be non-nullable"))

        non_decimal = copy.deepcopy(base)
        non_decimal["type"] = "Int64"
        non_decimal_snapshot = snapshot(non_decimal)
        non_decimal_snapshot["plan"]["nodes"][1]["predicate"]["right"] = {
            "kind": "literal",
            "type": "Int64",
            "value": 0,
        }
        cases.append(("non-Decimal result", non_decimal_snapshot, "canonical Decimal type"))

        no_integral_digit = copy.deepcopy(base)
        no_integral_digit["type"] = "Decimal(3,3)"
        no_integral_snapshot = snapshot(no_integral_digit)
        no_integral_snapshot["plan"]["nodes"][1]["predicate"]["right"]["type"] = "Decimal(3,3)"
        cases.append(("no integral digit", no_integral_snapshot, "at least one integral digit"))

        nullable_result = copy.deepcopy(base)
        nullable_result["nullable"] = True
        cases.append(("nullable result", snapshot(nullable_result), "must be non-nullable"))

        extra_field = copy.deepcopy(base)
        extra_field["source_type"] = "Int32"
        cases.append(("unknown field", snapshot(extra_field), "unknown fields"))

        missing_arg = copy.deepcopy(base)
        del missing_arg["arg"]
        cases.append(("missing arg", snapshot(missing_arg), "missing fields"))

        for label, value, message in cases:
            with self.subTest(case=label):
                with self.assertRaisesRegex(SnapshotError, message):
                    parse_snapshot(value)

    def test_unaudited_decimal_arithmetic_shapes_fail_closed(self):
        base = {
            "kind": "mul",
            "left": {"kind": "column", "column": "a.d"},
            "right": {"kind": "column", "column": "a.d"},
            "type": "Decimal(7,2)",
            "nullable": True,
        }

        def predicate(arithmetic):
            return {
                "kind": "eq",
                "left": arithmetic,
                "right": {
                    "kind": "literal",
                    "type": arithmetic["type"],
                    "value": {"kind": "finite", "scaled": "0"},
                },
            }

        cases = []
        for kind in ("add", "sub"):
            arithmetic = copy.deepcopy(base)
            arithmetic["kind"] = kind
            arithmetic["right"] = {"kind": "column", "column": "a.i"}
            cases.append((f"{kind} integral right", arithmetic, "right operand"))

        different_decimal = copy.deepcopy(base)
        different_decimal["right"] = {"kind": "column", "column": "a.wide"}
        cases.append(("mul different Decimal", different_decimal, "right operand"))

        reversed_operands = copy.deepcopy(base)
        reversed_operands["left"] = {"kind": "column", "column": "a.i"}
        cases.append(("mul integral left", reversed_operands, "left operand"))

        wrong_result = copy.deepcopy(base)
        wrong_result["type"] = "Decimal(12,2)"
        cases.append(("mul wrong result", wrong_result, "left operand"))

        non_scalar_right = copy.deepcopy(base)
        non_scalar_right["right"] = {"kind": "literal", "type": "Bool", "value": True}
        cases.append(("mul Boolean right", non_scalar_right, "right operand"))

        wrong_nullability = copy.deepcopy(base)
        wrong_nullability["nullable"] = False
        cases.append(("mul wrong nullability", wrong_nullability, "nullability"))

        for label, arithmetic, message in cases:
            with self.subTest(case=label):
                with self.assertRaisesRegex(SnapshotError, message):
                    parse_snapshot(_snapshot(predicate(arithmetic)))

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


@unittest.skipUnless(SOLVER, "run through ya or set RBO_Z3 for solver tests")
class DecimalVerificationTest(unittest.TestCase):
    def test_integral_cast_survives_normal_initial_to_stage_verification(self):
        result = solve(
            build_problem(
                _cast_snapshot(staged=False),
                _cast_snapshot(staged=True),
                1,
                10_000,
            ),
            SOLVER,
            1,
            10_000,
        )
        self.assertEqual(result.status, "VERIFIED_BOUNDED")

    def test_integral_cast_argument_mutation_produces_counterexample(self):
        incremented = {
            "kind": "add",
            "left": {"kind": "column", "column": "a.i"},
            "right": {"kind": "literal", "type": "Int8", "value": 1},
            "type": "Int8",
            "nullable": False,
        }
        result = solve(
            build_problem(
                _cast_snapshot(staged=False),
                _cast_snapshot(staged=True, argument=incremented),
                1,
                10_000,
            ),
            SOLVER,
            1,
            10_000,
        )
        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertEqual(len(result.witness["A"]), 1)

    def test_arithmetic_survives_normal_initial_to_stage_verification(self):
        for kind in ("add", "sub", "mul", "div"):
            result = solve(
                build_problem(
                    _arithmetic_snapshot(kind, staged=False),
                    _arithmetic_snapshot(kind, staged=True),
                    1,
                    10_000,
                ),
                SOLVER,
                1,
                10_000,
            )
            with self.subTest(kind=kind):
                self.assertEqual(result.status, "VERIFIED_BOUNDED")

    def test_wrong_arithmetic_transformations_produce_counterexamples(self):
        for before, after in (("add", "sub"), ("sub", "add"), ("mul", "add")):
            result = solve(
                build_problem(
                    _arithmetic_snapshot(before, staged=False),
                    _arithmetic_snapshot(after, staged=True),
                    1,
                    10_000,
                ),
                SOLVER,
                1,
                10_000,
            )
            with self.subTest(before=before, after=after):
                self.assertEqual(result.status, "COUNTEREXAMPLE")
                self.assertEqual(len(result.witness["A"]), 1)

    def test_division_is_not_multiplication(self):
        result = solve(
            build_problem(
                _arithmetic_snapshot("div", staged=False),
                _arithmetic_snapshot("mul", staged=True),
                1,
                10_000,
            ),
            SOLVER,
            1,
            10_000,
        )
        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertEqual(len(result.witness["A"]), 1)

    def test_decimal_integer_multiply_observes_the_integer_source_domain(self):
        integer = {"kind": "column", "column": "a.i"}
        wrapped_integer = {
            "kind": "add",
            "left": integer,
            "right": {"kind": "literal", "type": "Int8", "value": 0},
            "type": "Int8",
            "nullable": False,
        }
        result = solve(
            build_problem(
                _arithmetic_snapshot("mul", staged=False, right=integer),
                _arithmetic_snapshot("mul", staged=True, right=wrapped_integer),
                1,
                10_000,
            ),
            SOLVER,
            1,
            10_000,
        )
        self.assertEqual(result.status, "VERIFIED_BOUNDED")


if __name__ == "__main__":
    unittest.main()
