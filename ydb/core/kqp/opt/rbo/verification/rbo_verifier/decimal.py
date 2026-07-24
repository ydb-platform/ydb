"""Exact YDB Decimal values needed by scalar verification.

Decimal values are signed, scaled integers.  The three non-finite values use
the same codes as ``NYql::NDecimal``; keeping those codes explicit makes the
SMT model match YDB comparisons, casts, and arithmetic without SMT Reals.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from . import smt


MAX_PRECISION = 35
INF = 10**MAX_PRECISION
NAN = INF + 1

FINITE = "finite"
POS_INF = "pos_inf"
NEG_INF = "neg_inf"
NAN_KIND = "nan"
LITERAL_KINDS = frozenset({FINITE, POS_INF, NEG_INF, NAN_KIND})
_SPECIAL_CODES = {POS_INF: INF, NEG_INF: -INF, NAN_KIND: NAN}

_TYPE = re.compile(
    r"Decimal\((?P<precision>[1-9]|[12][0-9]|3[0-5]),"
    r"(?P<scale>0|[1-9]|[12][0-9]|3[0-5])\)"
)
_SCALED_INTEGER = re.compile(r"(?:0|-[1-9][0-9]*|[1-9][0-9]*)")
_INTEGER_TYPE = re.compile(r"(?:Int|Uint)(?P<bits>8|16|32|64)")
_INTEGER_DECIMAL_DIGITS = {8: 3, 16: 5, 32: 10, 64: 20}


@dataclass(frozen=True, slots=True)
class Type:
    precision: int
    scale: int

    @property
    def integral_digits(self) -> int:
        return self.precision - self.scale


@dataclass(frozen=True, slots=True)
class Literal:
    kind: str
    scaled: int | None = None

    def __post_init__(self) -> None:
        if self.kind not in LITERAL_KINDS:
            raise ValueError(f"unsupported Decimal literal kind {self.kind!r}")
        if self.kind == FINITE:
            if type(self.scaled) is not int:
                raise ValueError("finite Decimal literal requires an integer scaled value")
        elif self.scaled is not None:
            raise ValueError("non-finite Decimal literal must not have a scaled value")


def parse_type(scalar_type: str) -> Type | None:
    match = _TYPE.fullmatch(scalar_type)
    if match is None:
        return None
    result = Type(int(match["precision"]), int(match["scale"]))
    return result if result.scale <= result.precision else None


def is_type(scalar_type: str) -> bool:
    return parse_type(scalar_type) is not None


def sum_type(input_type: str) -> str | None:
    """YQL SUM state/result type for a Decimal input."""

    decimal_type = parse_type(input_type)
    if decimal_type is None:
        return None
    return f"Decimal({MAX_PRECISION},{decimal_type.scale})"


def parse_scaled_integer(value: str) -> int:
    if not _SCALED_INTEGER.fullmatch(value):
        raise ValueError("scaled Decimal value is not a canonical signed integer")
    return int(value)


def literal_code(value: Literal, scalar_type: str) -> int:
    decimal_type = parse_type(scalar_type)
    if decimal_type is None:
        raise ValueError(f"not a Decimal type: {scalar_type!r}")
    if value.kind == FINITE:
        assert value.scaled is not None
        bound = 10**decimal_type.precision
        if not -bound < value.scaled < bound:
            raise ValueError(f"finite value is outside {scalar_type}")
        return value.scaled
    return _SPECIAL_CODES[value.kind]


def literal_json(value: Literal) -> dict[str, str]:
    result = {"kind": value.kind}
    if value.kind == FINITE:
        assert value.scaled is not None
        result["scaled"] = str(value.scaled)
    return result


def domain(value: smt.Term, scalar_type: str) -> smt.Term:
    """Legal typed values imported by YDB for ``Decimal(p,s)``."""

    decimal_type = parse_type(scalar_type)
    if decimal_type is None:
        raise ValueError(f"not a Decimal type: {scalar_type!r}")
    return smt.or_(
        _normal(value, decimal_type.precision),
        smt.eq(value, smt.int_value(-INF)),
        smt.eq(value, smt.int_value(INF)),
        smt.eq(value, smt.int_value(NAN)),
    )


def comparable(value: smt.Term) -> smt.Term:
    return smt.and_(
        smt.not_(smt.lt(value, smt.int_value(-INF))),
        smt.not_(smt.lt(smt.int_value(INF), value)),
    )


def compare(kind: str, left: smt.Term, right: smt.Term) -> smt.Term:
    """YDB's ordinary, non-aggregate Decimal comparison."""

    if kind == "eq":
        return smt.and_(comparable(left), smt.eq(left, right))

    both_comparable = smt.and_(comparable(left), comparable(right))
    if kind == "lt":
        ordered = smt.lt(left, right)
    elif kind == "lte":
        ordered = smt.not_(smt.lt(right, left))
    elif kind == "gt":
        ordered = smt.lt(right, left)
    elif kind == "gte":
        ordered = smt.not_(smt.lt(left, right))
    else:
        raise ValueError(f"unsupported Decimal comparison {kind!r}")
    return smt.and_(both_comparable, ordered)


def sort_less(left: smt.Term, right: smt.Term) -> smt.Term:
    """YDB's total Decimal order used by Sort, TopSort, and Merge.

    MiniKQL's ``CompareValues<EDataSlot::Decimal>`` compares the signed
    128-bit representation directly.  Unlike ordinary Decimal comparison,
    NaN is therefore ordered after positive infinity instead of being
    incomparable.  Legal snapshot values have the exact order
    ``-Inf < finite < +Inf < NaN``.
    """

    return smt.lt(left, right)


def aggregate_max(
    guarded_values: tuple[tuple[smt.Term, smt.Term], ...],
) -> smt.Term:
    """Reduce Decimal ``AggrMax`` inputs in YDB's raw signed-code order.

    Aggregate MAX is deliberately different from ordinary Decimal comparison:
    MiniKQL compares the signed 128-bit codes directly, so NaN is greater than
    positive infinity.  Negative infinity is the identity over every legal
    Decimal value; guards let the relational layer exclude NULL and absent rows.
    """

    result = smt.int_value(-INF)
    for guard, value in guarded_values:
        result = smt.ite(
            guard,
            smt.ite(smt.lt(value, result), result, value),
            result,
        )
    return result


def aggregate_min(
    guarded_values: tuple[tuple[smt.Term, smt.Term], ...],
) -> smt.Term:
    """Reduce Decimal ``AggrMin`` inputs in YDB's raw signed-code order.

    MiniKQL initializes a non-empty aggregate from its first input, then
    compares signed 128-bit codes directly.  NaN is the greatest legal raw
    code, so it is the internal reduction sentinel; this preserves a lone NaN
    that a direct fold into ``+Inf`` would lose.  Positive infinity is only the
    hidden empty/all-NULL payload returned alongside the relational layer's
    NULL marker.
    """

    result = smt.int_value(NAN)
    guards: list[smt.Term] = []
    for guard, value in guarded_values:
        guards.append(guard)
        result = smt.ite(
            guard,
            smt.ite(smt.lt(value, result), value, result),
            result,
        )
    return smt.ite(smt.or_(*guards), result, smt.int_value(INF))


def add(left: smt.Term, right: smt.Term, result_type: str) -> smt.Term:
    """Exact same-type YQL Decimal addition."""

    return _add_or_subtract("add", left, right, result_type)


def subtract(left: smt.Term, right: smt.Term, result_type: str) -> smt.Term:
    """Exact same-type YQL Decimal subtraction."""

    return _add_or_subtract("sub", left, right, result_type)


def cast_integral(value: smt.Term, source_type: str, result_type: str) -> smt.Term:
    """Exactly cast a non-null YQL integer to ``Decimal(p,s)``.

    The finite coefficient is the integer multiplied by the target scale.
    YDB Decimal bounds are strict: coefficients with absolute value at least
    ``10**precision`` saturate to the infinity of their sign.  The caller and
    snapshot IR deliberately exclude nullable and non-integral sources.
    """

    if _integer_decimal_digits(source_type) is None:
        raise ValueError(f"Decimal cast source is not integral: {source_type!r}")
    decimal_type = parse_type(result_type)
    if decimal_type is None:
        raise ValueError(f"Decimal cast result is not Decimal: {result_type!r}")
    if decimal_type.integral_digits < 1:
        raise ValueError("Decimal cast result must have at least one integral digit")
    coefficient = smt.mul(value, smt.int_value(10**decimal_type.scale))
    return _saturate_finite(coefficient, decimal_type.precision)


def narrow_same_scale(
    value: smt.Term,
    source_type: str,
    result_type: str,
) -> smt.Term:
    """Exactly SafeCast a Decimal to lower precision at the same scale."""

    source = parse_type(source_type)
    result = parse_type(result_type)
    if source is None or result is None:
        raise ValueError("Decimal narrowing requires Decimal source and result types")
    if source.scale != result.scale or result.precision > source.precision:
        raise ValueError(
            "Decimal narrowing requires the same scale and non-increasing precision"
        )
    return _check_bounds(value, result.precision)


def sum_with_headroom(
    guarded_values: tuple[tuple[smt.Term, smt.Term], ...],
    result_type: str,
    finite_abs_bound: int,
) -> smt.Term:
    """Reduce YDB Decimal aggregate inputs with proven finite headroom.

    ``AggrAdd`` saturates each intermediate result and is not associative when
    finite overflow is possible.  Requiring the sum of absolute finite-input
    bounds to be strictly below the result precision makes every reduction
    order agree: NaN is absorbing, opposite infinities produce NaN, one
    infinity sign wins, and finite codes add exactly.
    """

    decimal_type = parse_type(result_type)
    if decimal_type is None:
        raise ValueError(f"not a Decimal type: {result_type!r}")
    if decimal_type.precision != MAX_PRECISION:
        raise ValueError("Decimal SUM result must have maximum precision")
    if type(finite_abs_bound) is not int or not (
        0 <= finite_abs_bound < 10**decimal_type.precision
    ):
        raise ValueError("Decimal SUM finite bound has insufficient headroom")

    active_nan = []
    active_pos_inf = []
    active_neg_inf = []
    finite_terms = []
    for guard, value in guarded_values:
        active_nan.append(smt.and_(guard, smt.eq(value, smt.int_value(NAN))))
        active_pos_inf.append(smt.and_(guard, smt.eq(value, smt.int_value(INF))))
        active_neg_inf.append(smt.and_(guard, smt.eq(value, smt.int_value(-INF))))
        finite_terms.append(
            smt.ite(
                smt.and_(guard, _normal(value, decimal_type.precision)),
                value,
                smt.ZERO,
            )
        )

    has_nan = smt.or_(*active_nan)
    has_pos_inf = smt.or_(*active_pos_inf)
    has_neg_inf = smt.or_(*active_neg_inf)
    return smt.ite(
        smt.or_(has_nan, smt.and_(has_pos_inf, has_neg_inf)),
        smt.int_value(NAN),
        smt.ite(
            has_pos_inf,
            smt.int_value(INF),
            smt.ite(
                has_neg_inf,
                smt.int_value(-INF),
                smt.add(*finite_terms),
            ),
        ),
    )


def multiply(
    left: smt.Term,
    right: smt.Term,
    result_type: str,
    right_type: str,
) -> smt.Term:
    """Exact ``DecimalMul`` for YQL's deliberately narrow operand shapes.

    The left operand and result have ``result_type``.  A same-type Decimal
    right operand is rescaled with round-to-nearest, ties-to-even; an integral
    right operand multiplies the scaled coefficient directly.  YDB represents
    Decimal specials in-band, so they must be handled before finite arithmetic.
    """

    decimal_type = parse_type(result_type)
    if decimal_type is None:
        raise ValueError(f"not a Decimal type: {result_type!r}")
    right_is_decimal = right_type == result_type
    if not right_is_decimal and _integer_decimal_digits(right_type) is None:
        raise ValueError(
            "Decimal multiplication requires a same-type Decimal or integral right operand"
        )

    product = smt.mul(left, right)
    if right_is_decimal and decimal_type.scale:
        product = _round_divide(product, 10**decimal_type.scale)
    finite_product = _saturate_finite(product, decimal_type.precision)

    left_is_inf = _is_inf(left)
    right_is_inf = _is_inf(right) if right_is_decimal else smt.FALSE
    has_nan = smt.or_(
        smt.eq(left, smt.int_value(NAN)),
        smt.eq(right, smt.int_value(NAN)) if right_is_decimal else smt.FALSE,
    )
    has_inf = smt.or_(left_is_inf, right_is_inf)
    has_zero = smt.or_(smt.eq(left, smt.ZERO), smt.eq(right, smt.ZERO))
    same_sign = smt.eq(smt.lt(left, smt.ZERO), smt.lt(right, smt.ZERO))
    infinite_product = smt.ite(same_sign, smt.int_value(INF), smt.int_value(-INF))
    return smt.ite(
        has_nan,
        smt.int_value(NAN),
        smt.ite(
            has_inf,
            smt.ite(has_zero, smt.int_value(NAN), infinite_product),
            finite_product,
        ),
    )


def divide(
    left: smt.Term,
    right: smt.Term,
    result_type: str,
    right_type: str,
) -> smt.Term:
    """Exact ``DecimalDiv`` for YQL's deliberately narrow operand shapes.

    The left operand and result have ``result_type``.  Dividing by a same-type
    Decimal restores the result scale before applying ``NDecimal::Div``'s
    signed rounding; dividing by an integral value preserves the left
    coefficient's scale.  Specials are handled before the symbolic finite
    quotient, exactly like ``NDecimal::TDecimalDivisor``.
    """

    decimal_type = parse_type(result_type)
    if decimal_type is None:
        raise ValueError(f"not a Decimal type: {result_type!r}")
    right_is_decimal = right_type == result_type
    if not right_is_decimal and _integer_decimal_digits(right_type) is None:
        raise ValueError(
            "Decimal division requires a same-type Decimal or integral right operand"
        )

    numerator = left
    if right_is_decimal and decimal_type.scale:
        numerator = smt.mul(left, smt.int_value(10**decimal_type.scale))
    quotient = _round_ratio(numerator, right)

    # The widened same-Decimal calculation normalizes to the global Decimal
    # representation before the result precision is checked.  In particular,
    # a finite quotient numerically equal to the in-band NaN code is greater
    # than +Inf and therefore normalizes to +Inf, not NaN.
    finite_quotient = _saturate_finite(quotient, MAX_PRECISION)
    finite_quotient = _saturate_finite(finite_quotient, decimal_type.precision)

    left_is_inf = _is_inf(left)
    right_is_inf = _is_inf(right) if right_is_decimal else smt.FALSE
    has_nan = smt.or_(
        smt.eq(left, smt.int_value(NAN)),
        smt.eq(right, smt.int_value(NAN)) if right_is_decimal else smt.FALSE,
    )
    same_sign = smt.eq(smt.lt(left, smt.ZERO), smt.lt(right, smt.ZERO))
    signed_inf = smt.ite(same_sign, smt.int_value(INF), smt.int_value(-INF))
    zero_divisor = smt.ite(
        smt.eq(left, smt.ZERO),
        smt.int_value(NAN),
        smt.ite(
            smt.lt(smt.ZERO, left),
            smt.int_value(INF),
            smt.int_value(-INF),
        ),
    )
    return smt.ite(
        has_nan,
        smt.int_value(NAN),
        smt.ite(
            smt.eq(right, smt.ZERO),
            zero_divisor,
            smt.ite(
                right_is_inf,
                smt.ite(left_is_inf, smt.int_value(NAN), smt.ZERO),
                smt.ite(left_is_inf, signed_inf, finite_quotient),
            ),
        ),
    )


def align(
    left: smt.Term,
    left_type: str,
    right: smt.Term,
    right_type: str,
) -> tuple[smt.Term, smt.Term]:
    """Apply the implicit conversions used by ``DataCompare``."""

    try:
        conversion = _alignment_conversion(left_type, right_type)
    except ValueError as error:
        raise ValueError(
            f"unsupported Decimal comparison alignment: {left_type!r} and {right_type!r}"
        ) from error
    if conversion is None:
        return left, right
    side, source, target = conversion
    if not _cast_supported(source, target):
        raise ValueError(
            f"unsupported Decimal comparison alignment: {left_type!r} and {right_type!r}"
        )
    if side == 0:
        return _cast_decimal(left, source, target), right
    return left, _cast_decimal(right, source, target)


def alignment_supported(left_type: str, right_type: str) -> bool:
    """Whether DataCompare can align without constructing Decimal(0,0)."""

    try:
        conversion = _alignment_conversion(left_type, right_type)
    except ValueError:
        return False
    return conversion is None or _cast_supported(conversion[1], conversion[2])


def _alignment_conversion(left_type: str, right_type: str) -> tuple[int, Type, Type] | None:
    left_decimal = parse_type(left_type)
    right_decimal = parse_type(right_type)
    if left_decimal is not None and right_decimal is not None:
        if left_decimal.scale < right_decimal.scale:
            target = Type(
                min(MAX_PRECISION, left_decimal.precision + right_decimal.scale - left_decimal.scale),
                right_decimal.scale,
            )
            return 0, left_decimal, target
        if right_decimal.scale < left_decimal.scale:
            target = Type(
                min(MAX_PRECISION, right_decimal.precision + left_decimal.scale - right_decimal.scale),
                left_decimal.scale,
            )
            return 1, right_decimal, target
        return None

    if left_decimal is not None:
        digits = _require_integer_decimal_digits(right_type)
        target = Type(min(MAX_PRECISION, digits + left_decimal.scale), left_decimal.scale)
        return 1, Type(digits, 0), target

    if right_decimal is not None:
        digits = _require_integer_decimal_digits(left_type)
        target = Type(min(MAX_PRECISION, digits + right_decimal.scale), right_decimal.scale)
        return 0, Type(digits, 0), target

    raise ValueError("Decimal alignment requires at least one Decimal operand")


def _cast_decimal(value: smt.Term, source: Type, target: Type) -> smt.Term:
    if source.scale > target.scale:
        raise ValueError("comparison alignment must not reduce Decimal scale")
    if target.integral_digits < source.integral_digits and target.scale != source.scale:
        intermediate = Type(target.integral_digits + source.scale, source.scale)
        narrowed = _cast_decimal(value, source, intermediate)
        return _cast_decimal(narrowed, intermediate, target)
    if source.scale < target.scale:
        return _scale_up(value, target.scale - source.scale)
    if target.precision < source.precision:
        return _check_bounds(value, target.precision)
    return value


def _cast_supported(source: Type, target: Type) -> bool:
    return not (
        target.integral_digits < source.integral_digits
        and target.scale != source.scale
        and target.integral_digits + source.scale == 0
    )


def _scale_up(value: smt.Term, places: int) -> smt.Term:
    factor = 10**places
    raw = smt.mul(value, smt.int_value(factor))
    scaled = smt.ite(
        smt.lt(smt.int_value(INF), raw),
        smt.int_value(INF),
        smt.ite(
            smt.lt(raw, smt.int_value(-INF)),
            smt.int_value(-INF),
            raw,
        ),
    )
    return smt.ite(_normal(value, MAX_PRECISION), scaled, value)


def _check_bounds(value: smt.Term, precision: int) -> smt.Term:
    if_normal = _normal(value, precision)
    overflow = smt.ite(
        smt.eq(value, smt.int_value(NAN)),
        smt.int_value(NAN),
        smt.ite(
            smt.lt(smt.ZERO, value),
            smt.int_value(INF),
            smt.int_value(-INF),
        ),
    )
    return smt.ite(if_normal, value, overflow)


def _saturate_finite(value: smt.Term, precision: int) -> smt.Term:
    """Apply a Decimal result bound to known-finite arithmetic.

    A finite calculation may numerically collide with the in-band NaN code.
    YDB normalizes that overflow to infinity before decoding specials, so this
    path deliberately never interprets ``value == NAN`` as NaN.
    """

    return smt.ite(
        _normal(value, precision),
        value,
        smt.ite(
            smt.lt(smt.ZERO, value),
            smt.int_value(INF),
            smt.int_value(-INF),
        ),
    )


def _add_or_subtract(
    kind: str,
    left: smt.Term,
    right: smt.Term,
    result_type: str,
) -> smt.Term:
    decimal_type = parse_type(result_type)
    if decimal_type is None:
        raise ValueError(f"not a Decimal type: {result_type!r}")
    if kind == "add":
        raw = smt.add(left, right)
    elif kind == "sub":
        raw = smt.sub(left, right)
    else:
        raise ValueError(f"unsupported Decimal additive operation {kind!r}")

    all_normal = smt.and_(
        _normal(left, decimal_type.precision),
        _normal(right, decimal_type.precision),
        _normal(raw, decimal_type.precision),
    )
    indeterminate = smt.or_(
        smt.eq(left, smt.int_value(NAN)),
        smt.eq(right, smt.int_value(NAN)),
        smt.eq(raw, smt.ZERO),
    )
    overflow = smt.ite(
        indeterminate,
        smt.int_value(NAN),
        smt.ite(
            smt.lt(smt.ZERO, raw),
            smt.int_value(INF),
            smt.int_value(-INF),
        ),
    )
    return smt.ite(all_normal, raw, overflow)


def _round_divide(value: smt.Term, divisor: int) -> smt.Term:
    """Divide by a positive constant, rounding to nearest with even ties."""

    negative = smt.lt(value, smt.ZERO)
    magnitude = smt.ite(negative, smt.sub(smt.ZERO, value), value)
    quotient = smt.div(magnitude, divisor)
    remainder = smt.mod(magnitude, divisor)
    twice_remainder = smt.mul(remainder, smt.int_value(2))
    round_up = smt.or_(
        smt.lt(smt.int_value(divisor), twice_remainder),
        smt.and_(
            smt.eq(twice_remainder, smt.int_value(divisor)),
            smt.eq(smt.mod(quotient, 2), smt.ONE),
        ),
    )
    rounded = smt.add(quotient, smt.ite(round_up, smt.ONE, smt.ZERO))
    return smt.ite(negative, smt.sub(smt.ZERO, rounded), rounded)


def _round_ratio(numerator: smt.Term, denominator: smt.Term) -> smt.Term:
    """Reproduce ``NDecimal::Div``'s exact signed integer rounding.

    The zero denominator is replaced before constructing SMT ``div``.  The
    public division kernel selects the required special result separately, but
    SMT's totalized division must never influence which quotient convention the
    model uses on that branch.  YDB's signed-remainder algorithm rounds a
    positive divisor to nearest with even ties, but truncates negative-divisor
    non-ties while retaining even-tie rounding.  The asymmetry here is therefore
    intentional rather than an algebraic sign simplification.
    """

    numerator_negative = smt.lt(numerator, smt.ZERO)
    denominator_negative = smt.lt(denominator, smt.ZERO)
    numerator_magnitude = smt.ite(
        numerator_negative,
        smt.sub(smt.ZERO, numerator),
        numerator,
    )
    denominator_magnitude = smt.ite(
        denominator_negative,
        smt.sub(smt.ZERO, denominator),
        denominator,
    )
    positive_denominator = smt.ite(
        smt.eq(denominator_magnitude, smt.ZERO),
        smt.ONE,
        denominator_magnitude,
    )
    quotient = smt.div_nonnegative_by_positive(
        numerator_magnitude,
        positive_denominator,
    )
    remainder = smt.sub(
        numerator_magnitude,
        smt.mul(quotient, positive_denominator),
    )
    twice_remainder = smt.mul(remainder, smt.int_value(2))
    tie_and_odd = smt.and_(
        smt.eq(twice_remainder, positive_denominator),
        smt.eq(smt.mod(quotient, 2), smt.ONE),
    )
    increment = smt.or_(
        tie_and_odd,
        smt.and_(
            smt.lt(smt.ZERO, denominator),
            smt.lt(positive_denominator, twice_remainder),
        ),
    )
    rounded = smt.add(quotient, smt.ite(increment, smt.ONE, smt.ZERO))
    negative = smt.not_(smt.eq(numerator_negative, denominator_negative))
    return smt.ite(negative, smt.sub(smt.ZERO, rounded), rounded)


def _is_inf(value: smt.Term) -> smt.Term:
    return smt.or_(
        smt.eq(value, smt.int_value(-INF)),
        smt.eq(value, smt.int_value(INF)),
    )


def _normal(value: smt.Term, precision: int) -> smt.Term:
    bound = 10**precision
    return smt.and_(smt.lt(smt.int_value(-bound), value), smt.lt(value, smt.int_value(bound)))


def _integer_decimal_digits(scalar_type: str) -> int | None:
    match = _INTEGER_TYPE.fullmatch(scalar_type)
    return None if match is None else _INTEGER_DECIMAL_DIGITS[int(match["bits"])]


def _require_integer_decimal_digits(scalar_type: str) -> int:
    digits = _integer_decimal_digits(scalar_type)
    if digits is None:
        raise ValueError(f"not an integral type: {scalar_type!r}")
    return digits
