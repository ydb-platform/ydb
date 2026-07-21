"""Exact YDB Decimal values needed by scalar comparison verification.

Decimal values are signed, scaled integers.  The three non-finite values use
the same codes as ``NYql::NDecimal``; keeping those codes explicit makes the
SMT model match YDB comparisons and casts without introducing SMT Reals.
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
