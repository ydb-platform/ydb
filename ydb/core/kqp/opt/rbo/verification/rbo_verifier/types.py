"""Exact scalar identities supported by semantic snapshot version one."""

import re

BOOL = "Bool"
DATE = "Date"
VOID = "Void"

# Mirrors NYql::NUdf::MAX_DATE.  Date is an unsigned day-since-epoch value and
# this upper bound is non-inclusive.
MAX_DATE = 49_673

INTEGER_TYPES = frozenset(
    {
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "Uint8",
        "Uint16",
        "Uint32",
        "Uint64",
    }
)

STRING_TYPES = frozenset({"String", "Utf8"})
FIXED_SCALAR_TYPES = frozenset({BOOL, DATE}) | INTEGER_TYPES | STRING_TYPES

_DECIMAL_TYPE = re.compile(
    r"Decimal\((?P<precision>[1-9]|[12][0-9]|3[0-5]),"
    r"(?P<scale>0|[1-9]|[12][0-9]|3[0-5])\)"
)


def is_decimal_type(scalar_type: str) -> bool:
    match = _DECIMAL_TYPE.fullmatch(scalar_type)
    return bool(match) and int(match["scale"]) <= int(match["precision"])


def is_scalar_type(scalar_type: str) -> bool:
    return scalar_type in FIXED_SCALAR_TYPES or is_decimal_type(scalar_type)


def integer_width(scalar_type: str) -> int | None:
    if scalar_type.startswith("Uint"):
        suffix = scalar_type[4:]
    elif scalar_type.startswith("Int"):
        suffix = scalar_type[3:]
    else:
        return None
    return int(suffix) if suffix in {"8", "16", "32", "64"} else None


def integer_comparison_compatible(left: str, right: str) -> bool:
    """Whether YQL's common integer type preserves both operands exactly."""

    left_width = integer_width(left)
    right_width = integer_width(right)
    if left_width is None or right_width is None:
        return False
    left_signed = left.startswith("Int")
    right_signed = right.startswith("Int")
    if left_signed == right_signed:
        return True
    signed_width = left_width if left_signed else right_width
    unsigned_width = right_width if left_signed else left_width
    return signed_width > unsigned_width


def equality_comparison_compatible(left: str, right: str) -> bool:
    """Whether ordinary equality compares both scalar values without loss."""

    return left == right or integer_comparison_compatible(left, right)


def family(scalar_type: str) -> str:
    if scalar_type == VOID:
        return "unit"
    if scalar_type == BOOL:
        return "bool"
    if scalar_type in INTEGER_TYPES:
        return "int"
    if scalar_type in STRING_TYPES:
        return "string"
    if scalar_type == DATE:
        return "date"
    if is_decimal_type(scalar_type):
        return "atom"
    raise ValueError(f"unsupported scalar type {scalar_type!r}")


def is_ordered_type(scalar_type: str) -> bool:
    return scalar_type in INTEGER_TYPES or scalar_type == DATE
