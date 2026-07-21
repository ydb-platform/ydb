"""Exact scalar identities supported by semantic snapshot version one."""

BOOL = "Bool"

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
SCALAR_TYPES = frozenset({BOOL}) | INTEGER_TYPES | STRING_TYPES


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


def family(scalar_type: str) -> str:
    if scalar_type == BOOL:
        return "bool"
    if scalar_type in INTEGER_TYPES:
        return "int"
    if scalar_type in STRING_TYPES:
        return "string"
    raise ValueError(f"unsupported scalar type {scalar_type!r}")
