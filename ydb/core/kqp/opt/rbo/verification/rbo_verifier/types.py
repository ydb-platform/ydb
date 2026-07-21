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


def family(scalar_type: str) -> str:
    if scalar_type == BOOL:
        return "bool"
    if scalar_type in INTEGER_TYPES:
        return "int"
    if scalar_type in STRING_TYPES:
        return "string"
    raise ValueError(f"unsupported scalar type {scalar_type!r}")
