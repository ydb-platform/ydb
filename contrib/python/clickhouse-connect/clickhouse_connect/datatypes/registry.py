import logging
from typing import Any

from clickhouse_connect.datatypes.base import ClickHouseType, TypeDef, _TypeArgs, type_map
from clickhouse_connect.driver.exceptions import InternalError
from clickhouse_connect.driver.parser import parse_callable, parse_columns, parse_enum

logger = logging.getLogger(__name__)
type_cache: dict[str, ClickHouseType] = {}
_WRAPPER_TYPE_ARGS = {
    "LowCardinality": _TypeArgs(1, 1, nested=(0,)),
    "Nullable": _TypeArgs(1, 1, nested=(0,)),
}


def _canonicalize_variant_name(type_name: str, ch_type: ClickHouseType) -> str:
    """Use the parsed name when a descendant Variant needs canonical ordering."""
    quote: str | None = None
    pos = 0
    while pos < len(type_name):
        char = type_name[pos]
        if quote:
            if char == "\\":
                pos += 2
                continue
            if char == quote and pos + 1 < len(type_name) and type_name[pos + 1] == quote:
                pos += 2
                continue
            if char == quote:
                quote = None
            pos += 1
            continue
        if char in ('"', "'", "`"):
            quote = char
            pos += 1
            continue
        if char.isalpha():
            end = pos + 1
            while end < len(type_name) and (type_name[end].isalnum() or type_name[end] == "_"):
                end += 1
            next_pos = end
            while next_pos < len(type_name) and type_name[next_pos].isspace():
                next_pos += 1
            if type_name[pos:end] == "Variant" and next_pos < len(type_name) and type_name[next_pos] == "(":
                return ch_type.name
            pos = end
            continue
        pos += 1
    return type_name


def parse_name(name: str) -> tuple[str, str, TypeDef]:
    """
    Converts a ClickHouse type name into the base class and the definition (TypeDef) needed for any
    additional instantiation
    :param name: ClickHouse type name as returned by clickhouse
    :return: The original base name (before arguments), the full name as passed in and the TypeDef object that
     captures any additional arguments
    """
    base = name
    wrappers = []
    keys: tuple[Any, ...] = ()
    values: tuple[Any, ...] = ()
    if base.startswith("LowCardinality"):
        wrappers.append("LowCardinality")
        base = base[15:-1]
    if base.startswith("Nullable"):
        wrappers.append("Nullable")
        base = base[9:-1]
    if base.startswith("Enum"):
        keys, values = parse_enum(base)
        base = base[: base.find("(")]
        if base == "Enum":
            if all(-128 <= value <= 127 for value in values):
                base = "Enum8"
            elif all(-32768 <= value <= 32767 for value in values):
                base = "Enum16"
            else:
                raise InternalError("Generic Enum values must fit in Enum16")
    elif base.startswith("Nested"):
        keys, values = parse_columns(base[6:])
        if not values:
            raise InternalError("Nested must contain at least one type")
        base = "Nested"
    elif base.startswith("Tuple"):
        if base != "Tuple":
            keys, values = parse_columns(base[5:])
        base = "Tuple"
    elif base.startswith("Variant"):
        keys, values = parse_columns(base[7:])
        if not values:
            raise InternalError("Variant must contain at least one type")
        base = "Variant"
    elif base.startswith("Dynamic") and len(base) > 7 and base[7] == "(":
        keys, values = parse_columns(base[7:])
        base = "Dynamic"
    elif base[:4].lower() == "json" and (len(base) == 4 or base[4] == "("):
        if len(base) > 4:
            keys, values = parse_columns(base[4:], preserve_names=True)
        base = "JSON"
    elif base == "GEOMETRY":
        base = "Geometry"
    elif base == "Point":
        values = ("Float64", "Float64")
    else:
        try:
            base, values, _ = parse_callable(base)
        except IndexError:
            raise InternalError(f"Can not parse ClickHouse data type: {name}") from None
    return base, name, TypeDef(tuple(wrappers), keys, values)


def get_from_name(name: str) -> ClickHouseType:
    """
    Returns the ClickHouseType instance parsed from the ClickHouse type name.  Instances are cached
    :param name: ClickHouse type name as returned by ClickHouse in WithNamesAndTypes FORMAT or the Native protocol
    :return: The instance of the ClickHouse Type
    """
    ch_type = type_cache.get(name, None)
    if not ch_type:
        base, name, type_def = parse_name(name)
        try:
            ch_type = type_map[base].build(type_def)
        except KeyError:
            err_str = f"Unrecognized ClickHouse type base: {base} name: {name}"
            logger.error(err_str)
            raise InternalError(err_str) from None
        type_cache[name] = ch_type
    return ch_type
