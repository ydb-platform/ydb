import logging

from clickhouse_connect.datatypes.base import ClickHouseType, TypeDef, type_map
from clickhouse_connect.driver.exceptions import InternalError
from clickhouse_connect.driver.parser import parse_callable, parse_columns, parse_enum

logger = logging.getLogger(__name__)
type_cache: dict[str, ClickHouseType] = {}


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
    keys = tuple()
    if base.startswith("LowCardinality"):
        wrappers.append("LowCardinality")
        base = base[15:-1]
    if base.startswith("Nullable"):
        wrappers.append("Nullable")
        base = base[9:-1]
    if base.startswith("Enum"):
        keys, values = parse_enum(base)
        base = base[: base.find("(")]
    elif base.startswith("Nested"):
        keys, values = parse_columns(base[6:])
        base = "Nested"
    elif base.startswith("Tuple"):
        keys, values = parse_columns(base[5:])
        base = "Tuple"
    elif base.startswith("Variant"):
        keys, values = parse_columns(base[7:])
        base = "Variant"
    elif base.startswith("JSON") and len(base) > 4 and base[4] == "(":
        keys, values = parse_columns(base[4:])
        base = "JSON"
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
