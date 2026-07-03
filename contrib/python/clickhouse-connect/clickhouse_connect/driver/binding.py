import ipaddress
import re
import uuid
import zoneinfo
from collections.abc import Sequence
from datetime import date, datetime, timezone, tzinfo
from enum import Enum
from typing import Any
from urllib.parse import quote, urlencode

from clickhouse_connect import common
from clickhouse_connect.driver import tzutil
from clickhouse_connect.driver.common import dict_copy
from clickhouse_connect.driver.parser import parse_callable
from clickhouse_connect.json_impl import any_to_json

BS = "\\"
must_escape = (BS, "'", "`", "\t", "\n")
external_bind_re = re.compile(r"\{(\w+):([^}]+)\}")


class DT64Param:
    def __init__(self, value: datetime):
        self.value = value

    def format(self, tz: tzinfo, top_level: bool) -> str:
        value = self.value
        if tz:
            value = value.astimezone(tz)
        s = value.strftime("%Y-%m-%d %H:%M:%S.%f")
        if top_level:
            return s
        return f"'{s}'"


def quote_identifier(identifier: str) -> str:
    if len(identifier) >= 2:
        quote = identifier[0]
        if quote in ("`", '"') and identifier[-1] == quote and _is_validly_quoted(identifier, quote):
            return identifier
    return f"`{escape_str(identifier)}`"


def _is_validly_quoted(identifier: str, quote: str) -> bool:
    # Accepts backslash escapes (\X) and doubled-quote escapes (`` or "").
    i, end = 1, len(identifier) - 1
    while i < end:
        c = identifier[i]
        if c == "\\":
            if i + 1 >= end:
                return False
            i += 2
        elif c == quote:
            if i + 1 < end and identifier[i + 1] == quote:
                i += 2
            else:
                return False
        else:
            i += 1
    return True


def finalize_query(query: str, parameters: Sequence | dict[str, Any] | None, server_tz: tzinfo | None = None) -> str:
    query = query.rstrip(";")
    if not parameters:
        return query
    if hasattr(parameters, "items"):
        return query % {k: format_query_value(v, server_tz) for k, v in parameters.items()}
    return query % tuple(format_query_value(v, server_tz) for v in parameters)


def _unwrap_outer(type_str: str) -> tuple[str, tuple]:
    """Strip LowCardinality/Nullable wrappers and return (base_name, args)"""
    base = type_str.strip()
    if base[:15].lower() == "lowcardinality(":
        base = base[15:-1]
    if base[:9].lower() == "nullable(":
        base = base[9:-1]
    base_name, values, _ = parse_callable(base)
    return base_name, values


def _extract_tz_from_type(type_str: str) -> tzinfo | None:
    """Resolve the timezone named in a ClickHouse type hint."""
    try:
        base_name, values = _unwrap_outer(type_str)
        if base_name.lower() in ("datetime", "datetime64"):
            for v in values:
                if isinstance(v, str) and v.startswith("'") and v.endswith("'"):
                    try:
                        return tzutil.resolve_zone(v[1:-1])
                    except zoneinfo.ZoneInfoNotFoundError:
                        return None
            return None

        if values:
            for v in values:
                if isinstance(v, str):
                    tz = _extract_tz_from_type(v)
                    if tz is not None:
                        return tz

        return None
    except Exception:
        return None


def _promote_datetime64(type_str: str, value):
    """Wrap values bound to a DateTime64 hint in DT64Param to preserve precision."""
    if value is None or "datetime64" not in type_str.lower():
        return value
    try:
        base_name, values = _unwrap_outer(type_str)
        base_name = base_name.lower()
        if base_name == "datetime64":
            return DT64Param(value) if isinstance(value, datetime) else value
        if base_name == "array" and values and isinstance(value, (list, tuple)):
            inner = str(values[0])
            return type(value)(_promote_datetime64(inner, x) for x in value)
        if base_name == "tuple" and isinstance(value, tuple) and len(values) == len(value):
            return tuple(_promote_datetime64(str(t), x) for t, x in zip(values, value))
        return value
    except Exception:
        return value


def bind_query(
    query: str,
    parameters: Sequence | dict[str, Any] | None,
    server_tz: tzinfo | None = None,
) -> tuple[str, dict[str, str]]:
    query = query.rstrip(";")
    if not parameters:
        return query, {}

    binary_binds = None

    if isinstance(parameters, dict):
        params_copy = dict_copy(parameters)
        binary_binds = {k: v for k, v in params_copy.items() if k.startswith("$") and k.endswith("$") and len(k) > 1}
        for key in binary_binds.keys():
            del params_copy[key]

        matches = external_bind_re.findall(query)
        placeholder_names = {name for name, _ in matches}
        final_params = {}
        for k, v in params_copy.items():
            # The _64 suffix is a precision hint, not part of the name, unless the
            # query binds the full name itself.
            if k.endswith("_64") and k not in placeholder_names:
                if isinstance(v, datetime):
                    k = k[:-3]
                    v = DT64Param(v)
                elif isinstance(v, list) and len(v) > 0 and isinstance(v[0], datetime):
                    k = k[:-3]
                    v = [DT64Param(x) for x in v]
            final_params[k] = v
        if not matches:
            query, bound_params = finalize_query(query, final_params, server_tz), {}
        else:
            param_types = {}
            for name, type_str in matches:
                if name not in param_types:
                    param_types[name] = type_str
            bound_params = {}
            for k, v in final_params.items():
                tz = server_tz
                type_str = param_types.get(k)
                if type_str is not None:
                    hint_tz = _extract_tz_from_type(type_str)
                    if hint_tz is not None:
                        tz = hint_tz
                    v = _promote_datetime64(type_str, v)
                bound_params[f"param_{k}"] = format_bind_value(v, tz)
    else:
        query, bound_params = finalize_query(query, parameters, server_tz), {}
    if binary_binds:
        binary_query = query.encode()
        binary_indexes = {}
        for k, v in binary_binds.items():
            key = k.encode()
            item_index = 0
            while True:
                item_index = binary_query.find(key, item_index)
                if item_index == -1:
                    break
                binary_indexes[item_index + len(key)] = key, v
                item_index += len(key)
        query = b""
        start = 0
        for loc in sorted(binary_indexes.keys()):
            key, value = binary_indexes[loc]
            query += binary_query[start:loc] + value + key
            start = loc
        query += binary_query[start:]
    return query, bound_params


# Server-side bind parameters are urlencoded into the request URL. Once the encoded length
# passes this budget the client routes them through multipart form data instead, keeping
# oversized payloads out of the URL where proxies (nginx, ALB, CloudFront) reject them with
# HTTP 414. The threshold leaves ample headroom under common request line limits.
MAX_URL_BIND_PARAM_LENGTH = 4096


def use_form_encoding(query: str | bytes, bind_params: dict[str, str], force_form: bool = False) -> bool:
    if force_form:
        return True
    # Binary binds embed bytes into the query, which the form path cannot round-trip; leave
    # those on the default path unless form encoding is explicitly requested.
    if isinstance(query, bytes):
        return False
    if not bind_params:
        return False
    # Raw length is a lower bound on the encoded length, so large payloads short-circuit
    # without materializing the encoded string.
    if sum(len(k) + len(str(v)) for k, v in bind_params.items()) > MAX_URL_BIND_PARAM_LENGTH:
        return True
    # Measure with quote so spaces count as %20, matching the longer of the two client encodings.
    return len(urlencode(bind_params, quote_via=quote)) > MAX_URL_BIND_PARAM_LENGTH


def format_str(value: str):
    return f"'{escape_str(value)}'"


def escape_str(value: str):
    return "".join(f"{BS}{c}" if c in must_escape else c for c in value)


def escape_bytes(value):
    return "".join(f"{BS}x{b:02x}" for b in value)


def format_query_value(value: Any, server_tz: tzinfo = timezone.utc):
    """
    Format Python values in a ClickHouse query
    :param value: Python object
    :param server_tz: Server timezone for adjusting datetime values
    :return: Literal string for python value
    """
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return format_str(value)
    if isinstance(value, (bytes, bytearray)):
        return f"'{escape_bytes(value)}'"
    if isinstance(value, DT64Param):
        return value.format(server_tz, False)
    if isinstance(value, datetime):
        if value.tzinfo is not None or not tzutil.is_utc_timezone(server_tz):
            value = value.astimezone(server_tz)
        return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
    if isinstance(value, date):
        return f"'{value.isoformat()}'"
    if isinstance(value, list):
        return f"[{', '.join(str_query_value(x, server_tz) for x in value)}]"
    if isinstance(value, tuple):
        return f"({', '.join(str_query_value(x, server_tz) for x in value)})"
    if isinstance(value, dict):
        if common.get_setting("dict_parameter_format") == "json":
            return format_str(any_to_json(value).decode())
        pairs = [str_query_value(k, server_tz) + ":" + str_query_value(v, server_tz) for k, v in value.items()]
        return f"{{{', '.join(pairs)}}}"
    if isinstance(value, Enum):
        return format_query_value(value.value, server_tz)
    if isinstance(value, (uuid.UUID, ipaddress.IPv4Address, ipaddress.IPv6Address)):
        return f"'{value}'"
    return value


def str_query_value(value: Any, server_tz: tzinfo = timezone.utc):
    return str(format_query_value(value, server_tz))


def format_bind_value(value: Any, server_tz: tzinfo = timezone.utc, top_level: bool = True):
    """
    Format Python values in a ClickHouse query
    :param value: Python object
    :param server_tz: Server timezone for adjusting datetime values
    :param top_level: Flag for top level for nested structures
    :return: Literal string for python value
    """

    def recurse(x):
        return format_bind_value(x, server_tz, False)

    if value is None:
        return "\\N"
    if isinstance(value, str):
        if top_level:
            # At the top levels, strings must not be surrounded by quotes
            return escape_str(value)
        return format_str(value)
    if isinstance(value, (bytes, bytearray)):
        if top_level:
            return escape_bytes(value)
        return f"'{escape_bytes(value)}'"
    if isinstance(value, DT64Param):
        return value.format(server_tz, top_level)
    if isinstance(value, datetime):
        value = value.astimezone(server_tz)
        val = value.strftime("%Y-%m-%d %H:%M:%S")
        if top_level:
            return val
        return f"'{val}'"
    if isinstance(value, date):
        if top_level:
            return value.isoformat()
        return f"'{value.isoformat()}'"
    if isinstance(value, list):
        return f"[{', '.join(recurse(x) for x in value)}]"
    if isinstance(value, tuple):
        return f"({', '.join(recurse(x) for x in value)})"
    if isinstance(value, dict):
        if common.get_setting("dict_parameter_format") == "json":
            return any_to_json(value).decode()
        pairs = [recurse(k) + ":" + recurse(v) for k, v in value.items()]
        return f"{{{', '.join(pairs)}}}"
    if isinstance(value, Enum):
        return recurse(value.value)
    if isinstance(value, (uuid.UUID, ipaddress.IPv4Address, ipaddress.IPv6Address)):
        if top_level:
            return str(value)
        return f"'{value}'"
    return str(value)
