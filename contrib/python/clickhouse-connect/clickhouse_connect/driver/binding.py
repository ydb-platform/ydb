import ipaddress
import re
import uuid
from datetime import tzinfo, datetime, date
from enum import Enum
from typing import Optional, Union, Sequence, Dict, Any, Tuple

import pytz

from clickhouse_connect import common
from clickhouse_connect.driver.common import dict_copy
from clickhouse_connect.json_impl import any_to_json

BS = '\\'
must_escape = (BS, '\'', '`', '\t', '\n')
external_bind_re = re.compile(r'{.+:.+}')


class DT64Param:
    def __init__(self, value: datetime):
        self.value = value

    def format(self, tz: tzinfo, top_level:bool) -> str:
        value = self.value
        if tz:
            value = value.astimezone(tz)
        s = value.strftime('%Y-%m-%d %H:%M:%S.%f')
        if top_level:
            return s
        return f"'{s}'"


def quote_identifier(identifier: str):
    first_char = identifier[0]
    if first_char in ('`', '"') and identifier[-1] == first_char:
        # Identifier is already quoted, assume that it's valid
        return identifier
    return f'`{escape_str(identifier)}`'


def finalize_query(query: str, parameters: Optional[Union[Sequence, Dict[str, Any]]],
                   server_tz: Optional[tzinfo] = None) -> str:
    query = query.rstrip(";")
    if not parameters:
        return query
    if hasattr(parameters, 'items'):
        return query % {k: format_query_value(v, server_tz) for k, v in parameters.items()}
    return query % tuple(format_query_value(v, server_tz) for v in parameters)


# pylint: disable=too-many-locals,too-many-branches
def bind_query(query: str, parameters: Optional[Union[Sequence, Dict[str, Any]]],
               server_tz: Optional[tzinfo] = None) -> Tuple[str, Dict[str, str]]:
    query = query.rstrip(";")
    if not parameters:
        return query, {}

    binary_binds = None

    if isinstance(parameters, dict):
        params_copy = dict_copy(parameters)
        binary_binds = {k: v for k, v in params_copy.items() if k.startswith('$') and k.endswith('$') and len(k) > 1}
        for key in binary_binds.keys():
            del params_copy[key]

        final_params = {}
        for k, v in params_copy.items():
            if k.endswith('_64'):
                if isinstance(v, datetime):
                    k = k[:-3]
                    v = DT64Param(v)
                elif isinstance(v, list) and len(v) > 0 and isinstance(v[0], datetime):
                    k = k[:-3]
                    v = [DT64Param(x) for x in v]
            final_params[k] = v
        if external_bind_re.search(query) is None:
            query, bound_params = finalize_query(query, final_params, server_tz), {}
        else:
            bound_params = {f'param_{k}': format_bind_value(v, server_tz) for k, v in final_params.items()}
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
        query = b''
        start = 0
        for loc in sorted(binary_indexes.keys()):
            key, value = binary_indexes[loc]
            query += binary_query[start:loc] + value + key
            start = loc
        query += binary_query[start:]
    return query, bound_params


def format_str(value: str):
    return f"'{escape_str(value)}'"


def escape_str(value: str):
    return ''.join(f'{BS}{c}' if c in must_escape else c for c in value)


# pylint: disable=too-many-return-statements
def format_query_value(value: Any, server_tz: tzinfo = pytz.UTC):
    """
    Format Python values in a ClickHouse query
    :param value: Python object
    :param server_tz: Server timezone for adjusting datetime values
    :return: Literal string for python value
    """
    if value is None:
        return 'NULL'
    if isinstance(value, str):
        return format_str(value)
    if isinstance(value, DT64Param):
        return value.format(server_tz, False)
    if isinstance(value, datetime):
        if value.tzinfo is not None or server_tz != pytz.UTC:
            value = value.astimezone(server_tz)
        return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
    if isinstance(value, date):
        return f"'{value.isoformat()}'"
    if isinstance(value, list):
        return f"[{', '.join(str_query_value(x, server_tz) for x in value)}]"
    if isinstance(value, tuple):
        return f"({', '.join(str_query_value(x, server_tz) for x in value)})"
    if isinstance(value, dict):
        if common.get_setting('dict_parameter_format') == 'json':
            return format_str(any_to_json(value).decode())
        pairs = [str_query_value(k, server_tz) + ':' + str_query_value(v, server_tz)
                 for k, v in value.items()]
        return f"{{{', '.join(pairs)}}}"
    if isinstance(value, Enum):
        return format_query_value(value.value, server_tz)
    if isinstance(value, (uuid.UUID, ipaddress.IPv4Address, ipaddress.IPv6Address)):
        return f"'{value}'"
    return value


def str_query_value(value: Any, server_tz: tzinfo = pytz.UTC):
    return str(format_query_value(value, server_tz))


# pylint: disable=too-many-branches
def format_bind_value(value: Any, server_tz: tzinfo = pytz.UTC, top_level: bool = True):
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
        return '\\N'
    if isinstance(value, str):
        if top_level:
            # At the top levels, strings must not be surrounded by quotes
            return escape_str(value)
        return format_str(value)
    if isinstance(value, DT64Param):
        return value.format(server_tz, top_level)
    if isinstance(value, datetime):
        value = value.astimezone(server_tz)
        val = value.strftime('%Y-%m-%d %H:%M:%S')
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
        if common.get_setting('dict_parameter_format') == 'json':
            return any_to_json(value).decode()
        pairs = [recurse(k) + ':' + recurse(v)
                 for k, v in value.items()]
        return f"{{{', '.join(pairs)}}}"
    if isinstance(value, Enum):
        return recurse(value.value)
    return str(value)
