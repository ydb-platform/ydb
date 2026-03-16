from __future__ import annotations

import json
import os
import re
import sys
from datetime import date, datetime, time
from typing import Any, BinaryIO, Callable, Dict, List, Match, Optional, Union

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover - fallback for Python < 3.11
    import tomli as tomllib

__version__ = '0.4.0'

RE_ENV_VAR: str = (
    r'\$\$'
    r'|\$\{(?P<braced>[A-Z_][A-Z0-9_]*)(?::-(?P<default>[^}]*))?\}'
    r'|\$(?P<simple>[A-Z_][A-Z0-9_]*)'
)

TOMLDict = Dict[str, 'TOMLValue']
TOMLList = List['TOMLValue']
TOMLPrimitive = Union[str, int, float, bool, datetime, date, time]
TOMLValue = Union[TOMLPrimitive, TOMLDict, TOMLList]
ParseFloat = Callable[[str], float]


def env_replace(match: Match[str], fail_on_missing: bool) -> str:
    if match.group(0) == '$$':
        return '$'
    env_var = match.group('simple') or match.group('braced')
    default = match.group('default')
    value = os.environ.get(env_var)
    if value:
        return value
    if default is not None:
        return default
    if fail_on_missing:
        raise ValueError(f'{env_var} not found in environment')
    return ''


def _load_inline_value(value: str, parse_float: ParseFloat) -> TOMLValue:
    data = tomllib.loads(f'v = {value}', parse_float=parse_float)
    return data['v']


def _replace_env_value(
    value: str,
    parse_float: ParseFloat,
    fail_on_missing: bool,
) -> Optional[TOMLValue]:
    if not re.search(RE_ENV_VAR, value):
        return None

    replaced = re.sub(
        RE_ENV_VAR,
        lambda match: env_replace(match, fail_on_missing),
        value,
    )

    # Try to parse the value as TOML (float, bool, inline table, etc.).
    # If that fails, fall back to a basic string.
    try:
        return _load_inline_value(replaced, parse_float)
    except tomllib.TOMLDecodeError:
        quoted = json.dumps(replaced)
        return _load_inline_value(quoted, parse_float)


def process(
    item: TOMLValue,
    parse_float: ParseFloat,
    fail_on_missing: bool,
) -> None:
    if isinstance(item, dict):
        for key, val in item.items():
            if isinstance(val, (dict, list)):
                process(val, parse_float, fail_on_missing)
            elif isinstance(val, str):
                replaced = _replace_env_value(val, parse_float, fail_on_missing)
                if replaced is not None:
                    item[key] = replaced
    elif isinstance(item, list):
        for index, val in enumerate(item):
            if isinstance(val, (dict, list)):
                process(val, parse_float, fail_on_missing)
            elif isinstance(val, str):
                replaced = _replace_env_value(val, parse_float, fail_on_missing)
                if replaced is not None:
                    item[index] = replaced


def load(
    fp: BinaryIO,
    /,
    *,
    parse_float: ParseFloat = float,
    fail_on_missing: bool = False,
) -> dict[str, Any]:
    """Parse TOML from a binary file object and replace environment variables.

    Args:
        fp: Binary file object to read.
        parse_float: Callable to parse TOML float values.
        fail_on_missing: Raise if an env var is missing or empty.
    """
    data = tomllib.load(fp, parse_float=parse_float)
    process(data, parse_float, fail_on_missing)
    return data


def loads(
    s: str,
    /,
    *,
    parse_float: ParseFloat = float,
    fail_on_missing: bool = False,
) -> dict[str, Any]:
    """Parse TOML from a string and replace environment variables.

    Args:
        s: TOML string to parse.
        parse_float: Callable to parse TOML float values.
        fail_on_missing: Raise if an env var is missing or empty.
    """
    data = tomllib.loads(s, parse_float=parse_float)
    process(data, parse_float, fail_on_missing)
    return data
