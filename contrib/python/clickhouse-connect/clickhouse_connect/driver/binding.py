import ipaddress
import re
import uuid
import zoneinfo
from collections.abc import Collection, Sequence
from datetime import date, datetime, time, timedelta, timezone, tzinfo
from enum import Enum
from typing import Any
from urllib.parse import quote, urlencode

from clickhouse_connect import common
from clickhouse_connect.driver import tzutil
from clickhouse_connect.driver.common import dict_copy
from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.parser import parse_callable
from clickhouse_connect.json_impl import any_to_json

BS = "\\"
must_escape = (BS, "'", "`", "\t", "\n")
# ClickHouse substitution names are ASCII BareWord tokens. Digit-led tokens promote to
# a bareword only through ASCII letters, digits, and underscores and never absorb `$`.
_BIND_NAME_PATTERN = r"(?:[A-Za-z_][A-Za-z0-9_$]*|\$[A-Za-z0-9_][A-Za-z0-9_$]*|[0-9]+[A-Za-z_][A-Za-z0-9_]*)"
_bind_name_re = re.compile(rf"{_BIND_NAME_PATTERN}\Z")
# Routing keeps the shipped 1.x `\w+` names in the union so decoy placeholders such as
# `{13:Int32}` or `{idé:Int32}` still select server-side binding. `\w` cannot absorb `$`.
external_bind_re = re.compile(rf"\{{({_BIND_NAME_PATTERN}|\w+):([^}}]+)\}}")
_heredoc_re = re.compile(r"\$[A-Za-z0-9_]*\$")
_heredoc_start_re = re.compile(r"(?=(\$[A-Za-z0-9_]*\$))")
_quote_closers = {"'": "'", '"': '"', "`": "`"}
_curly_quote_closers = {"\u2018": "\u2019", "\u201c": "\u201d"}
_SQL_WHITESPACE = (
    " \t\n\r\v\f"
    "\u0085\u00a0\u180e"
    "\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u200b\u200c\u200d"
    "\u2028\u2029\u202f\u205f\u2060\u3000\ufeff"
)


def _skip_quoted_token(query: str, index: int, quote: str) -> int:
    index += 1
    end = len(query)
    closer = query.find(quote, index)
    while closer != -1:
        backslash = query.find("\\", index, closer)
        if backslash != -1:
            index = backslash + 2
            if index > closer:
                closer = query.find(quote, index)
            continue
        if closer + 1 < end and query[closer + 1] == quote:
            index = closer + 2
            closer = query.find(quote, index)
            continue
        return closer + 1
    return -1


def _skip_block_comment(query: str, index: int) -> int:
    depth = 1
    index += 2
    end = len(query)
    while index < end:
        if query.startswith("/*", index):
            depth += 1
            index += 2
        elif query.startswith("*/", index):
            depth -= 1
            index += 2
            if depth == 0:
                return index
        else:
            index += 1
    return -1


_SQL_TOKEN_INVALID = 0
_SQL_TOKEN_TRIVIA = 1
_SQL_TOKEN_WORD = 2
_SQL_TOKEN_SEMICOLON = 3
_SQL_TOKEN_OPEN_PAREN = 4
_SQL_TOKEN_CLOSE_PAREN = 5
_SQL_TOKEN_OTHER = 6


def _skip_ascii_bareword(query: str, index: int) -> int:
    end = len(query)
    first = query[index]
    index += 1
    if "0" <= first <= "9":
        while index < end and "0" <= query[index] <= "9":
            index += 1
        if index >= end or not (query[index] == "_" or "A" <= query[index] <= "Z" or "a" <= query[index] <= "z"):
            return index
        index += 1
        while index < end and _is_ascii_word_char(query[index]):
            index += 1
        return index

    while index < end and _is_ascii_bareword_char(query[index]):
        index += 1
    return index


def _next_sql_token(query: str, index: int, heredoc_ends: dict[str, int]) -> tuple[int, int]:
    """Return the next SQL token kind and its exclusive end offset."""
    end = len(query)
    char = query[index]
    if char in _SQL_WHITESPACE:
        index += 1
        while index < end and query[index] in _SQL_WHITESPACE:
            index += 1
        return _SQL_TOKEN_TRIVIA, index

    curly_close = _curly_quote_closers.get(char)
    if curly_close is not None:
        close = query.find(curly_close, index + 1)
        return (_SQL_TOKEN_INVALID, end) if close == -1 else (_SQL_TOKEN_OTHER, close + 1)

    quote_close = _quote_closers.get(char)
    if quote_close is not None:
        close = _skip_quoted_token(query, index, quote_close)
        return (_SQL_TOKEN_INVALID, end) if close == -1 else (_SQL_TOKEN_OTHER, close)

    if query.startswith("/*", index):
        close = _skip_block_comment(query, index)
        return (_SQL_TOKEN_INVALID, end) if close == -1 else (_SQL_TOKEN_TRIVIA, close)

    if (
        query.startswith("--", index)
        or query.startswith("//", index)
        or (char == "#" and index + 1 < end and query[index + 1] in (" ", "!"))
    ):
        newline = query.find("\n", index + 1)
        return _SQL_TOKEN_TRIVIA, end if newline == -1 else newline + 1

    if char == "{":
        placeholder = external_bind_re.match(query, index)
        if placeholder is not None:
            return _SQL_TOKEN_OTHER, placeholder.end()

    if char == ";":
        return _SQL_TOKEN_SEMICOLON, index + 1
    if char == "(":
        return _SQL_TOKEN_OPEN_PAREN, index + 1
    if char == ")":
        return _SQL_TOKEN_CLOSE_PAREN, index + 1

    if char == "$":
        opener = _heredoc_re.match(query, index)
        if opener is not None:
            tag = opener.group()
            if heredoc_ends.get(tag, -1) >= opener.end():
                close = query.find(tag, opener.end())
                return _SQL_TOKEN_OTHER, close + len(tag)
        index += 1
        if index < end and _is_ascii_word_char(query[index]):
            index += 1
            while index < end and _is_ascii_bareword_char(query[index]):
                index += 1
        return _SQL_TOKEN_OTHER, index

    if char == "_" or "A" <= char <= "Z" or "a" <= char <= "z":
        return _SQL_TOKEN_WORD, _skip_ascii_bareword(query, index)
    if "0" <= char <= "9":
        return _SQL_TOKEN_OTHER, _skip_ascii_bareword(query, index)
    return _SQL_TOKEN_OTHER, index + 1


def _strip_trailing_semicolons(query: str) -> str:
    """Remove query-final statement terminators while preserving trailing SQL trivia.

    Only for queries that get a FORMAT clause appended. Must not see inline INSERT
    data, which the server never lexes. Token rules mirror _external_bind_matches.
    """
    if ";" not in query:
        return query

    # _heredoc_start_re finds all overlapping starts, so every _heredoc_re match key exists.
    heredoc_ends = {match.group(1): match.start() for match in _heredoc_start_re.finditer(query)} if "$" in query else {}
    terminators: list[int] = []
    index = 0
    end = len(query)
    while index < end:
        token, token_end = _next_sql_token(query, index, heredoc_ends)
        if token == _SQL_TOKEN_INVALID:
            return query
        if token == _SQL_TOKEN_TRIVIA:
            index = token_end
            continue
        if token == _SQL_TOKEN_SEMICOLON:
            terminators.append(index)
            index = token_end
            continue

        terminators.clear()
        index = token_end

    if not terminators:
        return query
    parts = []
    start = 0
    for position in terminators:
        parts.append(query[start:position])
        start = position + 1
    parts.append(query[start:])
    return "".join(parts)


def _needs_trailing_semicolon_lexer(query: str) -> bool:
    """Return whether query-final terminators need the SQL lexer."""
    if ";" not in query:
        return False
    if not query.endswith(";"):
        return True
    final_run = len(query) - 1
    while final_run > 0 and query[final_run - 1] == ";":
        final_run -= 1
    return query.rfind(";", 0, final_run) != -1


def _is_valid_bind_name(name: str) -> bool:
    return _bind_name_re.fullmatch(name) is not None


def _is_binary_bind_name(name: str) -> bool:
    return len(name) > 1 and name.startswith("$") and name.endswith("$")


def _is_ascii_word_char(char: str) -> bool:
    return char == "_" or "0" <= char <= "9" or "A" <= char <= "Z" or "a" <= char <= "z"


def _is_ascii_bareword_char(char: str) -> bool:
    return char == "$" or _is_ascii_word_char(char)


def _next_keyword_is_into(query: str, index: int, heredoc_ends: dict[str, int]) -> bool:
    end = len(query)
    while index < end:
        token, token_end = _next_sql_token(query, index, heredoc_ends)
        if token == _SQL_TOKEN_TRIVIA:
            index = token_end
            continue
        return token == _SQL_TOKEN_WORD and query[index:token_end].upper() == "INTO"
    return False


_INSERT_BOUNDARY_CHARS = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_$")


def _contains_insert_bareword(query: str) -> bool:
    upper_query = query.upper()
    index = upper_query.find("INSERT")
    while index != -1:
        keyword_end = index + 6
        before_is_word = index > 0 and upper_query[index - 1] in _INSERT_BOUNDARY_CHARS
        after_is_word = keyword_end < len(upper_query) and upper_query[keyword_end] in _INSERT_BOUNDARY_CHARS
        if not before_is_word and not after_is_word:
            return True
        index = upper_query.find("INSERT", keyword_end)
    return False


def _query_is_insert(query: str) -> bool:
    """Return whether the leading statement is INSERT, including WITH prefixes.

    Keywords are not reserved, so a bareword INSERT counts only when followed
    by INTO, a pair identifiers can never legally form.
    """
    index = 0
    end = len(query)

    # Ordinary statements return after their first token. WITH needs the deeper scan below.
    while index < end:
        token, token_end = _next_sql_token(query, index, {})
        if token == _SQL_TOKEN_TRIVIA:
            index = token_end
            continue
        if token != _SQL_TOKEN_WORD:
            return False
        keyword = query[index:token_end].upper()
        if keyword == "INSERT":
            return _next_keyword_is_into(query, token_end, {})
        if keyword != "WITH":
            return False
        index = token_end
        break
    else:
        return False

    if not _contains_insert_bareword(query):
        return False
    heredoc_ends = (
        {match.group(1): match.start() for match in _heredoc_start_re.finditer(query, index)} if query.find("$", index) != -1 else {}
    )
    depth = 0
    while index < end:
        token, token_end = _next_sql_token(query, index, heredoc_ends)
        if token == _SQL_TOKEN_INVALID:
            return False
        if token == _SQL_TOKEN_OPEN_PAREN:
            depth += 1
            index = token_end
            continue
        if token == _SQL_TOKEN_CLOSE_PAREN:
            depth = max(0, depth - 1)
            index = token_end
            continue
        if (
            token == _SQL_TOKEN_WORD
            and not depth
            and query[index:token_end].upper() == "INSERT"
            and _next_keyword_is_into(query, token_end, heredoc_ends)
        ):
            return True
        index = token_end
    return False


def _external_bind_matches(query: str, marker_keys: set[str]) -> tuple[list[re.Match[str]], dict[str, set[int]]]:
    matches = []
    raw_markers: dict[str, set[int]] = {}
    last_heredoc_starts = {match.group(1): match.start() for match in _heredoc_start_re.finditer(query)}
    index = 0
    end = len(query)
    while index < end:
        char = query[index]
        curly_close = _curly_quote_closers.get(char)
        if curly_close is not None:
            close = query.find(curly_close, index + 1)
            index = end if close == -1 else close + 1
            continue

        quote_close = _quote_closers.get(char)
        if quote_close is not None:
            index += 1
            while index < end:
                if query[index] == "\\" and index + 1 < end:
                    index += 2
                elif query[index] == quote_close:
                    if index + 1 < end and query[index + 1] == quote_close:
                        index += 2
                    else:
                        index += 1
                        break
                else:
                    index += 1
            continue

        if query.startswith("/*", index):
            depth = 1
            index += 2
            while index < end and depth:
                if query.startswith("/*", index):
                    depth += 1
                    index += 2
                elif query.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            continue

        line_comment = (
            query.startswith("--", index)
            or query.startswith("//", index)
            or (char == "#" and index + 1 < end and query[index + 1] in (" ", "!"))
        )
        if line_comment:
            newline = query.find("\n", index + 1)
            index = end if newline == -1 else newline + 1
            continue

        if char == "$":
            for marker_key in marker_keys:
                if query.startswith(marker_key, index):
                    raw_markers.setdefault(marker_key, set()).add(index)
            opener = _heredoc_re.match(query, index)
            if opener is not None:
                tag = opener.group()
                if last_heredoc_starts[tag] >= opener.end():
                    close = query.find(tag, opener.end())
                    index = close + len(tag)
                    continue

        if char == "{":
            match = external_bind_re.match(query, index)
            if match is not None:
                match_end = match.end()
                name_start, name_end = match.span(1)
                # A marker inside a placeholder can still open a server heredoc, except
                # mid-name, where the server lexes the whole name as one bareword.
                for marker_key in marker_keys:
                    found = query.find(marker_key, index, match_end)
                    while found != -1:
                        if not name_start < found < name_end:
                            raw_markers.setdefault(marker_key, set()).add(found)
                        found = query.find(marker_key, found + 1, match_end)
                matches.append(match)
                index = match_end
                continue

        if char == "_" or "A" <= char <= "Z" or "a" <= char <= "z":
            index += 1
            while index < end and _is_ascii_bareword_char(query[index]):
                index += 1
            continue

        if "0" <= char <= "9":
            index += 1
            while index < end and "0" <= query[index] <= "9":
                index += 1
            # Digit-led tokens promote to a bareword through word chars only, never `$`.
            if index < end and (query[index] == "_" or "A" <= query[index] <= "Z" or "a" <= query[index] <= "z"):
                index += 1
                while index < end and _is_ascii_word_char(query[index]):
                    index += 1
            continue

        if char == "$":
            token_end = index + 1
            if token_end < end and _is_ascii_word_char(query[token_end]):
                token_end += 1
                while token_end < end and _is_ascii_bareword_char(query[token_end]):
                    token_end += 1
                index = token_end
            else:
                # Lone DollarSign token, retry heredoc detection at the next character.
                index += 1
            continue

        index += 1
    return matches, raw_markers


def _binary_bind_value(value: Any) -> Any:
    # Buffer values splice into the query bytes uncopied, so return the original object.
    if isinstance(value, bytes):
        return value
    if isinstance(value, int):
        return None
    try:
        memoryview(value)
    except (TypeError, ValueError):
        return None
    return value


def _binary_bind_values(parameters: dict[str, Any]) -> dict[str, Any]:
    binary_binds = {}
    for key, value in parameters.items():
        if _is_binary_bind_name(key):
            binary_value = _binary_bind_value(value)
            if binary_value is not None:
                binary_binds[key] = binary_value
    return binary_binds


def _server_bind_matches(query: str, binary_names: Collection[str]) -> list[tuple[str, str]]:
    return [(name, type_str) for name, type_str in external_bind_re.findall(query) if name not in binary_names]


def _binding_keeps_query_structure(query: str, parameters: Sequence | dict[str, Any] | None) -> bool:
    if not parameters:
        return True
    if not isinstance(parameters, dict):
        return False
    binary_names = _binary_bind_values(parameters).keys()
    has_server_bind = bool(_server_bind_matches(query, binary_names))
    return has_server_bind or len(binary_names) == len(parameters) or "%" not in query


def _binding_has_binary_values(parameters: Sequence | dict[str, Any] | None) -> bool:
    return isinstance(parameters, dict) and bool(_binary_bind_values(parameters))


def _heredoc_collision_error(key: str) -> ProgrammingError:
    return ProgrammingError(
        f"Query parameter name {key!r} also appears elsewhere in the query text, and ClickHouse "
        "would parse the pair of names as a heredoc string. Rename the parameter or remove the "
        "other occurrence."
    )


def _datetime_is_aware(value: datetime) -> bool:
    return value.utcoffset() is not None


def _legacy_naive_datetime_binding() -> bool:
    return common.get_setting("naive_datetime_binding") == "legacy"


class DT64Param:
    def __init__(self, value: datetime):
        self.value = value

    def format(self, tz: tzinfo | None, top_level: bool) -> str:
        value = self.value
        if _legacy_naive_datetime_binding():
            if tz:
                value = value.astimezone(tz)
        elif tz is not None and _datetime_is_aware(value):
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
) -> tuple[str | bytes, dict[str, str]]:
    query = query.rstrip(";")
    if not parameters:
        return query, {}

    binary_binds = None
    bound_params: dict[str, str] = {}

    if isinstance(parameters, dict):
        params_copy = dict_copy(parameters)
        binary_binds = _binary_bind_values(params_copy)
        nonbinary_marker_keys = set()
        for key in params_copy:
            if _is_binary_bind_name(key) and key not in binary_binds:
                nonbinary_marker_keys.add(key)
        for key in binary_binds.keys():
            del params_copy[key]

        # Placeholder detection and routing always use the regex over the raw query text,
        # matching shipped 1.x semantics. The lexer scan below only validates non-buffer
        # `$name$` keys against server heredoc parsing.
        if nonbinary_marker_keys and any(key in query for key in nonbinary_marker_keys):
            match_objects, raw_markers = _external_bind_matches(query, nonbinary_marker_keys)
            for key in params_copy:
                if key not in nonbinary_marker_keys:
                    continue
                placeholder_spans = [match.span(1) for match in match_objects if match.group(1) == key]
                if len(placeholder_spans) > 1:
                    raise ProgrammingError(
                        f"Server-side query parameter {key!r} can appear only once because repeated "
                        "dollar-delimited names are parsed as a heredoc string. Rename the parameter."
                    )
                raw_marker_starts = raw_markers.get(key, set())
                if placeholder_spans:
                    placeholder_start, placeholder_end = placeholder_spans[0]
                    if any(start < placeholder_start for start in raw_marker_starts) or query.find(key, placeholder_end) != -1:
                        raise _heredoc_collision_error(key)
                elif raw_marker_starts:
                    if f"{{{key}:" in query:
                        # The user wrote a placeholder, but an earlier heredoc pairing swallowed it.
                        raise _heredoc_collision_error(key)
                    raise ProgrammingError(
                        f"Binary query parameter {key!r} must be a buffer value such as bytes, bytearray, or memoryview. "
                        "Use a valid {name:Type} placeholder for server-side binding."
                    )

        binary_names = binary_binds.keys()
        matches = _server_bind_matches(query, binary_names)
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
            for name, matched_type in matches:
                if name not in param_types:
                    param_types[name] = matched_type
            bound_params = {}
            for k, v in final_params.items():
                tz = server_tz
                param_type = param_types.get(k)
                if param_type is not None:
                    hint_tz = _extract_tz_from_type(param_type)
                    if hint_tz is not None:
                        tz = hint_tz
                    v = _promote_datetime64(param_type, v)
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
        binary_out = b""
        start = 0
        for loc in sorted(binary_indexes.keys()):
            key, value = binary_indexes[loc]
            binary_out += binary_query[start:loc] + value + key
            start = loc
        binary_out += binary_query[start:]
        return binary_out, bound_params
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


def _format_time_of_day(value: time | timedelta) -> str:
    """Format a time or timedelta as the [-]HH:MM:SS[.ffffff|.fffffffff] literal used by Time/Time64."""

    if isinstance(value, time):
        base = f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}"
        nanos = value.microsecond * 1_000
    else:
        total_nanos = (value.days * 86400 + value.seconds) * 1_000_000_000 + value.microseconds * 1_000 + getattr(value, "nanoseconds", 0)
        sign = "-" if total_nanos < 0 else ""
        total_seconds, nanos = divmod(abs(total_nanos), 1_000_000_000)
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        base = f"{sign}{hours:02d}:{minutes:02d}:{seconds:02d}"
    if nanos % 1_000:
        return f"{base}.{nanos:09d}"
    if nanos:
        return f"{base}.{nanos // 1_000:06d}"
    return base


def format_str(value: str):
    return f"'{escape_str(value)}'"


def escape_str(value: str):
    return "".join(f"{BS}{c}" if c in must_escape else c for c in value)


def escape_bytes(value):
    return "".join(f"{BS}x{b:02x}" for b in value)


def format_query_value(value: Any, server_tz: tzinfo | None = timezone.utc):
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
        if _legacy_naive_datetime_binding():
            if value.tzinfo is not None or not tzutil.is_utc_timezone(server_tz):
                value = value.astimezone(server_tz)
        elif _datetime_is_aware(value):
            value = value.astimezone(server_tz)
        return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
    if isinstance(value, date):
        return f"'{value.isoformat()}'"
    if isinstance(value, (time, timedelta)):
        return f"'{_format_time_of_day(value)}'"
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


def str_query_value(value: Any, server_tz: tzinfo | None = timezone.utc):
    return str(format_query_value(value, server_tz))


def format_bind_value(value: Any, server_tz: tzinfo | None = timezone.utc, top_level: bool = True):
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
        # Top-level NULL bind parameters use the escaped-text sentinel. Nested
        # container elements are parsed as SQL literals and must use NULL.
        return "\\N" if top_level else "NULL"
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
        if _legacy_naive_datetime_binding() or _datetime_is_aware(value):
            value = value.astimezone(server_tz)
        val = value.strftime("%Y-%m-%d %H:%M:%S")
        if top_level:
            return val
        return f"'{val}'"
    if isinstance(value, date):
        if top_level:
            return value.isoformat()
        return f"'{value.isoformat()}'"
    if isinstance(value, (time, timedelta)):
        val = _format_time_of_day(value)
        if top_level:
            return val
        return f"'{val}'"
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
