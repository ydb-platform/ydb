"""Render the exact isolated catalog, imports, and read-only replay query."""

from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Mapping

from ..rbo_verifier import decimal
from ..rbo_verifier.ir import Column
from .model import ReplayCase, ReplayError, ReplayTable, primary_key, safe_identifier


PATH_COMPONENT = re.compile(r"[A-Za-z0-9_.-]+\Z")
REPLAY_NAMESPACE = re.compile(r"_rbo_replay_[0-9a-f]{32}\Z")
SAFE_PRAGMAS = frozenset(
    {"ANSIIMPLICITCROSSJOIN", "YQLSELECT", "YQLSELECTALLOWUNNAMEDGROUPBYEXPR"}
)
WRITE_OR_CONTEXT_WORDS = frozenset(
    {
        "ACTION",
        "ALTER",
        "ANALYZE",
        "BACKUP",
        "COMMIT",
        "CREATE",
        "DELETE",
        "DO",
        "DROP",
        "EVALUATE",
        "EXECUTE",
        "EXPORT",
        "GRANT",
        "IMPORT",
        "INSERT",
        "INTO",
        "PROCESS",
        "REDUCE",
        "REPLACE",
        "RESTORE",
        "REVOKE",
        "ROLLBACK",
        "TRUNCATE",
        "UPDATE",
        "UPSERT",
        "USE",
    }
)


@dataclass(frozen=True, slots=True)
class TargetBundle:
    prefix: str
    paths: tuple[str, ...]
    query: str
    ddls: tuple[str, ...]
    imports: tuple[str, ...]


def target_bundle(case: ReplayCase, database: str, namespace: str) -> TargetBundle:
    database = validate_database_path(database)
    if not REPLAY_NAMESPACE.fullmatch(namespace):
        raise ReplayError("replay namespace is not a generated 128-bit identifier")
    prefix = f"{database.rstrip('/')}/{namespace}"
    paths = tuple(f"{prefix}/t{index:03d}" for index in range(len(case.tables)))
    query = rewrite_read_only_query(
        case.query,
        {table.source_path: target for table, target in zip(case.tables, paths)},
    )
    return TargetBundle(
        prefix,
        paths,
        query,
        tuple(render_ddl(table, path) for table, path in zip(case.tables, paths)),
        tuple(render_import(table) for table in case.tables),
    )


def rewrite_read_only_query(query: str, replacements: Mapping[str, str]) -> str:
    """Lex conservatively, rewriting code identifiers but never strings/comments."""

    hits = {source: 0 for source in replacements}
    output: list[str] = []
    select_statements = 0
    expect_pragma = False
    statement_first: str | None = None
    delimiters: list[str] = []
    closing = {"(": ")", "[": "]", "{": "}"}
    offset = 0
    while offset < len(query):
        character = query[offset]
        if character in {"'", '"'}:
            if expect_pragma:
                raise ReplayError("PRAGMA has no supported name")
            if not delimiters and statement_first is None:
                statement_first = "literal"
            end = _quoted_end(query, offset, character)
            output.append(query[offset:end])
            offset = end
            continue
        if query.startswith("--", offset):
            newline = query.find("\n", offset + 2)
            carriage = query.find("\r", offset + 2)
            endings = [item for item in (newline, carriage) if item >= 0]
            end = min(endings) if endings else len(query)
            output.append(query[offset:end])
            offset = end
            continue
        if query.startswith("/*", offset):
            end = query.find("*/", offset + 2)
            if end < 0:
                raise ReplayError("query has an unterminated block comment")
            end += 2
            output.append(query[offset:end])
            offset = end
            continue
        if query.startswith("@@", offset):
            raise ReplayError("query uses an unsupported raw-string literal")
        if character == "`":
            if expect_pragma:
                raise ReplayError("PRAGMA has no supported name")
            end = offset + 1
            content: list[str] = []
            while end < len(query):
                if query.startswith("``", end):
                    content.append("`")
                    end += 2
                elif query[end] == "`":
                    break
                else:
                    content.append(query[end])
                    end += 1
            if end >= len(query):
                raise ReplayError("query has an unterminated backtick identifier")
            identifier = "".join(content)
            if not delimiters and statement_first is None:
                statement_first = "identifier"
            if identifier in replacements:
                output.append(f"`{replacements[identifier]}`")
                hits[identifier] += 1
            elif identifier.startswith("/"):
                raise ReplayError(f"query references unmapped absolute path {identifier!r}")
            else:
                output.append(query[offset : end + 1])
            offset = end + 1
            continue
        if character.isascii() and (character.isalpha() or character == "_"):
            end = offset + 1
            while end < len(query) and query[end].isascii() and (
                query[end].isalnum() or query[end] == "_"
            ):
                end += 1
            word = query[offset:end]
            upper = word.upper()
            if not delimiters and statement_first is None:
                statement_first = upper
            if expect_pragma:
                if upper not in SAFE_PRAGMAS:
                    raise ReplayError(f"query uses unsupported PRAGMA {word!r}")
                expect_pragma = False
            elif upper == "PRAGMA":
                expect_pragma = True
            elif upper in WRITE_OR_CONTEXT_WORDS:
                raise ReplayError(f"query contains non-read-only keyword {word!r}")
            output.append(word)
            offset = end
            continue
        if character in closing:
            if not delimiters and statement_first is None:
                statement_first = character
            delimiters.append(character)
        elif character in closing.values():
            if not delimiters or closing[delimiters[-1]] != character:
                raise ReplayError("query has mismatched delimiters")
            delimiters.pop()
        elif character == ";" and not delimiters:
            select_statements += int(_validate_read_statement(statement_first))
            statement_first = None
        elif not delimiters and statement_first is None and not character.isspace():
            statement_first = "$" if character == "$" else character
        if expect_pragma and not character.isspace():
            raise ReplayError("PRAGMA has no supported name")
        output.append(character)
        offset += 1

    if expect_pragma:
        raise ReplayError("PRAGMA has no supported name")
    if delimiters:
        raise ReplayError("query has unterminated delimiters")
    select_statements += int(_validate_read_statement(statement_first))
    if select_statements != 1:
        raise ReplayError("query must contain exactly one top-level SELECT result statement")
    missing = [source for source, count in hits.items() if count == 0]
    if missing:
        raise ReplayError(
            "query has no executable backtick reference to: " + ", ".join(sorted(missing))
        )
    return "".join(output)


def _validate_read_statement(first: str | None) -> bool:
    if first is None or first == "PRAGMA" or first == "$":
        return False
    if first == "SELECT":
        return True
    raise ReplayError(f"query has unsupported top-level statement beginning with {first!r}")


def _quoted_end(query: str, offset: int, quote: str) -> int:
    cursor = offset + 1
    while cursor < len(query):
        if query[cursor] == "\\":
            cursor += 2
        elif query[cursor] == quote:
            if cursor + 1 < len(query) and query[cursor + 1] == quote:
                cursor += 2
            else:
                return cursor + 1
        else:
            cursor += 1
    raise ReplayError(f"query has an unterminated {quote} string")


def validate_database_path(path: str) -> str:
    if not isinstance(path, str) or not path.startswith("/") or path == "/":
        raise ReplayError("database path must be an absolute, non-root YDB path")
    if path.endswith("/") or "//" in path:
        raise ReplayError(f"database path is not canonical: {path!r}")
    for part in path.split("/")[1:]:
        if part in {"", ".", ".."} or not PATH_COMPONENT.fullmatch(part):
            raise ReplayError(f"unsafe database path component {part!r}")
    return path


def render_ddl(table: ReplayTable, path: str) -> str:
    key = primary_key(table.schema)
    definitions = [
        f"  `{safe_identifier(column.name)}` {column.type}"
        + ("" if column.nullable else " NOT NULL")
        for column in table.schema.columns
    ]
    definitions.append("  PRIMARY KEY (" + ", ".join(f"`{name}`" for name in key) + ")")
    body = ",\n".join(definitions)
    quoted_key = ", ".join(f"`{name}`" for name in key)
    if table.storage == "column":
        settings = (
            f"PARTITION BY HASH ({quoted_key})\n"
            "WITH (STORE = COLUMN, PARTITION_COUNT = 2)"
        )
    elif table.storage == "row":
        first_type = table.schema.column_map()[key[0]].type
        if first_type not in {"Uint32", "Uint64"}:
            raise ReplayError(
                "two-partition row replay requires a Uint32/Uint64 leading primary key"
            )
        settings = (
            "WITH (STORE = ROW, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2, "
            "UNIFORM_PARTITIONS = 2)"
        )
    else:
        raise ReplayError(f"unsupported source storage {table.storage!r}")
    return f"CREATE TABLE `{path}` (\n{body}\n)\n{settings};"


def render_import(table: ReplayTable) -> str:
    lines = []
    for row in table.rows:
        rendered = {
            column.name: _json_value(row[column.name], column)
            for column in table.schema.columns
        }
        lines.append(json.dumps(rendered, ensure_ascii=True, separators=(",", ":")))
    return "" if not lines else "\n".join(lines) + "\n"


def _json_value(value: Any, column: Column) -> Any:
    if value is None:
        return None
    if column.type == "String":
        return base64.b64encode(value.encode("utf-8", errors="strict")).decode("ascii")
    if column.type == "Date":
        return (date(1970, 1, 1) + timedelta(days=value)).isoformat()
    decimal_type = decimal.parse_type(column.type)
    if decimal_type is not None:
        return _decimal_text(value, decimal_type.scale)
    return value


def _decimal_text(coefficient: int, scale: int) -> str:
    if coefficient == -decimal.INF:
        return "-inf"
    if coefficient == decimal.INF:
        return "inf"
    if coefficient == decimal.NAN:
        return "nan"
    sign = "-" if coefficient < 0 else ""
    digits = str(abs(coefficient))
    if scale == 0:
        return sign + digits
    digits = digits.rjust(scale + 1, "0")
    return f"{sign}{digits[:-scale]}.{digits[-scale:]}"
