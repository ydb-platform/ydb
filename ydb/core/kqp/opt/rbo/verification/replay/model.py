"""Small shared types and validation primitives for real-YDB replay."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping

from ..rbo_verifier.ir import Table


IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class ReplayError(ValueError):
    """The replay input or environment cannot be used without guessing."""


class InconclusiveReplay(ReplayError):
    """The bounded trace permits more than one observable runtime result."""


@dataclass(frozen=True, slots=True)
class ReplayTable:
    schema: Table
    source_path: str
    storage: str
    rows: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True, slots=True)
class ReplayCase:
    tables: tuple[ReplayTable, ...]
    query: str
    output: tuple[str, ...]
    ordered: bool
    row_bound: int
    symbolic_string_cells: int


def require_mapping(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReplayError(f"{path} is not an object")
    if any(not isinstance(key, str) for key in value):
        raise ReplayError(f"{path} has a non-string key")
    return value


def canonical_json(value: Any) -> Any:
    if value is None:
        return ("null",)
    if type(value) is bool:
        return ("bool", value)
    if type(value) is int:
        return ("int", value)
    if isinstance(value, str):
        return ("string", value)
    if isinstance(value, list):
        return ("array", tuple(canonical_json(item) for item in value))
    if isinstance(value, Mapping):
        return (
            "object",
            tuple(sorted((key, canonical_json(item)) for key, item in value.items())),
        )
    raise ReplayError(f"unsupported JSON value {value!r}")


def safe_identifier(value: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise ReplayError(f"unsafe YDB identifier {value!r}")
    return value


def primary_key(table: Table) -> tuple[str, ...]:
    if len(table.unique_keys) != 1:
        raise ReplayError(
            f"table {table.name!r} does not have exactly one captured primary key"
        )
    key = table.unique_keys[0]
    if key.nulls_distinct or not key.columns:
        raise ReplayError(f"table {table.name!r} has unsupported primary-key semantics")
    columns = table.column_map()
    for name in key.columns:
        if name not in columns or columns[name].nullable:
            raise ReplayError(f"table {table.name!r} has an invalid nullable primary key")
        safe_identifier(name)
    return key.columns
