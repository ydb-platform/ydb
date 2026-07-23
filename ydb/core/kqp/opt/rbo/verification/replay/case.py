"""Validate one inspector certificate and extract its bounded replay case."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..inspector.plan import snapshot_digest
from ..rbo_verifier import decimal
from ..rbo_verifier.ir import (
    Aggregate,
    Column,
    Filter,
    Join,
    Limit,
    Project,
    Scan,
    Snapshot,
    Sort,
    Table,
    UnionAll,
    parse_snapshot,
)
from ..rbo_verifier.string_order import MAX_STRING_BYTES
from ..rbo_verifier.types import family
from .model import (
    InconclusiveReplay,
    ReplayCase,
    ReplayError,
    ReplayTable,
    canonical_json,
    primary_key,
    require_mapping,
)


TRACE_FORMAT = "ydb-rbo-concrete-trace"
TRACE_VERSION = 1
INTEGER_TYPE = re.compile(r"(Uint|Int)(8|16|32|64)\Z")
IDENTITY_FIELDS = ("cluster", "path", "path_id", "sys_view", "version")


def load_json(path: str | Path) -> Any:
    """Load strict JSON: duplicate keys, NaN, and infinities are errors."""

    def object_pairs(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ReplayError(f"{path}: duplicate JSON key {key!r}")
            result[key] = value
        return result

    def invalid_constant(value: str) -> None:
        raise ReplayError(f"{path}: non-standard JSON constant {value!r}")

    try:
        with Path(path).open("r", encoding="utf-8") as stream:
            return json.load(
                stream,
                object_pairs_hook=object_pairs,
                parse_constant=invalid_constant,
            )
    except (OSError, UnicodeError, json.JSONDecodeError, RecursionError) as error:
        raise ReplayError(f"{path}: invalid JSON: {error}") from error


def load_snapshot(path: str | Path) -> Snapshot:
    return parse_snapshot(load_json(path))


def prepare_case(
    before: Snapshot,
    after: Snapshot,
    trace: Any,
    query: str,
    query_sha256: str | None = None,
) -> ReplayCase:
    if before.stage_graph is not None or after.stage_graph is None:
        raise ReplayError("expected an initial logical snapshot and a final StageGraph snapshot")
    if before.tables != after.tables:
        raise ReplayError("snapshot catalogs differ")
    if before.output_schema() != after.output_schema():
        raise ReplayError("snapshot result schemas differ")
    if not isinstance(query, str) or not query.strip():
        raise ReplayError("query text is empty")

    document = require_mapping(trace, "trace")
    if document.get("format") != TRACE_FORMAT or document.get("version") != TRACE_VERSION:
        raise ReplayError("replay requires a version-one kqp_rbo_inspect concrete trace")
    if document.get("status") != "COUNTEREXAMPLE" or "witness" not in document:
        raise ReplayError("trace is not a counterexample with an extracted witness")
    expected_inputs = {
        "before_semantic_sha256": snapshot_digest(before),
        "after_semantic_sha256": snapshot_digest(after),
        "query_sha256": query_sha256 or hashlib.sha256(query.encode("utf-8")).hexdigest(),
    }
    if document.get("inputs") != expected_inputs:
        raise ReplayError("trace is not bound to these exact snapshots and query")
    row_bound = document.get("row_bound")
    if type(row_bound) is not int or row_bound < 0:
        raise ReplayError("trace.row_bound must be a non-negative integer")

    ordered = logical_root_is_ordered(before)
    _validate_trace_comparison(document, before, ordered)
    witness = require_mapping(document["witness"], "trace.witness")
    expected_tables = {table.name for table in before.tables}
    if set(witness) != expected_tables:
        raise ReplayError("witness table identities do not exactly match the snapshot catalog")

    storage = _table_storage(after)
    seen_paths: set[str] = set()
    replay_tables: list[ReplayTable] = []
    synthetic = 0
    for table in before.tables:
        source_path = table_path(table.name)
        if source_path in seen_paths:
            raise ReplayError(f"multiple table identities decode to path {source_path!r}")
        seen_paths.add(source_path)
        rows, table_synthetic = _validate_rows(table, witness[table.name], row_bound)
        synthetic += table_synthetic
        if table.name not in storage:
            raise ReplayError(f"final StageGraph has no physical source for {source_path!r}")
        replay_tables.append(ReplayTable(table, source_path, storage[table.name], rows))

    return ReplayCase(
        tuple(replay_tables),
        query,
        tuple(column.name for column in before.output_schema()),
        ordered,
        row_bound,
        synthetic,
    )


def table_path(identity: str) -> str:
    try:
        data = identity.encode("utf-8", errors="strict")
    except UnicodeError as error:
        raise ReplayError("table identity is not valid UTF-8") from error
    offset = 0
    values: dict[str, str] = {}
    for field in IDENTITY_FIELDS:
        prefix = field.encode("ascii") + b":"
        if not data.startswith(prefix, offset):
            raise ReplayError(f"malformed table identity: expected field {field!r}")
        offset += len(prefix)
        delimiter = data.find(b":", offset)
        if delimiter < 0:
            raise ReplayError("malformed table identity length")
        raw_length = data[offset:delimiter]
        if not raw_length.isdigit() or (
            len(raw_length) > 1 and raw_length.startswith(b"0")
        ):
            raise ReplayError("table identity has a non-canonical byte length")
        length = int(raw_length)
        offset = delimiter + 1
        end = offset + length
        if end > len(data) or data[end : end + 1] != b";":
            raise ReplayError(f"malformed table identity value for {field!r}")
        try:
            values[field] = data[offset:end].decode("utf-8", errors="strict")
        except UnicodeError as error:
            raise ReplayError(f"table identity field {field!r} is not UTF-8") from error
        offset = end + 1
    if offset != len(data):
        raise ReplayError("table identity has trailing bytes")
    if values["sys_view"]:
        raise ReplayError("system-view tables cannot be replayed as ordinary YDB tables")
    path = values["path"]
    if (
        not path.startswith("/")
        or path == "/"
        or path.endswith("/")
        or "//" in path
        or "`" in path
        or any(ord(character) < 32 for character in path)
    ):
        raise ReplayError(f"unsafe or non-canonical source table path {path!r}")
    return path


def logical_root_is_ordered(snapshot: Snapshot) -> bool:
    nodes = snapshot.plan.node_map()
    cache: dict[str, bool] = {}

    def ordered(node_id: str) -> bool:
        if node_id not in cache:
            node = nodes[node_id]
            if isinstance(node, Sort):
                result = True
            elif isinstance(node, (Project, Filter, Limit)):
                result = ordered(node.input)
            elif isinstance(node, UnionAll):
                result = node.ordered
            elif isinstance(node, (Aggregate, Join, Scan)):
                result = False
            else:
                result = False
            cache[node_id] = result
        return cache[node_id]

    return ordered(snapshot.plan.root)


def _validate_trace_comparison(
    document: Mapping[str, Any], before: Snapshot, ordered: bool
) -> None:
    trace = require_mapping(document.get("trace"), "trace.trace")
    comparison = require_mapping(trace.get("comparison"), "trace.trace.comparison")
    semantics = comparison.get("semantics")
    expected_semantics = "sequence" if ordered else "bag"
    if semantics != expected_semantics:
        raise ReplayError("trace comparison semantics disagree with the initial snapshot")
    expected_columns = [
        {"name": column.name, "type": column.type, "nullable": column.nullable}
        for column in before.output_schema()
    ]
    unique_results: dict[str, Any] = {}
    expected_mismatches: dict[tuple[str, int], tuple[tuple[str, int], ...]] = {}
    for side in ("before", "after"):
        family_value = require_mapping(comparison.get(side), f"trace comparison {side}")
        if set(family_value) != {"columns", "disabled_outcome_count", "outcomes"}:
            raise ReplayError(f"trace comparison {side} has unknown or missing fields")
        if family_value.get("columns") != expected_columns:
            raise ReplayError(f"trace comparison {side} schema differs from the snapshot")
        disabled = family_value.get("disabled_outcome_count")
        if type(disabled) is not int or disabled < 0:
            raise ReplayError(f"trace comparison {side} has an invalid disabled count")
        outcomes = family_value.get("outcomes")
        if not isinstance(outcomes, list) or not outcomes:
            raise ReplayError(f"trace comparison {side} has no enabled outcomes")
        rendered = [
            _trace_outcome(
                outcome,
                ordered,
                before.output_schema(),
                f"trace comparison {side}.outcomes[{index}]",
            )
            for index, outcome in enumerate(outcomes)
        ]
        indices = [
            require_mapping(outcome, f"trace comparison {side}.outcomes").get("index")
            for outcome in outcomes
        ]
        if (
            any(type(index) is not int or index < 0 for index in indices)
            or len(set(indices)) != len(indices)
        ):
            raise ReplayError(f"trace comparison {side} has invalid outcome indices")
        for position, (index, outcome) in enumerate(zip(indices, outcomes)):
            outcome_value = require_mapping(
                outcome, f"trace comparison {side}.outcomes[{position}]"
            )
            expected_mismatches[(side, index)] = _trace_decisions(
                outcome_value.get("decisions"),
                f"trace comparison {side}.outcomes[{position}].decisions",
            )
        distinct = set(rendered)
        if len(distinct) != 1:
            raise InconclusiveReplay(
                f"{side} admits {len(distinct)} distinct observable results for this witness"
            )
        unique_results[side] = next(iter(distinct))

    if unique_results["before"] == unique_results["after"]:
        raise ReplayError("trace root results are equal for this witness")

    mismatches = document.get("mismatches")
    if not isinstance(mismatches, list) or not mismatches:
        raise ReplayError("counterexample trace has no root mismatch")
    seen: set[tuple[str, int]] = set()
    for position, mismatch_value in enumerate(mismatches):
        path = f"trace.mismatches[{position}]"
        mismatch = require_mapping(mismatch_value, path)
        if set(mismatch) != {"source", "outcome", "decisions", "matching_outcomes"}:
            raise ReplayError(f"{path} has unknown or missing fields")
        source = mismatch.get("source")
        outcome = mismatch.get("outcome")
        if source not in {"before", "after"} or type(outcome) is not int:
            raise ReplayError(f"{path} does not identify an enabled root outcome")
        key = (source, outcome)
        if key not in expected_mismatches or key in seen:
            raise ReplayError(f"{path} does not identify one unique enabled root outcome")
        decisions = _trace_decisions(mismatch.get("decisions"), f"{path}.decisions")
        if decisions != expected_mismatches[key]:
            raise ReplayError(f"{path} decisions differ from the root outcome")
        if mismatch.get("matching_outcomes") != []:
            raise ReplayError(f"{path}.matching_outcomes is not empty")
        seen.add(key)
    if seen != set(expected_mismatches):
        raise ReplayError("trace mismatches do not cover every enabled root outcome")


def _trace_outcome(
    value: Any,
    ordered: bool,
    columns: tuple[Column, ...],
    path: str,
) -> Any:
    outcome = require_mapping(value, path)
    required = {"index", "decisions", "sequence", "order", "rows"}
    if set(outcome) not in (required, required | {"status"}):
        raise ReplayError(f"{path} has unknown or missing fields")
    status = outcome.get("status", "success")
    if status not in {"success", "error"}:
        raise ReplayError(f"{path}.status is invalid")
    if status == "error":
        raise InconclusiveReplay(
            "query-error outcomes require error-aware real-YDB replay"
        )
    if outcome.get("sequence") is not ordered:
        raise ReplayError(f"{path} outcome has inconsistent sequence semantics")
    _trace_decisions(outcome.get("decisions"), f"{path}.decisions")
    if outcome.get("order") is not None and not isinstance(outcome.get("order"), list):
        raise ReplayError(f"{path}.order is neither null nor an array")
    rows = outcome.get("rows")
    if not isinstance(rows, list):
        raise ReplayError(f"{path}.rows is not an array")
    present = []
    slots: list[int] = []
    for row_index, row_value in enumerate(rows):
        row_path = f"{path}.rows[{row_index}]"
        row = require_mapping(row_value, row_path)
        slot = row.get("slot")
        if type(slot) is not int or slot < 0:
            raise ReplayError(f"{row_path} has an invalid slot")
        slots.append(slot)
        if row.get("present") is True:
            if set(row) != {"slot", "present", "values"}:
                raise ReplayError(f"{row_path} has unknown or missing fields")
            values = row.get("values")
            if not isinstance(values, list) or len(values) != len(columns):
                raise ReplayError(f"{row_path} has the wrong number of values")
            decoded = []
            for cell_index, (cell_value, column) in enumerate(zip(values, columns)):
                cell_path = f"{row_path}.values[{cell_index}]"
                cell = require_mapping(cell_value, cell_path)
                if set(cell) != {"column", "type", "value"}:
                    raise ReplayError(f"{cell_path} has unknown or missing fields")
                if cell.get("column") != column.name or cell.get("type") != column.type:
                    raise ReplayError(f"{cell_path} does not match the snapshot schema")
                decoded.append(canonical_json(_validate_value(cell.get("value"), column)))
            present.append(tuple(decoded))
        elif row.get("present") is not False:
            raise ReplayError(f"{row_path} has invalid presence")
        elif set(row) != {"slot", "present"}:
            raise ReplayError(f"{row_path} absent row exposes a payload")
    if slots != list(range(len(rows))):
        raise ReplayError(f"{path} row slots are not canonical and contiguous")
    return tuple(present) if ordered else tuple(sorted(present, key=repr))


def _trace_decisions(value: Any, path: str) -> tuple[tuple[str, int], ...]:
    if not isinstance(value, list):
        raise ReplayError(f"{path} is not an array")
    result: list[tuple[str, int]] = []
    for index, raw_decision in enumerate(value):
        decision_path = f"{path}[{index}]"
        decision = require_mapping(raw_decision, decision_path)
        if set(decision) != {"id", "choice"}:
            raise ReplayError(f"{decision_path} has unknown or missing fields")
        identity = decision.get("id")
        choice = decision.get("choice")
        if (
            not isinstance(identity, str)
            or not identity
            or type(choice) is not int
            or choice < 0
        ):
            raise ReplayError(f"{decision_path} is invalid")
        result.append((identity, choice))
    if result != sorted(set(result)):
        raise ReplayError(f"{path} is not canonical and unique")
    return tuple(result)


def _validate_rows(
    table: Table, value: Any, row_bound: int
) -> tuple[tuple[Mapping[str, Any], ...], int]:
    if not isinstance(value, list) or len(value) > row_bound:
        raise ReplayError(f"witness rows for {table.name!r} exceed the declared bound")
    columns = table.column_map()
    expected = set(columns)
    rows: list[Mapping[str, Any]] = []
    synthetic = 0
    for index, raw_row in enumerate(value):
        row = require_mapping(raw_row, f"witness[{table.name!r}][{index}]")
        if set(row) != expected:
            raise ReplayError(f"witness row columns for {table.name!r} do not match its schema")
        checked: dict[str, Any] = {}
        for name, column in columns.items():
            checked[name] = _validate_value(row[name], column)
            if family(column.type) == "string" and row[name] is not None:
                synthetic += 1
        rows.append(checked)

    key = primary_key(table)
    seen: set[Any] = set()
    for row in rows:
        key_value = tuple(canonical_json(row[name]) for name in key)
        if key_value in seen:
            raise ReplayError(f"witness has duplicate primary key for table {table.name!r}")
        seen.add(key_value)
    return tuple(rows), synthetic


def _validate_value(value: Any, column: Column) -> Any:
    if value is None:
        if not column.nullable:
            raise ReplayError(f"non-nullable column {column.name!r} has NULL witness value")
        return None
    scalar_family = family(column.type)
    if scalar_family == "bool":
        if type(value) is not bool:
            raise ReplayError(f"column {column.name!r} witness is not Bool")
        return value
    integer = INTEGER_TYPE.fullmatch(column.type)
    if integer:
        if type(value) is not int:
            raise ReplayError(f"column {column.name!r} witness is not an integer")
        kind, raw_width = integer.groups()
        width = int(raw_width)
        minimum = 0 if kind == "Uint" else -(1 << (width - 1))
        maximum = (1 << width) - 1 if kind == "Uint" else (1 << (width - 1)) - 1
        if not minimum <= value <= maximum:
            raise ReplayError(
                f"column {column.name!r} witness {value} is outside {column.type}"
            )
        return value
    if column.type in {"String", "Utf8"}:
        if not isinstance(value, str):
            raise ReplayError(f"column {column.name!r} witness is not text")
        try:
            encoded = value.encode("utf-8", errors="strict")
        except UnicodeError as error:
            raise ReplayError(f"column {column.name!r} witness is not valid Unicode") from error
        if len(encoded) > MAX_STRING_BYTES:
            raise ReplayError(f"column {column.name!r} witness exceeds the cell-size audit cap")
        return value
    if column.type == "Date":
        if type(value) is not int or not 0 <= value < 49_673:
            raise ReplayError(f"column {column.name!r} witness is outside the Date domain")
        return value
    decimal_type = decimal.parse_type(column.type)
    if decimal_type is not None:
        if type(value) is not int:
            raise ReplayError(f"column {column.name!r} Decimal witness is not an atom integer")
        precision = decimal_type.precision
        special = value in {-decimal.INF, decimal.INF, decimal.NAN}
        if not special and abs(value) >= 10**precision:
            raise ReplayError(f"column {column.name!r} witness is outside {column.type}")
        return value
    raise ReplayError(f"column {column.name!r} has unsupported replay type {column.type!r}")


def _table_storage(snapshot: Snapshot) -> dict[str, str]:
    graph = snapshot.stage_graph
    if graph is None:
        raise ReplayError("final snapshot has no StageGraph")
    node_stage: dict[str, str] = {}
    stage_storage: dict[str, str | None] = {}
    for stage in graph.stages:
        stage_storage[stage.id] = stage.source_storage
        for node in stage.nodes:
            if node in node_stage:
                raise ReplayError(f"plan node {node!r} occurs in multiple stages")
            node_stage[node] = stage.id
    result: dict[str, str] = {}
    for node in snapshot.plan.nodes:
        if not isinstance(node, Scan):
            continue
        storage = stage_storage.get(node_stage.get(node.id, ""))
        if storage not in {"row", "column"}:
            raise ReplayError(f"scan {node.id!r} has no replayable source storage")
        previous = result.setdefault(node.table, storage)
        if previous != storage:
            raise ReplayError(f"table {node.table!r} has inconsistent source storage")
    return result
