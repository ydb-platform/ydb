"""Pure, fail-closed preparation for real-YDB counterexample replay."""

from __future__ import annotations

import base64
import hashlib
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..inspector.plan import snapshot_digest
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
from ..rbo_verifier.types import family


TRACE_FORMAT = "ydb-rbo-concrete-trace"
TRACE_VERSION = 1
REPLAY_FORMAT = "ydb-rbo-real-replay"
REPLAY_VERSION = 1
MAX_CELL_BYTES = 1_000_000
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
PATH_COMPONENT = re.compile(r"[A-Za-z0-9_.-]+\Z")
DECIMAL = re.compile(r"Decimal\(([1-9][0-9]*),([0-9]+)\)\Z")
INTEGER = re.compile(r"(Uint|Int)(8|16|32|64)\Z")
IDENTITY_FIELDS = ("cluster", "path", "path_id", "sys_view", "version")
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


@dataclass(frozen=True, slots=True)
class TargetBundle:
    prefix: str
    paths: tuple[str, ...]
    query: str
    ddls: tuple[str, ...]
    imports: tuple[str, ...]


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
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
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

    document = _mapping(trace, "trace")
    if document.get("format") != TRACE_FORMAT or document.get("version") != TRACE_VERSION:
        raise ReplayError("replay requires a version-one kqp_rbo_inspect concrete trace")
    if document.get("status") != "COUNTEREXAMPLE" or "witness" not in document:
        raise ReplayError("trace is not a counterexample with an extracted witness")
    if not isinstance(document.get("mismatches"), list) or not document["mismatches"]:
        raise ReplayError("counterexample trace has no root mismatch")
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
    witness = _mapping(document["witness"], "trace.witness")
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


def target_bundle(case: ReplayCase, database: str, namespace: str) -> TargetBundle:
    database = validate_database_path(database)
    if not re.fullmatch(r"_rbo_replay_[0-9a-f]{32}", namespace):
        raise ReplayError("replay namespace is not a generated 128-bit identifier")
    prefix = f"{database.rstrip('/')}/{namespace}"
    paths = tuple(f"{prefix}/t{index:03d}" for index in range(len(case.tables)))
    query = case.query
    query = rewrite_read_only_query(
        query,
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
        if not raw_length.isdigit() or (len(raw_length) > 1 and raw_length.startswith(b"0")):
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


def render_ddl(table: ReplayTable, path: str) -> str:
    key = _primary_key(table.schema)
    definitions = [
        f"  `{_identifier(column.name)}` {column.type}"
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


def optimizer_mode(plan: Any) -> tuple[str, Mapping[str, Any] | None]:
    simplified = _simplified_plans(plan)
    if len(simplified) != 1:
        raise ReplayError(f"explain output contains {len(simplified)} SimplifiedPlan objects")
    stats = simplified[0].get("OptimizerStats")
    if stats is None:
        return "LEGACY_RBO", None
    stats = _mapping(stats, "SimplifiedPlan.OptimizerStats")
    required = {"CBOTreesTotal", "CBOTreesOptimized"}
    if not required <= stats.keys():
        raise ReplayError("optimizer statistics omit CBO tree counters")
    for name in required:
        if type(stats[name]) is not int or stats[name] < 0:
            raise ReplayError(f"optimizer statistic {name!r} is not a non-negative integer")
    legacy = {"JoinsCount", "EquiJoinsCount"}
    if legacy <= stats.keys():
        return "LEGACY_RBO", stats
    if legacy & stats.keys():
        raise ReplayError("optimizer statistics have an ambiguous legacy marker")
    if stats["CBOTreesOptimized"] != stats["CBOTreesTotal"]:
        raise ReplayError("new RBO did not optimize every CBO tree")
    return "NEW_RBO", stats


def parse_result(text: str, expected_columns: tuple[str, ...]) -> list[Mapping[str, Any]]:
    def duplicate_keys(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ReplayError(f"query result contains duplicate key {key!r}")
            result[key] = value
        return result

    def reject_float(value: str) -> None:
        raise ReplayError(f"query result contains unsupported floating value {value!r}")

    def reject_constant(value: str) -> None:
        raise ReplayError(f"query result contains non-standard value {value!r}")

    try:
        result = json.loads(
            text,
            object_pairs_hook=duplicate_keys,
            parse_float=reject_float,
            parse_constant=reject_constant,
        )
    except json.JSONDecodeError as error:
        raise ReplayError(f"query did not return one JSON array: {error}") from error
    if not isinstance(result, list):
        raise ReplayError("query result is not one JSON array")
    expected = set(expected_columns)
    rows: list[Mapping[str, Any]] = []
    for index, row in enumerate(result):
        row = _mapping(row, f"result[{index}]")
        if set(row) != expected:
            raise ReplayError(f"result[{index}] columns do not match the snapshot output")
        rows.append(row)
    return rows


def compare_results(
    baseline: Sequence[Mapping[str, Any]],
    candidate: Sequence[Mapping[str, Any]],
    ordered: bool,
) -> tuple[bool, Mapping[str, Any]]:
    left = tuple(_canonical(row) for row in baseline)
    right = tuple(_canonical(row) for row in candidate)
    if ordered:
        if left == right:
            return True, {}
        mismatch = next(
            (index for index, pair in enumerate(zip(left, right)) if pair[0] != pair[1]),
            min(len(left), len(right)),
        )
        return False, {"first_mismatch": mismatch}
    left_count = Counter(left)
    right_count = Counter(right)
    if left_count == right_count:
        return True, {}
    return False, {
        "baseline_only": _counter_json(left_count - right_count),
        "candidate_only": _counter_json(right_count - left_count),
    }


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
            elif isinstance(node, (Aggregate, Join, UnionAll, Scan)):
                result = False
            else:
                result = False
            cache[node_id] = result
        return cache[node_id]

    return ordered(snapshot.plan.root)


def _validate_trace_comparison(
    document: Mapping[str, Any], before: Snapshot, ordered: bool
) -> None:
    trace = _mapping(document.get("trace"), "trace.trace")
    comparison = _mapping(trace.get("comparison"), "trace.trace.comparison")
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
        family_value = _mapping(comparison.get(side), f"trace comparison {side}")
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
            _mapping(outcome, f"trace comparison {side}.outcomes").get("index")
            for outcome in outcomes
        ]
        if any(type(index) is not int or index < 0 for index in indices) or len(set(indices)) != len(indices):
            raise ReplayError(f"trace comparison {side} has invalid outcome indices")
        for position, (index, outcome) in enumerate(zip(indices, outcomes)):
            outcome_value = _mapping(
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
        mismatch = _mapping(mismatch_value, path)
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
    outcome = _mapping(value, path)
    if set(outcome) != {"index", "decisions", "sequence", "order", "rows"}:
        raise ReplayError(f"{path} has unknown or missing fields")
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
        row = _mapping(row_value, row_path)
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
                cell = _mapping(cell_value, cell_path)
                if set(cell) != {"column", "type", "value"}:
                    raise ReplayError(f"{cell_path} has unknown or missing fields")
                if cell.get("column") != column.name or cell.get("type") != column.type:
                    raise ReplayError(f"{cell_path} does not match the snapshot schema")
                decoded.append(_canonical(_validate_value(cell.get("value"), column)))
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
        decision = _mapping(raw_decision, decision_path)
        if set(decision) != {"id", "choice"}:
            raise ReplayError(f"{decision_path} has unknown or missing fields")
        identity = decision.get("id")
        choice = decision.get("choice")
        if not isinstance(identity, str) or not identity or type(choice) is not int or choice < 0:
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
        row = _mapping(raw_row, f"witness[{table.name!r}][{index}]")
        if set(row) != expected:
            raise ReplayError(f"witness row columns for {table.name!r} do not match its schema")
        checked: dict[str, Any] = {}
        for name, column in columns.items():
            checked[name] = _validate_value(row[name], column)
            if family(column.type) == "string" and row[name] is not None:
                synthetic += 1
        rows.append(checked)

    key = _primary_key(table)
    seen: set[Any] = set()
    for row in rows:
        key_value = tuple(_canonical(row[name]) for name in key)
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
    integer = INTEGER.fullmatch(column.type)
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
        if len(encoded) > MAX_CELL_BYTES:
            raise ReplayError(f"column {column.name!r} witness exceeds the cell-size audit cap")
        return value
    if column.type == "Date":
        if type(value) is not int or not 0 <= value < 49_673:
            raise ReplayError(f"column {column.name!r} witness is outside the Date domain")
        return value
    decimal = DECIMAL.fullmatch(column.type)
    if decimal:
        if type(value) is not int:
            raise ReplayError(f"column {column.name!r} Decimal witness is not an atom integer")
        precision = int(decimal.group(1))
        if abs(value) >= 10**precision:
            raise ReplayError(f"column {column.name!r} witness is outside {column.type}")
        return value
    raise ReplayError(f"column {column.name!r} has unsupported replay type {column.type!r}")


def _primary_key(table: Table) -> tuple[str, ...]:
    if len(table.unique_keys) != 1:
        raise ReplayError(f"table {table.name!r} does not have exactly one captured primary key")
    key = table.unique_keys[0]
    if key.nulls_distinct or not key.columns:
        raise ReplayError(f"table {table.name!r} has unsupported primary-key semantics")
    columns = table.column_map()
    for name in key.columns:
        if name not in columns or columns[name].nullable:
            raise ReplayError(f"table {table.name!r} has an invalid nullable primary key")
        _identifier(name)
    return key.columns


def _table_storage(snapshot: Snapshot) -> dict[str, str]:
    assert snapshot.stage_graph is not None
    node_stage: dict[str, str] = {}
    stage_storage: dict[str, str | None] = {}
    for stage in snapshot.stage_graph.stages:
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


def _json_value(value: Any, column: Column) -> Any:
    if value is None:
        return None
    if column.type == "String":
        return base64.b64encode(value.encode("utf-8", errors="strict")).decode("ascii")
    if column.type == "Date":
        return (date(1970, 1, 1) + timedelta(days=value)).isoformat()
    decimal = DECIMAL.fullmatch(column.type)
    if decimal:
        return _decimal_text(value, int(decimal.group(2)))
    return value


def _decimal_text(coefficient: int, scale: int) -> str:
    sign = "-" if coefficient < 0 else ""
    digits = str(abs(coefficient))
    if scale == 0:
        return sign + digits
    digits = digits.rjust(scale + 1, "0")
    return f"{sign}{digits[:-scale]}.{digits[-scale:]}"


def _simplified_plans(value: Any) -> list[Mapping[str, Any]]:
    result: list[Mapping[str, Any]] = []
    if isinstance(value, Mapping):
        simplified = value.get("SimplifiedPlan")
        if isinstance(simplified, Mapping):
            result.append(simplified)
        for child in value.values():
            result.extend(_simplified_plans(child))
    elif isinstance(value, list):
        for child in value:
            result.extend(_simplified_plans(child))
    return result


def _identifier(value: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise ReplayError(f"unsafe YDB identifier {value!r}")
    return value


def _mapping(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReplayError(f"{path} is not an object")
    if any(not isinstance(key, str) for key in value):
        raise ReplayError(f"{path} has a non-string key")
    return value


def _canonical(value: Any) -> Any:
    if value is None:
        return ("null",)
    if type(value) is bool:
        return ("bool", value)
    if type(value) is int:
        return ("int", value)
    if isinstance(value, str):
        return ("string", value)
    if isinstance(value, list):
        return ("array", tuple(_canonical(item) for item in value))
    if isinstance(value, Mapping):
        return (
            "object",
            tuple(sorted((key, _canonical(item)) for key, item in value.items())),
        )
    raise ReplayError(f"unsupported JSON value {value!r}")


def _counter_json(counter: Counter[Any]) -> list[Mapping[str, Any]]:
    return [
        {"row": repr(row), "multiplicity": count}
        for row, count in sorted(counter.items(), key=lambda item: repr(item[0]))
    ]
