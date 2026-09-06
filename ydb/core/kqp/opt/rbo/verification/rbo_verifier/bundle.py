"""One buffered read-only result tuple, or one undifferentiated query error.

This is not a streaming contract: partial result prefixes, error diagnostics,
effects and cross-result relational bindings are outside this manifest version.
Root-local nondeterminism is independent; table rows, routing and scalar function
identities are shared. A proof compares the joint outcome language, not marginals.
"""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

from . import smt
from .analysis import AnalysisError
from .ir import Column, Snapshot, Table, load_snapshot
from .relation import Database, RelationError, bundle_mismatch
from .scalar import Encoder as ScalarEncoder
from .stages import Router, StageError
from .verify import (
    BoundaryObserver,
    Problem,
    VerificationError,
    _check_boundary_roles,
    _evaluate_boundary,
    _finish_problem,
    _problem_snapshots,
    _validate_pair,
)


SnapshotPair = tuple[Snapshot, Snapshot]


def _unique_object(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            raise VerificationError(f"bundle manifest repeats field {key!r}")
        result[key] = value
    return result


def load_bundle(path: str | Path) -> tuple[SnapshotPair, ...]:
    """Load a strict v1 manifest; array position is the client result slot."""
    path = Path(path)
    try:
        with path.open(encoding="utf-8") as stream:
            value = json.load(stream, object_pairs_hook=_unique_object)
        if (
            type(value) is not dict
            or set(value) != {"format", "version", "observation", "results"}
            or value["format"] != "ydb-rbo-result-bundle"
            or type(value["version"]) is not int
            or value["version"] != 1
            or value["observation"] != "buffered_tuple_or_error"
            or type(value["results"]) is not list
            or not value["results"]
        ):
            raise VerificationError("expected a nonempty v1 buffered result-bundle manifest")
        seen: tuple[set[Path], set[Path]] = (set(), set())
        pairs: list[SnapshotPair] = []
        for index, result in enumerate(value["results"]):
            if type(result) is not dict or set(result) != {"before", "after"}:
                raise VerificationError(f"bundle result {index} requires only before and after paths")
            snapshots: list[Snapshot] = []
            for side, field in enumerate(("before", "after")):
                name = result[field]
                if type(name) is not str or not name or "\0" in name:
                    raise VerificationError(f"bundle result {index} {field} path must be nonempty")
                source = (path.parent / name).resolve()
                if source in seen[side]:
                    raise VerificationError(f"bundle repeats a {field} root snapshot path")
                seen[side].add(source)
                snapshots.append(load_snapshot(source))
            pairs.append((snapshots[0], snapshots[1]))
        return tuple(pairs)
    except (OSError, json.JSONDecodeError, RecursionError) as error:
        raise VerificationError(f"{path}: cannot load result bundle: {error}") from error


def _union_catalog(pairs: tuple[SnapshotPair, ...]) -> tuple[Table, ...]:
    tables: dict[str, Table] = {}
    columns: dict[str, dict[str, Column]] = {}
    for before, _ in pairs:
        for table in before.tables:
            if table.name in tables and tables[table.name].unique_keys != table.unique_keys:
                raise VerificationError(f"bundle table {table.name!r} has conflicting unique keys")
            tables[table.name] = table
            merged = columns.setdefault(table.name, {})
            for column in table.columns:
                if column.name in merged and merged[column.name] != column:
                    raise VerificationError(f"bundle column {table.name!r}.{column.name!r} has conflicting metadata")
                merged[column.name] = column
    return tuple(
        replace(table, columns=tuple(columns[name].values()))
        for name, table in tables.items()
    )


def build_bundle_problem(
    pairs: tuple[SnapshotPair, ...],
    row_bound: int,
    timeout_ms: int | None = None,
    *,
    boundary_observer: BoundaryObserver | None = None,
) -> Problem:
    if not pairs:
        raise VerificationError("a result bundle must contain at least one result")
    snapshots = _problem_snapshots(tuple(snapshot for pair in pairs for snapshot in pair))
    pairs = tuple(zip(snapshots[::2], snapshots[1::2]))
    for before, after in pairs:
        _check_boundary_roles(before, after)
    validated = tuple(_validate_pair(before, after, row_bound) for before, after in pairs)
    try:
        script = smt.Script(timeout_ms)
        # Database consumes only the catalog. Keep each root's validated plan and
        # analysis separate: local node IDs are not global bundle identities.
        database = Database(replace(pairs[0][0], tables=_union_catalog(pairs)), row_bound, script)
        scalar = ScalarEncoder(script, semantic_mode=snapshots[0].semantic_mode)
        router = Router(script)
        exclusions = []
        families = []
        for index, (pair, plans) in enumerate(zip(pairs, validated)):
            families.append(tuple(
                _evaluate_boundary(
                    snapshot, plan, database, scalar, router, side, exclusions,
                    choice_scope=f"{side}:result:{index}",
                )
                for snapshot, plan, side in zip(pair, plans, ("before", "after"))
            ))
        mismatch = bundle_mismatch(tuple(families), scalar)
        if boundary_observer is not None:
            for pair in families:
                for side, family in zip(("before", "after"), pair):
                    boundary_observer(side, family)
        return _finish_problem(
            script, database, mismatch, exclusions, scalar.semantic_mode,
            abstract_integral_average=scalar.uses_abstract_integral_average,
        )
    except (AnalysisError, RelationError, StageError, smt.SmtError) as error:
        raise VerificationError(str(error)) from error
