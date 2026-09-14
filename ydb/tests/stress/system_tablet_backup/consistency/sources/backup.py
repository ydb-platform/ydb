# -*- coding: utf-8 -*-
"""Reader for system tablet backups.

Layout produced by ``ydb/core/tablet_flat/flat_executor_backup.cpp``::

    <root>/<tablet_type>/<tablet_id>/backup_<timestamp>_g<gen>_s<step>/
        snapshot/
            manifest.json         {tablet_type, tablet_id, generation, step, files:[{name,sha256}]}
            manifest.json.sha256
            schema.json           Proto2Json(TSchemeChanges), snake_case_dense field names
            <Table>.json          NDJSON, one JSON object per row
        changelog.json            NDJSON, one JSON object per commit

Cell encoding (``WriteColumnToJson``): NULL becomes JSON null with the column
still present, ``String`` is base64, ``Utf8``/``Json`` are written as-is and
``PairUi64Ui64`` becomes a two-element array.

An unfinished snapshot lives in ``snapshot.tmp`` and is skipped here, matching
the recovery guide's instruction to only use backups that have a ``snapshot``
directory.
"""

from __future__ import annotations

import calendar
import hashlib
import json
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from ..model import TABLET_SLICES, ClusterState, TabletDump

BACKUP_NAME_RE = re.compile(r"^backup_(?P<ts>.*)_g(?P<gen>\d+)_s(?P<step>\d+)$")

# The writer formats the timestamp as %Y%m%d%H%M%SZ; the recovery guide shows a
# variant with a T separator.  Accept both rather than depend on either.
_TIMESTAMP_FORMATS = ("%Y%m%d%H%M%SZ", "%Y%m%dT%H%M%SZ", "%Y%m%dT%H%M%S", "%Y%m%d%H%M%S")


def _parse_timestamp(raw: str) -> Optional[float]:
    for fmt in _TIMESTAMP_FORMATS:
        try:
            return calendar.timegm(time.strptime(raw, fmt))
        except ValueError:
            continue
    return None


SNAPSHOT_DIR = "snapshot"
INCOMPLETE_SNAPSHOT_DIR = "snapshot.tmp"
MANIFEST = "manifest.json"
SCHEMA = "schema.json"
CHANGELOG = "changelog.json"


class BackupError(Exception):
    pass


@dataclass(frozen=True)
class BackupRef:
    """A discovered backup directory, before its contents are parsed."""

    tablet_type: str
    tablet_id: int
    generation: int
    step: int
    path: str
    # When the snapshot started, from the directory name.  A lower bound on
    # data freshness, never a measure of it -- see TabletDump.snapshot_started_at.
    snapshot_started_at: Optional[float] = None

    @property
    def snapshot_dir(self) -> str:
        return os.path.join(self.path, SNAPSHOT_DIR)

    @property
    def changelog_path(self) -> str:
        return os.path.join(self.path, CHANGELOG)

    @property
    def gen_step(self) -> Tuple[int, int]:
        return (self.generation, self.step)

    @property
    def name(self) -> str:
        return os.path.basename(self.path.rstrip(os.sep))


# --------------------------------------------------------------------------
# Discovery
# --------------------------------------------------------------------------


def discover(root: str, tablet_types: Sequence[str] = TABLET_SLICES) -> List[BackupRef]:
    """Find every complete backup under ``root``.

    ``root`` is the directory named by ``system_tablet_backup_config.filesystem.path``.
    """
    if not os.path.isdir(root):
        raise BackupError("backup root %r is not a directory" % root)

    wanted = set(tablet_types)
    refs: List[BackupRef] = []

    for tablet_type in sorted(os.listdir(root)):
        if tablet_type not in wanted:
            continue
        type_dir = os.path.join(root, tablet_type)
        if not os.path.isdir(type_dir):
            continue

        for tablet_id_name in sorted(os.listdir(type_dir)):
            id_dir = os.path.join(type_dir, tablet_id_name)
            if not os.path.isdir(id_dir) or not tablet_id_name.isdigit():
                continue

            for backup_name in sorted(os.listdir(id_dir)):
                backup_dir = os.path.join(id_dir, backup_name)
                match = BACKUP_NAME_RE.match(backup_name)
                if not match or not os.path.isdir(backup_dir):
                    continue
                if not os.path.isdir(os.path.join(backup_dir, SNAPSHOT_DIR)):
                    # Snapshot was never finished (still snapshot.tmp).
                    continue
                refs.append(
                    BackupRef(
                        tablet_type=tablet_type,
                        tablet_id=int(tablet_id_name),
                        generation=int(match.group("gen")),
                        step=int(match.group("step")),
                        path=backup_dir,
                        snapshot_started_at=_parse_timestamp(match.group("ts")),
                    )
                )

    return refs


def latest_per_tablet(refs: Iterable[BackupRef]) -> List[BackupRef]:
    """Keep the freshest backup per (tablet type, tablet id).

    "Freshest" is max generation, then max step -- the rule from the recovery
    guide.
    """
    best: Dict[Tuple[str, int], BackupRef] = {}
    for ref in refs:
        key = (ref.tablet_type, ref.tablet_id)
        current = best.get(key)
        if current is None or ref.gen_step > current.gen_step:
            best[key] = ref
    return sorted(best.values(), key=lambda r: (r.tablet_type, r.tablet_id))


def _warn_about_older_incarnations(
    discovered: Sequence[BackupRef], chosen: Sequence[BackupRef]
) -> List[str]:
    """Flag the case where "max generation" picks a backup from a dead cluster.

    Tablet generations restart from 1 when the disks are reformatted, so backups
    left over from a previous incarnation of the cluster can carry a *higher*
    generation than today's.  The recovery guide's rule -- highest generation,
    then highest step -- then silently selects a months-old backup.  Observed on
    a real slice: Hive had g4 and g5 from May next to g2 from today.
    """
    notes: List[str] = []
    for ref in chosen:
        if ref.snapshot_started_at is None:
            continue
        siblings = [
            other
            for other in discovered
            if other.tablet_type == ref.tablet_type
            and other.tablet_id == ref.tablet_id
            and other.snapshot_started_at is not None
        ]
        newest = max(siblings, key=lambda r: r.snapshot_started_at, default=None)
        if newest is None or newest.path == ref.path:
            continue
        notes.append(
            "%s: picked %s by generation/step, but %s is newer on the clock -- generations "
            "restart when disks are reformatted, so the picked one may belong to a previous "
            "cluster. Use --tablet %s=<path> to choose explicitly."
            % (ref.tablet_type, ref.name, newest.name, ref.tablet_type)
        )
    return notes


def ref_from_backup_dir(path: str) -> BackupRef:
    """Build a ref for an explicitly given backup directory.

    Used by ``--tablet <type>=<path>``, which is how a production run points at
    backups collected from several hosts into arbitrary locations.
    """
    path = os.path.abspath(path.rstrip(os.sep))
    snapshot_dir = os.path.join(path, SNAPSHOT_DIR)
    if not os.path.isdir(snapshot_dir):
        if os.path.isdir(os.path.join(path, INCOMPLETE_SNAPSHOT_DIR)):
            raise BackupError(
                "%r contains only %s: the snapshot was never finished, pick an "
                "older backup" % (path, INCOMPLETE_SNAPSHOT_DIR)
            )
        raise BackupError("%r has no %s directory" % (path, SNAPSHOT_DIR))

    manifest = _read_json(os.path.join(snapshot_dir, MANIFEST))
    match = BACKUP_NAME_RE.match(os.path.basename(path))
    return BackupRef(
        tablet_type=str(manifest.get("tablet_type", "")),
        tablet_id=int(manifest.get("tablet_id", 0)),
        generation=int(manifest.get("generation", 0)),
        step=int(manifest.get("step", 0)),
        path=path,
        snapshot_started_at=_parse_timestamp(match.group("ts")) if match else None,
    )


# --------------------------------------------------------------------------
# Schema
# --------------------------------------------------------------------------


@dataclass
class TableSchema:
    table_id: int
    name: str
    key_columns: List[str] = field(default_factory=list)
    columns: Dict[int, str] = field(default_factory=dict)


def _parse_schema(delta_records: Iterable[Mapping[str, Any]]) -> Dict[str, TableSchema]:
    """Turn a TSchemeChanges delta list into per-table key column order.

    Only the record types that affect table/column identity are interpreted;
    storage-room and compaction records are irrelevant here.
    """
    by_id: Dict[int, TableSchema] = {}

    for record in delta_records:
        delta_type = record.get("delta_type")
        table_id = record.get("table_id")
        if table_id is None:
            continue
        table_id = int(table_id)

        if delta_type == "AddTable":
            table = by_id.setdefault(table_id, TableSchema(table_id, ""))
            table.name = str(record.get("table_name", "") or table.name)
        elif delta_type == "DropTable":
            by_id.pop(table_id, None)
        elif delta_type == "AddColumn":
            table = by_id.setdefault(table_id, TableSchema(table_id, ""))
            column_id = record.get("column_id")
            if column_id is not None:
                table.columns[int(column_id)] = str(record.get("column_name", ""))
        elif delta_type == "DropColumn":
            table = by_id.get(table_id)
            column_id = record.get("column_id")
            if table is not None and column_id is not None:
                name = table.columns.pop(int(column_id), None)
                if name in table.key_columns:
                    table.key_columns.remove(name)
        elif delta_type == "AddColumnToKey":
            table = by_id.setdefault(table_id, TableSchema(table_id, ""))
            column_id = record.get("column_id")
            if column_id is None:
                continue
            name = table.columns.get(int(column_id))
            # Key records arrive in key order, right after the column records.
            if name and name not in table.key_columns:
                table.key_columns.append(name)

    return {t.name: t for t in by_id.values() if t.name}


# --------------------------------------------------------------------------
# Loading
# --------------------------------------------------------------------------


# Backups are not guaranteed to be valid UTF-8.  SchemeShard declares
# Shards.PartitionConfig as Utf8 while storing a serialized protobuf in it (the
# schema even comments the doubt: "TPartitionConfig, String?"), and the snapshot
# writer emits Utf8 columns raw.  Raw protobuf bytes therefore land inside JSON
# string literals.  surrogateescape keeps those bytes round-trippable and leaves
# the JSON structure parseable, which is all the checks need.
_ENCODING_ARGS = {"encoding": "utf-8", "errors": "surrogateescape"}


def _read_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", **_ENCODING_ARGS) as handle:
            return json.load(handle)
    except (OSError, ValueError) as exc:
        raise BackupError("cannot read %r: %s" % (path, exc))


def _sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verify_checksums(snapshot_dir: str, manifest: Mapping[str, Any], only: Optional[Set[str]]) -> None:
    """Validate the sha256 recorded in the manifest for the files we read.

    Files the caller did not ask for are not hashed, so a targeted run stays
    cheap on a large production dump.
    """
    for entry in manifest.get("files", []) or []:
        name = entry.get("name")
        expected = entry.get("sha256")
        if not name or not expected:
            continue
        if only is not None and name not in only:
            continue
        path = os.path.join(snapshot_dir, name)
        if not os.path.isfile(path):
            raise BackupError("%r listed in manifest but missing" % path)
        actual = _sha256_file(path)
        if actual != expected:
            raise BackupError(
                "checksum mismatch for %r: manifest %s, actual %s "
                "(pass --skip-checksum-validation if the backup was edited on purpose)"
                % (path, expected, actual)
            )


def _read_ndjson(path: str) -> Tuple[List[Dict[str, Any]], bool]:
    """Read an NDJSON file.

    Returns the parsed rows and whether the file was truncated.  The tail of a
    backup can be cut short by a crash, which is exactly the "changelog is not
    fully restored" case from the recovery guide, so a bad final line is
    tolerated instead of failing the whole run.
    """
    rows: List[Dict[str, Any]] = []
    truncated = False

    with open(path, "r", **_ENCODING_ARGS) as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except ValueError:
                truncated = True
                break

    return rows, truncated


def _row_key(row: Mapping[str, Any], key_columns: Sequence[str]) -> Tuple:
    def hashable(value: Any) -> Any:
        if isinstance(value, list):
            return tuple(hashable(v) for v in value)
        if isinstance(value, dict):
            return tuple(sorted((k, hashable(v)) for k, v in value.items()))
        return value

    return tuple(hashable(row.get(column)) for column in key_columns)


def _apply_changelog(
    path: str,
    tables: Dict[str, Dict[Tuple, Dict[str, Any]]],
    schema: Mapping[str, TableSchema],
    wanted: Optional[Set[str]],
) -> Tuple[int, bool]:
    """Replay changelog commits on top of the snapshot rows.

    Without this the checker reports everything created after the last snapshot
    as missing -- which is noise, not a finding.
    """
    if not os.path.isfile(path):
        return 0, False

    commits, truncated = _read_ndjson(path)
    applied = 0

    for commit in commits:
        for change in commit.get("data_changes", []) or []:
            table_name = change.get("table")
            if not table_name or (wanted is not None and table_name not in wanted):
                continue
            table_schema = schema.get(table_name)
            if table_schema is None or not table_schema.key_columns:
                continue

            rows = tables.setdefault(table_name, {})
            key = _row_key(change, table_schema.key_columns)
            op = change.get("op")
            payload = {k: v for k, v in change.items() if k not in ("table", "op")}

            if op == "erase":
                rows.pop(key, None)
            elif op == "replace":
                rows[key] = payload
            else:  # "upsert" and anything unknown behaves as a merge
                existing = rows.get(key)
                if existing is None:
                    rows[key] = payload
                else:
                    existing.update(payload)
        applied += 1

    return applied, truncated


def load_dump(
    ref: BackupRef,
    tables: Optional[Iterable[str]] = None,
    apply_changelog: bool = True,
    verify_checksums: bool = True,
) -> TabletDump:
    """Parse one backup into a :class:`TabletDump`.

    ``tables`` restricts what is read; ``None`` loads everything.
    """
    snapshot_dir = ref.snapshot_dir
    manifest = _read_json(os.path.join(snapshot_dir, MANIFEST))
    wanted: Optional[Set[str]] = set(tables) if tables is not None else None

    available = tuple(
        sorted(
            name[: -len(".json")]
            for name in os.listdir(snapshot_dir)
            if name.endswith(".json") and name not in (MANIFEST, SCHEMA)
        )
    )

    if verify_checksums:
        files = None
        if wanted is not None:
            files = {SCHEMA} | {"%s.json" % name for name in wanted}
        _verify_checksums(snapshot_dir, manifest, files)

    schema = _parse_schema(_read_json(os.path.join(snapshot_dir, SCHEMA)).get("delta", []) or [])

    to_read = available if wanted is None else tuple(t for t in available if t in wanted)
    indexed: Dict[str, Dict[Tuple, Dict[str, Any]]] = {}

    for table_name in to_read:
        path = os.path.join(snapshot_dir, "%s.json" % table_name)
        rows, truncated = _read_ndjson(path)
        if truncated:
            raise BackupError(
                "snapshot table %r is truncated -- the backup is unusable, pick an older one" % path
            )
        table_schema = schema.get(table_name)
        key_columns = table_schema.key_columns if table_schema else []
        bucket: Dict[Tuple, Dict[str, Any]] = {}
        for index, row in enumerate(rows):
            # Without a known key each row is kept under a synthetic unique key,
            # which is still correct for read-only checks.
            key = _row_key(row, key_columns) if key_columns else (index,)
            bucket[key] = row
        indexed[table_name] = bucket

    changelog_commits = 0
    changelog_truncated = False
    if apply_changelog:
        changelog_commits, changelog_truncated = _apply_changelog(
            ref.changelog_path, indexed, schema, wanted
        )

    # Commits carry a tablet step, not a wall clock, so the file's own mtime is
    # the only freshness signal available.
    changelog_mtime = None
    try:
        changelog_mtime = os.stat(ref.changelog_path).st_mtime
    except OSError:
        pass

    return TabletDump(
        tablet_type=ref.tablet_type,
        tablet_id=ref.tablet_id,
        generation=ref.generation,
        step=ref.step,
        source=ref.path,
        tables={name: list(rows.values()) for name, rows in indexed.items()},
        available_tables=available,
        changelog_commits=changelog_commits,
        changelog_truncated=changelog_truncated,
        snapshot_started_at=ref.snapshot_started_at,
        changelog_mtime=changelog_mtime,
    )


def load_state(
    root: Optional[str] = None,
    explicit: Optional[Mapping[str, str]] = None,
    needed_tables: Optional[Mapping[str, Set[str]]] = None,
    apply_changelog: bool = True,
    verify_checksums: bool = True,
) -> Tuple[ClusterState, List[str]]:
    """Build a :class:`ClusterState` from a backup root and/or explicit paths.

    Returns the state and a list of human-readable notes (skipped tablets and
    the like).  Explicit paths win over discovery for the same tablet type.
    """
    notes: List[str] = []
    refs: List[BackupRef] = []

    if root:
        discovered = discover(root)
        if not discovered:
            notes.append("no complete backups found under %s" % root)
        chosen = latest_per_tablet(discovered)
        notes.extend(_warn_about_older_incarnations(discovered, chosen))
        refs.extend(chosen)

    if explicit:
        overridden = set(explicit)
        refs = [r for r in refs if r.tablet_type not in overridden]
        for tablet_type, path in sorted(explicit.items()):
            ref = ref_from_backup_dir(path)
            if ref.tablet_type and ref.tablet_type != tablet_type:
                notes.append(
                    "%s: manifest says tablet_type=%r, using the manifest value"
                    % (path, ref.tablet_type)
                )
            refs.append(ref)

    dumps: List[TabletDump] = []
    for ref in refs:
        tables = None
        if needed_tables is not None:
            tables = needed_tables.get(ref.tablet_type)
            if tables is None:
                continue
        try:
            dumps.append(
                load_dump(
                    ref,
                    tables=tables,
                    apply_changelog=apply_changelog,
                    verify_checksums=verify_checksums,
                )
            )
        except BackupError as exc:
            notes.append("%s: %s" % (ref.path, exc))

    return ClusterState(dumps=dumps), notes
