# -*- coding: utf-8 -*-
"""State model shared by all consistency checks.

Standard library only -- see the package docstring.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

# Slice names.  The first four match the directory names produced by the backup
# writer (snake_case of TTabletTypes::EType_Name, see
# ydb/core/tablet_flat/flat_executor_backup.cpp:CreateBackupPath).
HIVE = "hive"
# ToSnakeCaseDense does not split runs of capitals, so "BSController" becomes
# "bscontroller" and not "bs_controller".  Verified against a live cluster.
BS_CONTROLLER = "bscontroller"
SCHEME_SHARD = "scheme_shard"
NODE_BROKER = "node_broker"

# Pseudo-slice: an external record of what the workload actually did.  Only
# available on a test stand, never on production.
LEDGER = "ledger"

# Pseudo-slice: readings taken from the *running* cluster.  This is the only way
# to see tenant SchemeShards and tenant Hives, which have no backups at all
# (ITablet::NeedBackup returns false once TenantPathId is set).  They are also
# the side that was never rolled back, so they hold the references a restored
# cluster tablet has forgotten.
LIVE = "live"

TABLET_SLICES = (HIVE, BS_CONTROLLER, SCHEME_SHARD, NODE_BROKER)


class Severity(enum.IntEnum):
    """Ordered so that ``--fail-on`` can use a simple comparison."""

    INFO = 10
    WARNING = 20
    ERROR = 30
    CRITICAL = 40

    @classmethod
    def parse(cls, value: str) -> "Severity":
        try:
            return cls[value.strip().upper()]
        except KeyError:
            raise ValueError(
                "unknown severity %r, expected one of %s"
                % (value, ", ".join(s.name.lower() for s in cls))
            )


@dataclass
class Finding:
    """A single detected problem.

    ``check_id`` is filled in by the runner, so check bodies do not repeat it.
    """

    severity: Severity
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    check_id: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "check_id": self.check_id,
            "severity": self.severity.name,
            "message": self.message,
            "details": self.details,
        }


def critical(message: str, **details: Any) -> Finding:
    return Finding(Severity.CRITICAL, message, details)


def error(message: str, **details: Any) -> Finding:
    return Finding(Severity.ERROR, message, details)


def warning(message: str, **details: Any) -> Finding:
    return Finding(Severity.WARNING, message, details)


def info(message: str, **details: Any) -> Finding:
    return Finding(Severity.INFO, message, details)


# --------------------------------------------------------------------------
# Cell decoding helpers.
#
# The snapshot writer emits NULL cells as JSON null and keeps the column
# present, so every accessor has to treat None as "absent".
# --------------------------------------------------------------------------


def as_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    """Read an integer cell, tolerating nulls and stringified numbers."""
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value, 0)
        except ValueError:
            return default
    return default


def as_pair(value: Any) -> Optional[Tuple[int, int]]:
    """Read a PairUi64Ui64 cell, emitted as a two-element JSON array.

    Hive stores the tablet owner as (owner tablet id, owner idx) in this type.
    """
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        return None
    first, second = as_int(value[0]), as_int(value[1])
    if first is None or second is None:
        return None
    return (first, second)


@dataclass
class TabletDump:
    """One tablet's local database, as captured by a single backup."""

    tablet_type: str
    tablet_id: int
    generation: int
    step: int
    source: str
    tables: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    # Tables that exist in the backup but were not requested by any check.
    available_tables: Tuple[str, ...] = ()
    # Number of changelog commits applied on top of the snapshot.
    changelog_commits: int = 0
    # Set when the changelog tail could not be parsed: the last changes before
    # the crash are missing, mirroring the restore form's "changelog is not
    # fully restored" warning.
    changelog_truncated: bool = False
    # When the *snapshot* started, parsed from the backup directory name.
    # This is NOT a measure of how fresh the data is: the changelog keeps
    # receiving commits long after the snapshot finished, so a backup named
    # 11:00 can hold state from 12:00.  Treat it as a lower bound only.
    snapshot_started_at: Optional[float] = None
    # Last write time of changelog.json.  The only wall-clock signal of actual
    # data freshness the backup format offers -- commits carry a tablet step,
    # not a timestamp.  Filesystem metadata, so it survives `rsync -a` and
    # `scp -p` but is clobbered by a plain `scp -r`.
    changelog_mtime: Optional[float] = None

    def rows(self, table: str) -> List[Dict[str, Any]]:
        """Rows of ``table``; empty list when the table was not loaded."""
        return self.tables.get(table, [])

    def has_table(self, table: str) -> bool:
        return table in self.tables

    @property
    def gen_step(self) -> Tuple[int, int]:
        return (self.generation, self.step)

    @property
    def label(self) -> str:
        return "%s:%d (gen %d, step %d)" % (
            self.tablet_type,
            self.tablet_id,
            self.generation,
            self.step,
        )


@dataclass
class LedgerEntry:
    """One recorded workload operation.  See ``sources.ledger``."""

    ts: float
    op: str
    payload: Dict[str, Any]

    def get(self, key: str, default: Any = None) -> Any:
        return self.payload.get(key, default)


@dataclass
class Ledger:
    entries: List[LedgerEntry] = field(default_factory=list)
    source: str = ""

    def of_op(self, op: str) -> List[LedgerEntry]:
        return [e for e in self.entries if e.op == op]

    def __len__(self) -> int:
        return len(self.entries)


@dataclass
class LiveHive:
    """What a running Hive reports about itself."""

    hive_id: int
    # Full tablet ids this Hive owns, from /viewer/hiveinfo.
    tablet_ids: Set[int] = field(default_factory=set)
    # Owner keys, so a tenant Hive's tablets can be attributed.
    owners: Set[Tuple[int, int]] = field(default_factory=set)
    # Viewer VolatileState by full tablet id (2 is dead, 4 is running).
    tablet_states: Dict[int, int] = field(default_factory=dict)
    tablet_restarts: Dict[int, int] = field(default_factory=dict)
    tablet_generations: Dict[int, int] = field(default_factory=dict)
    reachable: bool = True
    error: str = ""


@dataclass
class LivePath:
    """What a running cluster reports about one schema object.

    Two kinds of question are asked this way.  A BlockStore volume or a
    FileStore refuses a config older than the one it has already applied, so its
    live ``version`` is the thing a backup has to be judged against.  Any path at
    all is also read for its identity -- ``owner_id``/``path_id`` plus the
    transaction that created it -- which is how a backup can tell that an
    operation it still holds in flight has long since finished.
    """

    path: str
    kind: str                       # "blockstore" | "filestore" | "path"
    version: Optional[int] = None
    exists: bool = True
    reachable: bool = True
    error: str = ""
    # Identity of the live object, from TDirEntry (flat_scheme_op.proto).
    owner_id: Optional[int] = None
    path_id: Optional[int] = None
    create_tx_id: Optional[int] = None
    create_finished: bool = False


@dataclass
class LiveCluster:
    """Readings from the running cluster, keyed by tablet id."""

    hives: Dict[int, LiveHive] = field(default_factory=dict)
    # Versioned schema objects, keyed by full path.
    paths: Dict[str, LivePath] = field(default_factory=dict)
    source: str = ""

    def unreachable(self) -> List[int]:
        return sorted(h.hive_id for h in self.hives.values() if not h.reachable)


@dataclass
class ClusterState:
    """Everything the checks may look at."""

    dumps: List[TabletDump] = field(default_factory=list)
    ledger: Optional[Ledger] = None
    live: Optional[LiveCluster] = None
    # Hive recovery assumes the surviving SchemeShard backup is current and
    # complete. Other restore modes must not infer this from a backup's age.
    authoritative_schemeshard: bool = False

    def by_type(self, tablet_type: str) -> List[TabletDump]:
        return [d for d in self.dumps if d.tablet_type == tablet_type]

    def one(self, tablet_type: str) -> Optional[TabletDump]:
        """The single dump of ``tablet_type``.

        Only cluster-wide (non-tenant) tablets are backed up, so in practice
        there is exactly one Hive, one BSController and one root SchemeShard.
        """
        found = self.by_type(tablet_type)
        return found[0] if found else None

    def slices(self) -> Set[str]:
        """Which slices are actually available, for requirement checking."""
        present = {d.tablet_type for d in self.dumps}
        if self.ledger is not None:
            present.add(LEDGER)
        if self.live is not None:
            present.add(LIVE)
        return present

    @property
    def foreign_tablet_ids(self) -> Set[int]:
        """Tablet ids seen live that no loaded backup accounts for.

        Almost always tenant tablets: a tenant Hive's own tablets never appear
        in the root Hive's table, yet their ids came out of the root Hive's
        allocator and must never be handed out again.
        """
        if self.live is None:
            return set()

        backed_up = {d.tablet_id for d in self.dumps}
        known: Set[int] = set()
        for dump in self.dumps:
            if dump.tablet_type != HIVE:
                continue
            for row in dump.rows("Tablet"):
                tablet_id = as_int(row.get("ID"))
                if tablet_id is not None:
                    known.add(tablet_id)

        foreign: Set[int] = set()
        for hive in self.live.hives.values():
            if hive.hive_id in backed_up:
                continue
            foreign |= hive.tablet_ids
        return foreign - known

    def describe(self) -> str:
        parts = [d.label for d in sorted(self.dumps, key=lambda d: d.tablet_type)]
        if self.ledger is not None:
            parts.append("ledger: %d entries" % len(self.ledger))
        if self.live is not None:
            parts.append(
                "live: %d hive(s), %d tenant tablet(s)"
                % (len(self.live.hives), len(self.foreign_tablet_ids))
            )
        return "; ".join(parts) if parts else "<empty>"


def collect_tablet_ids(dumps: Iterable[TabletDump]) -> Sequence[int]:
    return [d.tablet_id for d in dumps]
