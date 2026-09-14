# -*- coding: utf-8 -*-
"""Doctor mode: repair the invariants that can be repaired in a backup.

Identifier-sequence invariants are repairable offline.  A Hive tablet which is
absent from a loaded SchemeShard can also be erased from a Hive backup: this is
the inverse of inventing lost data and prevents a stale Hive restore from
resurrecting tablets which SchemeShard already dropped.  Other referential
findings still need reconciliation in the running cluster.

**How a repair is applied.** Not by editing the snapshot: the restore replays
``changelog.json`` on top of it, so a snapshot edit would simply be overwritten
by a later commit for the same row.  Instead doctor *appends a commit* to the
changelog.  That works with the replay's last-write-wins semantics and has two
useful consequences:

* the snapshot files and ``manifest.json`` are untouched, so their checksums stay
  valid and the backup restores **without** ``--skip-checksum-validation``;
* nothing is destroyed -- the original values remain earlier in the changelog.

The restore side accepts this: a changelog line only has to be a JSON map with
``data_changes`` and/or ``schema_changes`` (``TTxUploadChangelog`` in
``flat_executor_recovery.cpp``), the ``step`` field is not required, and the
changelog is not checksum-validated on the restore path.

Adding a repair is one decorated generator::

    @repair("I4")
    def fix_tablet_id(state):
        yield Edit(tablet_type=HIVE, table="State", key={"Key": 0},
                   values={"Value": new_next}, reason="...")
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple

from .model import BS_CONTROLLER, HIVE, SCHEME_SHARD, ClusterState, TabletDump
from .registry import CheckOutcome
from .views import (
    HIVE_STATE_NEXT_TABLET_ID,
    INVALID_TABLET_ID,
    SS_SYS_PARAM_NEXT_PATH_ID,
    SS_SYS_PARAM_NEXT_SHARD_IDX,
    TABLET_ID_BLACKHOLE_BEGIN,
    bsc_view,
    hive_view,
    schemeshard_views,
    uniq_part,
)

# Gaps left above the highest identifier in use.  Generous on purpose: the point
# is to make reuse impossible even if the checker was shown an incomplete view
# of the cluster (a tablet whose backup we never saw may hold higher ids).
#
# For tablet ids the gap has to be large enough to clear ranges the root Hive
# already delegated to tenant Hives but whose tail is still unused -- those are
# invisible offline.  A delegated range is at most MaxRequestSequenceSize
# (THiveConfig, default 1000000), so the gap covers several outstanding ranges.
# Note the whole space is only [0x10000, 0x800000), so this is not free: the
# repair refuses rather than run into the blackhole.
HIVE_MAX_REQUEST_SEQUENCE_SIZE = 1000000
TABLET_ID_GAP = 4 * HIVE_MAX_REQUEST_SEQUENCE_SIZE
GROUP_ID_GAP = 1 << 12
SHARD_IDX_GAP = 1 << 12
PATH_ID_GAP = 1 << 12
VSLOT_ID_GAP = 1 << 8


@dataclass(frozen=True)
class Edit:
    """One row update to append to a tablet's changelog."""

    tablet_type: str
    table: str
    key: Mapping[str, Any]
    values: Mapping[str, Any]
    reason: str
    current: Mapping[str, Any] = field(default_factory=dict)
    # "upsert" merges the values into the row, "erase" removes it.  Both are
    # spelled the same way in a changelog commit and both are understood by the
    # restore (FindOpFromJson in flat_executor_recovery.cpp).
    op: str = "upsert"
    check_id: str = ""

    def describe(self) -> str:
        key = ", ".join("%s=%s" % (k, v) for k, v in sorted(self.key.items()))
        if self.op == "erase":
            return "%s/%s [%s]: erase row" % (self.tablet_type, self.table, key)
        changes = ", ".join(
            "%s %s -> %s" % (column, self.current.get(column, "?"), value)
            for column, value in sorted(self.values.items())
        )
        return "%s/%s [%s]: %s" % (self.tablet_type, self.table, key, changes)

    def to_change(self) -> Dict[str, Any]:
        change: Dict[str, Any] = {"table": self.table, "op": self.op}
        change.update(self.key)
        change.update(self.values)
        return change


RepairFunc = Callable[[ClusterState], Iterable[Edit]]

_REPAIRS: Dict[str, RepairFunc] = {}

# Checks that cannot be fixed by editing a backup, and what to do instead.
GUIDANCE: Dict[str, str] = {
    "I1": "SchemeShard still knows the tablet ids. Re-create the missing tablets in Hive "
          "with TEvCreateTablet carrying the original TabletID (hive.proto field 22) so the "
          "reference from SchemeShard stays valid.",
    "I2": "For Hive recovery an authoritative current SchemeShard backup is sufficient "
          "to erase orphan rows. Otherwise confirmation from a reachable Hive is needed.",
    "I3": "The groups are gone from BSController; the data they held cannot be conjured back. "
          "Recover the group configuration from an older BSController backup, or accept the "
          "loss of those channels.",
    "I8": "Objects the workload created are missing from SchemeShard. Either restore "
          "SchemeShard from a fresher backup or re-create the objects.",
    "I9": "Overlapping delegated ranges need a decision about which Hive owns which range; "
          "doctor only advances Next inside an existing range.",
    "I11": "Informational: how far apart the tablet states are.",
    "I12": "Same reuse as I4, but witnessed only by the ledger.",
    "I16": "Hive forgot tablets that make up a database. The database cannot boot until they "
           "are re-created in Hive with their original TabletID (hive.proto field 22); "
           "SchemeShard still knows every id.",
    "I17": "The root SchemeShard forgot a whole database while its tablets keep running. "
           "Either restore SchemeShard from a fresher backup, or delete the orphaned tablets "
           "in Hive once you are sure the data is not needed.",
    "I18": "Informational: how much of the tenant side this run could see.",
    "I13": "A truncated changelog means data never reached the backup. Pick another backup.",
    "I19": "Both halves of this need the running cluster, so start by re-running with "
           "--mon-endpoint. What is left after that has no safe offline form: an alter or a "
           "drop cannot be dropped without stranding its alter row or its shards, and an "
           "operation whose target path is gone live may never have reached the shards at all. "
           "Let the operation finish in the live cluster before taking the backup, restore a "
           "copy taken between operations, or -- for an operation whose path no longer exists "
           "-- boot the restored tablet with the SchemeShardControls.TolerateOrphanedPaths "
           "control set, which makes it drop those operations itself.",
    "I14": "Informational: ledger-dated staleness.",
}


# Tables the repairs read.  A repair may need more than the check that triggered
# it (fixing I6 wants SchemeShard's Shards, which I6 itself does not read), so
# doctor mode widens what the loader fetches.
REQUIRED_TABLES: Dict[str, List[str]] = {
    HIVE: ["Tablet", "State", "Sequences", "TabletChannelGen"],
    SCHEME_SHARD: [
        "Shards", "MigratedShards", "Paths", "SysParams",
        # I19 removes whole operations and realigns config versions.
        "TxInFlightV2", "TxShardsV2", "TxShards", "MigratedTxShards",
        "BlockStoreVolumes", "BlockStoreVolumeAlters", "FileStoreInfos", "FileStoreAlters",
    ],
    BS_CONTROLLER: ["Group", "State", "VSlot", "PDisk"],
}


# A finding these checks report is resolved by repairing another check, so the
# plan must not present them as beyond reach.
COVERED_BY: Dict[str, str] = {"I12": "I4"}


def repair(*check_ids: str) -> Callable[[RepairFunc], RepairFunc]:
    """Register a repair for one or more checks.

    Several checks can share a repair when they report the same underlying
    defect from different angles -- I4 and I15 are both "the tablet id allocator
    rolled back", seen from the backups and from the live tenants.  Duplicate
    edits are collapsed when the plan is built.
    """

    def decorator(func: RepairFunc) -> RepairFunc:
        for check_id in check_ids:
            if check_id in _REPAIRS:
                raise ValueError("duplicate repair for %r" % check_id)
            _REPAIRS[check_id] = func
        return func

    return decorator


def has_repair(check_id: str) -> bool:
    return check_id in _REPAIRS


# --------------------------------------------------------------------------
# Repairs
# --------------------------------------------------------------------------


@repair("I2")
def repair_hive_orphans(state: ClusterState) -> Iterator[Edit]:
    """Prevent a stale Hive restore from resurrecting already dropped shards.

    Restore replays changes on top of the current local database; it does not
    replace the database wholesale.  Consequently an old active Tablet row can
    turn a long-deleted shard back into an endlessly restarting tablet.  The
    loaded SchemeShard dumps are the authority for their own shard indexes.
    """
    hive = hive_view(state)
    if hive is None:
        return
    live_hive = state.live.hives.get(hive.tablet_id) if state.live else None

    known_owners = set()
    owned_shards = set()
    for ss in schemeshard_views(state):
        if state.authoritative_schemeshard and (
                ss.dump.changelog_truncated or not ss.dump.has_table("Shards")):
            continue
        known_owners.add(ss.tablet_id)
        for shard in ss.shards():
            owned_shards.add((shard.owner_tablet_id, shard.shard_idx))

    for tablet in hive.tablets():
        if tablet.owner is None or tablet.is_deleting:
            continue
        if tablet.owner_tablet_id not in known_owners or tablet.owner in owned_shards:
            continue
        if state.authoritative_schemeshard:
            yield Edit(
                tablet_type=HIVE, table=hive.TABLE_TABLET,
                key={"ID": tablet.tablet_id}, values={},
                current={"Owner": list(tablet.owner)}, op="erase",
                reason="current authoritative SchemeShard backup has no shard %d:%d; "
                       "prevent Hive restore from resurrecting tablet %d"
                       % (*tablet.owner, tablet.tablet_id),
            )
            continue
        if live_hive is None or not live_hive.reachable:
            continue
        # Backups of different system tablets are not a consistent snapshot.
        # Never erase merely because a SchemeShard backup lacks the owner: a
        # currently running tablet is stronger evidence.  Absence from live
        # Hive, or the viewer's explicit dead state, confirms resurrection.
        live_state = live_hive.tablet_states.get(tablet.tablet_id)
        live_restarts = live_hive.tablet_restarts.get(tablet.tablet_id, 0)
        if (tablet.tablet_id in live_hive.tablet_ids
                and live_state != 2 and live_restarts < 10):
            continue
        yield Edit(
            tablet_type=HIVE,
            table=hive.TABLE_TABLET,
            key={"ID": tablet.tablet_id},
            values={},
            current={"Owner": list(tablet.owner)},
            op="erase",
            reason="owning SchemeShard has no shard %d and live Hive reports state=%s, "
                   "restarts=%d; prevent stale restore from resurrecting tablet %d"
                   % (tablet.owner_idx, live_state, live_restarts, tablet.tablet_id),
        )


@repair("I4", "I15")
def repair_hive_tablet_id(state: ClusterState) -> Iterator[Edit]:
    """Raise Hive's tablet id allocator above every referenced tablet id."""
    hive = hive_view(state)
    if hive is None:
        return

    # Hive's allocator counts unique parts, never composed tablet ids.
    referenced = {uniq_part(t.tablet_id) for t in hive.tablets()}
    for ss in schemeshard_views(state):
        for shard in ss.shards():
            if shard.tablet_id and shard.tablet_id != INVALID_TABLET_ID:
                referenced.add(uniq_part(shard.tablet_id))
    if state.ledger is not None:
        for entry in state.ledger.of_op("create"):
            for shard in entry.get("shards", []) or []:
                if shard.get("tablet_id"):
                    referenced.add(uniq_part(int(shard["tablet_id"])))
    for tablet_id in state.foreign_tablet_ids:
        referenced.add(uniq_part(tablet_id))
    if not referenced:
        return

    target = max(referenced) + 1 + TABLET_ID_GAP
    if target >= TABLET_ID_BLACKHOLE_BEGIN:
        raise ValueError(
            "raising NextTabletId to %d would cross the reserved window at %d; "
            "the tablet id space is nearly exhausted and this needs a human"
            % (target, TABLET_ID_BLACKHOLE_BEGIN)
        )
    stored_next = hive.next_tablet_id()

    if stored_next is None or stored_next < target:
        yield Edit(
            tablet_type=HIVE,
            table=hive.TABLE_STATE,
            key={"Key": HIVE_STATE_NEXT_TABLET_ID},
            values={"Value": target},
            current={"Value": stored_next},
            reason="raise NextTabletId above max referenced uniq part %d plus a %d gap "
                   "(covers delegated ranges whose tail is unused)"
                   % (max(referenced), TABLET_ID_GAP),
        )

    # With sequences present the root Hive allocates from them, so the plain
    # counter alone would not stop reuse.
    for seq in hive.sequences():
        if seq.next is None or seq.end is None or seq.begin is None:
            continue
        if seq.next > seq.end:
            continue  # exhausted, nothing to hand out
        if not any(seq.next <= part <= seq.end for part in referenced):
            continue
        highest = max(p for p in referenced if seq.next <= p <= seq.end)
        advanced = highest + 1
        if advanced > seq.end:
            # Cannot be fixed inside this range; leaving it exhausted is safe.
            advanced = seq.end + 1
        yield Edit(
            tablet_type=HIVE,
            table=hive.TABLE_SEQUENCES,
            key={"OwnerId": seq.owner_id, "OwnerIdx": seq.owner_idx},
            values={"Next": advanced},
            current={"Next": seq.next},
            reason="advance the delegated range past uniq part %d, still in use" % highest,
        )


@repair("I20")
def repair_hive_known_generations(state: ClusterState) -> Iterator[Edit]:
    """Raise stale Hive KnownGeneration values to the live generations."""
    hive = hive_view(state)
    if hive is None or state.live is None:
        return
    live_hive = state.live.hives.get(hive.tablet_id)
    if live_hive is None or not live_hive.reachable:
        return
    for tablet in hive.tablets():
        live_generation = live_hive.tablet_generations.get(tablet.tablet_id)
        if (live_generation is None or tablet.known_generation is None
                or tablet.known_generation >= live_generation):
            continue
        yield Edit(
            tablet_type=HIVE,
            table=hive.TABLE_TABLET,
            key={"ID": tablet.tablet_id},
            values={"KnownGeneration": live_generation},
            current={"KnownGeneration": tablet.known_generation},
            reason="avoid ReasonBootSuggestOutdated restart loop; live generation is %d"
                   % live_generation,
        )


@repair("I5")
def repair_bsc_group_id(state: ClusterState) -> Iterator[Edit]:
    """Raise BSController's group id allocator above every referenced group."""
    bsc = bsc_view(state)
    hive = hive_view(state)
    if bsc is None:
        return

    referenced = set(bsc.group_ids())
    if hive is not None:
        for entry in hive.channel_generations():
            if entry.group is not None:
                referenced.add(entry.group)
    if not referenced:
        return

    target = max(referenced) + 1 + GROUP_ID_GAP
    current = bsc.next_group_id()
    if current is None or current < target:
        yield Edit(
            tablet_type=BS_CONTROLLER,
            table=bsc.TABLE_STATE,
            key={"FixedKey": True},
            values={"NextGroupID": target},
            current={"NextGroupID": current},
            reason="raise NextGroupID above max referenced group %d plus a %d gap"
                   % (max(referenced), GROUP_ID_GAP),
        )


@repair("I6")
def repair_ss_shard_idx(state: ClusterState) -> Iterator[Edit]:
    """Raise each SchemeShard's shard index allocator above what Hive still uses."""
    hive = hive_view(state)
    by_owner: Dict[int, List[int]] = {}
    if hive is not None:
        for tablet in hive.tablets():
            if tablet.owner is None or tablet.is_deleting:
                continue
            by_owner.setdefault(tablet.owner_tablet_id, []).append(tablet.owner_idx or 0)

    for ss in schemeshard_views(state):
        used = list(by_owner.get(ss.tablet_id, []))
        used += [s.shard_idx for s in ss.shards() if s.owner_tablet_id == ss.tablet_id]
        if not used:
            continue

        target = max(used) + 1 + SHARD_IDX_GAP
        current = ss.next_shard_idx()
        if current is None or current < target:
            yield Edit(
                tablet_type=SCHEME_SHARD,
                table=ss.TABLE_SYS_PARAMS,
                key={"Id": SS_SYS_PARAM_NEXT_SHARD_IDX},
                # SysParams.Value is Utf8: numbers live there as decimal strings.
                values={"Value": str(target)},
                current={"Value": current},
                reason="raise NextShardIdx above max shard index in use %d plus a %d gap"
                       % (max(used), SHARD_IDX_GAP),
            )


@repair("I10")
def repair_ss_path_id(state: ClusterState) -> Iterator[Edit]:
    """Raise each SchemeShard's path id allocator above its own rows."""
    for ss in schemeshard_views(state):
        path_ids = [p.path_id for p in ss.paths()]
        if state.ledger is not None:
            path_ids += [
                int(e["path_id"])
                for e in state.ledger.of_op("create")
                if e.get("path_id") is not None
            ]
        if not path_ids:
            continue

        target = max(path_ids) + 1 + PATH_ID_GAP
        current = ss.next_path_id()
        if current is None or current < target:
            yield Edit(
                tablet_type=SCHEME_SHARD,
                table=ss.TABLE_SYS_PARAMS,
                key={"Id": SS_SYS_PARAM_NEXT_PATH_ID},
                values={"Value": str(target)},
                current={"Value": current},
                reason="raise NextPathId above max known PathId %d plus a %d gap"
                       % (max(path_ids), PATH_ID_GAP),
            )


@repair("I7")
def repair_pdisk_next_vslot(state: ClusterState) -> Iterator[Edit]:
    """Raise each PDisk's slot allocator above the slots already on it."""
    bsc = bsc_view(state)
    if bsc is None:
        return

    max_used: Dict[str, int] = {}
    for slot in bsc.vslots():
        if slot.vslot_id is None:
            continue
        ref = "%s:%s" % (slot.node_id, slot.pdisk_id)
        if slot.vslot_id > max_used.get(ref, -1):
            max_used[ref] = slot.vslot_id

    for pdisk in bsc.pdisks():
        used = max_used.get(pdisk.ref)
        if used is None or pdisk.next_vslot_id is None:
            continue
        target = used + 1 + VSLOT_ID_GAP
        if pdisk.next_vslot_id > used:
            continue
        yield Edit(
            tablet_type=BS_CONTROLLER,
            table=bsc.TABLE_PDISK,
            key={"NodeID": pdisk.node_id, "PDiskID": pdisk.pdisk_id},
            values={"NextVSlotId": target},
            current={"NextVSlotId": pdisk.next_vslot_id},
            reason="raise NextVSlotId above max used VSlotID %d plus a %d gap"
                   % (used, VSLOT_ID_GAP),
        )


# --------------------------------------------------------------------------
# I19: what the restored SchemeShard would replay at live shards
#
# Both halves of this repair are anchored on the running cluster, and neither
# has an offline form.  The backup cannot tell whether an operation it holds in
# flight has since finished, nor how far a volume's config version has moved --
# only the live tablets know, and both answers are exact rather than heuristic.
# Without --mon-endpoint the repair yields nothing and the check falls through
# to its guidance.
# --------------------------------------------------------------------------


def _live_path(ss, path_id: Optional[int], names: Dict[int, str], live_paths: Mapping[str, Any]):
    """The live reading for a path of this SchemeShard, or None."""
    if path_id is None:
        return None
    name = names.get(path_id)
    if not name:
        return None
    live = live_paths.get(name)
    if live is None or not live.reachable or not live.exists:
        return None
    return live


def _repair_rolled_back_versions(ss, names, live_paths) -> Iterator[Edit]:
    """Raise a rolled-back config version up to the one the tablet has applied.

    A BlockStore volume or a FileStore answers ERROR_BAD_VERSION to anything
    older than its own version, and SchemeShard aborts the node on that status.
    The live version is the one safe target: the tablet acknowledges a config it
    has already applied *without* applying anything, so the operation completes
    and the stale body in the backup is never pushed onto the running volume.
    One higher would be applied -- that is a rollback of the data, not a repair.
    """
    dropped = {p.path_id for p in ss.paths() if p.is_dropped}

    for obj in ss.versioned_objects():
        if obj.path_id in dropped:
            continue
        live = _live_path(ss, obj.path_id, names, live_paths)
        if live is None or live.version is None:
            continue
        backup = obj.pending_version
        if backup is None or backup >= live.version:
            continue

        path = names.get(obj.path_id, "<path %d>" % obj.path_id)
        table, alters, column = ss.VERSIONED_TABLES[obj.kind]
        reason = (
            "the live %s at %s has applied version %d; sending %s from the backup comes back "
            "as ERROR_BAD_VERSION and aborts the SchemeShard node, while the equal version is "
            "acknowledged without touching the volume"
            % (obj.kind, path, live.version, backup)
        )

        if not obj.has_alter:
            yield Edit(
                tablet_type=SCHEME_SHARD,
                table=table,
                key={"PathId": obj.path_id},
                values={column: live.version},
                current={column: obj.version},
                reason=reason,
            )
            continue

        # An alter row is what ConfigureParts sends, and FinishAlter insists the
        # two stay one apart -- ``++AlterVersion; Y_ENSURE(AlterVersion ==
        # AlterData->AlterVersion)`` in TBlockStoreVolumeInfo, the same shape in
        # TFileStoreInfo -- so the pair has to move together.
        base_target = live.version - 1
        if obj.version is not None and obj.version > base_target:
            # Would mean lowering the committed version to satisfy the pending
            # one; that is a decision for a human, not for doctor.
            continue

        yield Edit(
            tablet_type=SCHEME_SHARD,
            table=alters,
            key={"PathId": obj.path_id},
            values={column: live.version},
            current={column: obj.alter_version},
            reason=reason,
        )
        if obj.version is None or obj.version < base_target:
            yield Edit(
                tablet_type=SCHEME_SHARD,
                table=table,
                key={"PathId": obj.path_id},
                values={column: base_target},
                current={column: obj.version},
                reason="keep the committed version one below the pending alter, as FinishAlter "
                       "requires",
            )


def _finished_live(ss, tx, names, live_paths) -> bool:
    """True when the live cluster proves this sub-operation already completed.

    ``CreateTxId`` plus ``CreateFinished`` on the target path name the very
    transaction that created it and say that the creation ran to the end.  That
    is the only evidence strong enough to drop an operation: it means every
    shard the operation addresses has already done its part, so resuming it can
    only re-propose finished work.
    """
    live = _live_path(ss, tx.target_path_id, names, live_paths)
    if live is None:
        return False
    return bool(
        live.create_finished
        and live.create_tx_id == tx.tx_id
        and live.path_id == tx.target_path_id
        and live.owner_id == ss.tablet_id
    )


def _repair_finished_operations(ss, names, live_paths) -> Iterator[Edit]:
    """Drop operations the live cluster has already run to the end.

    A row in TxInFlightV2 is erased only when its operation completes, so every
    row a stale backup still carries is an operation the restored SchemeShard
    picks up and drives again -- re-proposing a create to a datashard that
    already has the table, which fails a Y_ENSURE and puts the shard in a
    restart loop.  Removing the operation is what SchemeShard itself does on
    completion (PersistRemoveTx), and it is safe only under two conditions this
    function enforces:

    * the whole transaction goes, never a single part -- boot asserts that the
      parts of an operation are numbered from zero without gaps
      (``Y_ABORT_UNLESS(subTxId == operation->Parts.size())``);
    * every shard row goes with it -- a shard row whose operation is missing
      aborts the tablet on boot ("There's shard for unknown Operation").
    """
    by_tx: Dict[int, List[Any]] = {}
    for tx in ss.txs_in_flight():
        by_tx.setdefault(tx.tx_id, []).append(tx)

    for tx_id in sorted(by_tx):
        parts = sorted(by_tx[tx_id], key=lambda p: p.part_id)
        if not any(part.talks_to_shards for part in parts):
            continue
        # A versioned config is better served by the version repair above: the
        # operation then completes on its own and clears its own alter row,
        # which dropping it here would strand.
        if any(part.versioned_config for part in parts):
            continue
        if not all(_finished_live(ss, part, names, live_paths) for part in parts):
            continue

        for part in parts:
            path = names.get(part.target_path_id, "<path %s>" % part.target_path_id)
            reason = (
                "%s %s has already finished in the live cluster -- %s is there and this very "
                "transaction created it -- so resuming it would only re-propose done work to "
                "shards that already carry it"
                % (part.type_name, part.ref, path)
            )
            for table, key in ss.tx_shard_keys(tx_id, part.part_id):
                yield Edit(
                    tablet_type=SCHEME_SHARD,
                    table=table,
                    key=key,
                    values={},
                    op="erase",
                    reason="drop the shard row along with %s; a shard row without its operation "
                           "aborts SchemeShard on boot" % part.ref,
                )
            yield Edit(
                tablet_type=SCHEME_SHARD,
                table=ss.TABLE_TX_IN_FLIGHT,
                key={"TxId": tx_id, "TxPartId": part.part_id},
                values={},
                op="erase",
                reason=reason,
            )


@repair("I19")
def repair_ss_replay(state: ClusterState) -> Iterator[Edit]:
    """Stop a restored SchemeShard from replaying finished work at live shards."""
    live_paths = state.live.paths if state.live is not None else {}
    if not live_paths:
        return

    for ss in schemeshard_views(state):
        names = ss.path_names()
        yield from _repair_rolled_back_versions(ss, names, live_paths)
        yield from _repair_finished_operations(ss, names, live_paths)


# --------------------------------------------------------------------------
# Planning and applying
# --------------------------------------------------------------------------


@dataclass
class DoctorPlan:
    edits: List[Edit] = field(default_factory=list)
    # check id -> number of findings that no repair can address
    unrepairable: Dict[str, int] = field(default_factory=dict)
    # check id -> the check whose repair also resolves it
    covered: Dict[str, str] = field(default_factory=dict)
    # tablet type -> unparsable trailing changelog records dropped when applying
    discarded_tail: Dict[str, int] = field(default_factory=dict)

    @property
    def empty(self) -> bool:
        return not self.edits


def plan(state: ClusterState, outcomes: Sequence[CheckOutcome]) -> DoctorPlan:
    """Turn findings into a repair plan.

    Only checks that actually produced findings are repaired, so a healthy
    backup is left completely alone.
    """
    result = DoctorPlan()
    seen_edits: Dict[Tuple, str] = {}

    for outcome in outcomes:
        if not outcome.findings or outcome.skipped_reason:
            continue
        actionable = [f for f in outcome.findings if f.severity.name != "INFO"]
        if not actionable:
            continue

        covering = COVERED_BY.get(outcome.spec.id)
        if covering is not None:
            result.covered[outcome.spec.id] = covering
            continue

        func = _REPAIRS.get(outcome.spec.id)
        if func is None:
            result.unrepairable[outcome.spec.id] = len(actionable)
            continue

        before = len(result.edits)
        for edit in func(state) or ():
            fingerprint = (
                edit.tablet_type,
                edit.table,
                edit.op,
                tuple(sorted(edit.key.items())),
                tuple(sorted(edit.values.items())),
            )
            if fingerprint in seen_edits:
                # Another check already asked for exactly this change.
                result.covered[outcome.spec.id] = seen_edits[fingerprint]
                continue
            seen_edits[fingerprint] = outcome.spec.id
            result.edits.append(
                Edit(
                    tablet_type=edit.tablet_type,
                    table=edit.table,
                    key=edit.key,
                    values=edit.values,
                    reason=edit.reason,
                    current=edit.current,
                    op=edit.op,
                    check_id=outcome.spec.id,
                )
            )
        if len(result.edits) == before and outcome.spec.id not in result.covered:
            # A repair exists but had nothing to change: the finding needs the
            # cluster, not the file.
            result.unrepairable[outcome.spec.id] = len(actionable)

    return result


def render_plan(plan_: DoctorPlan, applied_to: Optional[str] = None, verbose: bool = False) -> str:
    lines: List[str] = []
    header = "DOCTOR" if applied_to else "DOCTOR PLAN (dry run)"
    lines.append(header)
    lines.append("")

    if plan_.edits:
        lines.append("repairable in the backup:" if not applied_to else "applied:")
        by_check: Dict[str, List[Edit]] = {}
        for edit in plan_.edits:
            by_check.setdefault(edit.check_id, []).append(edit)
        for check_id in sorted(by_check):
            if not verbose:
                groups: Dict[Tuple[str, str, str], List[Edit]] = {}
                for edit in by_check[check_id]:
                    groups.setdefault((edit.tablet_type, edit.table, edit.op), []).append(edit)
                for (tablet_type, table, op), edits in groups.items():
                    lines.append("  %s  %s/%s: %d %s(s)" % (check_id, tablet_type, table, len(edits), op))
                    for edit in edits[:3]:
                        lines.append("      %s" % edit.describe())
                        lines.append("        reason: %s" % edit.reason)
                    if len(edits) > 3:
                        lines.append("      ... %d more (use -v to show all edits)" % (len(edits) - 3))
                continue
            lines.append("  %s" % check_id)
            for edit in by_check[check_id]:
                lines.append("      %s" % edit.describe())
                lines.append("        reason: %s" % edit.reason)
    else:
        lines.append("nothing to repair in the backup files")

    if plan_.covered:
        lines.append("")
        lines.append("resolved by the repairs above:")
        for check_id in sorted(plan_.covered):
            lines.append("  %s  covered by repairing %s" % (check_id, plan_.covered[check_id]))

    if plan_.unrepairable:
        lines.append("")
        lines.append("not repairable by editing the backup:")
        for check_id in sorted(plan_.unrepairable):
            count = plan_.unrepairable[check_id]
            lines.append("  %s  %d finding(s)" % (check_id, count))
            guidance = GUIDANCE.get(check_id)
            if guidance:
                lines.append("      %s" % guidance)

    if plan_.discarded_tail:
        lines.append("")
        lines.append("discarded unparsable changelog tail:")
        for tablet_type in sorted(plan_.discarded_tail):
            lines.append(
                "  %s  %d trailing record(s) -- restore would have stopped there anyway, "
                "and the repair has to be reachable"
                % (tablet_type, plan_.discarded_tail[tablet_type])
            )

    if applied_to:
        lines.append("")
        lines.append("backup written to: %s" % applied_to)
        lines.append(
            "snapshot files and manifest are untouched, so restore works with checksum "
            "validation enabled"
        )
    elif plan_.edits:
        lines.append("")
        lines.append("pass --doctor-out DIR to repair a copy, or --in-place to modify the original")

    return "\n".join(lines)


def _dump_for(state: ClusterState, tablet_type: str) -> Optional[TabletDump]:
    return state.one(tablet_type)


def apply(
    state: ClusterState,
    plan_: DoctorPlan,
    out_dir: Optional[str] = None,
    in_place: bool = False,
) -> str:
    """Append the repair commits, optionally to a copy of the backup tree.

    Returns the directory the repaired backup lives in.
    """
    if not plan_.edits:
        raise ValueError("nothing to apply")
    if not out_dir and not in_place:
        raise ValueError("apply needs either out_dir or in_place")

    by_tablet: Dict[str, List[Edit]] = {}
    for edit in plan_.edits:
        by_tablet.setdefault(edit.tablet_type, []).append(edit)

    for tablet_type in by_tablet:
        if _dump_for(state, tablet_type) is None:
            raise ValueError("no loaded backup for %s, cannot repair it" % tablet_type)

    if in_place:
        for tablet_type, edits in by_tablet.items():
            dropped = _append_commit(_dump_for(state, tablet_type).source, edits)
            if dropped:
                plan_.discarded_tail[tablet_type] = dropped
        return ", ".join(sorted(_dump_for(state, t).source for t in by_tablet))

    # Copy every loaded backup, not just the edited ones, so the output is a
    # self-contained set that can be re-checked and restored as a whole.
    paths: Dict[str, str] = {}
    for dump in state.dumps:
        relative = os.path.join(
            dump.tablet_type, str(dump.tablet_id), os.path.basename(dump.source.rstrip(os.sep))
        )
        destination = os.path.join(out_dir, relative)
        if os.path.exists(destination):
            raise ValueError("%s already exists, refusing to overwrite" % destination)
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        # copytree uses copy2, which keeps mtimes, so freshness reporting stays
        # meaningful on the copy.
        shutil.copytree(dump.source, destination)
        paths[dump.tablet_type] = destination

    for tablet_type, edits in by_tablet.items():
        dropped = _append_commit(paths[tablet_type], edits)
        if dropped:
            plan_.discarded_tail[tablet_type] = dropped

    return out_dir


def _append_commit(backup_dir: str, edits: Sequence[Edit]) -> int:
    """Append one repair commit, discarding an unusable changelog tail first.

    A backup written up to a crash ends in a partial record.  Replay -- both the
    checker's and ``TTxUploadChangelog``'s -- stops at the first line it cannot
    parse, so a commit appended *after* that line is unreachable and the repair
    silently does nothing.  Dropping the trailing records that cannot be parsed
    loses nothing: restore was never going to apply them either.

    Returns how many trailing records were discarded.
    """
    changelog = os.path.join(backup_dir, "changelog.json")

    kept: List[bytes] = []
    discarded = 0
    if os.path.isfile(changelog):
        with open(changelog, "rb") as handle:
            kept = handle.read().split(b"\n")
        # split leaves a trailing empty element for a newline-terminated file
        if kept and kept[-1] == b"":
            kept.pop()
        while kept:
            try:
                json.loads(kept[-1].decode("utf-8", errors="surrogateescape"))
                break
            except (ValueError, UnicodeDecodeError):
                kept.pop()
                discarded += 1

    # Every changelog line carries prev_sha256: the running sha256 of all
    # preceding lines, each with its newline (flat_executor_backup.cpp writes
    # Checksum.Intermediate(); the restore recomputes and compares it in
    # flat_executor_recovery.cpp).  A line without a matching value is rejected,
    # so the appended commit has to continue the chain.
    prefix = b"".join(entry + b"\n" for entry in kept)
    commit = {
        "data_changes": [edit.to_change() for edit in edits],
        "prev_sha256": hashlib.sha256(prefix).hexdigest(),
    }
    line = json.dumps(commit, sort_keys=True) + "\n"

    body = prefix + line.encode()
    with open(changelog, "wb") as handle:
        handle.write(body)

    # The writer keeps a running sha256 of the whole changelog next to it.  The
    # restore path does not verify it, but leaving it stale would be misleading.
    with open(changelog + ".sha256", "w") as handle:
        handle.write(hashlib.sha256(body).hexdigest())

    return discarded
