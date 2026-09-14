# -*- coding: utf-8 -*-
"""Identifier-sequence invariants.

These are the ones that predict *silent data corruption* rather than loss.  A
rolled-back allocator hands out an identifier that is still in use elsewhere,
and the two objects then collide in blob storage.  Check these first.

Note that Hive already repairs its own counter on load::

    TTabletId nextTabletId = Max(maxTabletId + 1, Self->NextTabletId);
    (ydb/core/mind/hive/tx__load_everything.cpp)

so it is protected against tablets it still remembers.  What it cannot protect
against are tablet ids that only SchemeShard remembers -- which is exactly the
cross-tablet check below.
"""

from __future__ import annotations

from typing import Dict, Iterator, List

from ..model import BS_CONTROLLER, HIVE, LIVE, SCHEME_SHARD, ClusterState, Finding, critical, error, info
from ..registry import check
from ..views import HiveSequence, bsc_view, hive_view, schemeshard_views, uniq_part
from ._util import capped


def _reissue_predicate(sequences: List[HiveSequence], effective_next: int):
    """Return a predicate telling whether Hive would hand out an id again.

    Everything here is in *unique part* space: Hive's allocator counts the low
    44 bits of a tablet id, not the composed id (``tabletid.h``).  Callers must
    pass ids through ``uniq_part`` first.

    The root Hive allocates from ``Sequences`` when they are present and falls
    back to the plain ``NextTabletId`` counter otherwise.
    """
    # Rows granted to another Hive are reservations, not allocation sources --
    # their Next is a snapshot from grant time.  Treating them as live ranges
    # reports the whole delegated span as about to be reissued, which on a
    # healthy cluster is pure noise.  End is exclusive.
    ranges = [
        (seq.next, seq.end)
        for seq in sequences
        if seq.is_allocation_source
        and seq.next is not None
        and seq.end is not None
        and seq.next < seq.end
    ]

    if ranges:
        def will_reissue(tablet_id: int) -> bool:
            return any(low <= tablet_id < high for low, high in ranges)
    else:
        def will_reissue(tablet_id: int) -> bool:
            return tablet_id >= effective_next

    return will_reissue


@check(
    id="I4",
    title="Hive will not hand out a tablet id that is still referenced",
    needs={
        HIVE: ["Tablet", "State", "Sequences"],
        SCHEME_SHARD: ["Shards", "MigratedShards"],
    },
    tags=("sequences", "hive", "corruption"),
)
def hive_tablet_id_reuse(state: ClusterState) -> Iterator[Finding]:
    """Broken by a stale Hive: the allocator rolls back below ids SchemeShard
    still uses, so a brand new tablet gets an id that already owns blobs, a
    blocked generation and a channel history."""
    hive = hive_view(state)

    # full tablet id -> unique part, the space the allocator counts in
    referenced: Dict[int, int] = {}
    known = set()
    for tablet in hive.tablets():
        referenced[tablet.tablet_id] = uniq_part(tablet.tablet_id)
        known.add(tablet.tablet_id)
    for ss in schemeshard_views(state):
        for shard in ss.shards():
            if shard.tablet_id:
                referenced[shard.tablet_id] = uniq_part(shard.tablet_id)

    if not referenced:
        yield info("no tablet ids referenced anywhere, nothing to check")
        return

    stored_next = hive.next_tablet_id()
    # Hive repairs its counter against its own tablets on load, so the effective
    # floor is already above everything it remembers.
    own_max = max((uniq_part(t) for t in known), default=0)
    effective_next = max(stored_next or 0, own_max + 1 if known else 0)

    will_reissue = _reissue_predicate(hive.sequences(), effective_next)
    at_risk = sorted(
        (full for full, part in referenced.items() if will_reissue(part)),
        key=lambda full: referenced[full],
    )

    yield from capped(
        at_risk,
        lambda tablet_id: critical(
            "tablet id %d (uniq part %d) is still referenced but Hive will allocate it "
            "again (stored NextTabletId %s, effective %d)"
            % (tablet_id, referenced[tablet_id], stored_next, effective_next),
            tablet_id=tablet_id,
            uniq_part=referenced[tablet_id],
            known_to_hive=tablet_id in known,
            stored_next_tablet_id=stored_next,
            effective_next_tablet_id=effective_next,
        ),
        lambda total, rest: critical(
            "%d referenced tablet ids will be handed out again by Hive" % total,
            total=total,
            stored_next_tablet_id=stored_next,
            effective_next_tablet_id=effective_next,
            sample=rest[:100],
        ),
    )

    if not at_risk:
        max_part = max(referenced.values())
        yield info(
            "tablet id allocator is ahead of every reference "
            "(next %d, max referenced uniq part %d)" % (effective_next, max_part),
            effective_next_tablet_id=effective_next,
            max_referenced_uniq_part=max_part,
        )


@check(
    id="I5",
    title="BSController will not hand out a group id that is still referenced",
    needs={BS_CONTROLLER: ["Group", "State"], HIVE: ["TabletChannelGen"]},
    tags=("sequences", "bscontroller", "corruption"),
)
def bsc_group_id_reuse(state: ClusterState) -> Iterator[Finding]:
    """Broken by a stale BSController: a new group is created with an id that
    tablet channels still point at, so the old group's data is read from the
    new group's disks."""
    bsc = bsc_view(state)
    hive = hive_view(state)

    next_group_id = bsc.next_group_id()
    if next_group_id is None:
        yield error("BSController State has no NextGroupID, cannot check group id reuse")
        return

    referenced = set(bsc.group_ids())
    for entry in hive.channel_generations():
        if entry.group is not None:
            referenced.add(entry.group)

    at_risk = sorted(g for g in referenced if g >= next_group_id)

    yield from capped(
        at_risk,
        lambda group_id: critical(
            "group %d is still referenced but BSController will allocate it again "
            "(NextGroupID %d)" % (group_id, next_group_id),
            group_id=group_id,
            next_group_id=next_group_id,
        ),
        lambda total, rest: critical(
            "%d referenced group ids will be handed out again by BSController" % total,
            total=total,
            next_group_id=next_group_id,
            sample=rest[:100],
        ),
    )

    if not at_risk and referenced:
        yield info(
            "group id allocator is ahead of every reference "
            "(next %d, max referenced %d)" % (next_group_id, max(referenced)),
            next_group_id=next_group_id,
            max_referenced=max(referenced),
        )


@check(
    id="I6",
    title="SchemeShard will not hand out a shard index that Hive still uses",
    needs={SCHEME_SHARD: ["SysParams"], HIVE: ["Tablet"]},
    tags=("sequences", "schemeshard", "corruption"),
)
def ss_shard_idx_reuse(state: ClusterState) -> Iterator[Finding]:
    """Broken by a stale SchemeShard: it re-issues a ShardIdx that Hive still
    maps to a live tablet, so two different objects claim the same owner key."""
    hive = hive_view(state)
    by_owner = {}
    for tablet in hive.tablets():
        if tablet.owner is None or tablet.is_deleting:
            continue
        by_owner.setdefault(tablet.owner_tablet_id, []).append(tablet)

    for ss in schemeshard_views(state):
        next_shard_idx = ss.next_shard_idx()
        if next_shard_idx is None:
            yield error(
                "SchemeShard %d has no NextShardIdx in SysParams, cannot check shard index reuse"
                % ss.tablet_id,
                schemeshard=ss.tablet_id,
            )
            continue

        at_risk = sorted(
            (t for t in by_owner.get(ss.tablet_id, []) if (t.owner_idx or 0) >= next_shard_idx),
            key=lambda t: t.owner_idx or 0,
        )

        yield from capped(
            at_risk,
            lambda tablet: critical(
                "shard index %s is used by tablet %d but SchemeShard %d will "
                "allocate it again (NextShardIdx %d)"
                % (tablet.owner_idx, tablet.tablet_id, ss.tablet_id, next_shard_idx),
                shard_idx=tablet.owner_idx,
                tablet_id=tablet.tablet_id,
                schemeshard=ss.tablet_id,
                next_shard_idx=next_shard_idx,
            ),
            lambda total, rest: critical(
                "%d shard indexes in use will be handed out again by SchemeShard %d"
                % (total, ss.tablet_id),
                total=total,
                schemeshard=ss.tablet_id,
                next_shard_idx=next_shard_idx,
                sample=[t.owner_idx for t in rest[:100]],
            ),
        )


@check(
    id="I9",
    title="Hive tablet id sequences are internally sane",
    needs={HIVE: ["Sequences", "State", "TabletOwners"]},
    tags=("sequences", "hive"),
)
def hive_sequences_sane(state: ClusterState) -> Iterator[Finding]:
    """Guards a hand-edited backup: ``Next`` must sit inside ``[Begin, End]``
    and the delegated ranges must not overlap."""
    hive = hive_view(state)
    sequences = hive.sequences()
    if not sequences:
        return

    for seq in sequences:
        if seq.begin is None or seq.end is None or seq.next is None:
            yield error(
                "sequence (%s, %s) has null bounds" % (seq.owner_id, seq.owner_idx),
                owner=[seq.owner_id, seq.owner_idx],
                begin=seq.begin,
                end=seq.end,
                next=seq.next,
            )
            continue
        if seq.begin > seq.end:
            yield error(
                "sequence (%s, %s) is inverted: begin %d > end %d"
                % (seq.owner_id, seq.owner_idx, seq.begin, seq.end),
                owner=[seq.owner_id, seq.owner_idx],
                begin=seq.begin,
                end=seq.end,
            )
        if not (seq.begin <= seq.next <= seq.end + 1):
            yield error(
                "sequence (%s, %s) has next %d outside [%d, %d]"
                % (seq.owner_id, seq.owner_idx, seq.next, seq.begin, seq.end),
                owner=[seq.owner_id, seq.owner_idx],
                begin=seq.begin,
                end=seq.end,
                next=seq.next,
            )

    # Only the ranges this Hive allocates from may not overlap each other.  A
    # granted range is carved out of a free span and therefore nests inside it
    # by construction -- comparing raw [Begin, End) spans across both kinds
    # reports every healthy cluster as broken.
    sources = sorted(
        (s for s in hive.allocation_sources() if s.begin is not None and s.end is not None),
        key=lambda s: s.begin,
    )
    for previous, current in zip(sources, sources[1:]):
        if current.begin < previous.end:
            yield critical(
                "allocation ranges (%s, %s) [%d, %d) and (%s, %s) [%d, %d) overlap, "
                "the same tablet id can be handed out twice"
                % (
                    previous.owner_id,
                    previous.owner_idx,
                    previous.begin,
                    previous.end,
                    current.owner_id,
                    current.owner_idx,
                    current.begin,
                    current.end,
                ),
                first=[previous.owner_id, previous.owner_idx, previous.begin, previous.end],
                second=[current.owner_id, current.owner_idx, current.begin, current.end],
            )

    # The real cross-Hive hazard: a range still granted to a tenant Hive that
    # this Hive is about to allocate from again.
    for grant in hive.grants():
        for source in sources:
            if source.next is None:
                continue
            if source.next < grant.end and grant.begin < source.end:
                yield critical(
                    "range [%d, %d) is granted to Hive %d but this Hive allocates from %d "
                    "inside it -- both would mint the same tablet ids"
                    % (grant.begin, grant.end, grant.owner_id, source.next),
                    grant=[grant.begin, grant.end],
                    grant_owner=grant.owner_id,
                    allocating_from=source.next,
                )


@check(
    id="I10",
    title="SchemeShard counters are ahead of its own tables",
    needs={SCHEME_SHARD: ["SysParams", "Paths", "Shards", "MigratedShards"]},
    tags=("sequences", "schemeshard"),
)
def ss_counters_sane(state: ClusterState) -> Iterator[Finding]:
    """Guards a hand-edited backup: bumping a counter must not leave it below
    the rows already present in the same dump."""
    for ss in schemeshard_views(state):
        path_ids = [p.path_id for p in ss.paths()]
        shard_idxs = [s.shard_idx for s in ss.shards() if s.owner_tablet_id == ss.tablet_id]

        next_path_id = ss.next_path_id()
        if next_path_id is not None and path_ids and next_path_id <= max(path_ids):
            yield critical(
                "SchemeShard %d NextPathId %d is not above max existing PathId %d"
                % (ss.tablet_id, next_path_id, max(path_ids)),
                schemeshard=ss.tablet_id,
                next_path_id=next_path_id,
                max_path_id=max(path_ids),
            )

        next_shard_idx = ss.next_shard_idx()
        if next_shard_idx is not None and shard_idxs and next_shard_idx <= max(shard_idxs):
            yield critical(
                "SchemeShard %d NextShardIdx %d is not above max existing ShardIdx %d"
                % (ss.tablet_id, next_shard_idx, max(shard_idxs)),
                schemeshard=ss.tablet_id,
                next_shard_idx=next_shard_idx,
                max_shard_idx=max(shard_idxs),
            )


@check(
    id="I20",
    title="Hive backup generations are not behind live tablets",
    needs={HIVE: ["Tablet"], LIVE: []},
    tags=("hive", "generation", "restarts"),
)
def hive_generations_not_behind_live(state: ClusterState) -> Iterator[Finding]:
    """A stale KnownGeneration makes Hive issue obsolete boot suggestions.

    The tablet rejects each suggestion with ReasonBootSuggestOutdated, producing
    a tight restart loop after restore even though the tablet data is intact.
    """
    hive = hive_view(state)
    live_hive = state.live.hives.get(hive.tablet_id) if state.live else None
    if live_hive is None or not live_hive.reachable:
        return

    behind = []
    for tablet in hive.tablets():
        live_generation = live_hive.tablet_generations.get(tablet.tablet_id)
        if live_generation is None or tablet.known_generation is None:
            continue
        if tablet.known_generation < live_generation:
            behind.append((tablet, live_generation))

    yield from capped(
        behind,
        lambda pair: critical(
            "tablet %d KnownGeneration %d is behind live generation %d"
            % (pair[0].tablet_id, pair[0].known_generation, pair[1]),
            tablet_id=pair[0].tablet_id,
            backup_generation=pair[0].known_generation,
            live_generation=pair[1],
        ),
        lambda total, rest: critical(
            "%d Hive tablets have stale KnownGeneration" % total,
            total=total,
            sample=[pair[0].tablet_id for pair in rest[:100]],
        ),
    )
