# -*- coding: utf-8 -*-
"""Referential invariants between the cluster system tablets.

These catch what a stale restore *loses* (objects one tablet still references
and another has forgotten).  They are recoverable by reconciliation; the silent
data corruption lives in ``sequences.py``.
"""

from __future__ import annotations

from typing import Iterator

from ..model import (
    BS_CONTROLLER,
    HIVE,
    SCHEME_SHARD,
    ClusterState,
    Finding,
    critical,
    error,
    info,
    warning,
)
from ..registry import check
from ..views import ETabletState, INVALID_TABLET_ID, bsc_view, hive_view, schemeshard_views
from ._util import capped

# Shards.TabletId is 0 until Hive answers the create request.
NOT_CREATED_YET = (None, 0, INVALID_TABLET_ID)


@check(
    id="I1",
    title="Every shard SchemeShard knows is known to Hive under the same owner",
    needs={
        HIVE: ["Tablet"],
        SCHEME_SHARD: ["Shards", "MigratedShards", "SubDomains", "SubDomainShards", "Paths"],
    },
    tags=("refs", "hive", "schemeshard"),
)
def ss_shards_known_to_hive(state: ClusterState) -> Iterator[Finding]:
    """Broken by a stale Hive: SchemeShard points at tablets Hive forgot, so
    those shards never boot again."""
    hive = hive_view(state)
    tablets = hive.tablets_by_id()

    missing = []
    mismatched = []
    pending = 0
    delegated = 0
    unverified = 0

    # A database's system tablets may sit in the root Hive or in the database's
    # own Hive -- it varies, and both are legitimate.  So a shard missing from
    # the root Hive is not evidence of loss by itself; it only becomes one when
    # a live reading shows no Hive has it either.
    live_tablets = set()
    if state.live is not None:
        for live_hive in state.live.hives.values():
            live_tablets |= live_hive.tablet_ids

    for ss in schemeshard_views(state):
        may_be_elsewhere = set()
        for subdomain in ss.databases():
            tenant_hive = subdomain.effective_hive_id
            if not tenant_hive or tenant_hive == hive.tablet_id:
                continue
            for shard in subdomain.shards:
                if shard.tablet_id != tenant_hive:
                    may_be_elsewhere.add(shard.shard_idx)

        for shard in ss.shards():
            if shard.tablet_id in NOT_CREATED_YET:
                pending += 1
                continue

            tablet = tablets.get(shard.tablet_id)
            if tablet is None:
                if shard.tablet_id in live_tablets:
                    delegated += 1          # another Hive has it: accounted for
                elif shard.shard_idx in may_be_elsewhere and state.live is None:
                    unverified += 1         # could be in the tenant Hive, unread
                else:
                    missing.append(shard)
                continue

            expected = (shard.owner_tablet_id, shard.shard_idx)
            if tablet.owner != expected:
                mismatched.append((shard, tablet))

    yield from capped(
        missing,
        lambda shard: error(
            "shard %s (tablet %d, path %s) is unknown to Hive"
            % (shard.ref, shard.tablet_id, shard.path_id),
            shard=shard.ref,
            tablet_id=shard.tablet_id,
            path_id=shard.path_id,
            migrated=shard.migrated,
        ),
        lambda total, rest: error(
            "%d SchemeShard shards are unknown to Hive" % total,
            total=total,
            sample=[s.tablet_id for s in rest[:100]],
        ),
    )

    yield from capped(
        mismatched,
        lambda pair: critical(
            "tablet %d is owned by %s in Hive but by %s in SchemeShard"
            % (pair[0].tablet_id, pair[1].owner, (pair[0].owner_tablet_id, pair[0].shard_idx)),
            tablet_id=pair[0].tablet_id,
            hive_owner=list(pair[1].owner) if pair[1].owner else None,
            schemeshard_owner=[pair[0].owner_tablet_id, pair[0].shard_idx],
        ),
        lambda total, rest: critical(
            "%d tablets have conflicting owners between Hive and SchemeShard" % total,
            total=total,
            sample=[p[0].tablet_id for p in rest[:100]],
        ),
    )

    if pending:
        yield info(
            "%d shards have no tablet id yet (creation in flight), not checked" % pending,
            count=pending,
        )

    if delegated:
        yield info(
            "%d shards are absent from this Hive but present in a live tenant Hive" % delegated,
            count=delegated,
        )

    if unverified:
        yield warning(
            "%d shards of databases with their own Hive are absent here and could not be "
            "verified: tenant Hives have no backups, pass --mon-endpoint to check them"
            % unverified,
            count=unverified,
        )


@check(
    id="I2",
    title="Hive holds no tablets that SchemeShard no longer knows about",
    needs={HIVE: ["Tablet"], SCHEME_SHARD: ["Shards", "MigratedShards"]},
    tags=("refs", "hive", "schemeshard"),
)
def hive_tablets_known_to_ss(state: ClusterState) -> Iterator[Finding]:
    """Broken by a stale SchemeShard: Hive keeps running orphans that consume
    resources and will never be deleted, because nothing owns them any more."""
    hive = hive_view(state)

    known_owners = set()
    owned_shards = set()
    for ss in schemeshard_views(state):
        known_owners.add(ss.tablet_id)
        for shard in ss.shards():
            owned_shards.add((shard.owner_tablet_id, shard.shard_idx))
            known_owners.add(shard.owner_tablet_id)

    orphans = []
    for tablet in hive.tablets():
        if tablet.owner is None or tablet.is_deleting:
            continue
        # Only judge tablets owned by a SchemeShard we actually have a dump for.
        if tablet.owner_tablet_id not in known_owners:
            continue
        if tablet.owner not in owned_shards:
            orphans.append(tablet)

    yield from capped(
        orphans,
        lambda tablet: error(
            "tablet %d (owner %s, %s) has no shard in SchemeShard"
            % (tablet.tablet_id, tablet.owner, ETabletState.name(tablet.state)),
            tablet_id=tablet.tablet_id,
            owner=list(tablet.owner),
            state=ETabletState.name(tablet.state),
        ),
        lambda total, rest: error(
            "%d Hive tablets are orphaned (owner SchemeShard has no such shard)" % total,
            total=total,
            sample=[t.tablet_id for t in rest[:100]],
        ),
    )


@check(
    id="I3",
    title="Every storage group Hive references exists in BSController",
    needs={HIVE: ["TabletChannelGen"], BS_CONTROLLER: ["Group"]},
    tags=("refs", "hive", "bscontroller"),
)
def hive_groups_exist_in_bsc(state: ClusterState) -> Iterator[Finding]:
    """Broken by a stale BSController: tablet channels point at groups the
    controller does not have, so those channels cannot be read."""
    hive = hive_view(state)
    groups = bsc_view(state).group_ids()

    dangling_current = []
    dangling_history = []

    for entry in hive.channel_generations():
        if entry.group is None or entry.group in groups:
            continue
        if entry.is_history:
            dangling_history.append(entry)
        else:
            dangling_current.append(entry)

    yield from capped(
        dangling_current,
        lambda e: critical(
            "tablet %d channel %s generation %s points at group %d, missing in BSController"
            % (e.tablet_id, e.channel, e.generation, e.group),
            tablet_id=e.tablet_id,
            channel=e.channel,
            generation=e.generation,
            group=e.group,
        ),
        lambda total, rest: critical(
            "%d live tablet channels point at groups missing in BSController" % total,
            total=total,
            sample=sorted({e.group for e in rest})[:100],
        ),
    )

    yield from capped(
        dangling_history,
        lambda e: error(
            "tablet %d channel %s history generation %s points at group %d, "
            "missing in BSController" % (e.tablet_id, e.channel, e.generation, e.group),
            tablet_id=e.tablet_id,
            channel=e.channel,
            generation=e.generation,
            group=e.group,
        ),
        lambda total, rest: error(
            "%d historical tablet channels point at groups missing in BSController" % total,
            total=total,
            sample=sorted({e.group for e in rest})[:100],
        ),
    )
