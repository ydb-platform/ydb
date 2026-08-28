# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.markers import (
    SLOT_CRASH_ON_PRIMARY_DDISK_LOSS,
    known_bug,
)


def _is_primary_ddisk(host):
    return host.ddisk_role.lower() in ('primary', '0')


class TestF3_03OfflinePromotesHandoff(NbsCase):
    """F3.3 — Offline promotes a HandOff to Primary with watermark 0.

    Does not execute: stopping a Primary DDisk host segfaults the NBS slot.
    """

    @known_bug(SLOT_CRASH_ON_PRIMARY_DDISK_LOSS)
    def test_offline_promotes_handoff(self):
        disk = self.make_disk()
        self.write_pattern(disk, 0, 8)
        before = self.vchunk_hosts(disk.tablet_id)
        before_primaries = {h.index for h in before if _is_primary_ddisk(h)}
        host = self.pick_primary_ddisk_host(disk.tablet_id)
        self.faults.stop_node(host.node_id)
        self.wait_host_offline(disk, host.node_id)

        def promoted():
            hosts = self.vchunk_hosts(disk.tablet_id)
            # PromoteHost sets watermark 0; OnCopyProgress may already have
            # persisted an 8 MiB tick; OnCopyComplete unsets it. Any of those
            # on a former HandOff is a completed promotion.
            return any(
                h.index not in before_primaries and _is_primary_ddisk(h)
                for h in hosts
            )

        self.wait_until(
            promoted,
            timeout_seconds=15,
            sleep_seconds=0.2,
            description='HandOff promoted to Primary',
        )
