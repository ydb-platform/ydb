# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.markers import (
    SLOT_CRASH_ON_PRIMARY_DDISK_LOSS,
    known_bug,
)


class TestF3_08FreshDdiskNotReadableAboveWatermark(NbsCase):
    """F3.8 — reads of not-yet-copied ranges are served from other replicas.

    Does not execute: stopping a Primary DDisk host segfaults the NBS slot.
    """

    @known_bug(SLOT_CRASH_ON_PRIMARY_DDISK_LOSS)
    def test_fresh_ddisk_not_readable_above_watermark(self):
        disk = self.make_disk()
        payloads = self.write_pattern(disk, 0, 16)
        host = self.pick_primary_ddisk_host(disk.tablet_id)
        self.faults.stop_node(host.node_id)
        self.wait_host_offline(disk, host.node_id)
        self.assert_pattern(disk, payloads)
        more = self.write_pattern(disk, 64, 4)
        self.assert_pattern(disk, more)
