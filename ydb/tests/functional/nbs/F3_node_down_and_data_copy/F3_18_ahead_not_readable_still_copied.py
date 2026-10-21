# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.markers import (
    SLOT_CRASH_ON_PRIMARY_DDISK_LOSS,
    known_bug,
)


class TestF3_18AheadNotReadableStillCopied(NbsCase):
    """F3.18 — AheadField is not a read source, but the copier still copies it.

    Does not execute: stopping a Primary DDisk host segfaults the NBS slot.
    """

    @known_bug(SLOT_CRASH_ON_PRIMARY_DDISK_LOSS)
    def test_ahead_not_readable_still_copied(self):
        disk = self.make_disk()
        seed = self.write_pattern(disk, 0, 8)
        host = self.pick_primary_ddisk_host(disk.tablet_id)
        self.faults.stop_node(host.node_id)
        self.wait_host_offline(disk, host.node_id)

        # Writes flushed to the fresh DDisk above the watermark become Ahead.
        ahead = self.write_pattern(disk, 128, 8)
        self.assert_pattern(disk, seed)
        self.assert_pattern(disk, ahead)
