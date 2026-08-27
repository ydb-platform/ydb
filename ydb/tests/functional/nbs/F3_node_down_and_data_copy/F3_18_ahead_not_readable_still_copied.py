# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF3_18AheadNotReadableStillCopied(NbsCase):
    """F3.18 — AheadField is not a read source, but the copier still copies it."""

    def test_ahead_not_readable_still_copied(self):
        disk = self.make_disk()
        seed = self.write_pattern(disk, 0, 8)
        host = self.pick_dbg_storage_node(disk.tablet_id)
        self.faults.stop_node(host.node_id)
        self.wait_host_state(disk.tablet_id, host.node_id, ('Offline', '2'), timeout_seconds=45)

        # Writes flushed to the fresh DDisk above the watermark become Ahead.
        ahead = self.write_pattern(disk, 128, 8)
        self.assert_pattern(disk, seed)
        self.assert_pattern(disk, ahead)
