# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF3_08FreshDdiskNotReadableAboveWatermark(NbsCase):
    """F3.8 — reads of not-yet-copied ranges are served from other replicas."""

    def test_fresh_ddisk_not_readable_above_watermark(self):
        disk = self.make_disk()
        payloads = self.write_pattern(disk, 0, 16)
        host = self.pick_dbg_storage_node(disk.tablet_id)
        self.faults.stop_node(host.node_id)
        self.wait_host_state(disk.tablet_id, host.node_id, ('Offline', '2'), timeout_seconds=45)
        self.assert_pattern(disk, payloads)
        more = self.write_pattern(disk, 64, 4)
        self.assert_pattern(disk, more)
