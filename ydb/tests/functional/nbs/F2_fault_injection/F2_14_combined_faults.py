# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF2_14CombinedFaults(NbsCase):
    """F2.14 — tablet kill + one host SIGSTOP + one host stop."""

    def test_combined_faults(self):
        disk = self.make_disk()
        payloads = self.write_and_verify(disk, 0, 8)
        frozen = self.pick_dbg_storage_node(disk.tablet_id)
        stopped = self.pick_dbg_storage_node(disk.tablet_id, exclude={frozen.node_id})

        self.faults.freeze_node(frozen.node_id)
        self.faults.stop_node(stopped.node_id)
        self.faults.tablet_kill(disk.tablet_id)

        self.faults.thaw_node(frozen.node_id)
        self.faults.start_node(stopped.node_id)
        self.wait_io_ok(disk, timeout_seconds=120)
        self.assert_pattern(disk, payloads)
        self.write_and_verify(disk, 16, 4)
