# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF2_03OneDbgHostDown(NbsCase):
    """F2.3 — one DBG host down: writes still ack, acked data is readable."""

    def test_one_dbg_host_down(self):
        disk = self.make_disk()
        payloads = self.write_and_verify(disk, 0, 8)
        host = self.pick_dbg_storage_node(disk.tablet_id)
        self.faults.stop_node(host.node_id)

        self.write_and_verify(disk, 16, 4)
        self.assert_pattern(disk, payloads)
