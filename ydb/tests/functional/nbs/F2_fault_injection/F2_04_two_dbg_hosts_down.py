# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF2_04TwoDbgHostsDown(NbsCase):
    """F2.4 — two DBG hosts down: writes still ack, reads still served."""

    def test_two_dbg_hosts_down(self):
        disk = self.make_disk()
        payloads = self.write_and_verify(disk, 0, 8)
        first = self.pick_dbg_storage_node(disk.tablet_id)
        second = self.pick_dbg_storage_node(disk.tablet_id, exclude={first.node_id})
        self.faults.stop_node(first.node_id)
        self.faults.stop_node(second.node_id)

        self.write_and_verify(disk, 32, 4)
        self.assert_pattern(disk, payloads)
