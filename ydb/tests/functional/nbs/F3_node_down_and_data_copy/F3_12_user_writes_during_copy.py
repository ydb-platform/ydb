# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF3_12UserWritesDuringCopy(NbsCase):
    """F3.12 — user writes during copy ack and are not overwritten by the copier."""

    def test_user_writes_during_copy(self):
        disk = self.make_disk()
        seed = self.write_pattern(disk, 0, 8)
        host = self.pick_dbg_storage_node(disk.tablet_id)
        self.faults.stop_node(host.node_id)
        self.wait_host_state(disk.tablet_id, host.node_id, ('Offline', '2'), timeout_seconds=45)

        during = self.write_pattern(disk, 32, 8)
        self.assert_pattern(disk, seed)
        self.assert_pattern(disk, during)
