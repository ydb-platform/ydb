# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF2_07PdiskStopRestart(NbsCase):
    """F2.7 — dstool pdisk stop then restart; IO resumes, acked data survives."""

    def test_pdisk_stop_restart(self):
        disk = self.make_disk()
        payloads = self.write_and_verify(disk, 0, 4)
        host = self.pick_dbg_storage_node(disk.tablet_id)
        assert host.pdisk_id is not None, host
        self.faults.pdisk_stop(host.node_id, host.pdisk_id)
        self.faults.pdisk_restart(host.node_id, host.pdisk_id)

        self.wait_io_ok(disk, timeout_seconds=90)
        self.assert_pattern(disk, payloads)
        self.write_and_verify(disk, 16, 2)
