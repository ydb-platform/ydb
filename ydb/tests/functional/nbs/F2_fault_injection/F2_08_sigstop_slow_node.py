# -*- coding: utf-8 -*-
import time

from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF2_08SigstopSlowNode(NbsCase):
    """F2.8 — SIGSTOP one DBG host longer than hedge / PBuffer timeouts."""

    def test_sigstop_slow_node(self):
        disk = self.make_disk()
        payloads = self.write_and_verify(disk, 0, 4)
        host = self.pick_dbg_storage_node(disk.tablet_id)
        self.faults.freeze_node(host.node_id)
        time.sleep(0.2)
        more = self.write_and_verify(disk, 16, 2)
        self.faults.thaw_node(host.node_id)

        self.assert_pattern(disk, payloads)
        self.assert_pattern(disk, more)
        self.wait_io_ok(disk)
