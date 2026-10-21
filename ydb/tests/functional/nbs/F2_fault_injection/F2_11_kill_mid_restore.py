# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF2_11KillMidRestore(NbsCase):
    """F2.11 — second tablet_kill while restore is listing PBuffers."""

    def test_kill_mid_restore(self):
        disk = self.make_disk()
        payloads = self.write_and_verify(disk, 0, 16)
        self.faults.tablet_kill(disk.tablet_id)
        self.faults.tablet_kill(disk.tablet_id)
        self.wait_io_ok(disk, timeout_seconds=8)
        self.assert_pattern(disk, payloads)
        self.write_and_verify(disk, 16, 4)
