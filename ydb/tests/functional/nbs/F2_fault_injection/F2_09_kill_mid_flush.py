# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF2_09KillMidFlush(NbsCase):
    """F2.9 — tablet_kill while a burst of writes is flushing to DDisk."""

    def test_kill_mid_flush(self):
        disk = self.make_disk()
        payloads = self.write_and_verify(disk, 0, 64)
        self.faults.tablet_kill(disk.tablet_id)
        self.wait_io_ok(disk, timeout_seconds=90)
        self.assert_pattern(disk, payloads)
        self.write_and_verify(disk, 64, 4)
