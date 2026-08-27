# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF2_10KillMidErase(NbsCase):
    """F2.10 — tablet_kill after enough writes that erase is in flight."""

    def test_kill_mid_erase(self):
        disk = self.make_disk()
        payloads = self.write_and_verify(disk, 0, 128)
        self.faults.tablet_kill(disk.tablet_id)
        self.wait_io_ok(disk, timeout_seconds=90)
        self.assert_pattern(disk, payloads)
        self.write_and_verify(disk, 128, 4)
