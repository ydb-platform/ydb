# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF2_01TabletKillUnderLoad(NbsCase):
    """F2.1 — partition tablet kill while vhost IO is in flight."""

    def test_tablet_kill_under_load(self):
        disk = self.make_disk()
        payloads = self.write_and_verify(disk, 0, 8)
        io = self.start_vhost_io(disk, start_index=8)
        self.wait_until(
            lambda: io.writes[0] > 0,
            timeout_seconds=15,
            description='vhost read-after-write started',
        )

        self.faults.tablet_kill(disk.tablet_id)
        io.stop_and_join()
        self.wait_io_ok(disk, timeout_seconds=8)
        self.assert_pattern(disk, payloads)
        self.write_and_verify(disk, 8, 4)
