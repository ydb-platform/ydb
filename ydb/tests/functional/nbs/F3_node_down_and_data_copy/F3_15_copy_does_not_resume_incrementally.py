# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF3_15CopyDoesNotResumeIncrementally(NbsCase):
    """F3.15 — OperationalBlockCount is RAM-only; tablet kill resets watermark to 0.

    Documented current behaviour, not an xfail: incremental copy progress
    is lost across tablet restart.
    """

    def test_copy_does_not_resume_incrementally(self):
        disk = self.make_disk()
        self.write_pattern(disk, 0, 16)
        host = self.pick_dbg_storage_node(disk.tablet_id)
        self.faults.stop_node(host.node_id)
        self.wait_host_state(disk.tablet_id, host.node_id, ('Offline', '2'), timeout_seconds=45)

        self.wait_until(
            lambda: any(h.watermark is not None for h in self.vchunk_hosts(disk.tablet_id)),
            timeout_seconds=60,
            description='copy started',
        )
        self.faults.tablet_kill(disk.tablet_id)
        self.wait_io_ok(disk, timeout_seconds=90)

        hosts = self.vchunk_hosts(disk.tablet_id)
        watermarks = [h.watermark for h in hosts]
        # After recovery the persisted watermark is still 0 (or unset until
        # the next ApplyConfig). Incremental progress must not survive.
        assert all(w in (0, None) for w in watermarks), watermarks
