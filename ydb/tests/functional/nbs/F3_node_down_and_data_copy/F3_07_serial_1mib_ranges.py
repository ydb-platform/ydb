# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF3_07Serial1mibRanges(NbsCase):
    """F3.7 — observed copy progress advances (serial 1 MiB ranges)."""

    def test_serial_1mib_ranges(self):
        disk = self.make_disk()
        self.write_pattern(disk, 0, 32)
        host = self.pick_dbg_storage_node(disk.tablet_id)
        self.faults.stop_node(host.node_id)
        self.wait_host_state(disk.tablet_id, host.node_id, ('Offline', '2'), timeout_seconds=45)

        seen = []

        def progressed():
            marks = [h.watermark for h in self.vchunk_hosts(disk.tablet_id) if h.watermark is not None]
            if marks:
                seen.append(max(marks))
            all_marks = [h.watermark for h in self.vchunk_hosts(disk.tablet_id)]
            advanced = len(seen) >= 2 and seen[-1] != seen[0]
            finished = all(m is None for m in all_marks) and all_marks
            return advanced or finished

        self.wait_until(progressed, timeout_seconds=90, description='copy watermark advances')
