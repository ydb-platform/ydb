# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF3_09CopyCompleteWatermarkUnset(NbsCase):
    """F3.9 — copy complete: watermark becomes unset / nullopt."""

    def test_copy_complete_watermark_unset(self):
        disk = self.make_disk()
        self.write_pattern(disk, 0, 8)
        host = self.pick_dbg_storage_node(disk.tablet_id)
        self.faults.stop_node(host.node_id)
        self.wait_host_state(disk.tablet_id, host.node_id, ('Offline', '2'), timeout_seconds=45)

        def complete():
            hosts = self.vchunk_hosts(disk.tablet_id)
            if not hosts:
                return False
            return all(h.watermark is None for h in hosts)

        self.wait_until(complete, timeout_seconds=180, description='all watermarks unset after copy')
        self.wait_io_ok(disk)
