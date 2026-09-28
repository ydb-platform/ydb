# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.markers import (
    SLOT_CRASH_ON_PRIMARY_DDISK_LOSS,
    known_bug,
)


class TestF3_09CopyCompleteWatermarkUnset(NbsCase):
    """F3.9 — copy complete: watermark becomes unset / nullopt.

    Does not execute: stopping a Primary DDisk host segfaults the NBS slot.
    """

    @known_bug(SLOT_CRASH_ON_PRIMARY_DDISK_LOSS)
    def test_copy_complete_watermark_unset(self):
        disk = self.make_disk()
        self.write_pattern(disk, 0, 8)
        host = self.pick_primary_ddisk_host(disk.tablet_id)
        self.faults.stop_node(host.node_id)
        self.wait_host_offline(disk, host.node_id)

        def complete():
            hosts = self.vchunk_hosts(disk.tablet_id)
            if not hosts:
                return False
            return all(h.watermark is None for h in hosts)

        self.wait_until(complete, timeout_seconds=15, description='all watermarks unset after copy')
        self.wait_io_ok(disk)
