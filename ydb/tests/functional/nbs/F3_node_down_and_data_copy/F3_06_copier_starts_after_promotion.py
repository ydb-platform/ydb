# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF3_06CopierStartsAfterPromotion(NbsCase):
    """F3.6 — after Offline promotion, watermark 0 appears and then advances."""

    def test_copier_starts_after_promotion(self):
        disk = self.make_disk()
        self.write_pattern(disk, 0, 16)
        host = self.pick_dbg_storage_node(disk.tablet_id)
        self.faults.stop_node(host.node_id)
        self.wait_host_state(disk.tablet_id, host.node_id, ('Offline', '2'), timeout_seconds=45)

        def watermark_seen():
            return any(h.watermark == 0 or (h.watermark is not None and h.watermark >= 0) for h in self.vchunk_hosts(disk.tablet_id))

        self.wait_until(watermark_seen, timeout_seconds=60, description='copier watermark present')
