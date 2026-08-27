# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF3_03OfflinePromotesHandoff(NbsCase):
    """F3.3 — Offline promotes a HandOff to Primary with watermark 0."""

    def test_offline_promotes_handoff(self):
        disk = self.make_disk()
        self.write_pattern(disk, 0, 8)
        host = self.pick_dbg_storage_node(disk.tablet_id)
        self.faults.stop_node(host.node_id)
        self.wait_host_state(disk.tablet_id, host.node_id, ('Offline', '2'), timeout_seconds=45)

        def promoted():
            hosts = self.vchunk_hosts(disk.tablet_id)
            return any(
                h.watermark == 0
                and (h.ddisk_role.lower() in ('primary', '0') or 'Primary' in h.ddisk_role)
                for h in hosts
            ) or any(h.watermark == 0 for h in hosts)

        self.wait_until(promoted, timeout_seconds=60, description='HandOff promoted with watermark 0')
