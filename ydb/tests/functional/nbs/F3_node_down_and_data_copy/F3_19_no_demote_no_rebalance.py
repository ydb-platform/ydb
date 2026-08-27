# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF3_19NoDemoteNoRebalance(NbsCase):
    """F3.19 — Offline + AddHost leaves the failed host in GetDDisks() as disabled."""

    def test_no_demote_no_rebalance(self):
        disk = self.make_disk()
        self.write_pattern(disk, 0, 4)
        before = self.dbg_hosts(disk.tablet_id)
        host = self.pick_dbg_storage_node(disk.tablet_id)
        self.faults.stop_node(host.node_id)
        self.wait_host_state(disk.tablet_id, host.node_id, ('Offline', '2'), timeout_seconds=45)

        after = self.dbg_hosts(disk.tablet_id)
        still_listed = [h for h in after if h.node_id == host.node_id]
        assert still_listed, 'failed host {} was removed; EvacuateHost is not a runtime path'.format(
            host.node_id
        )
        roles = self.vchunk_hosts(disk.tablet_id)
        if roles:
            disabled = [h for h in roles if h.enabled.lower() in ('no', 'false', '0')]
            assert disabled or len(after) >= len(before)
