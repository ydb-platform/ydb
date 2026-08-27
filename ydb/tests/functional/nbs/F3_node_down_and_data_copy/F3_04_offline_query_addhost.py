# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF3_04OfflineQueryAddhost(NbsCase):
    """F3.4 — Offline requests AddHost until a spare is appended as HandOff."""

    def test_offline_query_addhost(self):
        disk = self.make_disk()
        self.write_pattern(disk, 0, 4)
        before = self.dbg_hosts(disk.tablet_id)
        host = self.pick_dbg_storage_node(disk.tablet_id)
        self.faults.stop_node(host.node_id)
        self.wait_host_state(disk.tablet_id, host.node_id, ('Offline', '2'), timeout_seconds=45)

        def addhost_done():
            after = self.dbg_hosts(disk.tablet_id)
            return len(after) > len(before)

        self.wait_until(addhost_done, timeout_seconds=90, description='AddHost appended a host')
        after = self.dbg_hosts(disk.tablet_id)
        roles = self.vchunk_hosts(disk.tablet_id)
        assert len(after) >= len(before), (len(before), len(after))
        if roles:
            handoff = [h for h in roles if 'handoff' in h.ddisk_role.lower() or h.ddisk_role in ('1', 'HandOff')]
            # New host is HandOff / None when 3 DDisks already exist.
            assert handoff or any(h.ddisk_role.lower() in ('none', '2') for h in roles)
