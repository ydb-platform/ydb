# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF3_05OndiskbrokenStaysBroken(NbsCase):
    """F3.5 — OnDDiskBroken sets Broken/Offline and Think does not un-break it."""

    def test_ondiskbroken_stays_broken(self):
        disk = self.make_disk()
        self.write_pattern(disk, 0, 4)
        host = self.pick_dbg_storage_node(disk.tablet_id)
        assert host.pdisk_id is not None
        self.faults.set_pdisk_broken(host.node_id, pdisk_id=host.pdisk_id)

        self.wait_until(
            lambda: any(
                h.node_id == host.node_id
                and (
                    h.health.lower() in ('broken', 'offline')
                    or h.state.lower() in ('offline',)
                    or h.health in ('4', '3')
                    or h.state in ('2',)
                )
                for h in self.dbg_hosts(disk.tablet_id)
            ),
            timeout_seconds=45,
            description='host becomes Broken/Offline',
        )

        # A later Think tick must not bring it back.
        def still_broken():
            current = [h for h in self.dbg_hosts(disk.tablet_id) if h.node_id == host.node_id]
            assert current, 'host {} disappeared from DBG'.format(host.node_id)
            return current[0].health.lower() not in ('online',) or current[0].state.lower() != 'online'

        self.wait_until(still_broken, timeout_seconds=15, description='host stays Broken/Offline')
