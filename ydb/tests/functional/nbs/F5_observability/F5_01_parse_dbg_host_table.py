# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.mon import parse_dbg_connections, parse_dbg_hosts


class TestF5_01ParseDbgHostTable(NbsCase):
    """F5.1 — parse the DBG host table on a live tablet."""

    def test_parse_dbg_host_table(self):
        disk = self.make_disk()
        self.write_blocks(disk, 0, self.generate_random_data(4096))

        listing = self.fetch_partition_dbg_page(disk.tablet_id)
        indexes = self.parse_dbg_indexes(listing)
        assert indexes, 'expected DBG drill-down links; html={}'.format(listing[:1000])

        html = self.fetch_partition_dbg_page(disk.tablet_id, indexes[0])
        hosts = parse_dbg_hosts(html)
        assert len(hosts) >= 3, 'expected at least 3 DBG hosts; html={}'.format(html[:2000])
        for host in hosts:
            assert host.state, 'host H{} has empty State'.format(host.index)
            assert host.health, 'host H{} has empty Health'.format(host.index)

        connections = parse_dbg_connections(html)
        assert connections, 'expected Connections table with DDisk ids; html={}'.format(html[:2000])
        assert any(c['node_id'] for c in connections), connections
