# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.mon import parse_dbg_connections, parse_ddisk_directio


class TestF5_05DdiskDirectio(NbsCase):
    """F5.5 — fetch a DDisk actor mon page and parse DirectIO / pending fields."""

    def test_ddisk_directio(self):
        disk = self.make_disk()
        self.write_blocks(disk, 0, self.generate_random_data(4096))

        listing = self.fetch_partition_dbg_page(disk.tablet_id)
        indexes = self.parse_dbg_indexes(listing)
        html = self.fetch_partition_dbg_page(disk.tablet_id, indexes[0])
        connections = parse_dbg_connections(html)
        assert connections, 'no DDisk connections to open'

        conn = next(c for c in connections if c['node_id'] and c['pdisk_id'] is not None)
        parts = conn['ddisk_id'].split(':')
        path = '/node/{}/actors/ddisks/ddisk_p{:09d}_s{:09d}'.format(
            conn['node_id'], int(parts[1]), int(parts[2])
        )
        ddisk_html = self.fetch_mon(path)
        snapshot = parse_ddisk_directio(ddisk_html)
        assert ddisk_html, 'empty DDisk mon page at {}'.format(path)
        assert isinstance(snapshot, dict)
