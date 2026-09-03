# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.mon import parse_pbuffer_occupancy


class TestF5_02ParsePbufferOccupancy(NbsCase):
    """F5.2 — parse PBuffer occupancy / tablet LSN presence."""

    def test_parse_pbuffer_occupancy(self):
        disk = self.make_disk()
        self.write_blocks(disk, 0, self.generate_random_data(4096))

        listing = self.fetch_partition_dbg_page(disk.tablet_id)
        indexes = self.parse_dbg_indexes(listing)
        pb_ids = self.collect_pbuffer_service_ids(disk.tablet_id, indexes[: min(3, len(indexes))])
        assert pb_ids, 'expected PBuffer service ids on DBG pages'

        html = self.fetch_pbuffer_page(pb_ids)
        snapshot = parse_pbuffer_occupancy(html)
        assert disk.tablet_id in html or disk.tablet_id in snapshot['tablet_ids'], (
            'expected tablet {} on PBuffer mon; html={}'.format(disk.tablet_id, html[:2000])
        )
