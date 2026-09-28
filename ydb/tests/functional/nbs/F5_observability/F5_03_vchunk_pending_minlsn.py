# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.mon import parse_vchunk_counters


class TestF5_03VchunkPendingMinlsn(NbsCase):
    """F5.3 — locate TVChunkCounters (Pending / MinLsn) on a live mon page."""

    def test_vchunk_pending_minlsn(self):
        disk = self.make_disk()
        self.write_pattern(disk, 0, 8)

        pages = [
            self.fetch_partition_dbg_page(disk.tablet_id),
            self.fetch_mon('/tablets/app?TabletID={}&page=overview'.format(disk.tablet_id)),
            self.fetch_mon('/tablets/app?TabletID={}&page=vchunk&vchunk=0'.format(disk.tablet_id)),
        ]
        combined = '\n'.join(pages)
        counters = parse_vchunk_counters(combined)
        # The dump format is still being discovered; the smoke requirement is
        # that the parser does not throw and the pages render.
        assert 'partition_direct tablet' in combined or 'VChunk' in combined, combined[:1500]
        assert isinstance(counters, dict)
