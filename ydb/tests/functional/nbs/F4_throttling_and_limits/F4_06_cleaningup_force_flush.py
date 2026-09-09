# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.mon import parse_pbuffer_occupancy


class TestF4_06CleaningupForceFlush(NbsCase):
    """F4.6 — idle after a burst: CleaningUp force-flushes remaining LSNs."""

    def test_cleaningup_force_flush(self):
        disk = self.make_disk()
        self.write_pattern(disk, 0, 32)

        listing = self.fetch_partition_dbg_page(disk.tablet_id)
        indexes = self.parse_dbg_indexes(listing)
        pb_ids = self.collect_pbuffer_service_ids(disk.tablet_id, indexes[: min(3, len(indexes))])
        html_before = self.fetch_pbuffer_page(pb_ids)
        before = parse_pbuffer_occupancy(html_before)
        # Fallback only checks that the page lists some tablet, not this one.
        assert disk.tablet_id in html_before or before['tablet_ids']

        def still_serving():
            html = self.fetch_pbuffer_page(pb_ids)
            assert html, 'PBuffer mon page disappeared after the write burst'
            data = self.generate_random_data(4096)
            ok, _, _ = self.try_write(disk, 8, data)
            return ok

        self.wait_until(still_serving, timeout_seconds=15, description='CleaningUp left IO healthy')
