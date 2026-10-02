# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.geometry import DEFAULT_DISK_BLOCKS_COUNT, region_boundary_block


class TestF1_11WriteAcrossRegion(NbsCase):
    """F1.11 — write two blocks across a 4 GiB region boundary."""

    def test_write_across_region(self):
        disk = self.make_disk(blocks_count=DEFAULT_DISK_BLOCKS_COUNT * 2)
        start = region_boundary_block()
        payloads = self.write_pattern(disk, start, 2)
        self.assert_pattern(disk, payloads)
