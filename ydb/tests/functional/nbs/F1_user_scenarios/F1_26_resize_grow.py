# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.geometry import DEFAULT_DISK_BLOCKS_COUNT


class TestF1_26ResizeGrow(NbsCase):
    """F1.26 — ResizePartition RPC: grow, same size, shrink. Capacity is not grown yet."""

    def test_resize_grow(self):
        disk = self.make_disk(blocks_count=DEFAULT_DISK_BLOCKS_COUNT)
        payloads = self.write_and_verify(disk, 0, 4)

        grown = self.resize_disk(disk.disk_id, disk.blocks_count * 2)
        assert grown == disk.blocks_count * 2

        self.assert_pattern(disk, payloads)

    def test_resize_same_size(self):
        disk = self.make_disk(blocks_count=DEFAULT_DISK_BLOCKS_COUNT)
        payloads = self.write_and_verify(disk, 0, 4)

        same = self.resize_disk(disk.disk_id, disk.blocks_count)
        assert same == disk.blocks_count

        self.assert_pattern(disk, payloads)

    def test_resize_shrink(self):
        disk = self.make_disk(blocks_count=DEFAULT_DISK_BLOCKS_COUNT)
        payloads = self.write_and_verify(disk, 0, 4)

        output = self.resize_partition(disk.disk_id, disk.blocks_count // 2)
        assert output.get('status') == 'BAD_REQUEST', (
            'shrink must fail, got {}'.format(output)
        )

        self.assert_pattern(disk, payloads)
        self.write_and_verify(disk, 16, 1)
