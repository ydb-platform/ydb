# -*- coding: utf-8 -*-
from dataclasses import replace

from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.geometry import DEFAULT_DISK_BLOCKS_COUNT


class TestF1_26ResizeGrow(NbsCase):
    """F1.26 — grow a disk and use the new capacity after tablet restart."""

    def _assert_write_beyond_fails(self, disk, index):
        ok, _, name = self.try_write(
            disk, index, self.generate_random_data(disk.block_size)
        )
        assert not ok, 'write past capacity succeeded: {}'.format(name)

    def _wait_capacity(self, disk):
        """Wait until the restarted endpoint accepts IO at the new tail."""
        data = self.generate_random_data(disk.block_size)
        index = max(disk.blocks_count - 16, 0)

        def _ready():
            ok, _, _ = self.try_write(disk, index, data)
            return ok

        self.wait_until(
            _ready,
            timeout_seconds=60,
            description='resized capacity live for {}'.format(disk.disk_id),
        )

    def _resize(self, disk, blocks_count):
        grown = self.resize_disk(disk.disk_id, blocks_count)
        disk = replace(disk, blocks_count=grown)
        self._wait_capacity(disk)
        return disk

    def test_resize_grow(self):
        # 4 GiB = one region
        disk = self.make_disk(blocks_count=DEFAULT_DISK_BLOCKS_COUNT)
        payloads = self.write_and_verify(disk, 0, 4)
        # lands in the second region
        beyond = disk.blocks_count + 16
        self._assert_write_beyond_fails(disk, beyond)

        disk = self._resize(disk, disk.blocks_count * 2)

        self.assert_pattern(disk, payloads)
        self.write_and_verify(disk, beyond, 2)

    def test_resize_grow_within_region(self):
        # 2 GiB -> 4 GiB stays in the first region.
        half_region = DEFAULT_DISK_BLOCKS_COUNT // 2
        disk = self.make_disk(blocks_count=half_region)
        payloads = self.write_and_verify(disk, 0, 4)
        beyond = disk.blocks_count + 16
        self._assert_write_beyond_fails(disk, beyond)

        disk = self._resize(disk, DEFAULT_DISK_BLOCKS_COUNT)

        self.assert_pattern(disk, payloads)
        self.write_and_verify(disk, beyond, 2)

    def test_resize_same_size(self):
        disk = self.make_disk(blocks_count=DEFAULT_DISK_BLOCKS_COUNT)
        payloads = self.write_and_verify(disk, 0, 4)

        same = self.resize_disk(disk.disk_id, disk.blocks_count)
        assert same == disk.blocks_count

        self.assert_pattern(disk, payloads)
        self._assert_write_beyond_fails(disk, disk.blocks_count + 16)

    def test_resize_shrink(self):
        disk = self.make_disk(blocks_count=DEFAULT_DISK_BLOCKS_COUNT)
        payloads = self.write_and_verify(disk, 0, 4)

        output = self.resize_partition(disk.disk_id, disk.blocks_count // 2)
        assert output.get('status') == 'BAD_REQUEST', (
            'shrink must fail, got {}'.format(output)
        )

        self.assert_pattern(disk, payloads)
        self.write_and_verify(disk, 16, 1)
        self._assert_write_beyond_fails(disk, disk.blocks_count + 16)

    def test_resize_grow_under_io(self):
        # Endpoint is detached and the tablet restarts, so in-flight vhost
        # IO is expected to fail. Data written before the resize must survive
        # and IO must work again at the new size.
        disk = self.make_disk(blocks_count=DEFAULT_DISK_BLOCKS_COUNT)
        payloads = self.write_and_verify(disk, 0, 8)
        io = self.start_vhost_io(disk, range_blocks=4, start_index=8)
        self.wait_until(
            lambda: io.writes[0] > 0,
            timeout_seconds=15,
            description='vhost IO started',
        )

        grown = self.resize_disk(disk.disk_id, disk.blocks_count * 2)
        io.stop_and_join()
        assert io.writes[0] > 0

        disk = replace(disk, blocks_count=grown)
        self._wait_capacity(disk)
        self.assert_pattern(disk, payloads)
        self.write_and_verify(disk, disk.blocks_count // 2 + 16, 2)
