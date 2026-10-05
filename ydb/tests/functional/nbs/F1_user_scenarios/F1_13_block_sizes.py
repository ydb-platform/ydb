# -*- coding: utf-8 -*-
import pytest

from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.geometry import (
    BLOCK_SIZE_MATRIX_DISK_BYTES,
    SUPPORTED_BLOCK_SIZES,
    blocks_for_bytes,
)


class TestF1_13BlockSizes(NbsCase):
    """F1.13 — create a 512 GiB disk at every supported block size and verify IO."""

    @pytest.mark.timeout(120, func_only=True)
    @pytest.mark.parametrize('block_size', SUPPORTED_BLOCK_SIZES)
    def test_block_sizes(self, block_size):
        blocks_count = blocks_for_bytes(BLOCK_SIZE_MATRIX_DISK_BYTES, block_size)
        disk = self.make_disk(blocks_count=blocks_count, block_size=block_size)
        try:
            assert disk.block_size == block_size
            assert disk.blocks_count == blocks_count

            indexes = (0, blocks_count // 2, blocks_count - 1)
            payloads = {}
            for index in indexes:
                payloads[index] = self.as_bytes(self.generate_random_data(block_size))
                timeout = 10.0 if index == 0 else 60.0
                self.write_blocks(disk, index, payloads[index], timeout=timeout)

            for index, expected in payloads.items():
                timeout = 10.0 if index == 0 else 60.0
                got = self.read_blocks(disk, index, timeout=timeout)
                assert got == expected, 'block_size={} block {} read mismatch'.format(
                    block_size, index
                )
        finally:
            self.drop_disk(disk)
