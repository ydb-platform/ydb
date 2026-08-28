# -*- coding: utf-8 -*-
import pytest

from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.geometry import (
    DEFAULT_BLOCK_SIZE,
    MAX_BLOCKS_PER_DISK,
    SUPPORTED_BLOCK_SIZES,
    max_disk_bytes,
)
from ydb.tests.functional.nbs.lib.fixtures.markers import known_bug


def _block_size_params():
    reason = 'Non 4 KiB block size fails IO'
    params = []
    for block_size in SUPPORTED_BLOCK_SIZES:
        if block_size == DEFAULT_BLOCK_SIZE:
            params.append(pytest.param(block_size))
        else:
            params.append(pytest.param(block_size, marks=known_bug(reason)))
    return params


class TestF1_25MaxDiskSize(NbsCase):
    """F1.25 — max-size disk IO at 4 KiB; larger sizes are xfail without create."""

    @pytest.mark.timeout(300, func_only=True)
    @pytest.mark.parametrize('block_size', _block_size_params())
    def test_max_disk_size(self, block_size):
        blocks_count = MAX_BLOCKS_PER_DISK
        assert blocks_count * block_size == max_disk_bytes(block_size)

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

            ok, _, err = self.try_write(disk, blocks_count, payloads[0])
            assert not ok, (
                'write past max disk at block_size={} succeeded: {}'.format(
                    block_size, err
                )
            )
        finally:
            self.drop_disk(disk)
