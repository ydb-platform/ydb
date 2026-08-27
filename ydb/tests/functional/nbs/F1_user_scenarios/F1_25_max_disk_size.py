# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.geometry import (
    DEFAULT_BLOCK_SIZE,
    MAX_BLOCKS_PER_DISK,
    SUPPORTED_BLOCK_SIZES,
    max_disk_bytes,
)
from ydb.tests.functional.nbs.lib.fixtures.markers import known_bug


class TestF1_25MaxDiskSize(NbsCase):
    """F1.25 — create a max-size disk at every supported block size."""

    @known_bug(
        'PBuffer write selector is 4 KiB; volume block size > 4 KiB is rejected'
    )
    def test_max_disk_size(self):
        io_failed = []
        for block_size in SUPPORTED_BLOCK_SIZES:
            blocks_count = MAX_BLOCKS_PER_DISK
            assert blocks_count * block_size == max_disk_bytes(block_size)
            disk = self.make_disk(blocks_count=blocks_count, block_size=block_size)
            payload = self.as_bytes(self.generate_random_data(block_size))
            try:
                if block_size == DEFAULT_BLOCK_SIZE:
                    self.write_blocks(disk, 0, payload)
                    assert self.read_blocks(disk, 0) == payload, (
                        'block_size={} first-block mismatch'.format(block_size)
                    )

                    last = blocks_count - 1
                    last_payload = self.as_bytes(self.generate_random_data(block_size))
                    self.write_blocks(disk, last, last_payload, timeout=60.0)
                    assert self.read_blocks(disk, last, timeout=60.0) == last_payload, (
                        'block_size={} last-block mismatch'.format(block_size)
                    )

                    ok, _, err = self.try_write(disk, blocks_count, payload)
                    assert not ok, (
                        'write past max disk at block_size={} succeeded: {}'.format(
                            block_size, err
                        )
                    )
                else:
                    ok, _, err = self.try_write(disk, 0, payload, timeout=3.0)
                    if not ok:
                        io_failed.append((block_size, err))
            finally:
                self.drop_disk(disk)

        assert not io_failed, (
            'IO at block size > 4 KiB still rejected by PBuffer: {}'.format(io_failed)
        )
