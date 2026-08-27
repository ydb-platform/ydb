# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.geometry import (
    DEFAULT_BLOCK_SIZE,
    SUPPORTED_BLOCK_SIZES,
)
from ydb.tests.functional.nbs.lib.fixtures.markers import known_bug


class TestF1_13BlockSizes(NbsCase):
    """F1.13 — create + IO at every supported block size."""

    # PBuffer/DDisk writes are 4 KiB units. A volume block > 4 KiB is
    # forwarded as size=8192 (etc.) with selector.Size=4096 and rejected:
    # INCORRECT_REQUEST. The vhost client then waits until timeout.
    @known_bug(
        'PBuffer write selector is 4 KiB; volume block size > 4 KiB is rejected'
    )
    def test_block_sizes(self):
        io_failed = []
        for block_size in SUPPORTED_BLOCK_SIZES:
            blocks_count = max(1024, (4 * 1024 * 1024) // block_size)
            disk = self.make_disk(blocks_count=blocks_count, block_size=block_size)
            payload = self.as_bytes(self.generate_random_data(block_size))
            try:
                if block_size == DEFAULT_BLOCK_SIZE:
                    self.write_blocks(disk, 0, payload)
                    got = self.read_blocks(disk, 0)
                    assert got == payload, 'block_size={} read mismatch'.format(block_size)
                    ok, _, err = self.try_write(disk, blocks_count + 10, payload)
                    assert not ok, 'out-of-range write at block_size={} succeeded: {}'.format(
                        block_size, err
                    )
                else:
                    ok, _, err = self.try_write(disk, 0, payload, timeout=3.0)
                    if ok:
                        assert self.read_blocks(disk, 0) == payload
                    else:
                        io_failed.append((block_size, err))
            finally:
                self.drop_disk(disk)

        assert not io_failed, (
            'IO at block size > 4 KiB still rejected by PBuffer: {}'.format(io_failed)
        )
