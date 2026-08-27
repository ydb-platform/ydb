# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.markers import known_bug
from ydb.tests.functional.nbs.lib.vhost_user_blk_client import VIRTIO_BLK_S_OK


class TestF1_23ZeroBlocks(NbsCase):
    """F1.23 — ZeroBlocks / discard. Currently Y_ABORT in ZeroBlocksLocal."""

    @known_bug('ZeroBlocksLocal aborts in TFastPathService')
    def test_zero_blocks(self):
        disk = self.make_disk()
        payload = self.as_bytes(self.generate_random_data(4096))
        other = self.as_bytes(self.generate_random_data(4096))
        self.write_blocks(disk, 0, payload)
        self.write_blocks(disk, 1, other)

        with self.open_vhost(disk) as client:
            status = client.write_zeroes(0, 4096)
            assert status == VIRTIO_BLK_S_OK

        zeros = self.read_blocks(disk, 0)
        assert zeros == b'\x00' * 4096
        kept = self.read_blocks(disk, 1)
        assert kept == other
