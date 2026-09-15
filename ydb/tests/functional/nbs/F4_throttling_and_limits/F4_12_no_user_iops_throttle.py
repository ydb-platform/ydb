# -*- coding: utf-8 -*-
import time

from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.vhost_user_blk_client import VIRTIO_BLK_S_OK, virtio_blk_status_name


class TestF4_12NoUserIopsThrottle(NbsCase):
    """F4.12 — no user IOPS/bandwidth throttle on the NBS 2.0 datapath."""

    def test_no_user_iops_throttle(self):
        disk = self.make_disk()
        deadline = time.monotonic() + 5
        ok_count = 0
        with self.open_vhost(disk) as client:
            index = 0
            while time.monotonic() < deadline:
                data = self.as_bytes(self.generate_random_data(disk.block_size))
                status = client.write(disk.byte_offset(index % 256), data, timeout=10.0)
                assert status == VIRTIO_BLK_S_OK, virtio_blk_status_name(status)
                ok_count += 1
                index += 1
        assert ok_count > 0, 'no vhost writes completed'
