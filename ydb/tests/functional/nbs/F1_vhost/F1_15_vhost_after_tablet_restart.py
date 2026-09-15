# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.vhost_user_blk_client import VIRTIO_BLK_S_OK, virtio_blk_status_name


class TestF1_15VhostAfterTabletRestart(NbsCase):
    """F1.15 — same vhost session keeps working after partition tablet restart."""

    def test_vhost_after_tablet_restart(self):
        disk = self.make_disk()
        payload = bytes([ord('v')] * disk.block_size)

        with self.open_vhost(disk) as client:
            status = client.write(0, payload)
            assert status == VIRTIO_BLK_S_OK, virtio_blk_status_name(status)
            generation = self.tablet_generation(disk.tablet_id)
            assert generation is not None, 'tablet {} has no generation before kill'.format(
                disk.tablet_id
            )

            self.faults.tablet_kill(disk.tablet_id)
            self.wait_tablet_restarted(disk.tablet_id, generation, timeout_seconds=60)

            # Endpoint is detached and reattached under the same session; the
            # durable wrapper replays the request, so IO must still complete.
            status, data = client.read(0, disk.block_size, timeout=60.0)
            assert status == VIRTIO_BLK_S_OK, virtio_blk_status_name(status)
            assert data == payload
            status = client.write(disk.byte_offset(1), payload, timeout=60.0)
            assert status == VIRTIO_BLK_S_OK, virtio_blk_status_name(status)

        # F1.15 spec: the socket is usable for a fresh session too.
        assert self.read_blocks(disk, 0, timeout=60.0)[: disk.block_size] == payload
