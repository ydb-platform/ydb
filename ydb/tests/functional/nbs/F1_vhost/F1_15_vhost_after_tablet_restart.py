# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.markers import known_bug
from ydb.tests.functional.nbs.lib.vhost_user_blk_client import VIRTIO_BLK_S_OK, virtio_blk_status_name


class TestF1_15VhostAfterTabletRestart(NbsCase):
    """F1.15 — vhost reconnect after partition tablet kill + recovery."""

    @known_bug('RestartTablet after a vhost session times out (DEADLINE_EXCEEDED)')
    def test_vhost_after_tablet_restart(self):
        disk = self.make_disk()
        payload = bytes([ord('v')] * 4096)

        with self.open_vhost(disk) as client:
            status = client.write(0, payload)
            assert status == VIRTIO_BLK_S_OK, virtio_blk_status_name(status)

        self.faults.tablet_kill(disk.tablet_id)

        def socket_usable():
            try:
                with self.open_vhost(disk, socket_timeout=3.0) as client:
                    status, data = client.read(0, 4096)
                    return status == VIRTIO_BLK_S_OK and data == payload
            except Exception:
                return False

        self.wait_until(socket_usable, timeout_seconds=90, description='vhost socket usable after tablet kill')
