# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.vhost_user_blk_client import VIRTIO_BLK_S_OK


class TestF4_09OverloadedPendingQueue(NbsCase):
    """F4.9 — burst writes into PBuffer; client sees success or a retriable error."""

    def test_overloaded_pending_queue(self):
        disk = self.make_disk()
        results = []
        with self.open_vhost(disk) as client:
            for index in range(64):
                data = self.as_bytes(self.generate_random_data(4096))
                try:
                    status = client.write(disk.byte_offset(index), data, timeout=10.0)
                    results.append((status == VIRTIO_BLK_S_OK, status))
                except Exception as e:
                    results.append((False, e))

        assert results, 'no write results'
        # Either the burst is absorbed or the client sees a retriable error.
        self.wait_io_ok(disk)
        failures = [r for r in results if not r[0]]
        for _, detail in failures:
            text = str(detail).upper()
            assert any(
                token in text
                for token in (
                    'IOERR',
                    'UNSUPP',
                    'TIMEOUT',
                    'OVERLOADED',
                    'REJECTED',
                    'UNAVAILABLE',
                    'VHOST',
                )
            ), detail
