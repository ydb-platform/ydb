# -*- coding: utf-8 -*-
import pytest

from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.vhost_user_blk_client import VIRTIO_BLK_S_OK


class TestF4_10WriteOver512kib(NbsCase):
    """F4.10 — a single vhost write larger than 512 KiB.

    User IO is split at stripe granularity (512 KiB), so a successful
    write here does not prove an oversized PBuffer record was stored.
    """

    def test_write_over_512kib(self):
        disk = self.make_disk()
        oversized = self.as_bytes(self.generate_random_data(513 * 1024))
        ok, status, err = self.try_write(disk, 0, oversized)
        if ok:
            pytest.skip(
                'user IO is split at stripe granularity; a >512 KiB vhost write '
                'does not produce an oversized PBuffer record'
            )
        assert status != VIRTIO_BLK_S_OK
        text = '{} {}'.format(status, err).upper()
        assert any(
            token in text
            for token in (
                'IOERR',
                'UNSUPP',
                'INCORRECT',
                'ARGUMENT',
                'INVALID',
                'FAILURE',
                'TIMEOUT',
                'VHOST',
            )
        ), (status, err)
