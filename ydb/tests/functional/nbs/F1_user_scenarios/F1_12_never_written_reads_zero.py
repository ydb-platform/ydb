# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF1_12NeverWrittenReadsZero(NbsCase):
    """F1.12 — a never-written range reads as zeroes."""

    def test_never_written_reads_zero(self):
        disk = self.make_disk()
        data = self.read_blocks(disk, 0)
        assert data == b'\x00' * 4096, 'never-written block 0 is not zeroes: {!r}'.format(
            data[:32]
        )
        data = self.read_blocks(disk, 100)
        assert data == b'\x00' * 4096, 'never-written block 100 is not zeroes'
