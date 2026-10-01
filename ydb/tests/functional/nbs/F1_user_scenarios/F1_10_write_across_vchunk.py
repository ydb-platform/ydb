# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.geometry import vchunk_boundary_block


class TestF1_10WriteAcrossVchunk(NbsCase):
    """F1.10 — write two blocks across a 32 MiB vchunk span."""

    def test_write_across_vchunk(self):
        disk = self.make_disk()
        start = vchunk_boundary_block()
        payloads = self.write_pattern(disk, start, 2)
        self.assert_pattern(disk, payloads)
