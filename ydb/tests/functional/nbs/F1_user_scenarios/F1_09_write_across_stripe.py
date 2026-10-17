# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.geometry import stripe_boundary_block


class TestF1_09WriteAcrossStripe(NbsCase):
    """F1.9 — write two blocks that sit on either side of a 512 KiB stripe."""

    def test_write_across_stripe(self):
        disk = self.make_disk()
        start = stripe_boundary_block()
        payloads = self.write_pattern(disk, start, 2)
        self.assert_pattern(disk, payloads)
