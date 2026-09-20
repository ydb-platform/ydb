# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF1_17NoisyNeighbour(NbsCase):
    """F1.17 — vhost load on disk A while doing verified IO on disk B."""

    def test_noisy_neighbour(self):
        disk_a = self.make_disk()
        disk_b = self.make_disk()

        io = self.start_vhost_io(disk_a)
        self.wait_until(
            lambda: io.writes[0] > 0,
            timeout_seconds=15,
            description='vhost IO on disk A started',
        )

        payloads = self.write_pattern(disk_b, 0, 8)
        self.assert_pattern(disk_b, payloads)

        io.stop_and_join()
        assert io.writes[0] > 0, 'disk A completed no vhost writes'
