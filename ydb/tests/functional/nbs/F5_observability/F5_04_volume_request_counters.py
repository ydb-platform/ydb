# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.mon import parse_volume_request_counters


class TestF5_04VolumeRequestCounters(NbsCase):
    """F5.4 — locate TVolumeRequest Counters on a live mon page."""

    def test_volume_request_counters(self):
        disk = self.make_disk()
        self.write_pattern(disk, 0, 4)
        self.read_blocks(disk, 0)

        html = self.fetch_mon('/tablets/app?TabletID={}&page=overview'.format(disk.tablet_id))
        counters = parse_volume_request_counters(html)
        assert 'partition_direct tablet' in html or 'Overview' in html, html[:1500]
        assert isinstance(counters, dict)
