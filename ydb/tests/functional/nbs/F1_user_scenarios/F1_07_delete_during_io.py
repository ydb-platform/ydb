# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF1_07DeleteDuringIo(NbsCase):
    """F1.7 — delete a disk while vhost IO is in flight."""

    def test_delete_during_io(self):
        disk = self.make_disk()
        io = self.start_vhost_io(disk)
        self.wait_until(
            lambda: io.writes[0] > 0,
            timeout_seconds=15,
            description='vhost IO started',
        )

        deleted = self.delete_disk(disk.disk_id)
        assert deleted == disk.disk_id
        self._created_disks.remove(disk.disk_id)
        io.stop_and_join()

        self.delete_disk_expect_failure(disk.disk_id)

        def tablet_dbg_cleared():
            html = self.fetch_partition_dbg_page(disk.tablet_id, allow_missing=True)
            return html == '' or not self.parse_dbg_indexes(html)

        self.wait_until(tablet_dbg_cleared, timeout_seconds=30, description='tablet DBG mon cleared')
