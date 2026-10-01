# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF1_24DeletePartitionWipe(NbsCase):
    """F1.24 — DeletePartition actually wipes DDisk chunks."""

    def test_delete_partition_wipe(self):
        disk = self.make_disk()
        payload = self.as_bytes(self.generate_random_data(4096))
        self.write_blocks(disk, 0, payload)

        listing = self.fetch_partition_dbg_page(disk.tablet_id)
        indexes = self.parse_dbg_indexes(listing)
        pb_ids = self.collect_pbuffer_service_ids(disk.tablet_id, indexes[: min(3, len(indexes))])

        self.delete_disk(disk.disk_id)
        self._created_disks.remove(disk.disk_id)

        def pbuffer_wiped():
            html = self.fetch_pbuffer_page(pb_ids)
            assert disk.tablet_id not in html, html[:1500]
            return True

        self.wait_until(pbuffer_wiped, description='PBuffer tablet LSN gone after delete')

        old_tablet = disk.tablet_id
        new_tablet = self.create_disk(disk.disk_id)
        self.register_disk(disk.disk_id)
        disk = self.bind_vhost(disk.disk_id, new_tablet, disk.blocks_count, disk.block_size)
        data = self.read_blocks(disk, 0)
        assert str(new_tablet) != str(old_tablet)
        assert data == b'\x00' * 4096, 're-created disk still holds wiped payload'
