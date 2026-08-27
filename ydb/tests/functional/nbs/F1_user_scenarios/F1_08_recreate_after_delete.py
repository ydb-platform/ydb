# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF1_08RecreateAfterDelete(NbsCase):
    """F1.8 — re-create the same disk id after a successful delete."""

    def test_recreate_after_delete(self):
        disk = self.make_disk()
        payload = self.as_bytes(self.generate_random_data(4096))
        self.write_blocks(disk, 0, payload)

        deleted = self.delete_disk(disk.disk_id)
        assert deleted == disk.disk_id
        self._created_disks.remove(disk.disk_id)

        new_tablet = self.create_disk(disk.disk_id)
        self.register_disk(disk.disk_id)
        assert str(new_tablet) != str(disk.tablet_id), (new_tablet, disk.tablet_id)

        disk = self.bind_vhost(disk.disk_id, new_tablet, disk.blocks_count, disk.block_size)
        data = self.read_blocks(disk, 0)
        assert data == b'\x00' * 4096 or data != payload, (
            're-created disk still holds the previous payload'
        )
        fresh = self.as_bytes(self.generate_random_data(4096))
        self.write_blocks(disk, 0, fresh)
        assert self.read_blocks(disk, 0) == fresh
