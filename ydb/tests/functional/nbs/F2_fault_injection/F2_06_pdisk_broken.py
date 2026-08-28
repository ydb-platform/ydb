# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF2_06PdiskBroken(NbsCase):
    """F2.6 — PDisk BROKEN on a DBG host; writes continue on the remaining quorum.

    CMS ``pdisk set --status BROKEN`` is metadata only: DDisk IO does not
    fail, so this case checks that user writes keep succeeding.
    """

    def test_pdisk_broken(self):
        disk = self.make_disk()
        payloads = self.write_and_verify(disk, 0, 4)
        host = self.pick_dbg_storage_node(disk.tablet_id)
        assert host.pdisk_id is not None, host
        self.assert_ddisk_on_isolated_pdisk(disk)
        self.faults.set_pdisk_broken(host.node_id, pdisk_id=host.pdisk_id)

        self.write_and_verify(disk, 16, 4)
        self.assert_pattern(disk, payloads)
