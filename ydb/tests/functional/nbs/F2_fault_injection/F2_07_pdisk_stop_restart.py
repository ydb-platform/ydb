# -*- coding: utf-8 -*-
import logging

from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase

logger = logging.getLogger(__name__)


class TestF2_07PdiskStopRestart(NbsCase):
    """F2.7 — dstool pdisk stop then restart; IO resumes, acked data survives.

    Stopping the isolated SSD PDisk of one DBG host does not fail user IO
    (the write quorum remains). Restart is still exercised so sessions
    reconnect.
    """

    def test_pdisk_stop_restart(self):
        disk = self.make_disk()
        payloads = self.write_and_verify(disk, 0, 4)
        host = self.pick_dbg_storage_node(disk.tablet_id)
        assert host.pdisk_id is not None, host
        self.assert_ddisk_on_isolated_pdisk(disk)
        self.faults.pdisk_stop(host.node_id, host.pdisk_id)
        payload = self.as_bytes(self.generate_random_data(disk.block_size))
        ok, stdout, stderr = self.try_write(disk, 32, payload)
        logger.info(
            'IO after pdisk stop node=%s pdisk=%s: ok=%s stdout=%s stderr=%s',
            host.node_id,
            host.pdisk_id,
            ok,
            stdout,
            stderr,
        )
        self.faults.pdisk_restart(host.node_id, host.pdisk_id)

        self.wait_io_ok(disk)
        self.assert_pattern(disk, payloads)
        self.write_and_verify(disk, 16, 2)
