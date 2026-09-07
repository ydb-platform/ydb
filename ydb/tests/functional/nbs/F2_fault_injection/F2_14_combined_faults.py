# -*- coding: utf-8 -*-
import logging

from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.markers import known_bug

logger = logging.getLogger(__name__)


class TestF2_14CombinedFaults(NbsCase):
    """F2.14 — tablet kill + one host SIGSTOP + one host stop."""

    @known_bug(
        'IO does not recover within 120s after tablet kill plus one host '
        'stop and one host freeze, even with the DDisk pool on its own SSD'
    )
    def test_combined_faults(self):
        disk = self.make_disk()
        payloads = self.write_and_verify(disk, 0, 8)
        self.assert_ddisk_on_isolated_pdisk(disk)
        frozen = self.pick_dbg_storage_node(disk.tablet_id)
        stopped = self.pick_dbg_storage_node(disk.tablet_id, exclude={frozen.node_id})
        hosts = self.dbg_hosts(disk.tablet_id)
        logger.info(
            'F2.14 freeze node=%s dc=%s stop node=%s dc=%s dbg=%s',
            frozen.node_id,
            frozen.node_id % 3,
            stopped.node_id,
            stopped.node_id % 3,
            [
                'H{} node={} dc={} ddisk={} pbuffer={} state={}'.format(
                    h.index,
                    h.node_id,
                    (h.node_id or 0) % 3,
                    h.ddisk_role,
                    h.pbuffer_role,
                    h.state,
                )
                for h in hosts
            ],
        )

        self.faults.freeze_node(frozen.node_id)
        self.faults.stop_node(stopped.node_id)
        self.faults.tablet_kill(disk.tablet_id)

        self.faults.thaw_node(frozen.node_id)
        self.faults.start_node(stopped.node_id)
        self.wait_io_ok(disk, timeout_seconds=8)
        self.assert_pattern(disk, payloads)
        self.write_and_verify(disk, 16, 4)
