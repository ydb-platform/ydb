# -*- coding: utf-8 -*-
import logging

from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.markers import known_bug

logger = logging.getLogger(__name__)


class TestF2_05ThreeDbgHostsDown(NbsCase):
    """F2.5 — three PBuffer nodes down: writes fail cleanly, no silent SUCCESS."""

    @known_bug(
        'IO does not recover within 120s after three PBuffer nodes across '
        'all three DCs are restarted, even with the DDisk pool on its own SSD'
    )
    def test_three_dbg_hosts_down(self):
        disk = self.make_disk()
        payloads = self.write_and_verify(disk, 0, 4)
        self.assert_ddisk_on_isolated_pdisk(disk)

        # Writes need 3 PBuffer acks. Stop PBuffer nodes, not DDisk nodes:
        # in the 9-node / 5-domain pool they are not always the same machine.
        nodes = self.dbg_pbuffer_nodes(disk.tablet_id)
        assert len(nodes) >= 3, 'need at least 3 PBuffer nodes to lose quorum: {}'.format(nodes)
        ordered = [n for n in nodes if n != 1] + [n for n in nodes if n == 1]
        stopped = ordered[:3]
        hosts = self.dbg_hosts(disk.tablet_id)
        logger.info(
            'F2.5 victims=%s dcs=%s dbg=%s',
            stopped,
            [n % 3 for n in stopped],
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

        for node_id in stopped:
            self.faults.stop_node(node_id)

        payload = self.as_bytes(self.generate_random_data(disk.block_size))
        ok, stdout, stderr = self.try_write(disk, 64, payload)
        assert not ok, 'write succeeded without quorum: stdout={} stderr={}'.format(stdout, stderr)

        ok_read, got = self.try_read(disk, 0)
        if ok_read:
            assert got[:disk.block_size] == payloads[0][:disk.block_size], (
                'read after failed write returned corrupt data at block 0'
            )

        for node_id in stopped:
            self.faults.start_node(node_id)
        self.wait_io_ok(disk, timeout_seconds=8)
        self.assert_pattern(disk, payloads)
        self.write_and_verify(disk, 16, 4)
