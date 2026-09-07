# -*- coding: utf-8 -*-
import time

from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF4_07FlushCooldownOnErrors(NbsCase):
    """F4.7 — SIGSTOP a host so ConsecutiveErrorCount rises and flush slows."""

    def test_flush_cooldown_on_errors(self):
        disk = self.make_disk()
        self.write_pattern(disk, 0, 8)
        host = self.pick_dbg_storage_node(disk.tablet_id)
        self.faults.freeze_node(host.node_id)
        time.sleep(0.2)
        self.write_pattern(disk, 16, 8)

        def errors_seen():
            for current in self.dbg_hosts(disk.tablet_id):
                if current.node_id == host.node_id and current.consecutive_errors > 0:
                    return True
            return False

        self.wait_until(errors_seen, timeout_seconds=30, description='ConsecutiveErrorCount rises')
        self.faults.thaw_node(host.node_id)
        self.wait_io_ok(disk)
