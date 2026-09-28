# -*- coding: utf-8 -*-
import pytest

from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


@pytest.mark.timeout(120, func_only=True)
class TestF2_02SlotStopStart(NbsCase):
    """F2.2 — stop and start the /Root/NBS dynamic slot."""

    def test_slot_stop_start(self):
        disk = self.make_disk()
        payloads = self.write_and_verify(disk, 0, 4)
        socket_identity = self.vhost_socket_identity(disk)

        # Write-only across the fault: a read-after-write pair can straddle
        # the old and new endpoint and report a mismatch that is not a
        # lost ack. Seed re-read and the post-fault write-and-verify are
        # the durability checks.
        io = self.start_vhost_io(disk, start_index=48, range_blocks=1, verify=False)
        try:
            slot = self.faults.stop_slot()
            self.wait_io_down(disk, timeout_seconds=10)
            self.faults.start_slot(slot)
            self.wait_vhost_endpoint_returned(disk, socket_identity, timeout_seconds=60)
        finally:
            io.stop_and_join()

        self.wait_io_ok(disk, timeout_seconds=60)
        self.assert_pattern(disk, payloads, timeout=30.0)
        self.write_and_verify(disk, 16, 4, timeout=30.0)
