# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF2_02SlotStopStart(NbsCase):
    """F2.2 — stop and start the /Root/NBS dynamic slot."""

    def test_slot_stop_start(self):
        disk = self.make_disk()
        payloads = self.write_and_verify(disk, 0, 4)

        slot = self.faults.stop_slot()
        self.faults.start_slot(slot)

        self.wait_io_ok(disk, timeout_seconds=120)
        self.assert_pattern(disk, payloads)
        self.write_and_verify(disk, 16, 4)
