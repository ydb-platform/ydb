# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase


class TestF3_16DostartDoesNotStartCopiers(NbsCase):
    """F3.16 — DoStart does not start copiers; they wait for the next ApplyConfig.

    Documented current behaviour. After F3.15 the copy is stuck until another
    host-state change; this case records whichever of those we observe.
    """

    def test_dostart_does_not_start_copiers(self):
        disk = self.make_disk()
        self.write_pattern(disk, 0, 16)
        host = self.pick_dbg_storage_node(disk.tablet_id)
        self.faults.stop_node(host.node_id)
        self.wait_host_state(disk.tablet_id, host.node_id, ('Offline', '2'), timeout_seconds=45)
        self.faults.tablet_kill(disk.tablet_id)
        self.wait_io_ok(disk, timeout_seconds=90)

        before = [h.watermark for h in self.vchunk_hosts(disk.tablet_id)]
        # Poll for several Think ticks. If DoStart restarted copiers the
        # watermark would leave {0, None}; today it does not.
        moved = {'value': False}

        def still_stuck():
            after = [h.watermark for h in self.vchunk_hosts(disk.tablet_id)]
            if after != before and any(w not in (0, None) for w in after):
                moved['value'] = True
                return True
            return False

        try:
            self.wait_until(still_stuck, timeout_seconds=8, description='copier unexpectedly resumed')
        except AssertionError:
            pass
        assert not moved['value'], 'DoStart restarted copiers: {} -> {}'.format(
            before, [h.watermark for h in self.vchunk_hosts(disk.tablet_id)]
        )
