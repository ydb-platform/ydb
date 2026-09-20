# -*- coding: utf-8 -*-
import time

from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.markers import (
    SLOT_CRASH_ON_PRIMARY_DDISK_LOSS,
    known_bug,
)


class TestF3_16DostartDoesNotStartCopiers(NbsCase):
    """F3.16 — DoStart does not start copiers; they wait for the next ApplyConfig.

    After a tablet kill the persisted watermark of the promoted host is
    restored (F3.15 / NBS-7656), but ``DoStart`` only restores PBuffers.
    Copiers start in ``ApplyConfig``. Several Think ticks must not advance
    the promoted host.

    Does not execute: stopping a Primary DDisk host segfaults the NBS slot.
    """

    @known_bug(SLOT_CRASH_ON_PRIMARY_DDISK_LOSS)
    def test_dostart_does_not_start_copiers(self):
        disk = self.make_disk()
        self.write_pattern(disk, 0, 16)
        before = self.vchunk_hosts(disk.tablet_id)
        before_primaries = {h.index for h in before if self.is_primary_ddisk(h)}
        host = self.pick_primary_ddisk_host(disk.tablet_id)
        self.faults.stop_node(host.node_id)
        self.wait_host_offline(disk, host.node_id)

        def promoted():
            hosts = self.vchunk_hosts(disk.tablet_id)
            return any(
                h.index not in before_primaries and self.is_primary_ddisk(h)
                for h in hosts
            )

        # Kill as soon as a HandOff is promoted so the copy is still open
        # when DoStart runs. A finished copy cannot show a stuck copier.
        self.wait_until(
            promoted,
            timeout_seconds=15,
            sleep_seconds=0.2,
            description='HandOff promoted to Primary',
        )

        self.faults.tablet_kill(disk.tablet_id)

        def mon_up():
            try:
                return bool(self.vchunk_ddisk_states(disk.tablet_id))
            except Exception:
                return False

        self.wait_until(
            mon_up,
            timeout_seconds=20,
            sleep_seconds=0.5,
            description='tablet mon after kill',
        )

        def promoted_progress():
            hosts = {h.index: h for h in self.vchunk_hosts(disk.tablet_id)}
            states = self.vchunk_ddisk_states(disk.tablet_id)
            progress = {}
            for idx in promoted_idx:
                host_row = hosts.get(idx)
                state = states.get(idx)
                progress[idx] = (
                    None if host_row is None else host_row.watermark,
                    None if state is None else state.operational_block_count,
                )
            return progress

        after_hosts = self.vchunk_hosts(disk.tablet_id)
        promoted_idx = {
            h.index
            for h in after_hosts
            if h.index not in before_primaries and self.is_primary_ddisk(h)
        }
        if not promoted_idx:
            promoted_idx = {
                h.index for h in after_hosts if self.is_primary_ddisk(h)
            }
        assert promoted_idx, 'no promoted host after tablet kill'

        first = promoted_progress()
        deadline = time.time() + 1.0
        while time.time() < deadline:
            time.sleep(0.2)
            later = promoted_progress()
            for idx, (mark, count) in first.items():
                later_mark, later_count = later[idx]
                if mark is None:
                    assert later_mark is None, (
                        'DoStart restarted a completed copier on H{}: '
                        'watermark {} -> {}'.format(idx, mark, later_mark)
                    )
                else:
                    assert later_mark is not None and later_mark <= mark, (
                        'DoStart advanced copy on H{}: watermark {} -> {}'.format(
                            idx, mark, later_mark
                        )
                    )
                if count is not None and later_count is not None:
                    assert later_count <= count, (
                        'DoStart restarted copiers on H{}: '
                        'operational_block_count {} -> {}'.format(
                            idx, count, later_count
                        )
                    )
