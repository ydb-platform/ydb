# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.markers import (
    SLOT_CRASH_ON_PRIMARY_DDISK_LOSS,
    known_bug,
)


class TestF3_15CopyDoesNotResumeIncrementally(NbsCase):
    """F3.15 — persisted watermark survives tablet restart (NBS-7656).

    ``OnCopyProgress`` writes the operational prefix every 8 MiB. After a
    tablet kill the promoted host must keep that watermark, or stay
    complete if the 32 MiB vchunk finished before the first poll.

    Does not execute: stopping a Primary DDisk host segfaults the NBS slot.
    """

    @known_bug(SLOT_CRASH_ON_PRIMARY_DDISK_LOSS)
    def test_copy_progress_survives_tablet_kill(self):
        disk = self.make_disk()
        self.write_pattern(disk, 0, 16)
        before = self.vchunk_hosts(disk.tablet_id)
        before_primaries = {h.index for h in before if self.is_primary_ddisk(h)}
        host = self.pick_primary_ddisk_host(disk.tablet_id)
        self.faults.stop_node(host.node_id)
        self.wait_host_offline(disk, host.node_id)

        def copy_observable():
            hosts = self.vchunk_hosts(disk.tablet_id)
            promoted = [
                h
                for h in hosts
                if h.index not in before_primaries and self.is_primary_ddisk(h)
            ]
            if not promoted:
                return False
            # PromoteHost sets watermark 0; OnCopyProgress may persist an
            # 8 MiB tick; OnCopyComplete unsets it. All of those count.
            return True

        self.wait_until(
            copy_observable,
            timeout_seconds=15,
            sleep_seconds=0.2,
            description='promoted-host copy is in progress or already complete',
        )

        before_kill = self.vchunk_hosts(disk.tablet_id)
        promoted_before = [
            h
            for h in before_kill
            if h.index not in before_primaries and self.is_primary_ddisk(h)
        ]
        assert promoted_before, 'no promoted host before tablet kill: {}'.format(
            [(h.index, h.ddisk_role, h.watermark) for h in before_kill]
        )
        before_marks = {h.index: h.watermark for h in promoted_before}

        self.faults.tablet_kill(disk.tablet_id)

        def mon_up():
            try:
                return bool(self.vchunk_hosts(disk.tablet_id))
            except Exception:
                return False

        self.wait_until(
            mon_up,
            timeout_seconds=20,
            sleep_seconds=0.5,
            description='tablet mon after kill',
        )

        after = self.vchunk_hosts(disk.tablet_id)
        for idx, mark in before_marks.items():
            found = [h for h in after if h.index == idx]
            assert found, 'promoted host H{} missing after tablet kill'.format(idx)
            after_mark = found[0].watermark
            if mark is None:
                assert after_mark is None, (
                    'completed copy must stay complete: H{} was -, now {}'.format(
                        idx, after_mark
                    )
                )
                continue
            if after_mark is None:
                continue
            assert after_mark >= mark, (
                'watermark rewound on H{}: before={} after={}'.format(
                    idx, mark, after_mark
                )
            )
