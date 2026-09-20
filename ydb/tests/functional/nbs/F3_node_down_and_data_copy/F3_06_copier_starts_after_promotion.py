# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.markers import (
    SLOT_CRASH_ON_PRIMARY_DDISK_LOSS,
    known_bug,
)


class TestF3_06CopierStartsAfterPromotion(NbsCase):
    """F3.6 — after Offline promotion, the promoted host is Fresh and copy advances.

    Does not execute: stopping a Primary DDisk host segfaults the NBS slot.
    """

    @known_bug(SLOT_CRASH_ON_PRIMARY_DDISK_LOSS)
    def test_copier_starts_after_promotion(self):
        disk = self.make_disk()
        self.write_pattern(disk, 0, 16)
        before = self.vchunk_hosts(disk.tablet_id)
        before_primaries = {
            h.index for h in before if h.ddisk_role.lower() in ('primary', '0')
        }
        host = self.pick_primary_ddisk_host(disk.tablet_id)
        self.faults.stop_node(host.node_id)
        self.wait_host_offline(disk, host.node_id)

        # Fresh starts at operational_block_count 0, but a 32 MiB vchunk can
        # finish before the first poll. Copy-complete (all watermarks unset)
        # counts only after a HandOff was promoted.
        seen = []

        def progressed():
            hosts = self.vchunk_hosts(disk.tablet_id)
            promoted = any(
                h.index not in before_primaries
                and h.ddisk_role.lower() in ('primary', '0')
                for h in hosts
            )
            if promoted and hosts and all(h.watermark is None for h in hosts):
                return True
            states = self.vchunk_ddisk_states(disk.tablet_id)
            fresh = [
                s.operational_block_count for s in states.values() if s.state == 'Fresh'
            ]
            if fresh:
                seen.append(max(fresh))
            if any(c == 0 for c in seen) and any(c > 0 for c in seen):
                return True
            if len(seen) >= 2 and seen[-1] > seen[0]:
                return True
            if seen and not fresh:
                return True
            return False

        self.wait_until(
            progressed,
            timeout_seconds=15,
            sleep_seconds=0.2,
            description='Fresh host appears and copy advances',
        )
