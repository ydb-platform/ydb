# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.markers import (
    SLOT_CRASH_ON_PRIMARY_DDISK_LOSS,
    known_bug,
)
from ydb.tests.functional.nbs.lib.fixtures.mon import (
    parse_ddisk_states,
    parse_inflight_ddisk_syncs,
    parse_vchunk_hosts,
)

# CopyRangeSize = 1 MiB / 4 KiB.
COPY_RANGE_BLOCKS = 256


class TestF3_07Serial1mibRanges(NbsCase):
    """F3.7 — observed copy progress advances in serial 1 MiB ranges.

    Does not execute: stopping a Primary DDisk host segfaults the NBS slot.
    """

    @known_bug(SLOT_CRASH_ON_PRIMARY_DDISK_LOSS)
    def test_serial_1mib_ranges(self):
        disk = self.make_disk()
        self.write_pattern(disk, 0, 32)
        host = self.pick_primary_ddisk_host(disk.tablet_id)
        self.faults.stop_node(host.node_id)
        self.wait_host_offline(disk, host.node_id)

        counts = []
        overlaps = []
        completed = {'value': False}

        def progressed():
            html = self.vchunk_page(disk.tablet_id)
            hosts = parse_vchunk_hosts(html)
            if hosts and all(h.watermark is None for h in hosts):
                completed['value'] = True
                return True
            states = parse_ddisk_states(html)
            syncs = parse_inflight_ddisk_syncs(html)
            dests = {}
            for sync in syncs:
                dests[sync.destination_host] = dests.get(sync.destination_host, 0) + 1
            if any(n > 1 for n in dests.values()):
                overlaps.append(dict(dests))
            fresh = [
                s.operational_block_count for s in states.values() if s.state == 'Fresh'
            ]
            if fresh:
                counts.append(max(fresh))
            if len(counts) >= 2 and counts[-1] > counts[0]:
                return True
            if counts and not fresh:
                return True
            return False

        self.wait_until(
            progressed,
            timeout_seconds=15,
            sleep_seconds=0.2,
            description='copy operational_block_count advances',
        )
        assert not overlaps, 'overlapping copy ranges on one destination: {}'.format(overlaps)
        if completed['value'] and not counts:
            return
        assert counts, 'no Fresh operational_block_count samples'
        assert all(c % COPY_RANGE_BLOCKS == 0 for c in counts), counts
        assert all(counts[i] <= counts[i + 1] for i in range(len(counts) - 1)), counts
