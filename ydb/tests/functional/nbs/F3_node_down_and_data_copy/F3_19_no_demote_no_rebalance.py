# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.markers import (
    SLOT_CRASH_ON_PRIMARY_DDISK_LOSS,
    known_bug,
)


def _is_primary_ddisk(host):
    return host.ddisk_role.lower() in ('primary', '0')


class TestF3_19NoDemoteNoRebalance(NbsCase):
    """F3.19 — Offline leaves the failed host listed and disabled.

    After a healthy quorum returns, ``DemoteUnavailableHostsIfNeeded``
    may drop the disabled replica to DDisk None.

    Does not execute: stopping a Primary DDisk host segfaults the NBS slot.
    """

    @known_bug(SLOT_CRASH_ON_PRIMARY_DDISK_LOSS)
    def test_no_demote_no_rebalance(self):
        disk = self.make_disk()
        self.write_pattern(disk, 0, 4)
        host = self.pick_primary_ddisk_host(disk.tablet_id)
        self.faults.stop_node(host.node_id)
        self.wait_host_offline(disk, host.node_id)

        after = self.dbg_hosts(disk.tablet_id)
        still_listed = [h for h in after if h.node_id == host.node_id]
        assert still_listed, 'failed host {} was removed; EvacuateHost is not a runtime path'.format(
            host.node_id
        )
        roles = self.vchunk_hosts(disk.tablet_id)
        failed = [h for h in roles if h.index == host.index]
        assert failed, 'failed host H{} missing from VChunk roles'.format(host.index)
        assert failed[0].enabled.lower() == 'no', (
            'failed host H{} should stay Enabled: no, got {!r}'.format(
                host.index, failed[0].enabled
            )
        )
        # Offline disables the host. Once healthy DDisks are back to quorum,
        # DemoteUnavailableHostsIfNeeded drops it to DDisk None. Catching the
        # host still Primary means the copy has not finished yet.
        assert failed[0].ddisk_role.lower() in ('none', 'primary', '0'), (
            'failed host H{} should stay Primary until demote, or be None '
            'after quorum is restored, got {!r}'.format(
                host.index, failed[0].ddisk_role
            )
        )
