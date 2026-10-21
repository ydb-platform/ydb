# -*- coding: utf-8 -*-
from ydb.tests.functional.nbs.lib.fixtures.base import NbsCase
from ydb.tests.functional.nbs.lib.fixtures.markers import (
    SLOT_CRASH_ON_PRIMARY_DDISK_LOSS,
    known_bug,
)


def _is_handoff_pbuffer(host):
    return host.pbuffer_role.lower() in ('handoff', '1')


def _is_none_ddisk(host):
    return host.ddisk_role.lower() in ('none', '2')


class TestF3_04OfflineQueryAddhost(NbsCase):
    """F3.4 — Offline requests AddHost until a spare is appended as HandOff.

    Does not execute: stopping a Primary DDisk host segfaults the NBS slot.
    """

    @known_bug(SLOT_CRASH_ON_PRIMARY_DDISK_LOSS)
    def test_offline_query_addhost(self):
        disk = self.make_disk()
        self.write_pattern(disk, 0, 4)
        before = self.dbg_hosts(disk.tablet_id)
        before_indexes = {h.index for h in before}
        host = self.pick_primary_ddisk_host(disk.tablet_id)
        self.faults.stop_node(host.node_id)
        self.wait_host_offline(disk, host.node_id)

        def addhost_done():
            after = self.dbg_hosts(disk.tablet_id)
            return len(after) > len(before)

        self.wait_until(addhost_done, timeout_seconds=8, description='AddHost appended a host')
        roles = self.vchunk_hosts(disk.tablet_id)
        appended = [h for h in roles if h.index not in before_indexes]
        assert appended, 'no new host on the VChunk page after AddHost: {}'.format(
            [(h.index, h.pbuffer_role, h.ddisk_role) for h in roles]
        )
        for added in appended:
            assert _is_handoff_pbuffer(added) and _is_none_ddisk(added), (
                'appended host H{} should be PBuffer HandOff / DDisk None, got {} / {}'.format(
                    added.index, added.pbuffer_role, added.ddisk_role
                )
            )
