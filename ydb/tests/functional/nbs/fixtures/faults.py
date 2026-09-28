# -*- coding: utf-8 -*-
"""Fault primitives with LIFO undo for a shared-cluster suite.

Each method records the inverse action on the per-case ``FaultScope`` so
teardown can restart nodes, thaw SIGSTOP, and un-break PDisks even when
the test itself fails mid-fault.
"""

import logging
import signal

from ydb.tests.library.common.msgbus_types import EDriveStatus
from ydb.tests.library.harness.daemon import DaemonError
from ydb.tests.functional.nbs.lib.helpers import execute_dstool_grpc

logger = logging.getLogger(__name__)


class FaultScope:
    """Records inverses of injected faults and applies them in LIFO order."""

    def __init__(self, nbs_cluster):
        self._nbs_cluster = nbs_cluster
        self._cluster = nbs_cluster.cluster
        self._undos = []

    def push_undo(self, action, description):
        """Register a teardown action. ``action`` is a zero-arg callable."""
        self._undos.append((description, action))

    def undo_all(self):
        """Apply recorded inverses, last fault first. Best effort."""
        errors = []
        while self._undos:
            description, action = self._undos.pop()
            try:
                logger.info('undo fault: %s', description)
                action()
            except Exception as e:
                logger.exception('undo failed for %s: %s', description, e)
                errors.append('{}: {}'.format(description, e))
        if errors:
            logger.error('fault undo had errors: %s', errors)

    def tablet_kill(self, tablet_id):
        """Kill the partition tablet. Hive restarts it; no undo needed.

        A second kill while the tablet is already dying can return
        UNAVAILABLE; the first poison is what Hive acts on.
        """
        logger.info('tablet_kill %s', tablet_id)
        result = self._cluster.client.tablet_kill(int(tablet_id))
        logger.info(
            'tablet_kill %s status=%s issues=%s',
            tablet_id,
            getattr(result, 'status', None),
            getattr(result, 'issues', None),
        )

    def stop_node(self, node_id):
        """Stop a static storage node. Undo restarts it."""
        node = self._cluster.nodes[node_id]
        logger.info('stop node %s', node_id)
        node.stop()
        self.push_undo(lambda: self._safe_start_node(node_id), 'start node {}'.format(node_id))

    def start_node(self, node_id):
        """Start a previously stopped static node and drop the matching undo."""
        self._safe_start_node(node_id)
        self._drop_undo_containing('start node {}'.format(node_id))

    def freeze_node(self, node_id):
        """SIGSTOP a static node. Undo is SIGCONT."""
        node = self._cluster.nodes[node_id]
        logger.info('SIGSTOP node %s', node_id)
        node.send_signal(signal.SIGSTOP)
        self.push_undo(lambda: self._safe_cont(node_id), 'SIGCONT node {}'.format(node_id))

    def thaw_node(self, node_id):
        """SIGCONT a frozen node and drop the matching undo."""
        self._safe_cont(node_id)
        self._drop_undo_containing('SIGCONT node {}'.format(node_id))

    def stop_slot(self, slot=None):
        """Stop the NBS dynamic slot. Undo restarts it.

        A SIGTERM during vhost IO can SIGSEGV the slot. The process is
        still gone, which is the fault, so a bad exit code is logged and
        ignored the same way ``recycle_nbs_slot`` swallows ``stop()``.
        """
        slot = slot or self._first_slot()
        logger.info('stop slot %s', slot.node_id)
        try:
            slot.stop()
        except DaemonError as e:
            logger.warning('stop slot %s raised %s; treating as stopped', slot.node_id, e)
        self.push_undo(lambda: self._safe_start_slot(slot), 'start slot {}'.format(slot.node_id))
        return slot

    def start_slot(self, slot=None):
        """Start the NBS dynamic slot and drop the matching undo."""
        slot = slot or self._first_slot()
        self._safe_start_slot(slot)
        self._nbs_cluster.wait_tenant_ready(timeout_seconds=20)
        self._drop_undo_containing('start slot {}'.format(slot.node_id))

    def set_pdisk_broken(self, node_id, pdisk_id=None, path=None):
        """Mark a PDisk BROKEN. Undo sets ACTIVE."""
        self._set_pdisk_status(node_id, EDriveStatus.BROKEN, pdisk_id=pdisk_id, path=path)
        self.push_undo(
            lambda: self._set_pdisk_status(
                node_id, EDriveStatus.ACTIVE, pdisk_id=pdisk_id, path=path
            ),
            'pdisk ACTIVE node={} pdisk={}'.format(node_id, pdisk_id),
        )

    def set_pdisk_active(self, node_id, pdisk_id=None, path=None):
        """Mark a PDisk ACTIVE and drop the matching undo."""
        self._set_pdisk_status(node_id, EDriveStatus.ACTIVE, pdisk_id=pdisk_id, path=path)
        self._drop_undo_containing('pdisk ACTIVE node={}'.format(node_id))

    def pdisk_stop(self, node_id, pdisk_id):
        """``dstool pdisk stop``. Undo is ``pdisk restart``."""
        logger.info('pdisk stop node=%s pdisk=%s', node_id, pdisk_id)
        execute_dstool_grpc(
            self._cluster,
            'token',
            ['pdisk', 'stop', '--node-id={}'.format(node_id), '--pdisk-id={}'.format(pdisk_id)],
            check_exit_code=False,
        )
        self.push_undo(
            lambda: self._pdisk_restart(node_id, pdisk_id),
            'pdisk restart node={} pdisk={}'.format(node_id, pdisk_id),
        )

    def pdisk_restart(self, node_id, pdisk_id):
        """``dstool pdisk restart`` and drop the matching undo."""
        self._pdisk_restart(node_id, pdisk_id)
        self._drop_undo_containing('pdisk restart node={}'.format(node_id))

    def _first_slot(self):
        slots = list(self._cluster.slots.values())
        assert slots, 'no NBS slots registered'
        return slots[0]

    def _safe_start_node(self, node_id):
        node = self._cluster.nodes[node_id]
        if not node.is_alive():
            logger.info('start node %s', node_id)
            node.start()
            self._nbs_cluster.mark_restarted()

    def _safe_start_slot(self, slot):
        if not slot.is_alive():
            logger.info('start slot %s', slot.node_id)
            slot.start()
            self._nbs_cluster.mark_restarted()

    def _safe_cont(self, node_id):
        node = self._cluster.nodes[node_id]
        logger.info('SIGCONT node %s', node_id)
        try:
            node.send_signal(signal.SIGCONT)
        except Exception as e:
            logger.warning('SIGCONT node %s failed: %s', node_id, e)

    def _set_pdisk_status(self, node_id, status, pdisk_id=None, path=None):
        node = self._cluster.nodes[node_id]
        if path:
            logger.info('update_drive_status node=%s path=%s status=%s', node_id, path, status)
            self._cluster.client.update_drive_status(node.host, node.ic_port, path, status)
            return
        if pdisk_id is None:
            raise ValueError('pdisk_id or path is required')
        status_name = EDriveStatus(status).name
        logger.info('pdisk set node=%s pdisk=%s status=%s', node_id, pdisk_id, status_name)
        extra = ['--allow-working-disks'] if status == EDriveStatus.BROKEN else []
        execute_dstool_grpc(
            self._cluster,
            'token',
            [
                'pdisk',
                'set',
                '--status={}'.format(status_name),
                '--pdisk-ids',
                '[{}:{}]'.format(node_id, pdisk_id),
            ]
            + extra,
            check_exit_code=False,
        )

    def _pdisk_restart(self, node_id, pdisk_id):
        logger.info('pdisk restart node=%s pdisk=%s', node_id, pdisk_id)
        execute_dstool_grpc(
            self._cluster,
            'token',
            ['pdisk', 'restart', '--node-id={}'.format(node_id), '--pdisk-id={}'.format(pdisk_id)],
            check_exit_code=False,
        )

    def _drop_undo_containing(self, fragment):
        self._undos = [(d, a) for d, a in self._undos if fragment not in d]
