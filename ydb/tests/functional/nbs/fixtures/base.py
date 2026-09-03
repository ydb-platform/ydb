# -*- coding: utf-8 -*-
"""Per-case fixture that binds the suite cluster and cleans up after itself."""

import logging
import os
import re
import threading
from dataclasses import dataclass

import pytest

from ydb.tests.functional.nbs.lib.common import DEFAULT_DISK_BLOCKS_COUNT, NbsTestBase
from ydb.tests.functional.nbs.lib.helpers import execute_dstool_grpc
from ydb.tests.functional.nbs.lib.fixtures.faults import FaultScope
from ydb.tests.functional.nbs.lib.fixtures.geometry import DEFAULT_BLOCK_SIZE
from ydb.tests.functional.nbs.lib.fixtures.mon import (
    merge_hosts_with_connections,
    parse_ddisk_states,
    parse_dbg_connections,
    parse_dbg_hosts,
    parse_vchunk_hosts,
)
from ydb.tests.functional.nbs.lib.vhost_user_blk_client import (
    VIRTIO_BLK_S_OK,
    VhostUserBlkClient,
    VhostUserBlkDisconnected,
    virtio_blk_status_name,
    wait_for_socket,
)

logger = logging.getLogger(__name__)


@dataclass
class DiskHandle:
    """One disk created for a single case. IO goes through the vhost socket."""

    disk_id: str
    tablet_id: str
    socket_path: str
    blocks_count: int = DEFAULT_DISK_BLOCKS_COUNT
    block_size: int = DEFAULT_BLOCK_SIZE

    def byte_offset(self, index):
        """Byte offset of block ``index`` on this disk."""
        return index * self.block_size


@dataclass
class VhostIo:
    """Background vhost IO started by ``NbsCase.start_vhost_io``."""

    stop: threading.Event
    thread: threading.Thread
    writes: list
    errors: list

    def stop_and_join(self, timeout=10):
        """Signal the writer and wait for the thread to exit."""
        self.stop.set()
        self.thread.join(timeout)


class NbsCase(NbsTestBase):
    """Base class for one-file-per-case NBS functional tests.

    Shadows ``NbsTestBase.setup`` so the suite cluster is reused. Every
    case must create its own disks and reverse its own faults: the
    collector does not promise order, and ``--test-retries`` can rerun
    one case in isolation.
    """

    @pytest.fixture(autouse=True)
    def setup(self, nbs_cluster):
        self.cluster = nbs_cluster.cluster
        self.ddisk_pool_name = nbs_cluster.ddisk_pool_name
        self._nbs_cluster = nbs_cluster
        self._created_disks = []
        self.faults = FaultScope(nbs_cluster)
        self._recycled_for_create = False
        nbs_cluster.wait_healthy(timeout_seconds=30)
        yield
        self.faults.undo_all()
        nbs_cluster.wait_healthy()
        self.drop_created_disks()

    def on_create_unavailable(self):
        """Recycle the NBS slot once when CreatePartition stays UNAVAILABLE.

        ``ProxyShardNotAvailable`` after a previous case's tablet kill or
        host-loss means the tenant is up (mon is fine) but TxProxy cannot
        reach a shard. Retrying alone does not help.
        """
        if self._recycled_for_create:
            return
        self._recycled_for_create = True
        logger.warning(
            'CreatePartition UNAVAILABLE; recovering cluster and recycling NBS slot'
        )
        self._nbs_cluster.recover()
        self._nbs_cluster.recycle_nbs_slot()

    def register_disk(self, disk_id):
        """Remember ``disk_id`` so teardown deletes it."""
        self._created_disks.append(disk_id)

    def make_disk(self, blocks_count=DEFAULT_DISK_BLOCKS_COUNT, block_size=DEFAULT_BLOCK_SIZE):
        """Create a disk, wait for its vhost socket, and track it for cleanup."""
        disk_id = self.generate_disk_id()
        tablet_id = self.create_disk(disk_id, blocks_count, block_size=block_size)
        self.register_disk(disk_id)
        disk = self.bind_vhost(disk_id, tablet_id, blocks_count, block_size)
        # Map a block past the usual 0–32 seed so wait_host_offline can drive
        # IO without a first-touch hang or clobbering asserted data.
        if disk.blocks_count > 48:
            try:
                self.write_blocks(
                    disk, 48, bytes([0xA5]) * disk.block_size, timeout=5.0
                )
            except Exception as e:
                logger.warning('oracle probe write at block 48 failed: %s', e)
        return disk

    def bind_vhost(self, disk_id, tablet_id, blocks_count=DEFAULT_DISK_BLOCKS_COUNT,
                   block_size=DEFAULT_BLOCK_SIZE):
        """Wait for ``/tmp/<disk_id>.sock`` and return a handle for vhost IO."""
        socket_path = self.vhost_socket_path(disk_id)
        wait_for_socket(socket_path)
        return DiskHandle(
            disk_id=disk_id,
            tablet_id=str(tablet_id),
            socket_path=socket_path,
            blocks_count=blocks_count,
            block_size=block_size,
        )

    @staticmethod
    def vhost_socket_path(disk_id):
        """Path of the partition vhost-user-blk socket."""
        return '/tmp/{}.sock'.format(disk_id)

    @staticmethod
    def as_bytes(data):
        """Encode ``generate_random_data`` strings as virtio-blk payloads."""
        if isinstance(data, bytes):
            return data
        return data.encode('ascii')

    @pytest.fixture
    def disk(self):
        """Default 4 GiB / 4 KiB disk for cases that do not need custom geometry."""
        return self.make_disk()

    def drop_disk(self, disk):
        """Delete ``disk`` now so later cases in the same test keep capacity."""
        try:
            self.delete_disk(disk.disk_id)
        except Exception as e:
            logger.warning('drop_disk(%s) failed: %s', disk.disk_id, e)
        if disk.disk_id in self._created_disks:
            self._created_disks.remove(disk.disk_id)

    def drop_created_disks(self):
        """Best-effort delete so DDisk capacity stays available for later cases.

        A hung delete leaves the disk (and its copier) on the shared session
        cluster. Recycle the NBS slot so the next case starts clean.
        """
        delete_failed = False
        while self._created_disks:
            disk_id = self._created_disks.pop()
            try:
                result = execute_dstool_grpc(
                    self.cluster,
                    'token',
                    ['nbs', 'partition', 'delete', '--disk-id', disk_id],
                    check_exit_code=False,
                    return_process=True,
                    timeout=60,
                )
                if result.exit_code != 0:
                    logger.warning(
                        'delete_disk(%s) during teardown exit %s',
                        disk_id,
                        result.exit_code,
                    )
                    delete_failed = True
            except Exception as e:
                logger.warning('delete_disk(%s) during teardown failed: %s', disk_id, e)
                delete_failed = True
        if delete_failed:
            logger.warning('recycle NBS slot after failed partition delete')
            self._nbs_cluster.recycle_nbs_slot()

    def open_vhost(self, disk, socket_timeout=30.0):
        """Open a vhost-user-blk client for ``disk``."""
        return VhostUserBlkClient(disk.socket_path, socket_timeout=socket_timeout)

    def _retry_on_disconnect(self, client, action, attempts=3):
        """Reissue ``action`` after a socket drop, the way qemu reconnects."""
        last = None
        for attempt in range(attempts):
            try:
                return action()
            except VhostUserBlkDisconnected as e:
                last = e
                if attempt + 1 == attempts:
                    raise
                logger.warning(
                    'vhost disconnected, reconnecting (%s/%s): %s',
                    attempt + 1,
                    attempts,
                    e,
                )
                client.reconnect()
        raise last

    def write_blocks(self, disk, index, data, client=None, timeout=10.0):
        """Write one block through vhost. Asserts ``VIRTIO_BLK_S_OK``."""
        payload = self.as_bytes(data)
        if client is not None:
            status = self._retry_on_disconnect(
                client,
                lambda: client.write(disk.byte_offset(index), payload, timeout=timeout),
            )
        else:
            with self.open_vhost(disk) as opened:
                status = opened.write(disk.byte_offset(index), payload, timeout=timeout)
        assert status == VIRTIO_BLK_S_OK, 'vhost write block {} status {}'.format(
            index, virtio_blk_status_name(status)
        )

    def read_blocks(self, disk, index, blocks_count=1, client=None, timeout=10.0):
        """Read ``blocks_count`` blocks through vhost. Returns bytes."""
        length = blocks_count * disk.block_size
        if client is not None:
            status, data = self._retry_on_disconnect(
                client,
                lambda: client.read(disk.byte_offset(index), length, timeout=timeout),
            )
        else:
            with self.open_vhost(disk) as opened:
                status, data = opened.read(disk.byte_offset(index), length, timeout=timeout)
        assert status == VIRTIO_BLK_S_OK, 'vhost read block {} status {}'.format(
            index, virtio_blk_status_name(status)
        )
        return data

    def write_pattern(self, disk, start_index, block_count, block_size=None, timeout=10.0):
        """Write ``block_count`` distinct random blocks starting at ``start_index``."""
        block_size = disk.block_size if block_size is None else block_size
        payloads = {}
        with self.open_vhost(disk) as client:
            for offset in range(block_count):
                index = start_index + offset
                data = self.as_bytes(self.generate_random_data(block_size))
                self.write_blocks(disk, index, data, client=client, timeout=timeout)
                payloads[index] = data
        return payloads

    def assert_pattern(self, disk, payloads, block_size=None, timeout=10.0):
        """Read each block in ``payloads`` and compare byte-exactly."""
        block_size = disk.block_size if block_size is None else block_size
        with self.open_vhost(disk) as client:
            for index, expected in payloads.items():
                expected = self.as_bytes(expected)
                got = self.read_blocks(disk, index, client=client, timeout=timeout)
                assert got[:block_size] == expected[:block_size], (
                    'data mismatch at block {}: expected {!r}... got {!r}...'.format(
                        index, expected[:32], got[:32]
                    )
                )

    def write_and_verify(self, disk, start_index, block_count, block_size=None, timeout=10.0):
        """Write distinct blocks and immediately read each one back."""
        payloads = self.write_pattern(
            disk, start_index, block_count, block_size, timeout=timeout
        )
        self.assert_pattern(disk, payloads, block_size, timeout=timeout)
        return payloads

    def try_write(self, disk, index, data, timeout=10.0):
        """Write without asserting success. Returns (ok, status, error)."""
        payload = self.as_bytes(data)
        try:
            with self.open_vhost(disk, socket_timeout=5.0) as client:
                status = client.write(disk.byte_offset(index), payload, timeout=timeout)
            return status == VIRTIO_BLK_S_OK, status, virtio_blk_status_name(status)
        except Exception as e:
            return False, None, str(e)

    def try_read(self, disk, index, blocks_count=1, timeout=10.0):
        """Read without asserting success. Returns (ok, data_or_error)."""
        length = blocks_count * disk.block_size
        try:
            with self.open_vhost(disk, socket_timeout=5.0) as client:
                status, data = client.read(disk.byte_offset(index), length, timeout=timeout)
            if status != VIRTIO_BLK_S_OK:
                return False, virtio_blk_status_name(status)
            return True, data
        except Exception as e:
            return False, str(e)

    def start_vhost_io(self, disk, range_blocks=256, start_index=0, verify=True):
        """Write sequentially on ``disk`` until stopped.

        When ``verify`` is true (the default), each successful write is
        followed by a read of the same block. Only a matching
        read-after-write increments ``writes``.
        """
        stop = threading.Event()
        writes = [0]
        errors = []

        def _run():
            # Keep issuing requests across reconnects so TOracle::Think can
            # accumulate failures after a host is stopped.
            while not stop.is_set():
                try:
                    with self.open_vhost(disk, socket_timeout=5.0) as client:
                        index = start_index
                        while not stop.is_set():
                            data = self.as_bytes(self.generate_random_data(disk.block_size))
                            try:
                                status = client.write(
                                    disk.byte_offset(index), data, timeout=5.0
                                )
                                if status != VIRTIO_BLK_S_OK:
                                    index = start_index + (
                                        (index - start_index + 1) % range_blocks
                                    )
                                    continue
                                if verify:
                                    read_status, got = client.read(
                                        disk.byte_offset(index), disk.block_size, timeout=5.0
                                    )
                                    if read_status != VIRTIO_BLK_S_OK or got != data:
                                        errors.append(
                                            'read-after-write mismatch at block {}'.format(index)
                                        )
                                        return
                                writes[0] += 1
                                index = start_index + (
                                    (index - start_index + 1) % range_blocks
                                )
                            except Exception as e:
                                errors.append(e)
                                break
                except Exception as e:
                    errors.append(e)
                    if stop.wait(0.2):
                        return

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        return VhostIo(stop=stop, thread=thread, writes=writes, errors=errors)

    @staticmethod
    def vhost_socket_identity(disk):
        """(inode, ctime) of the vhost socket, or None if the path is gone.

        Inode alone is not enough: Linux often recycles the same inode when
        a unix socket is unlinked and rebound on the same path.
        """
        try:
            stat = os.stat(disk.socket_path)
        except OSError:
            return None
        return (stat.st_ino, stat.st_ctime)

    def wait_vhost_endpoint_returned(self, disk, previous_identity, timeout_seconds=60):
        """Wait until the socket is a new bind, not the leftover from Stop(false).

        ``TEndpoint::Stop(deleteSocket=false)`` leaves the old file on disk,
        so ``os.path.exists`` is not a readiness signal. The new process
        unlinks and rebinds; that changes ctime even when the inode is reused.
        """

        def _returned():
            identity = self.vhost_socket_identity(disk)
            return identity is not None and identity != previous_identity

        self.wait_until(
            _returned,
            timeout_seconds=timeout_seconds,
            description='vhost endpoint returned at {}'.format(disk.socket_path),
        )

    def wait_io_ok(self, disk, index=None, timeout_seconds=20):
        """Wait until a one-block vhost read succeeds.

        Reads an already-written block (default 0) so a recovery probe
        does not allocate a new extent or overwrite the seed pattern.
        """
        if index is None:
            index = 0

        def _roundtrip():
            try:
                with self.open_vhost(disk, socket_timeout=3.0) as client:
                    read_status, got = client.read(
                        disk.byte_offset(index), disk.block_size, timeout=5.0
                    )
                    return read_status == VIRTIO_BLK_S_OK and got is not None
            except Exception:
                return False

        self.wait_until(_roundtrip, timeout_seconds=timeout_seconds, description='IO ok')

    def wait_io_down(self, disk, index=None, timeout_seconds=10):
        """Wait until a one-block vhost read stops succeeding."""
        if index is None:
            index = 0

        def _down():
            try:
                with self.open_vhost(disk, socket_timeout=2.0) as client:
                    read_status, got = client.read(
                        disk.byte_offset(index), disk.block_size, timeout=2.0
                    )
                    return read_status != VIRTIO_BLK_S_OK or got is None
            except Exception:
                return True

        self.wait_until(_down, timeout_seconds=timeout_seconds, description='IO down')

    def tablet_generation(self, tablet_id):
        """Generation from the standard tablet mon page, or None if it is down.

        ``RenderHtmlPage`` runs in the user actor, so a zombie tablet (user
        actor gone, TTablet still alive) cannot produce this field.
        """
        try:
            html = self.fetch_mon('/tablets?TabletID={}'.format(tablet_id))
        except Exception as e:
            logger.debug('tablet_generation(%s) fetch failed: %s', tablet_id, e)
            return None
        match = re.search(r'Tablet generation:\s*(\d+)', html)
        if match is None:
            return None
        return int(match.group(1))

    def wait_tablet_restarted(self, tablet_id, previous_generation, timeout_seconds=60):
        """Wait until the tablet reports a generation greater than ``previous_generation``."""

        def _restarted():
            generation = self.tablet_generation(tablet_id)
            assert generation is not None and generation > previous_generation, (
                'tablet {} generation {} is not greater than {}'.format(
                    tablet_id, generation, previous_generation
                )
            )
            return True

        self.wait_until(
            _restarted,
            timeout_seconds=timeout_seconds,
            description='tablet {} restarted past generation {}'.format(
                tablet_id, previous_generation
            ),
        )

    def dbg_hosts(self, tablet_id, dbg_index=None):
        """Host snapshots for one DBG, with node / PDisk ids from Connections."""
        if dbg_index is None:
            listing = self.fetch_partition_dbg_page(tablet_id, allow_missing=True)
            indexes = self.parse_dbg_indexes(listing)
            if not indexes:
                return []
            dbg_index = indexes[0]
        html = self.fetch_partition_dbg_page(tablet_id, dbg_index)
        hosts = parse_dbg_hosts(html)
        connections = parse_dbg_connections(html)
        return merge_hosts_with_connections(hosts, connections)

    def vchunk_page(self, tablet_id, vchunk_index=0):
        """HTML of the VChunk mon page (host roles + dirty-map dump)."""
        return self.fetch_mon(
            '/tablets/app?TabletID={}&page=vchunk&vchunk={}'.format(tablet_id, vchunk_index)
        )

    def vchunk_hosts(self, tablet_id, vchunk_index=0):
        """Host-role / watermark rows from the VChunk mon page."""
        return parse_vchunk_hosts(self.vchunk_page(tablet_id, vchunk_index))

    def vchunk_ddisk_states(self, tablet_id, vchunk_index=0):
        """Per-host dirty-map DDisk state from the VChunk page."""
        return parse_ddisk_states(self.vchunk_page(tablet_id, vchunk_index))

    def dbg_pbuffer_nodes(self, tablet_id):
        """Unique static nodes that hold this disk's PBuffers (write quorum).

        DDisk and PBuffer for the same host can sit on different nodes in
        a fail domain (9 nodes / 5 domains). Writes ack on PBuffers, so
        host-loss cases must stop these nodes, not the DDisk ones.
        """
        listing = self.fetch_partition_dbg_page(tablet_id)
        indexes = self.parse_dbg_indexes(listing)
        assert indexes, 'no DBG links on tablet {}'.format(tablet_id)
        pb_ids = self.collect_pbuffer_service_ids(tablet_id, indexes)
        nodes = sorted({self.pbuffer_node_id(pb) for pb in pb_ids})
        if nodes:
            return nodes
        fallback = []
        seen = set()
        for host in self.dbg_hosts(tablet_id, indexes[0]):
            node_id = host.pbuffer_node_id or host.node_id
            if node_id and node_id not in seen:
                seen.add(node_id)
                fallback.append(node_id)
        assert fallback, 'DBG has no parseable PBuffer node ids'
        return fallback

    def pick_dbg_storage_node(self, tablet_id, exclude=None):
        """Pick a DBG host that is a static storage node, preferring not node 1.

        Node 1 serves the suite's mon and dstool endpoint; stopping it
        makes later assertions in the same case harder.
        """
        exclude = set(exclude or ())
        hosts = [h for h in self.dbg_hosts(tablet_id) if h.node_id]
        assert hosts, 'DBG host table has no parseable node ids'
        preferred = [h for h in hosts if h.node_id not in exclude and h.node_id != 1]
        if preferred:
            return preferred[0]
        fallback = [h for h in hosts if h.node_id not in exclude]
        assert fallback, 'no remaining DBG host to fault'
        return fallback[0]

    def pick_primary_ddisk_host(self, tablet_id, exclude=None):
        """Pick a Primary DDisk host, preferring not node 1.

        Offline of a HandOff does not run PromoteHostIfNeeded, so copy
        cases must take down a primary.
        """
        exclude = set(exclude or ())
        roles = {h.index: h for h in self.vchunk_hosts(tablet_id)}
        hosts = [h for h in self.dbg_hosts(tablet_id) if h.node_id]
        primaries = []
        for host in hosts:
            if host.node_id in exclude:
                continue
            role = roles.get(host.index)
            if role is None:
                continue
            if role.ddisk_role.lower() not in ('primary', '0'):
                continue
            primaries.append(host)
        preferred = [h for h in primaries if h.node_id != 1]
        chosen = preferred or primaries
        assert chosen, 'no Primary DDisk host to fault; roles={}'.format(
            [(h.index, h.ddisk_role) for h in roles.values()]
        )
        return chosen[0]

    @staticmethod
    def is_primary_ddisk(host):
        """True if the VChunk host row is a Primary DDisk."""
        return host.ddisk_role.lower() in ('primary', '0')

    def promoted_vchunk_hosts(self, tablet_id, before_primaries, vchunk_index=0):
        """Hosts that became Primary DDisk after ``before_primaries`` was taken."""
        return [
            h
            for h in self.vchunk_hosts(tablet_id, vchunk_index)
            if h.index not in before_primaries and self.is_primary_ddisk(h)
        ]

    def assert_ddisk_on_isolated_pdisk(self, disk, tenant_pdisk_id=1):
        """DBG DDisk connections must sit on the dedicated SSD PDisk.

        BSC numbers the dynamic SSD as PDisk 1000 (not the local slot id 2).
        The tenant's mirror-3-dc groups stay on static PDisk 1.
        """
        hosts = [h for h in self.dbg_hosts(disk.tablet_id) if h.pdisk_id is not None]
        assert hosts, 'DBG has no parseable DDisk PDisk ids'
        ids = {h.pdisk_id for h in hosts}
        detail = [(h.index, h.node_id, h.pdisk_id, h.ddisk_id) for h in hosts]
        assert len(ids) == 1, 'DDisks span multiple PDisks: {}'.format(detail)
        pdisk_id = next(iter(ids))
        assert pdisk_id != tenant_pdisk_id, (
            'DDisk pool shares tenant PDisk {}: {}'.format(pdisk_id, detail)
        )

    def wait_host_state(self, tablet_id, node_id, states, timeout_seconds=60):
        """Wait until the DBG host on ``node_id`` reports one of ``states``."""
        wanted = {s.lower() for s in states}

        def _reached():
            try:
                for host in self.dbg_hosts(tablet_id):
                    if host.node_id == node_id and host.state.lower() in wanted:
                        return True
                    # ToString may print the enumerator number.
                    if host.node_id == node_id and host.state in states:
                        return True
            except Exception:
                return False
            return False

        self.wait_until(
            _reached,
            timeout_seconds=timeout_seconds,
            description='host {} in {}'.format(node_id, states),
        )

    def wait_host_offline(self, disk, node_id, timeout_seconds=20):
        """Wait until the DBG host on ``node_id`` reports Offline.

        Stopping a node is not enough: TOracle::Think demotes a host from
        consecutive request failures, and OnDDiskDisconnected is a no-op. With
        no IO in flight nothing fails, so the host stays Online forever.
        """
        # Write past the usual seed (blocks 0–512) so the pattern stays intact.
        # The last region of a 4 GiB disk is a poor choice: first-touch there
        # is slow and does not produce the error streak Think needs.
        # make_disk maps block 48 for this: already-written, not in the seed.
        io = self.start_vhost_io(disk, start_index=48, range_blocks=1, verify=False)
        try:
            self.wait_host_state(
                disk.tablet_id, node_id, ('Offline', '2'), timeout_seconds=timeout_seconds
            )
        finally:
            io.stop_and_join()

    def wait_watermark(self, tablet_id, predicate, timeout_seconds=30, vchunk_index=0):
        """Wait until ``predicate(hosts)`` is true on the VChunk page."""

        def _reached():
            return predicate(self.vchunk_hosts(tablet_id, vchunk_index))

        self.wait_until(
            _reached,
            timeout_seconds=timeout_seconds,
            description='vchunk watermark condition',
        )
