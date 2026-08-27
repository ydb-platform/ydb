# -*- coding: utf-8 -*-
"""Per-case fixture that binds the suite cluster and cleans up after itself."""

import logging
import threading
from dataclasses import dataclass

import pytest

from ydb.tests.functional.nbs.lib.common import DEFAULT_DISK_BLOCKS_COUNT, NbsTestBase
from ydb.tests.functional.nbs.lib.helpers import execute_dstool_grpc
from ydb.tests.functional.nbs.lib.fixtures.faults import FaultScope
from ydb.tests.functional.nbs.lib.fixtures.geometry import DEFAULT_BLOCK_SIZE
from ydb.tests.functional.nbs.lib.fixtures.mon import (
    merge_hosts_with_connections,
    parse_dbg_connections,
    parse_dbg_hosts,
    parse_vchunk_hosts,
)
from ydb.tests.functional.nbs.lib.vhost_user_blk_client import (
    VIRTIO_BLK_S_OK,
    VhostUserBlkClient,
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
        nbs_cluster.recover()
        nbs_cluster.assert_healthy()
        yield
        self.faults.undo_all()
        nbs_cluster.wait_healthy()
        self.drop_created_disks()

    def register_disk(self, disk_id):
        """Remember ``disk_id`` so teardown deletes it."""
        self._created_disks.append(disk_id)

    def make_disk(self, blocks_count=DEFAULT_DISK_BLOCKS_COUNT, block_size=DEFAULT_BLOCK_SIZE):
        """Create a disk, wait for its vhost socket, and track it for cleanup."""
        disk_id = self.generate_disk_id()
        tablet_id = self.create_disk(disk_id, blocks_count, block_size=block_size)
        self.register_disk(disk_id)
        return self.bind_vhost(disk_id, tablet_id, blocks_count, block_size)

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
        """Best-effort delete so DDisk capacity stays available for later cases."""
        while self._created_disks:
            disk_id = self._created_disks.pop()
            try:
                execute_dstool_grpc(
                    self.cluster,
                    'token',
                    ['nbs', 'partition', 'delete', '--disk-id', disk_id],
                    check_exit_code=False,
                    timeout=60,
                )
            except Exception as e:
                logger.warning('delete_disk(%s) during teardown failed: %s', disk_id, e)

    def open_vhost(self, disk, socket_timeout=30.0):
        """Open a vhost-user-blk client for ``disk``."""
        return VhostUserBlkClient(disk.socket_path, socket_timeout=socket_timeout)

    def write_blocks(self, disk, index, data, client=None, timeout=10.0):
        """Write one block through vhost. Asserts ``VIRTIO_BLK_S_OK``."""
        payload = self.as_bytes(data)
        if client is not None:
            status = client.write(disk.byte_offset(index), payload, timeout=timeout)
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
            status, data = client.read(disk.byte_offset(index), length, timeout=timeout)
        else:
            with self.open_vhost(disk) as opened:
                status, data = opened.read(disk.byte_offset(index), length, timeout=timeout)
        assert status == VIRTIO_BLK_S_OK, 'vhost read block {} status {}'.format(
            index, virtio_blk_status_name(status)
        )
        return data

    def write_pattern(self, disk, start_index, block_count, block_size=None):
        """Write ``block_count`` distinct random blocks starting at ``start_index``."""
        block_size = disk.block_size if block_size is None else block_size
        payloads = {}
        with self.open_vhost(disk) as client:
            for offset in range(block_count):
                index = start_index + offset
                data = self.as_bytes(self.generate_random_data(block_size))
                self.write_blocks(disk, index, data, client=client)
                payloads[index] = data
        return payloads

    def assert_pattern(self, disk, payloads, block_size=None):
        """Read each block in ``payloads`` and compare byte-exactly."""
        block_size = disk.block_size if block_size is None else block_size
        with self.open_vhost(disk) as client:
            for index, expected in payloads.items():
                expected = self.as_bytes(expected)
                got = self.read_blocks(disk, index, client=client)
                assert got[:block_size] == expected[:block_size], (
                    'data mismatch at block {}: expected {!r}... got {!r}...'.format(
                        index, expected[:32], got[:32]
                    )
                )

    def write_and_verify(self, disk, start_index, block_count, block_size=None):
        """Write distinct blocks and immediately read each one back."""
        payloads = self.write_pattern(disk, start_index, block_count, block_size)
        self.assert_pattern(disk, payloads, block_size)
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
            try:
                with self.open_vhost(disk) as client:
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
                                    break
                            writes[0] += 1
                            index = start_index + (
                                (index - start_index + 1) % range_blocks
                            )
                        except Exception as e:
                            errors.append(e)
                            break
            except Exception as e:
                errors.append(e)

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        return VhostIo(stop=stop, thread=thread, writes=writes, errors=errors)

    def wait_io_ok(self, disk, index=None, timeout_seconds=60):
        """Wait until a one-block vhost write+read round trip succeeds.

        Defaults to the last block so a recovery probe does not overwrite
        the seed pattern at the start of the disk.
        """
        if index is None:
            index = max(0, disk.blocks_count - 1)

        def _roundtrip():
            data = self.as_bytes(self.generate_random_data(disk.block_size))
            try:
                with self.open_vhost(disk, socket_timeout=3.0) as client:
                    status = client.write(disk.byte_offset(index), data, timeout=5.0)
                    if status != VIRTIO_BLK_S_OK:
                        return False
                    read_status, got = client.read(
                        disk.byte_offset(index), disk.block_size, timeout=5.0
                    )
                    return read_status == VIRTIO_BLK_S_OK and got == data
            except Exception:
                return False

        self.wait_until(_roundtrip, timeout_seconds=timeout_seconds, description='IO ok')

    def dbg_hosts(self, tablet_id, dbg_index=None):
        """Host snapshots for one DBG, with node / PDisk ids from Connections."""
        if dbg_index is None:
            listing = self.fetch_partition_dbg_page(tablet_id)
            indexes = self.parse_dbg_indexes(listing)
            assert indexes, 'no DBG links on tablet {}'.format(tablet_id)
            dbg_index = indexes[0]
        html = self.fetch_partition_dbg_page(tablet_id, dbg_index)
        hosts = parse_dbg_hosts(html)
        connections = parse_dbg_connections(html)
        return merge_hosts_with_connections(hosts, connections)

    def vchunk_hosts(self, tablet_id, vchunk_index=0):
        """Host-role / watermark rows from the VChunk mon page."""
        html = self.fetch_mon(
            '/tablets/app?TabletID={}&page=vchunk&vchunk={}'.format(tablet_id, vchunk_index)
        )
        return parse_vchunk_hosts(html)

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

    def wait_host_state(self, tablet_id, node_id, states, timeout_seconds=60):
        """Wait until the DBG host on ``node_id`` reports one of ``states``."""
        wanted = {s.lower() for s in states}

        def _reached():
            for host in self.dbg_hosts(tablet_id):
                if host.node_id == node_id and host.state.lower() in wanted:
                    return True
                # ToString may print the enumerator number.
                if host.node_id == node_id and host.state in states:
                    return True
            return False

        self.wait_until(
            _reached,
            timeout_seconds=timeout_seconds,
            description='host {} in {}'.format(node_id, states),
        )

    def wait_watermark(self, tablet_id, predicate, timeout_seconds=120, vchunk_index=0):
        """Wait until ``predicate(hosts)`` is true on the VChunk page."""

        def _reached():
            return predicate(self.vchunk_hosts(tablet_id, vchunk_index))

        self.wait_until(
            _reached,
            timeout_seconds=timeout_seconds,
            description='vchunk watermark condition',
        )
