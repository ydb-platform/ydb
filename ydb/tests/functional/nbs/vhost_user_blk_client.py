# -*- coding: utf-8 -*-
"""
Minimal vhost-user-blk client used by functional tests to exercise the
vhost endpoint that the NBS partition actor exposes at
/tmp/<disk_id>.sock.

The client implements just enough of the vhost-user master role and of
the virtio-blk device protocol to perform a handful of synchronous read
and write requests against the running vhost server. Notably, it lets us
send writes at arbitrary byte offsets - including byte offsets that are
not aligned to the device block size - to validate that the unaligned
read-modify-write path in the device handler works end to end.
"""

import array
import ctypes
import logging
import mmap
import os
import select
import socket
import struct
import time

logger = logging.getLogger(__name__)

# vhost-user message IDs (see ydb/core/nbs/cloud/contrib/vhost/vhost_spec.h).
VHOST_USER_GET_FEATURES = 1
VHOST_USER_SET_FEATURES = 2
VHOST_USER_SET_OWNER = 3
VHOST_USER_SET_MEM_TABLE = 5
VHOST_USER_SET_VRING_NUM = 8
VHOST_USER_SET_VRING_ADDR = 9
VHOST_USER_SET_VRING_BASE = 10
VHOST_USER_GET_VRING_BASE = 11
VHOST_USER_SET_VRING_KICK = 12
VHOST_USER_SET_VRING_CALL = 13
VHOST_USER_GET_PROTOCOL_FEATURES = 15
VHOST_USER_SET_PROTOCOL_FEATURES = 16
VHOST_USER_GET_QUEUE_NUM = 17
VHOST_USER_SET_VRING_ENABLE = 18

# Message flags.
VHOST_USER_VERSION = 0x1
VHOST_USER_REPLY_FLAG = (1 << 2)
VHOST_USER_NEED_REPLY_FLAG = (1 << 3)

# Feature bits.
VIRTIO_F_VERSION_1 = 32
VHOST_USER_F_PROTOCOL_FEATURES = 30

# vhost-user protocol features.
VHOST_USER_PROTOCOL_F_MQ = 0
VHOST_USER_PROTOCOL_F_REPLY_ACK = 3

# virtio-blk request types.
VIRTIO_BLK_T_IN = 0
VIRTIO_BLK_T_OUT = 1

# virtio-blk request status codes.
VIRTIO_BLK_S_OK = 0
VIRTIO_BLK_S_IOERR = 1
VIRTIO_BLK_S_UNSUPP = 2

# virtq descriptor flags.
VIRTQ_DESC_F_NEXT = 1
VIRTQ_DESC_F_WRITE = 2

# virtio-blk uses 512 byte logical sectors regardless of the underlying
# block size.
VIRTIO_BLK_SECTOR_SIZE = 512

# Header sent before every vhost-user message: req(u32) flags(u32) size(u32).
_MSG_HDR = struct.Struct("<III")

# vhost_user_vring_addr payload.
_VRING_ADDR = struct.Struct("<IIQQQQ")

# vhost_user_vring_state payload.
_VRING_STATE = struct.Struct("<II")

# vhost_user_mem_region (single region, sent inside SET_MEM_TABLE).
_MEM_REGION = struct.Struct("<QQQQ")

# virtio_blk_outhdr: type(u32) ioprio(u32) sector(u64).
_VIRTIO_BLK_OUTHDR = struct.Struct("<IIQ")

# virtq_desc: addr(u64) len(u32) flags(u16) next(u16). 16 bytes total.
_VIRTQ_DESC = struct.Struct("<QIHH")
_VIRTQ_DESC_SIZE = _VIRTQ_DESC.size

# Layout inside the shared memory file.
# Page 0: descriptor table.
# Page 1: avail ring.
# Page 2: used ring.
# >= page 4: data area (out header, payload, status byte).
_PAGE_SIZE = 4096
_DESC_OFFSET = 0
_AVAIL_OFFSET = _PAGE_SIZE
_USED_OFFSET = 2 * _PAGE_SIZE
_DATA_OFFSET = 4 * _PAGE_SIZE


def _wait_for_socket(socket_path, timeout_seconds=30.0):
    """Polls for the vhost socket to be created."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if os.path.exists(socket_path):
            return
        time.sleep(0.1)
    raise TimeoutError(
        "vhost socket {} did not appear within {} seconds".format(
            socket_path, timeout_seconds))


class VhostUserBlkError(Exception):
    """Raised on vhost-user protocol or virtio-blk failures."""


class VhostUserBlkClient(object):
    """
    Minimal synchronous vhost-user-blk master.

    Usage:
        with VhostUserBlkClient(socket_path) as client:
            client.write(offset, data)
            status, data = client.read(offset, length)
    """

    def __init__(self, socket_path, queue_size=64, mem_size=4 * 1024 * 1024):
        if (queue_size & (queue_size - 1)) != 0:
            raise ValueError(
                "queue_size must be a power of two, got {}".format(queue_size))

        self._socket_path = socket_path
        self._queue_size = queue_size
        self._mem_size = mem_size

        self._sock = None
        self._memfd = None
        self._mem = None
        self._kickfd = -1
        self._callfd = -1
        # Next virtq_avail.idx value we will publish to the slave.
        self._avail_idx = 0
        # Next position in used ring we have not yet processed.
        self._used_idx = 0
        # Becomes True after VHOST_USER_PROTOCOL_F_REPLY_ACK has been
        # successfully negotiated.
        self._reply_ack_negotiated = False

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def connect(self):
        _wait_for_socket(self._socket_path)

        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.connect(self._socket_path)

        # 1. Take ownership of the vhost device.
        self._send(VHOST_USER_SET_OWNER)

        # 2. Feature negotiation. The contrib server requires
        #    VHOST_USER_F_PROTOCOL_FEATURES to be honored; we also keep
        #    VIRTIO_F_VERSION_1 since that is what real guests would do.
        slave_features = self._send_u64_request(VHOST_USER_GET_FEATURES)
        logger.debug("vhost slave features: 0x%x", slave_features)

        wanted_features = (
            (1 << VIRTIO_F_VERSION_1)
            | (1 << VHOST_USER_F_PROTOCOL_FEATURES)
        )
        self._send(VHOST_USER_SET_FEATURES,
                   struct.pack("<Q", slave_features & wanted_features))

        slave_proto = self._send_u64_request(VHOST_USER_GET_PROTOCOL_FEATURES)
        logger.debug("vhost slave protocol features: 0x%x", slave_proto)

        wanted_proto = (
            (1 << VHOST_USER_PROTOCOL_F_MQ)
            | (1 << VHOST_USER_PROTOCOL_F_REPLY_ACK)
        )
        negotiated_proto = slave_proto & wanted_proto
        self._send(VHOST_USER_SET_PROTOCOL_FEATURES,
                   struct.pack("<Q", negotiated_proto))
        self._reply_ack_negotiated = bool(
            negotiated_proto & (1 << VHOST_USER_PROTOCOL_F_REPLY_ACK))

        # 3. Set up a single shared memory region. We use memfd_create so
        #    that the FD can be passed through SCM_RIGHTS and mmap'd by
        #    the slave with no on-disk backing.
        self._memfd = os.memfd_create("vhost-user-blk-test")
        os.ftruncate(self._memfd, self._mem_size)
        # Map our copy of the shared memory so we can author descriptors
        # and inspect responses.
        self._mem = mmap.mmap(self._memfd, self._mem_size,
                              prot=mmap.PROT_READ | mmap.PROT_WRITE,
                              flags=mmap.MAP_SHARED)

        # In libvhost, descriptor addresses (the .addr field inside the
        # virtq_desc table) are resolved through gpa_range_to_ptr against
        # region.guest_addr, while the desc/used/avail ring addresses
        # passed in SET_VRING_ADDR are resolved through uva_to_ptr
        # against region.user_addr. We therefore have to:
        #   * pick guest_addr=0, so that descriptor addresses are simple
        #     offsets into the shared memory region;
        #   * pass user_addr equal to our mmap's virtual address, so the
        #     slave can translate ring addresses we send to it.
        user_addr = ctypes.addressof(ctypes.c_char.from_buffer(self._mem))
        self._user_addr = user_addr
        mem_payload = struct.pack("<II", 1, 0) + _MEM_REGION.pack(
            0,             # guest_addr
            self._mem_size,
            user_addr,     # user_addr (used to resolve ring addresses)
            0,             # mmap_offset
        )
        self._send(VHOST_USER_SET_MEM_TABLE, mem_payload, fds=[self._memfd])

        # 4. Configure virtqueue 0.
        self._send(VHOST_USER_SET_VRING_NUM,
                   _VRING_STATE.pack(0, self._queue_size))
        self._send(VHOST_USER_SET_VRING_BASE,
                   _VRING_STATE.pack(0, 0))
        self._send(VHOST_USER_SET_VRING_ADDR, _VRING_ADDR.pack(
            0,                              # index
            0,                              # flags
            user_addr + _DESC_OFFSET,       # desc_addr (UVA)
            user_addr + _USED_OFFSET,       # used_addr (UVA)
            user_addr + _AVAIL_OFFSET,      # avail_addr (UVA)
            0,                              # used_gpa_base (logging only)
        ))

        # 5. Pass eventfds for kick (master -> slave) and call (slave ->
        #    master). Both are level-triggered eventfds; the low 8 bits
        #    of the u64 payload carry the vring index.
        #
        # The slave processes SET_VRING_KICK asynchronously - the actual
        # vring is started on a backend request-queue thread. We therefore
        # request a REPLY_ACK so we know the kick handler is attached
        # before we go on. Same for SET_VRING_ENABLE which transitions
        # the vring to the enabled state via another bottom half.
        self._kickfd = os.eventfd(0, os.EFD_CLOEXEC)
        self._callfd = os.eventfd(0, os.EFD_CLOEXEC)
        self._send_with_ack(
            VHOST_USER_SET_VRING_KICK,
            struct.pack("<Q", 0),
            fds=[self._kickfd])
        self._send_with_ack(
            VHOST_USER_SET_VRING_CALL,
            struct.pack("<Q", 0),
            fds=[self._callfd])

        # 6. Enable the queue. The contrib slave toggles
        #    shadow_vq.enabled on each SET_VRING_ENABLE, so by the time
        #    REPLY_ACK comes back the queue is guaranteed to be live.
        self._send_with_ack(VHOST_USER_SET_VRING_ENABLE,
                            _VRING_STATE.pack(0, 1))

    def close(self):
        if self._mem is not None:
            try:
                self._mem.close()
            except (BufferError, ValueError):
                pass
            self._mem = None
        for fd_attr in ("_memfd", "_kickfd", "_callfd"):
            fd = getattr(self, fd_attr, -1)
            if fd is not None and fd >= 0:
                try:
                    os.close(fd)
                except OSError:
                    pass
                setattr(self, fd_attr, -1)
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    # ----- public IO -----

    def write(self, byte_offset, data, timeout=5.0):
        """
        Issues a single virtio-blk write request. The byte_offset and the
        length of data do not have to be aligned to the device block size.
        Returns the virtio-blk status byte (0 == VIRTIO_BLK_S_OK).
        """
        if byte_offset % VIRTIO_BLK_SECTOR_SIZE != 0:
            raise ValueError(
                "virtio-blk addresses are in 512-byte sectors, offset {}"
                " is not a multiple of {}".format(
                    byte_offset, VIRTIO_BLK_SECTOR_SIZE))
        return self._do_request(
            VIRTIO_BLK_T_OUT, byte_offset, bytes(data), len(data), timeout)

    def read(self, byte_offset, length, timeout=5.0):
        """
        Issues a single virtio-blk read request and returns
        (status_byte, payload_bytes).
        """
        if byte_offset % VIRTIO_BLK_SECTOR_SIZE != 0:
            raise ValueError(
                "virtio-blk addresses are in 512-byte sectors, offset {}"
                " is not a multiple of {}".format(
                    byte_offset, VIRTIO_BLK_SECTOR_SIZE))
        status, payload = self._do_request_read(
            byte_offset, length, timeout)
        return status, payload

    # ----- internal helpers -----

    def _do_request(self, request_type, byte_offset, payload, payload_len,
                    timeout):
        sector = byte_offset // VIRTIO_BLK_SECTOR_SIZE
        outhdr_off = _DATA_OFFSET
        data_off = outhdr_off + _VIRTIO_BLK_OUTHDR.size
        status_off = data_off + payload_len

        # Write virtio-blk out header.
        self._mem.seek(outhdr_off)
        self._mem.write(_VIRTIO_BLK_OUTHDR.pack(request_type, 0, sector))

        # Write payload (only for VIRTIO_BLK_T_OUT).
        if request_type == VIRTIO_BLK_T_OUT and payload_len:
            self._mem.seek(data_off)
            self._mem.write(payload)

        # Reset the status byte to a sentinel value (anything != 0) so we
        # can detect that the device actually overwrote it.
        self._mem.seek(status_off)
        self._mem.write(b"\xff")

        self._build_descriptor_chain(
            request_type, outhdr_off, data_off, payload_len, status_off)
        self._submit_and_wait(timeout)

        self._mem.seek(status_off)
        return self._mem.read(1)[0]

    def _do_request_read(self, byte_offset, length, timeout):
        sector = byte_offset // VIRTIO_BLK_SECTOR_SIZE
        outhdr_off = _DATA_OFFSET
        data_off = outhdr_off + _VIRTIO_BLK_OUTHDR.size
        status_off = data_off + length

        self._mem.seek(outhdr_off)
        self._mem.write(_VIRTIO_BLK_OUTHDR.pack(VIRTIO_BLK_T_IN, 0, sector))

        # Pre-fill data area with a sentinel value so we can tell the
        # difference between "device wrote zeros" and "device did not
        # touch this region".
        self._mem.seek(data_off)
        self._mem.write(b"\xaa" * length)
        self._mem.seek(status_off)
        self._mem.write(b"\xff")

        self._build_descriptor_chain(
            VIRTIO_BLK_T_IN, outhdr_off, data_off, length, status_off)
        self._submit_and_wait(timeout)

        self._mem.seek(status_off)
        status = self._mem.read(1)[0]
        self._mem.seek(data_off)
        data = self._mem.read(length)
        return status, bytes(data)

    def _build_descriptor_chain(self, request_type, outhdr_off, data_off,
                                data_len, status_off):
        # We always use three descriptors per request:
        #   desc[0] - out header (RO)
        #   desc[1] - payload    (RO for write, WO for read)
        #   desc[2] - status     (WO)
        data_flags = VIRTQ_DESC_F_NEXT
        if request_type == VIRTIO_BLK_T_IN:
            data_flags |= VIRTQ_DESC_F_WRITE

        descs = [
            (outhdr_off, _VIRTIO_BLK_OUTHDR.size, VIRTQ_DESC_F_NEXT, 1),
            (data_off, data_len, data_flags, 2),
            (status_off, 1, VIRTQ_DESC_F_WRITE, 0),
        ]
        for i, (addr, length, flags, nxt) in enumerate(descs):
            self._mem.seek(_DESC_OFFSET + i * _VIRTQ_DESC_SIZE)
            self._mem.write(_VIRTQ_DESC.pack(addr, length, flags, nxt))

    def _submit_and_wait(self, timeout):
        # Publish descriptor 0 into the avail ring.
        slot = self._avail_idx % self._queue_size
        # avail layout: flags(u16) idx(u16) ring[queue_size](u16) ...
        ring_slot_off = _AVAIL_OFFSET + 4 + slot * 2
        self._mem.seek(ring_slot_off)
        self._mem.write(struct.pack("<H", 0))   # head descriptor id

        self._avail_idx = (self._avail_idx + 1) & 0xFFFF
        self._mem.seek(_AVAIL_OFFSET + 2)
        self._mem.write(struct.pack("<H", self._avail_idx))

        # Kick the slave. Any non-zero write to the eventfd is sufficient.
        os.eventfd_write(self._kickfd, 1)

        # Wait for the slave to mark the request as used.
        deadline = time.monotonic() + timeout
        while True:
            self._mem.seek(_USED_OFFSET + 2)
            used_idx = struct.unpack("<H", self._mem.read(2))[0]
            if used_idx != self._used_idx:
                self._used_idx = used_idx
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise VhostUserBlkError(
                    "virtio-blk request did not complete within"
                    " {} seconds".format(timeout))
            # Block on the call eventfd. The slave writes to it whenever
            # it advances used_idx.
            self._wait_callfd(remaining)

    def _wait_callfd(self, timeout):
        rlist, _, _ = select.select([self._callfd], [], [], timeout)
        if rlist:
            try:
                os.eventfd_read(self._callfd)
            except (BlockingIOError, OSError):
                pass

    # ----- vhost-user low-level send/recv -----

    def _send(self, request, payload=b"", fds=(), need_reply=False):
        flags = VHOST_USER_VERSION
        if need_reply:
            flags |= VHOST_USER_NEED_REPLY_FLAG
        header = _MSG_HDR.pack(request, flags, len(payload))
        msg = header + payload
        if fds:
            anc = [(socket.SOL_SOCKET, socket.SCM_RIGHTS,
                    array.array("i", fds).tobytes())]
            self._sock.sendmsg([msg], anc)
        else:
            self._sock.sendall(msg)

    def _send_with_ack(self, request, payload=b"", fds=()):
        """
        Sends a request and, if REPLY_ACK was negotiated, waits for the
        u64 acknowledgement that the slave returns once the request is
        fully processed. A non-zero ack value indicates an error.
        """
        if not self._reply_ack_negotiated:
            self._send(request, payload, fds=fds)
            return

        self._send(request, payload, fds=fds, need_reply=True)
        hdr = self._recv_exact(_MSG_HDR.size)
        rsp_req, rsp_flags, rsp_size = _MSG_HDR.unpack(hdr)
        if rsp_req != request:
            raise VhostUserBlkError(
                "expected REPLY_ACK for request {}, got {}".format(
                    request, rsp_req))
        if not (rsp_flags & VHOST_USER_REPLY_FLAG):
            raise VhostUserBlkError(
                "REPLY_ACK for request {} missing reply flag (0x{:x})".format(
                    request, rsp_flags))
        if rsp_size != 8:
            raise VhostUserBlkError(
                "REPLY_ACK for request {} has unexpected payload size {}".format(
                    request, rsp_size))
        payload = self._recv_exact(rsp_size)
        ack = struct.unpack("<Q", payload)[0]
        if ack != 0:
            raise VhostUserBlkError(
                "REPLY_ACK for request {} returned error {}".format(
                    request, ack))

    def _send_u64_request(self, request):
        self._send(request)
        hdr = self._recv_exact(_MSG_HDR.size)
        rsp_req, rsp_flags, rsp_size = _MSG_HDR.unpack(hdr)
        if rsp_req != request:
            raise VhostUserBlkError(
                "expected reply for request {}, got {}".format(
                    request, rsp_req))
        if not (rsp_flags & VHOST_USER_REPLY_FLAG):
            raise VhostUserBlkError(
                "expected reply flag for request {}, flags=0x{:x}".format(
                    request, rsp_flags))
        if rsp_size != 8:
            raise VhostUserBlkError(
                "expected 8-byte u64 reply for request {}, got {} bytes".format(
                    request, rsp_size))
        payload = self._recv_exact(rsp_size)
        return struct.unpack("<Q", payload)[0]

    def _recv_exact(self, n):
        data = b""
        while len(data) < n:
            chunk = self._sock.recv(n - len(data))
            if not chunk:
                raise VhostUserBlkError(
                    "vhost-user connection closed while expecting {} bytes"
                    " (got {})".format(n, len(data)))
            data += chunk
        return data


def virtio_blk_status_name(status):
    return {
        VIRTIO_BLK_S_OK: "VIRTIO_BLK_S_OK",
        VIRTIO_BLK_S_IOERR: "VIRTIO_BLK_S_IOERR",
        VIRTIO_BLK_S_UNSUPP: "VIRTIO_BLK_S_UNSUPP",
    }.get(status, "status={}".format(status))
