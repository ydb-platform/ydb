"""
This file implements Session Multiplex Protocol used by MARS connections
Protocol documentation https://msdn.microsoft.com/en-us/library/cc219643.aspx
"""
from __future__ import annotations

import struct
import logging
import threading
import socket
import errno
from typing import Dict, Tuple

from . import tds_base

try:
    from bitarray import bitarray  # type: ignore # fix typing later
except ImportError:

    class BitArray(list):
        def __init__(self, size: int):
            super(BitArray, self).__init__()
            self[:] = [False] * size

        def setall(self, val: bool) -> None:
            for i in range(len(self)):
                self[i] = val

    bitarray = BitArray
from .tds_base import Error, skipall, TransportProtocol

logger = logging.getLogger(__name__)


SMP_HEADER = struct.Struct("<BBHLLL")
SMP_ID = 0x53


class _SmpSession(tds_base.TransportProtocol):
    def __init__(self, mgr: SmpManager, session_id: int):
        super().__init__()
        self.session_id = session_id
        self.seq_num_for_send = 0
        self.high_water_for_send = 4
        self._seq_num_for_recv = 0
        self.high_water_for_recv = 4
        self._last_high_water_for_recv = 4
        self._mgr = mgr
        self.recv_queue: list[bytes] = []
        self.send_queue: list[bytes] = []
        self._state: int | None = None
        self._curr_buf_pos = 0
        self._curr_buf = b""
        self._last_recv_seq_num = 0

    def __repr__(self):
        fmt = "<_SmpSession sid={} state={} recv_queue={} send_queue={} seq_num_for_send={}>"
        return fmt.format(
            self.session_id,
            SessionState.to_str(self._state),
            self.recv_queue,
            self.send_queue,
            self.seq_num_for_send,
        )

    def get_state(self) -> int | None:
        return self._state

    def close(self) -> None:
        self._mgr.close_smp_session(self)

    def sendall(self, data: bytes, flags: int = 0) -> None:
        self._mgr.send_packet(self, data)

    def _recv_internal(self, size: int) -> Tuple[int, int]:
        if not self._curr_buf[self._curr_buf_pos :]:
            self._curr_buf = self._mgr.recv_packet(self)
            self._curr_buf_pos = 0
            if not self._curr_buf:
                return 0, 0
        to_read = min(size, len(self._curr_buf) - self._curr_buf_pos)
        offset = self._curr_buf_pos
        self._curr_buf_pos += to_read
        return offset, to_read

    def recv_into(
        self, buffer: bytearray | memoryview, size: int = 0, flags: int = 0
    ) -> int:
        if size == 0:
            size = len(buffer)

        offset, to_read = self._recv_internal(size)
        buffer[:to_read] = self._curr_buf[offset : offset + to_read]
        return to_read

    def recv(self, size: int) -> bytes:
        offset, to_read = self._recv_internal(size)
        return self._curr_buf[offset : offset + to_read]

    def is_connected(self) -> bool:
        return self._state == SessionState.SESSION_ESTABLISHED

    def gettimeout(self) -> float | None:
        return self._mgr._transport.gettimeout()

    def settimeout(self, timeout: float | None) -> None:
        self._mgr._transport.settimeout(timeout)


class PacketTypes:
    SYN = 0x1
    ACK = 0x2
    FIN = 0x4
    DATA = 0x8

    # @staticmethod
    # def type_to_str(t):
    #    if t == PacketTypes.SYN:
    #        return 'SYN'
    #    elif t == PacketTypes.ACK:
    #        return 'ACK'
    #    elif t == PacketTypes.DATA:
    #        return 'DATA'
    #    elif t == PacketTypes.FIN:
    #        return 'FIN'


class SessionState:
    SESSION_ESTABLISHED = 1
    CLOSED = 2
    FIN_SENT = 3
    FIN_RECEIVED = 4

    @staticmethod
    def to_str(st: int) -> str:
        if st == SessionState.SESSION_ESTABLISHED:
            return "SESSION ESTABLISHED"
        elif st == SessionState.CLOSED:
            return "CLOSED"
        elif st == SessionState.FIN_SENT:
            return "FIN SENT"
        elif st == SessionState.FIN_RECEIVED:
            return "FIN RECEIVED"
        else:
            raise RuntimeError(f"invalid session state: {st}")


class SmpManager:
    def __init__(self, transport: TransportProtocol, max_sessions: int = 2**16):
        self._transport = transport
        self._sessions: Dict[int, _SmpSession] = {}
        self._used_ids_ba = bitarray(max_sessions)
        self._used_ids_ba.setall(False)
        self._lock = threading.RLock()
        self._hdr_buf = memoryview(bytearray(b"\x00" * SMP_HEADER.size))

    def __repr__(self):
        return "<SmpManager sessions={}>".format(self._sessions)

    def create_session(self) -> _SmpSession:
        try:
            session_id = self._used_ids_ba.index(False)
        except ValueError:
            raise Error(
                "Can't create more MARS sessions, close some sessions and try again"
            )
        session = _SmpSession(self, session_id)
        with self._lock:
            self._sessions[session_id] = session
            self._used_ids_ba[session_id] = True
            hdr = SMP_HEADER.pack(
                SMP_ID,
                PacketTypes.SYN,
                session_id,
                SMP_HEADER.size,
                0,
                session.high_water_for_recv,
            )
            self._transport.sendall(hdr)
            session._state = SessionState.SESSION_ESTABLISHED
        return session

    def close_all_sessions(self, keep):
        for sess in list(self._sessions.values()):
            if sess is not keep:
                self.close_smp_session(sess)

    def close_smp_session(self, session: _SmpSession) -> None:
        if session._state in (SessionState.CLOSED, SessionState.FIN_SENT):
            return
        elif session._state == SessionState.SESSION_ESTABLISHED:
            with self._lock:
                hdr = SMP_HEADER.pack(
                    SMP_ID,
                    PacketTypes.FIN,
                    session.session_id,
                    SMP_HEADER.size,
                    session.seq_num_for_send,
                    session.high_water_for_recv,
                )
                session._state = SessionState.FIN_SENT
                try:
                    self._transport.sendall(hdr)
                    self.recv_packet(session)
                except (socket.error, OSError) as ex:
                    if ex.errno in (errno.ECONNRESET, errno.EPIPE):
                        session._state = SessionState.CLOSED
                    else:
                        raise ex

    def send_queued_packets(self, session: _SmpSession) -> None:
        with self._lock:
            while (
                session.send_queue
                and session.seq_num_for_send < session.high_water_for_send
            ):
                data = session.send_queue.pop(0)
                self.send_packet(session, data)

    @staticmethod
    def _add_one_wrap(val: int) -> int:
        return 0 if val == 2**32 - 1 else val + 1

    def send_packet(self, session: _SmpSession, data: bytes) -> None:
        with self._lock:
            if (
                session._state == SessionState.CLOSED
                or session._state == SessionState.FIN_SENT
            ):
                raise Error("Stream closed")
            if session.seq_num_for_send < session.high_water_for_send:
                size = SMP_HEADER.size + len(data)
                seq_num = self._add_one_wrap(session.seq_num_for_send)
                hdr = SMP_HEADER.pack(
                    SMP_ID,
                    PacketTypes.DATA,
                    session.session_id,
                    size,
                    seq_num,
                    session.high_water_for_recv,
                )
                session._last_high_water_for_recv = session.high_water_for_recv
                self._transport.sendall(hdr + data)
                session.seq_num_for_send = self._add_one_wrap(session.seq_num_for_send)
            else:
                session.send_queue.append(data)
                self._read_smp_message()

    def recv_packet(self, session: _SmpSession) -> bytes:
        with self._lock:
            if session._state == SessionState.CLOSED:
                return b""
            while not session.recv_queue:
                self._read_smp_message()
                if session._state in (SessionState.CLOSED, SessionState.FIN_RECEIVED):
                    return b""
            session.high_water_for_recv = self._add_one_wrap(
                session.high_water_for_recv
            )
            if session.high_water_for_recv - session._last_high_water_for_recv >= 2:
                hdr = SMP_HEADER.pack(
                    SMP_ID,
                    PacketTypes.ACK,
                    session.session_id,
                    SMP_HEADER.size,
                    session.seq_num_for_send,
                    session.high_water_for_recv,
                )
                self._transport.sendall(hdr)
                session._last_high_water_for_recv = session.high_water_for_recv
            return session.recv_queue.pop(0)

    def _bad_stm(self, message: str) -> None:
        self.close()
        raise Error(message)

    def _read_smp_message(self) -> None:
        # caller should acquire lock before calling this function
        buf_pos = 0
        while buf_pos < SMP_HEADER.size:
            read = self._transport.recv_into(self._hdr_buf[buf_pos:])
            buf_pos += read
            if read == 0:
                self._bad_stm("Unexpected EOF while reading SMP header")
        smid, flags, sid, length, seq_num, wnd = SMP_HEADER.unpack(self._hdr_buf)
        if smid != SMP_ID:
            self._bad_stm("Invalid SMP packet signature")
        try:
            session = self._sessions[sid]
        except KeyError:
            self._bad_stm("Invalid SMP packet session id")
        if wnd < session.high_water_for_send:
            self._bad_stm("Invalid WNDW in packet from server")
        if seq_num > session.high_water_for_recv:
            self._bad_stm("Invalid SEQNUM in packet from server")
        if length < SMP_HEADER.size:
            self._bad_stm("Invalid LENGTH in packet from server")
        session._last_recv_seq_num = seq_num
        if flags == PacketTypes.DATA:
            if session._state == SessionState.SESSION_ESTABLISHED:
                if seq_num != self._add_one_wrap(session._seq_num_for_recv):
                    self._bad_stm("Invalid SEQNUM in DATA packet from server")
                session._seq_num_for_recv = seq_num
                remains = length - SMP_HEADER.size
                while remains:
                    data = self._transport.recv(remains)
                    session.recv_queue.append(data)
                    remains -= len(data)
                if wnd > session.high_water_for_send:
                    session.high_water_for_send = wnd
                    self.send_queued_packets(session)

            elif session._state == SessionState.FIN_SENT:
                skipall(self._transport, length - SMP_HEADER.size)
            else:
                self._bad_stm("Unexpected DATA packet from server")
        elif flags == PacketTypes.ACK:
            if session._state in (SessionState.FIN_RECEIVED, SessionState.CLOSED):
                self._bad_stm("Unexpected ACK packet from server")
            if seq_num != session._seq_num_for_recv:
                self._bad_stm("Invalid SEQNUM in ACK packet from server")
            session.high_water_for_send = wnd
            self.send_queued_packets(session)
        elif flags == PacketTypes.FIN:
            assert session._state in (
                SessionState.SESSION_ESTABLISHED,
                SessionState.FIN_SENT,
                SessionState.FIN_RECEIVED,
            )
            if session._state == SessionState.SESSION_ESTABLISHED:
                session._state = SessionState.FIN_RECEIVED
            elif session._state == SessionState.FIN_SENT:
                session._state = SessionState.CLOSED
                del self._sessions[session.session_id]
                self._used_ids_ba[session.session_id] = False
            elif session._state == SessionState.FIN_RECEIVED:
                self._bad_stm("Unexpected FIN packet from server")
        elif flags == PacketTypes.SYN:
            self._bad_stm("Unexpected SYN packet from server")
        else:
            self._bad_stm("Unexpected FLAGS in packet from server")

    def close(self) -> None:
        self._transport.close()

    def transport_closed(self) -> None:
        for session in self._sessions.values():
            session._state = SessionState.CLOSED
