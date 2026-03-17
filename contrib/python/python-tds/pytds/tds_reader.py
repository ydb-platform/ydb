"""
This module implements TdsReader class
"""
from __future__ import annotations

import struct
import typing
from typing import Tuple, Any

from pytds import tds_base
from pytds.collate import Collation, ucs2_codec
from pytds.tds_base import (
    readall,
    readall_fast,
    _header,
    _int_le,
    _uint_be,
    _uint_le,
    _uint8_le,
    _int8_le,
    _byte,
    _smallint_le,
    _usmallint_le,
)


if typing.TYPE_CHECKING:
    from pytds.tds_session import _TdsSession


class ResponseMetadata:
    """
    This class represents response metadata extracted from first response packet.
    This includes response type and session ID
    """

    def __init__(self):
        self.type = 0
        self.spid = 0


class _TdsReader:
    """TDS stream reader

    Provides stream-like interface for TDS packeted stream.
    Also provides convinience methods to decode primitive data like
    different kinds of integers etc.
    """

    def __init__(
        self,
        tds_session: _TdsSession,
        transport: tds_base.TransportProtocol,
        bufsize: int = 4096,
    ):
        self._buf = bytearray(b"\x00" * bufsize)
        self._bufview = memoryview(self._buf)
        self._pos = len(self._buf)  # position in the buffer
        self._have = 0  # number of bytes read from packet
        self._size = 0  # size of current packet
        self._transport = transport
        self._session = tds_session
        self._type: int | None = None
        # value of status field from packet header
        # 0 - means not last packet
        # 1 - means last packet
        self._status = 1
        self._spid = 0

    @property
    def session(self):
        return self._session

    def set_block_size(self, size: int) -> None:
        self._buf = bytearray(b"\x00" * size)
        self._bufview = memoryview(self._buf)

    def get_block_size(self) -> int:
        return len(self._buf)

    @property
    def packet_type(self) -> int | None:
        """Type of current packet

        Possible values are TDS_QUERY, TDS_LOGIN, etc.
        """
        return self._type

    def stream_finished(self) -> bool:
        """
        Verifies whether the current response packet stream has finished reading.
        If the function returns True, it indicates that you should invoke the begin_response
        method to initiate the reading of the next stream.
        :return:
        """
        if self._pos >= self._size:
            return self._status == 1
        else:
            return False

    def read_fast(self, size: int) -> Tuple[bytes, int]:
        """Faster version of read

        Instead of returning sliced buffer it returns reference to internal
        buffer and the offset to this buffer.

        :param size: Number of bytes to read
        :returns: Tuple of bytes buffer, and offset in this buffer
        """
        # Current response stream finished
        if self._pos >= self._size:
            if self._status == 1:
                return b"", 0
            self._read_packet()
        offset = self._pos
        to_read = min(size, self._size - self._pos)
        self._pos += to_read
        return self._buf, offset

    def recv(self, size: int) -> bytes:
        if self._pos >= self._size:
            # Current response stream finished
            if self._status == 1:
                return b""
            self._read_packet()
        offset = self._pos
        to_read = min(size, self._size - self._pos)
        self._pos += to_read
        return self._buf[offset : offset + to_read]

    def unpack(self, struc: struct.Struct) -> Tuple[Any, ...]:
        """Unpacks given structure from stream

        :param struc: A struct.Struct instance
        :returns: Result of unpacking
        """
        buf, offset = readall_fast(self, struc.size)
        return struc.unpack_from(buf, offset)

    def get_byte(self) -> int:
        """Reads one byte from stream"""
        return self.unpack(_byte)[0]

    def get_smallint(self) -> int:
        """Reads 16bit signed integer from the stream"""
        return self.unpack(_smallint_le)[0]

    def get_usmallint(self) -> int:
        """Reads 16bit unsigned integer from the stream"""
        return self.unpack(_usmallint_le)[0]

    def get_int(self) -> int:
        """Reads 32bit signed integer from the stream"""
        return self.unpack(_int_le)[0]

    def get_uint(self) -> int:
        """Reads 32bit unsigned integer from the stream"""
        return self.unpack(_uint_le)[0]

    def get_uint_be(self) -> int:
        """Reads 32bit unsigned big-endian integer from the stream"""
        return self.unpack(_uint_be)[0]

    def get_uint8(self) -> int:
        """Reads 64bit unsigned integer from the stream"""
        return self.unpack(_uint8_le)[0]

    def get_int8(self) -> int:
        """Reads 64bit signed integer from the stream"""
        return self.unpack(_int8_le)[0]

    def read_ucs2(self, num_chars: int) -> str:
        """Reads num_chars UCS2 string from the stream"""
        buf = readall(self, num_chars * 2)
        return ucs2_codec.decode(buf)[0]

    def read_str(self, size: int, codec) -> str:
        """Reads byte string from the stream and decodes it

        :param size: Size of string in bytes
        :param codec: Instance of codec to decode string
        :returns: Unicode string
        """
        return codec.decode(readall(self, size))[0]

    def get_collation(self) -> Collation:
        """Reads :class:`Collation` object from stream"""
        buf = readall(self, Collation.wire_size)
        return Collation.unpack(buf)

    def begin_response(self) -> ResponseMetadata:
        """
        This method should be called first before reading anything.
        It will read first response packet and return its metadata, after that
        read methods can be called to read contents of the response packet stream until it ends.
        """
        if self._status != 1 or self._pos < self._size:
            raise RuntimeError(
                "begin_response was called before previous response was fully consumed"
            )
        self._read_packet()
        res = ResponseMetadata()
        res.type = self._type
        res.spid = self._spid
        return res

    def _read_packet(self) -> None:
        """Reads next TDS packet from the underlying transport
        Can only be called when transport's read pointer is at the beginning
        of the packet.
        """
        pos = 0
        while pos < _header.size:
            received = self._transport.recv_into(
                self._bufview[pos:], _header.size - pos
            )
            if received == 0:
                raise tds_base.ClosedConnectionError()
            pos += received
        self._pos = _header.size
        self._type, self._status, self._size, self._spid, _ = _header.unpack_from(
            self._bufview, 0
        )
        self._have = pos
        while pos < self._size:
            received = self._transport.recv_into(self._bufview[pos:], self._size - pos)
            if received == 0:
                raise tds_base.ClosedConnectionError()
            pos += received
            self._have += received

    def read_whole_packet(self) -> bytes:
        """Reads single packet and returns bytes payload of the packet

        Can only be called when transport's read pointer is at the beginning
        of the packet.
        """
        # self._read_packet()
        return readall(self, self._size - _header.size)
