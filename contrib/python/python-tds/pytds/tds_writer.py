"""
This module implements TdsWriter class
"""
import struct

from pytds import tds_base
from pytds.collate import Collation, ucs2_codec
from pytds.tds_base import (
    _uint_be,
    _byte,
    _smallint_le,
    _usmallint_le,
    _usmallint_be,
    _int_le,
    _uint_le,
    _int8_le,
    _uint8_le,
    _header,
)


class _TdsWriter:
    """TDS stream writer

    Handles splitting of incoming data into TDS packets according to TDS protocol.
    Provides convinience methods for writing primitive data types.
    """

    def __init__(
        self, transport: tds_base.TransportProtocol, bufsize: int, tds_session
    ):
        self._transport = transport
        self._tds = tds_session
        self._pos = 0
        self._buf = bytearray(bufsize)
        self._packet_no = 0
        self._type = 0

    @property
    def session(self):
        return self._tds

    @property
    def bufsize(self) -> int:
        """Size of the buffer"""
        return len(self._buf)

    @bufsize.setter
    def bufsize(self, bufsize: int) -> None:
        if len(self._buf) == bufsize:
            return

        if bufsize > len(self._buf):
            self._buf.extend(b"\0" * (bufsize - len(self._buf)))
        else:
            self._buf = self._buf[0:bufsize]

    def begin_packet(self, packet_type: int) -> None:
        """Starts new packet stream

        :param packet_type: Type of TDS stream, e.g. TDS_PRELOGIN, TDS_QUERY etc.
        """
        self._type = packet_type
        self._pos = 8

    def pack(self, struc: struct.Struct, *args) -> None:
        """Packs and writes structure into stream"""
        self.write(struc.pack(*args))

    def put_byte(self, value: int) -> None:
        """Writes single byte into stream"""
        self.pack(_byte, value)

    def put_smallint(self, value: int) -> None:
        """Writes 16-bit signed integer into the stream"""
        self.pack(_smallint_le, value)

    def put_usmallint(self, value: int) -> None:
        """Writes 16-bit unsigned integer into the stream"""
        self.pack(_usmallint_le, value)

    def put_usmallint_be(self, value: int) -> None:
        """Writes 16-bit unsigned big-endian integer into the stream"""
        self.pack(_usmallint_be, value)

    def put_int(self, value: int) -> None:
        """Writes 32-bit signed integer into the stream"""
        self.pack(_int_le, value)

    def put_uint(self, value: int) -> None:
        """Writes 32-bit unsigned integer into the stream"""
        self.pack(_uint_le, value)

    def put_uint_be(self, value: int) -> None:
        """Writes 32-bit unsigned big-endian integer into the stream"""
        self.pack(_uint_be, value)

    def put_int8(self, value: int) -> None:
        """Writes 64-bit signed integer into the stream"""
        self.pack(_int8_le, value)

    def put_uint8(self, value: int) -> None:
        """Writes 64-bit unsigned integer into the stream"""
        self.pack(_uint8_le, value)

    def put_collation(self, collation: Collation) -> None:
        """Writes :class:`Collation` structure into the stream"""
        self.write(collation.pack())

    def write(self, data: bytes) -> None:
        """Writes given bytes buffer into the stream

        Function returns only when entire buffer is written
        """
        data_off = 0
        while data_off < len(data):
            left = len(self._buf) - self._pos
            if left <= 0:
                self._write_packet(final=False)
            else:
                to_write = min(left, len(data) - data_off)
                self._buf[self._pos : self._pos + to_write] = data[
                    data_off : data_off + to_write
                ]
                self._pos += to_write
                data_off += to_write

    def write_b_varchar(self, s: str) -> None:
        self.put_byte(len(s))
        self.write_ucs2(s)

    def write_ucs2(self, s: str) -> None:
        """Write string encoding it in UCS2 into stream"""
        self.write_string(s, ucs2_codec)

    def write_string(self, s: str, codec) -> None:
        """Write string encoding it with codec into stream"""
        for i in range(0, len(s), self.bufsize):
            chunk = s[i : i + self.bufsize]
            buf, consumed = codec.encode(chunk)
            assert consumed == len(chunk)
            self.write(buf)

    def flush(self) -> None:
        """Closes current packet stream"""
        return self._write_packet(final=True)

    def _write_packet(self, final: bool) -> None:
        """Writes single TDS packet into underlying transport.

        Data for the packet is taken from internal buffer.

        :param final: True means this is the final packet in substream.
        """
        status = 1 if final else 0
        _header.pack_into(
            self._buf, 0, self._type, status, self._pos, 0, self._packet_no
        )
        self._packet_no = (self._packet_no + 1) % 256
        self._transport.sendall(self._buf[: self._pos])
        self._pos = 8
