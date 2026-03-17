"""
Implements support for BLF (Binary Logging Format) which is a proprietary
CAN log format from Vector Informatik GmbH (Germany).

No official specification of the binary logging format is available.
This implementation is based on Toby Lorenz' C++ library "Vector BLF" which is
licensed under GPLv3. https://bitbucket.org/tobylorenz/vector_blf.

The file starts with a header. The rest is one or more "log containers"
which consists of a header and some zlib compressed data, usually up to 128 kB
of uncompressed data each. This data contains the actual CAN messages and other
objects types.
"""

import datetime
import logging
import struct
import time
import zlib
from collections.abc import Generator, Iterator
from decimal import Decimal
from typing import Any, BinaryIO, Optional, Union, cast

from ..message import Message
from ..typechecking import StringPathLike
from ..util import channel2int, dlc2len, len2dlc
from .generic import BinaryIOMessageReader, BinaryIOMessageWriter

TSystemTime = tuple[int, int, int, int, int, int, int, int]


class BLFParseError(Exception):
    """BLF file could not be parsed correctly."""


LOG = logging.getLogger(__name__)

# signature ("LOGG"), header size,
# application ID, application major, application minor, application build,
# bin log major, bin log minor, bin log build, bin log patch,
# file size, uncompressed size, count of objects, count of objects read,
# time start (SYSTEMTIME), time stop (SYSTEMTIME)
FILE_HEADER_STRUCT = struct.Struct("<4sLBBBBBBBBQQLL8H8H")

# Pad file header to this size
FILE_HEADER_SIZE = 144

# signature ("LOBJ"), header size, header version, object size, object type
OBJ_HEADER_BASE_STRUCT = struct.Struct("<4sHHLL")

# flags, client index, object version, timestamp
OBJ_HEADER_V1_STRUCT = struct.Struct("<LHHQ")

# flags, timestamp status, object version, timestamp, (original timestamp)
OBJ_HEADER_V2_STRUCT = struct.Struct("<LBxHQ8x")

# compression method, size uncompressed
LOG_CONTAINER_STRUCT = struct.Struct("<H6xL4x")

# channel, flags, dlc, arbitration id, data
CAN_MSG_STRUCT = struct.Struct("<HBBL8s")

# channel, flags, dlc, arbitration id, frame length, bit count, FD flags,
# valid data bytes, data
CAN_FD_MSG_STRUCT = struct.Struct("<HBBLLBBB5x64s")

# channel, dlc, valid payload length of data, tx count, arbitration id,
# frame length, flags, bit rate used in arbitration phase,
# bit rate used in data phase, time offset of brs field,
# time offset of crc delimiter field, bit count, direction,
# offset if extDataOffset is used, crc
CAN_FD_MSG_64_STRUCT = struct.Struct("<BBBBLLLLLLLHBBL")

# channel, length, flags, ecc, position, dlc, frame length, id, flags ext, data
CAN_ERROR_EXT_STRUCT = struct.Struct("<HHLBBBxLLH2x8s")

# commented event type, foreground color, background color, relocatable,
# group name length, marker name length, description length
GLOBAL_MARKER_STRUCT = struct.Struct("<LLL3xBLLL12x")


CAN_MESSAGE = 1
LOG_CONTAINER = 10
CAN_ERROR_EXT = 73
CAN_MESSAGE2 = 86
GLOBAL_MARKER = 96
CAN_FD_MESSAGE = 100
CAN_FD_MESSAGE_64 = 101

NO_COMPRESSION = 0
ZLIB_DEFLATE = 2

CAN_MSG_EXT = 0x80000000
REMOTE_FLAG = 0x80
EDL = 0x1
BRS = 0x2
ESI = 0x4
DIR = 0x1

TIME_TEN_MICS = 0x00000001
TIME_ONE_NANS = 0x00000002

TIME_TEN_MICS_FACTOR = Decimal("1e-5")
TIME_ONE_NANS_FACTOR = Decimal("1e-9")


def timestamp_to_systemtime(timestamp: Optional[float]) -> TSystemTime:
    if timestamp is None or timestamp < 631152000:
        # Probably not a Unix timestamp
        return 0, 0, 0, 0, 0, 0, 0, 0
    t = datetime.datetime.fromtimestamp(round(timestamp, 3), tz=datetime.timezone.utc)
    return (
        t.year,
        t.month,
        t.isoweekday() % 7,
        t.day,
        t.hour,
        t.minute,
        t.second,
        t.microsecond // 1000,
    )


def systemtime_to_timestamp(systemtime: TSystemTime) -> float:
    try:
        t = datetime.datetime(
            systemtime[0],
            systemtime[1],
            systemtime[3],
            systemtime[4],
            systemtime[5],
            systemtime[6],
            systemtime[7] * 1000,
            tzinfo=datetime.timezone.utc,
        )
        return t.timestamp()
    except ValueError:
        return 0


class BLFReader(BinaryIOMessageReader):
    """
    Iterator of CAN messages from a Binary Logging File.

    Only CAN messages and error frames are supported. Other object types are
    silently ignored.
    """

    def __init__(
        self,
        file: Union[StringPathLike, BinaryIO],
        **kwargs: Any,
    ) -> None:
        """
        :param file: a path-like object or as file-like object to read from
                     If this is a file-like object, is has to opened in binary
                     read mode, not text read mode.
        """
        super().__init__(file, mode="rb")
        data = self.file.read(FILE_HEADER_STRUCT.size)
        header = FILE_HEADER_STRUCT.unpack(data)
        if header[0] != b"LOGG":
            raise BLFParseError("Unexpected file format")
        self.file_size = header[10]
        self.uncompressed_size = header[11]
        self.object_count = header[12]
        self.start_timestamp = systemtime_to_timestamp(
            cast("TSystemTime", header[14:22])
        )
        self.stop_timestamp = systemtime_to_timestamp(
            cast("TSystemTime", header[22:30])
        )
        # Read rest of header
        self.file.read(header[1] - FILE_HEADER_STRUCT.size)
        self._tail = b""
        self._pos = 0

    def __iter__(self) -> Generator[Message, None, None]:
        while True:
            data = self.file.read(OBJ_HEADER_BASE_STRUCT.size)
            if not data:
                # EOF
                break

            signature, _, _, obj_size, obj_type = OBJ_HEADER_BASE_STRUCT.unpack(data)
            if signature != b"LOBJ":
                raise BLFParseError()
            obj_data = self.file.read(obj_size - OBJ_HEADER_BASE_STRUCT.size)
            # Read padding bytes
            self.file.read(obj_size % 4)

            if obj_type == LOG_CONTAINER:
                method, _ = LOG_CONTAINER_STRUCT.unpack_from(obj_data)
                container_data = obj_data[LOG_CONTAINER_STRUCT.size :]
                if method == NO_COMPRESSION:
                    data = container_data
                elif method == ZLIB_DEFLATE:
                    zobj = zlib.decompressobj()
                    data = zobj.decompress(container_data)
                else:
                    # Unknown compression method
                    LOG.warning("Unknown compression method (%d)", method)
                    continue
                yield from self._parse_container(data)
        self.stop()

    def _parse_container(self, data: bytes) -> Iterator[Message]:
        if self._tail:
            data = b"".join((self._tail, data))
        try:
            yield from self._parse_data(data)
        except struct.error:
            # There was not enough data in the container to unpack a struct
            pass
        # Save the remaining data that could not be processed
        self._tail = data[self._pos :]

    def _parse_data(self, data: bytes) -> Iterator[Message]:
        """Optimized inner loop by making local copies of global variables
        and class members and hardcoding some values."""
        unpack_obj_header_base = OBJ_HEADER_BASE_STRUCT.unpack_from
        obj_header_base_size = OBJ_HEADER_BASE_STRUCT.size
        unpack_obj_header_v1 = OBJ_HEADER_V1_STRUCT.unpack_from
        obj_header_v1_size = OBJ_HEADER_V1_STRUCT.size
        unpack_obj_header_v2 = OBJ_HEADER_V2_STRUCT.unpack_from
        obj_header_v2_size = OBJ_HEADER_V2_STRUCT.size
        unpack_can_msg = CAN_MSG_STRUCT.unpack_from
        unpack_can_fd_msg = CAN_FD_MSG_STRUCT.unpack_from
        unpack_can_fd_64_msg = CAN_FD_MSG_64_STRUCT.unpack_from
        can_fd_64_msg_size = CAN_FD_MSG_64_STRUCT.size
        unpack_can_error_ext = CAN_ERROR_EXT_STRUCT.unpack_from

        start_timestamp = self.start_timestamp
        max_pos = len(data)
        pos = 0

        # Loop until a struct unpack raises an exception
        while True:
            self._pos = pos
            # Find next object after padding (depends on object type)
            try:
                pos = data.index(b"LOBJ", pos, pos + 8)
            except ValueError:
                if pos + 8 > max_pos:
                    # Not enough data in container
                    return
                raise BLFParseError("Could not find next object") from None
            header = unpack_obj_header_base(data, pos)
            # print(header)
            signature, header_size, header_version, obj_size, obj_type = header
            if signature != b"LOBJ":
                raise BLFParseError()

            # Calculate position of next object
            next_pos = pos + obj_size
            if next_pos > max_pos:
                # This object continues in the next container
                return
            pos += obj_header_base_size

            # Read rest of header
            if header_version == 1:
                flags, _, _, timestamp = unpack_obj_header_v1(data, pos)
                pos += obj_header_v1_size
            elif header_version == 2:
                flags, _, _, timestamp = unpack_obj_header_v2(data, pos)
                pos += obj_header_v2_size
            else:
                LOG.warning("Unknown object header version (%d)", header_version)
                pos = next_pos
                continue

            # Calculate absolute timestamp in seconds
            factor = TIME_TEN_MICS_FACTOR if flags == 1 else TIME_ONE_NANS_FACTOR
            timestamp = float(Decimal(timestamp) * factor) + start_timestamp

            if obj_type in (CAN_MESSAGE, CAN_MESSAGE2):
                channel, flags, dlc, can_id, can_data = unpack_can_msg(data, pos)
                yield Message(
                    timestamp=timestamp,
                    arbitration_id=can_id & 0x1FFFFFFF,
                    is_extended_id=bool(can_id & CAN_MSG_EXT),
                    is_remote_frame=bool(flags & REMOTE_FLAG),
                    is_rx=not bool(flags & DIR),
                    dlc=dlc,
                    data=can_data[:dlc],
                    channel=channel - 1,
                )
            elif obj_type == CAN_ERROR_EXT:
                members = unpack_can_error_ext(data, pos)
                channel = members[0]
                dlc = members[5]
                can_id = members[7]
                can_data = members[9]
                yield Message(
                    timestamp=timestamp,
                    is_error_frame=True,
                    is_extended_id=bool(can_id & CAN_MSG_EXT),
                    arbitration_id=can_id & 0x1FFFFFFF,
                    dlc=dlc,
                    data=can_data[:dlc],
                    channel=channel - 1,
                )
            elif obj_type == CAN_FD_MESSAGE:
                members = unpack_can_fd_msg(data, pos)
                (
                    channel,
                    flags,
                    dlc,
                    can_id,
                    _,
                    _,
                    fd_flags,
                    valid_bytes,
                    can_data,
                ) = members
                yield Message(
                    timestamp=timestamp,
                    arbitration_id=can_id & 0x1FFFFFFF,
                    is_extended_id=bool(can_id & CAN_MSG_EXT),
                    is_remote_frame=bool(flags & REMOTE_FLAG),
                    is_fd=bool(fd_flags & 0x1),
                    is_rx=not bool(flags & DIR),
                    bitrate_switch=bool(fd_flags & 0x2),
                    error_state_indicator=bool(fd_flags & 0x4),
                    dlc=dlc2len(dlc),
                    data=can_data[:valid_bytes],
                    channel=channel - 1,
                )
            elif obj_type == CAN_FD_MESSAGE_64:
                (
                    channel,
                    dlc,
                    valid_bytes,
                    _,
                    can_id,
                    _,
                    fd_flags,
                    _,
                    _,
                    _,
                    _,
                    _,
                    direction,
                    ext_data_offset,
                    _,
                ) = unpack_can_fd_64_msg(data, pos)

                # :issue:`1905`: `valid_bytes` can be higher than the actually available data.
                # Add zero-byte padding to mimic behavior of CANoe and binlog.dll.
                data_field_length = min(
                    valid_bytes,
                    (ext_data_offset or obj_size) - header_size - can_fd_64_msg_size,
                )
                msg_data_offset = pos + can_fd_64_msg_size
                msg_data = data[msg_data_offset : msg_data_offset + data_field_length]
                msg_data = msg_data.ljust(valid_bytes, b"\x00")

                yield Message(
                    timestamp=timestamp,
                    arbitration_id=can_id & 0x1FFFFFFF,
                    is_extended_id=bool(can_id & CAN_MSG_EXT),
                    is_remote_frame=bool(fd_flags & 0x0010),
                    is_fd=bool(fd_flags & 0x1000),
                    is_rx=not direction,
                    bitrate_switch=bool(fd_flags & 0x2000),
                    error_state_indicator=bool(fd_flags & 0x4000),
                    dlc=dlc2len(dlc),
                    data=msg_data,
                    channel=channel - 1,
                )

            pos = next_pos


class BLFWriter(BinaryIOMessageWriter):
    """
    Logs CAN data to a Binary Logging File compatible with Vector's tools.
    """

    #: Max log container size of uncompressed data
    max_container_size = 128 * 1024

    #: Application identifier for the log writer
    application_id = 5

    def __init__(
        self,
        file: Union[StringPathLike, BinaryIO],
        append: bool = False,
        channel: int = 1,
        compression_level: int = -1,
        **kwargs: Any,
    ) -> None:
        """
        :param file: a path-like object or as file-like object to write to
                     If this is a file-like object, is has to opened in mode "wb+".
        :param channel:
            Default channel to log as if not specified by the interface.
        :param append:
            Append messages to an existing log file.
        :param compression_level:
            An integer from 0 to 9 or -1 controlling the level of compression.
            1 (Z_BEST_SPEED) is fastest and produces the least compression.
            9 (Z_BEST_COMPRESSION) is slowest and produces the most.
            0 means that data will be stored without processing.
            The default value is -1 (Z_DEFAULT_COMPRESSION).
            Z_DEFAULT_COMPRESSION represents a default compromise between
            speed and compression (currently equivalent to level 6).
        """
        try:
            super().__init__(file, mode="rb+" if append else "wb")
        except FileNotFoundError:
            # Trying to append to a non-existing file, create a new one
            append = False
            super().__init__(file, mode="wb")
        assert self.file is not None
        self.channel = channel
        self.compression_level = compression_level
        self._buffer: list[bytes] = []
        self._buffer_size = 0
        # If max container size is located in kwargs, then update the instance
        if kwargs.get("max_container_size", False):
            self.max_container_size = kwargs["max_container_size"]
        if append:
            # Parse file header
            data = self.file.read(FILE_HEADER_STRUCT.size)
            header = FILE_HEADER_STRUCT.unpack(data)
            if header[0] != b"LOGG":
                raise BLFParseError("Unexpected file format")
            self.uncompressed_size = header[11]
            self.object_count = header[12]
            self.start_timestamp: Optional[float] = systemtime_to_timestamp(
                cast("TSystemTime", header[14:22])
            )
            self.stop_timestamp: Optional[float] = systemtime_to_timestamp(
                cast("TSystemTime", header[22:30])
            )
            # Jump to the end of the file
            self.file.seek(0, 2)
        else:
            self.object_count = 0
            self.uncompressed_size = FILE_HEADER_SIZE
            self.start_timestamp = None
            self.stop_timestamp = None
            # Write a default header which will be updated when stopped
            self._write_header(FILE_HEADER_SIZE)

    def _write_header(self, filesize: int) -> None:
        header = [b"LOGG", FILE_HEADER_SIZE, self.application_id, 0, 0, 0, 2, 6, 8, 1]
        # The meaning of "count of objects read" is unknown
        header.extend([filesize, self.uncompressed_size, self.object_count, 0])
        header.extend(timestamp_to_systemtime(self.start_timestamp))
        header.extend(timestamp_to_systemtime(self.stop_timestamp))
        self.file.write(FILE_HEADER_STRUCT.pack(*header))
        # Pad to header size
        self.file.write(b"\x00" * (FILE_HEADER_SIZE - FILE_HEADER_STRUCT.size))

    def on_message_received(self, msg: Message) -> None:
        channel = channel2int(msg.channel)
        if channel is None:
            channel = self.channel
        else:
            # Many interfaces start channel numbering at 0 which is invalid
            channel += 1

        arb_id = msg.arbitration_id
        if msg.is_extended_id:
            arb_id |= CAN_MSG_EXT
        flags = REMOTE_FLAG if msg.is_remote_frame else 0
        if not msg.is_rx:
            flags |= DIR
        can_data = bytes(msg.data)

        if msg.is_error_frame:
            data = CAN_ERROR_EXT_STRUCT.pack(
                channel,
                0,  # length
                0,  # flags
                0,  # ecc
                0,  # position
                len2dlc(msg.dlc),
                0,  # frame length
                arb_id,
                0,  # ext flags
                can_data,
            )
            self._add_object(CAN_ERROR_EXT, data, msg.timestamp)
        elif msg.is_fd:
            fd_flags = EDL
            if msg.bitrate_switch:
                fd_flags |= BRS
            if msg.error_state_indicator:
                fd_flags |= ESI
            data = CAN_FD_MSG_STRUCT.pack(
                channel,
                flags,
                len2dlc(msg.dlc),
                arb_id,
                0,
                0,
                fd_flags,
                len(can_data),
                can_data,
            )
            self._add_object(CAN_FD_MESSAGE, data, msg.timestamp)
        else:
            data = CAN_MSG_STRUCT.pack(channel, flags, msg.dlc, arb_id, can_data)
            self._add_object(CAN_MESSAGE, data, msg.timestamp)

    def log_event(self, text: str, timestamp: Optional[float] = None) -> None:
        """Add an arbitrary message to the log file as a global marker.

        :param str text:
            The group name of the marker.
        :param float timestamp:
            Absolute timestamp in Unix timestamp format. If not given, the
            marker will be placed along the last message.
        """
        try:
            # Only works on Windows
            encoded = text.encode("mbcs")
        except LookupError:
            encoded = text.encode("ascii")
        comment = b"Added by python-can"
        marker = b"python-can"
        data = GLOBAL_MARKER_STRUCT.pack(
            0, 0xFFFFFF, 0xFF3300, 0, len(encoded), len(marker), len(comment)
        )
        self._add_object(GLOBAL_MARKER, data + encoded + marker + comment, timestamp)

    def _add_object(
        self, obj_type: int, data: bytes, timestamp: Optional[float] = None
    ) -> None:
        if timestamp is None:
            timestamp = self.stop_timestamp or time.time()
        if self.start_timestamp is None:
            # Save start timestamp using the same precision as the BLF format
            # Truncating to milliseconds to avoid rounding errors when calculating
            # the timestamp difference
            self.start_timestamp = int(timestamp * 1000) / 1000
        self.stop_timestamp = timestamp
        timestamp = int((timestamp - self.start_timestamp) * 1e9)
        header_size = OBJ_HEADER_BASE_STRUCT.size + OBJ_HEADER_V1_STRUCT.size
        obj_size = header_size + len(data)
        base_header = OBJ_HEADER_BASE_STRUCT.pack(
            b"LOBJ", header_size, 1, obj_size, obj_type
        )
        obj_header = OBJ_HEADER_V1_STRUCT.pack(TIME_ONE_NANS, 0, 0, max(timestamp, 0))

        self._buffer.append(base_header)
        self._buffer.append(obj_header)
        self._buffer.append(data)
        padding_size = len(data) % 4
        if padding_size:
            self._buffer.append(b"\x00" * padding_size)

        self._buffer_size += obj_size + padding_size
        self.object_count += 1
        if self._buffer_size >= self.max_container_size:
            self._flush()

    def _flush(self) -> None:
        """Compresses and writes data in the buffer to file."""
        if self.file.closed:
            return
        buffer = b"".join(self._buffer)
        if not buffer:
            # Nothing to write
            return
        uncompressed_data = memoryview(buffer)[: self.max_container_size]
        # Save data that comes after max size to next container
        tail = buffer[self.max_container_size :]
        self._buffer = [tail]
        self._buffer_size = len(tail)
        if not self.compression_level:
            data: "Union[bytes, memoryview[int]]" = uncompressed_data  # noqa: UP037
            method = NO_COMPRESSION
        else:
            data = zlib.compress(uncompressed_data, self.compression_level)
            method = ZLIB_DEFLATE
        obj_size = OBJ_HEADER_BASE_STRUCT.size + LOG_CONTAINER_STRUCT.size + len(data)
        base_header = OBJ_HEADER_BASE_STRUCT.pack(
            b"LOBJ", OBJ_HEADER_BASE_STRUCT.size, 1, obj_size, LOG_CONTAINER
        )
        container_header = LOG_CONTAINER_STRUCT.pack(method, len(uncompressed_data))
        self.file.write(base_header)
        self.file.write(container_header)
        self.file.write(data)
        # Write padding bytes
        self.file.write(b"\x00" * (obj_size % 4))
        self.uncompressed_size += OBJ_HEADER_BASE_STRUCT.size
        self.uncompressed_size += LOG_CONTAINER_STRUCT.size
        self.uncompressed_size += len(uncompressed_data)

    def file_size(self) -> int:
        """Return an estimate of the current file size in bytes."""
        return self.file.tell() + self._buffer_size

    def stop(self) -> None:
        """Stops logging and closes the file."""
        self._flush()
        if self.file.seekable():
            filesize = self.file.tell()
            # Write header in the beginning of the file
            self.file.seek(0)
            self._write_header(filesize)
        else:
            LOG.error("Could not write BLF header since file is not seekable")
        super().stop()
