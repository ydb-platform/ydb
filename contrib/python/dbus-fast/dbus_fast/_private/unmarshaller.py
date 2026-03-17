# cython: freethreading_compatible = True

from __future__ import annotations

import array
import errno
import io
import socket
import sys
from collections.abc import Callable, Iterable
from struct import Struct
from typing import TYPE_CHECKING, Any

from ..constants import MESSAGE_FLAG_MAP, MESSAGE_TYPE_MAP, MessageFlag
from ..errors import InvalidMessageError
from ..message import Message
from ..signature import SignatureType, Variant, get_signature_tree
from .constants import BIG_ENDIAN, LITTLE_ENDIAN, PROTOCOL_VERSION

MESSAGE_FLAG_INTENUM = MessageFlag

MAX_UNIX_FDS = 16
MAX_UNIX_FDS_SIZE = array.array("i").itemsize
UNIX_FDS_CMSG_LENGTH = socket.CMSG_LEN(MAX_UNIX_FDS_SIZE * MAX_UNIX_FDS)

UNPACK_SYMBOL = {LITTLE_ENDIAN: "<", BIG_ENDIAN: ">"}

UINT32_CAST = "I"
UINT32_SIZE = 4
UINT32_DBUS_TYPE = "u"

INT16_CAST = "h"
INT16_SIZE = 2
INT16_DBUS_TYPE = "n"

UINT16_CAST = "H"
UINT16_SIZE = 2
UINT16_DBUS_TYPE = "q"

SYS_IS_LITTLE_ENDIAN = sys.byteorder == "little"
SYS_IS_BIG_ENDIAN = sys.byteorder == "big"

DBUS_TO_CTYPE = {
    "y": ("B", 1),  # byte
    INT16_DBUS_TYPE: (INT16_CAST, INT16_SIZE),  # int16
    UINT16_DBUS_TYPE: (UINT16_CAST, UINT16_SIZE),  # uint16
    "i": ("i", 4),  # int32
    UINT32_DBUS_TYPE: (UINT32_CAST, UINT32_SIZE),  # uint32
    "x": ("q", 8),  # int64
    "t": ("Q", 8),  # uint64
    "d": ("d", 8),  # double
    "h": (UINT32_CAST, UINT32_SIZE),  # uint32
}

UNPACK_HEADER_LITTLE_ENDIAN = Struct("<III").unpack_from
UNPACK_HEADER_BIG_ENDIAN = Struct(">III").unpack_from

UINT32_UNPACK_LITTLE_ENDIAN = Struct(f"<{UINT32_CAST}").unpack_from
UINT32_UNPACK_BIG_ENDIAN = Struct(f">{UINT32_CAST}").unpack_from

INT16_UNPACK_LITTLE_ENDIAN = Struct(f"<{INT16_CAST}").unpack_from
INT16_UNPACK_BIG_ENDIAN = Struct(f">{INT16_CAST}").unpack_from

UINT16_UNPACK_LITTLE_ENDIAN = Struct(f"<{UINT16_CAST}").unpack_from
UINT16_UNPACK_BIG_ENDIAN = Struct(f">{UINT16_CAST}").unpack_from

HEADER_SIGNATURE_SIZE = 16
HEADER_ARRAY_OF_STRUCT_SIGNATURE_POSITION = 12


# Most common signatures

SIGNATURE_TREE_EMPTY = get_signature_tree("")
SIGNATURE_TREE_B = get_signature_tree("b")
SIGNATURE_TREE_N = get_signature_tree("n")
SIGNATURE_TREE_S = get_signature_tree("s")
SIGNATURE_TREE_O = get_signature_tree("o")
SIGNATURE_TREE_U = get_signature_tree("u")
SIGNATURE_TREE_Y = get_signature_tree("y")

SIGNATURE_TREE_AY = get_signature_tree("ay")
SIGNATURE_TREE_AS = get_signature_tree("as")
SIGNATURE_TREE_AS_TYPES_0 = SIGNATURE_TREE_AS.root_type
SIGNATURE_TREE_A_SV = get_signature_tree("a{sv}")
SIGNATURE_TREE_A_SV_TYPES_0 = SIGNATURE_TREE_A_SV.root_type

SIGNATURE_TREE_AO = get_signature_tree("ao")
SIGNATURE_TREE_AO_TYPES_0 = SIGNATURE_TREE_AO.root_type

SIGNATURE_TREE_OAS = get_signature_tree("oas")
SIGNATURE_TREE_OAS_TYPES_1 = SIGNATURE_TREE_OAS.types[1]

SIGNATURE_TREE_AY_TYPES_0 = SIGNATURE_TREE_AY.root_type
SIGNATURE_TREE_A_QV = get_signature_tree("a{qv}")
SIGNATURE_TREE_A_QV_TYPES_0 = SIGNATURE_TREE_A_QV.root_type

SIGNATURE_TREE_SA_SV_AS = get_signature_tree("sa{sv}as")
SIGNATURE_TREE_SA_SV_AS_TYPES_1 = SIGNATURE_TREE_SA_SV_AS.types[1]
SIGNATURE_TREE_SA_SV_AS_TYPES_2 = SIGNATURE_TREE_SA_SV_AS.types[2]

SIGNATURE_TREE_OA_SA_SV = get_signature_tree("oa{sa{sv}}")
SIGNATURE_TREE_OA_SA_SV_TYPES_1 = SIGNATURE_TREE_OA_SA_SV.types[1]

SIGNATURE_TREE_A_OA_SA_SV = get_signature_tree("a{oa{sa{sv}}}")
SIGNATURE_TREE_A_OA_SA_SV_TYPES_0 = SIGNATURE_TREE_A_OA_SA_SV.root_type


TOKEN_B_AS_INT = ord("b")
TOKEN_U_AS_INT = ord("u")
TOKEN_Y_AS_INT = ord("y")
TOKEN_A_AS_INT = ord("a")
TOKEN_O_AS_INT = ord("o")
TOKEN_S_AS_INT = ord("s")
TOKEN_G_AS_INT = ord("g")
TOKEN_N_AS_INT = ord("n")
TOKEN_X_AS_INT = ord("x")
TOKEN_T_AS_INT = ord("t")
TOKEN_D_AS_INT = ord("d")
TOKEN_Q_AS_INT = ord("q")
TOKEN_V_AS_INT = ord("v")
TOKEN_LEFT_CURLY_AS_INT = ord("{")
TOKEN_LEFT_PAREN_AS_INT = ord("(")


VARIANT_BOOL_TRUE = Variant._factory(SIGNATURE_TREE_B, True)
VARIANT_BOOL_FALSE = Variant._factory(SIGNATURE_TREE_B, False)


ARRAY = array.array
SOL_SOCKET = socket.SOL_SOCKET
SCM_RIGHTS = socket.SCM_RIGHTS

EAGAIN = errno.EAGAIN
EWOULDBLOCK = errno.EWOULDBLOCK

HEADER_IDX_TO_ARG_NAME = [
    "",
    "path",
    "interface",
    "member",
    "error_name",
    "reply_serial",
    "destination",
    "sender",
    "signature",
    "unix_fds",
]
HEADER_PATH_IDX = HEADER_IDX_TO_ARG_NAME.index("path")
HEADER_INTERFACE_IDX = HEADER_IDX_TO_ARG_NAME.index("interface")
HEADER_MEMBER_IDX = HEADER_IDX_TO_ARG_NAME.index("member")
HEADER_ERROR_NAME_IDX = HEADER_IDX_TO_ARG_NAME.index("error_name")
HEADER_REPLY_SERIAL_IDX = HEADER_IDX_TO_ARG_NAME.index("reply_serial")
HEADER_DESTINATION_IDX = HEADER_IDX_TO_ARG_NAME.index("destination")
HEADER_SENDER_IDX = HEADER_IDX_TO_ARG_NAME.index("sender")
HEADER_SIGNATURE_IDX = HEADER_IDX_TO_ARG_NAME.index("signature")
HEADER_UNIX_FDS_IDX = HEADER_IDX_TO_ARG_NAME.index("unix_fds")

_EMPTY_HEADERS: list[Any | None] = [None] * len(HEADER_IDX_TO_ARG_NAME)

_SignatureType = SignatureType
_int = int

READER_TYPE = Callable[["Unmarshaller", SignatureType], Any]

MARSHALL_STREAM_END_ERROR = BlockingIOError

DEFAULT_BUFFER_SIZE = io.DEFAULT_BUFFER_SIZE


def unpack_parser_factory(unpack_from: Callable, size: int) -> READER_TYPE:
    """Build a parser that unpacks the bytes using the given unpack_from function."""

    def _unpack_from_parser(self: Unmarshaller, signature: SignatureType) -> Any:
        self._pos += size + (-self._pos & (size - 1))  # align
        return unpack_from(self._buf, self._pos - size)[0]

    return _unpack_from_parser


def build_simple_parsers(
    endian: int,
) -> dict[str, Callable[[Unmarshaller, SignatureType], Any]]:
    """Build a dict of parsers for simple types."""
    parsers: dict[str, READER_TYPE] = {}
    for dbus_type, ctype_size in DBUS_TO_CTYPE.items():
        ctype, size = ctype_size
        size = ctype_size[1]
        parsers[dbus_type] = unpack_parser_factory(
            Struct(f"{UNPACK_SYMBOL[endian]}{ctype}").unpack_from, size
        )
    return parsers


try:
    import cython
except ImportError:
    from ._cython_compat import FAKE_CYTHON as cython
int_ = int
bytearray_ = bytearray


def is_compiled() -> bool:
    return cython.compiled


def _ustr_uint32(buf: bytearray_, pos: int_, endian: int_) -> int_:
    if endian == LITTLE_ENDIAN:
        return (
            buf[pos] | (buf[pos + 1] << 8) | (buf[pos + 2] << 16) | (buf[pos + 3] << 24)
        )
    return buf[pos + 3] | (buf[pos + 2] << 8) | (buf[pos + 1] << 16) | (buf[pos] << 24)


def buffer_to_uint32(buf: bytearray, pos: int, endian: int) -> int:
    return _ustr_uint32(buf, pos, endian)


def _ustr_int16(buf: bytearray_, pos: int_, endian: int_) -> int_:
    # Caution: this function will only work with Cython
    # because it relies on casting the result to a signed int
    # and will return an unsigned int if not compiled.
    if endian == LITTLE_ENDIAN:
        return buf[pos] | (buf[pos + 1] << 8)  # pragma: no cover
    return buf[pos + 1] | (buf[pos] << 8)  # pragma: no cover


def buffer_to_int16(buf: bytearray | bytes, pos: int, endian: int) -> int:
    # Caution: this function will only work with Cython
    # because it relies on casting the result to a signed int
    # and will return an unsigned int if not compiled.
    return _ustr_int16(buf, pos, endian)


def _ustr_uint16(buf: bytearray_, pos: int_, endian: int_) -> int_:
    if endian == LITTLE_ENDIAN:
        return buf[pos] | (buf[pos + 1] << 8)
    return buf[pos + 1] | (buf[pos] << 8)


def buffer_to_uint16(buf: bytearray, pos: int, endian: int) -> int:
    return _ustr_uint16(buf, pos, endian)


# Alignment padding is handled with the following formula below
#
# For any align value, the correct padding formula is:
#
#    (align - (pos % align)) % align
#
# However, if align is a power of 2 (always the case here), the slow MOD
# operator can be replaced by a bitwise AND:
#
#    (align - (pos & (align - 1))) & (align - 1)
#
# Which can be simplified to:
#
#    (-pos) & (align - 1)
#
#
class Unmarshaller:
    """Unmarshall messages from a stream.

    When calling with sock and _negotiate_unix_fd False, the unmashaller must
    be called continuously for each new message as it will buffer the data
    until a complete message is available.
    """

    __slots__ = (
        "_body_len",
        "_buf",
        "_buf_len",
        "_buf_ustr",
        "_endian",
        "_flag",
        "_header_len",
        "_int16_unpack",
        "_message",
        "_message_type",
        "_msg_len",
        "_negotiate_unix_fd",
        "_pos",
        "_read_complete",
        "_readers",
        "_serial",
        "_sock",
        "_sock_with_fds_reader",
        "_sock_without_fds_reader",
        "_stream",
        "_stream_reader",
        "_uint16_unpack",
        "_uint32_unpack",
        "_unix_fds",
    )

    _stream_reader: Callable[[int], bytes]

    def __init__(
        self,
        stream: io.BufferedRWPair | None = None,
        sock: socket.socket | None = None,
        negotiate_unix_fd: bool = True,
    ) -> None:
        self._unix_fds: list[int] = []
        self._buf: bytearray = bytearray.__new__(bytearray)  # Actual buffer
        self._buf_ustr = self._buf  # Used to avoid type checks
        self._buf_len = 0
        self._stream = stream
        self._sock = sock
        self._message: Message | None = None
        self._readers: dict[str, READER_TYPE] = {}
        self._pos = 0
        self._body_len = 0
        self._serial = 0
        self._header_len = 0
        self._message_type = 0
        self._flag = 0
        self._msg_len = 0
        self._uint32_unpack: Callable[[bytearray, int], tuple[int]] | None = None
        self._int16_unpack: Callable[[bytearray, int], tuple[int]] | None = None
        self._uint16_unpack: Callable[[bytearray, int], tuple[int]] | None = None
        self._negotiate_unix_fd = negotiate_unix_fd
        self._read_complete = False
        if stream:
            if isinstance(stream, io.BufferedRWPair) and hasattr(stream, "reader"):
                self._stream_reader = stream.reader.read
            else:
                self._stream_reader = stream.read
        elif self._negotiate_unix_fd:
            if TYPE_CHECKING:
                assert self._sock is not None
            self._sock_with_fds_reader = self._sock.recvmsg
        else:
            if TYPE_CHECKING:
                assert self._sock is not None
            self._sock_without_fds_reader = self._sock.recv
        self._endian = 0

    def _next_message(self) -> None:
        """Reset the unmarshaller to its initial state.

        Call this before processing a new message.
        """
        if self._unix_fds:
            self._unix_fds = []
        to_clear = HEADER_SIGNATURE_SIZE + self._msg_len
        if self._buf_len == to_clear:
            self._buf = bytearray.__new__(bytearray)
            self._buf_len = 0
        else:
            del self._buf[:to_clear]
            self._buf_len -= to_clear
            self._buf_ustr = self._buf
        self._msg_len = 0  # used to check if we have ready the header
        self._read_complete = False  # used to check if we have ready the message
        # No need to reset the unpack functions, they are set in _read_header
        # every time a new message is processed.

    @property
    def message(self) -> Message | None:
        """Return the message that has been unmarshalled."""
        if self._read_complete:
            return self._message
        return None

    def _has_another_message_in_buffer(self) -> bool:
        """Check if there is another message in the buffer."""
        return self._buf_len > HEADER_SIGNATURE_SIZE + self._msg_len

    def _read_sock_with_fds(self, pos: _int, missing_bytes: _int) -> None:
        """reads from the socket, storing any fds sent and handling errors
        from the read itself.

        This function is greedy and will read as much data as possible
        from the underlying socket.
        """
        # This will raise BlockingIOError if there is no data to read
        # which we store in the MARSHALL_STREAM_END_ERROR object
        try:
            recv = self._sock_with_fds_reader(missing_bytes, UNIX_FDS_CMSG_LENGTH)
        except OSError as e:
            errno = e.errno
            if errno == EAGAIN or errno == EWOULDBLOCK:
                raise MARSHALL_STREAM_END_ERROR
            raise
        msg = recv[0]
        ancdata = recv[1]
        if ancdata:
            for level, type_, data in ancdata:
                if not (level == SOL_SOCKET and type_ == SCM_RIGHTS):
                    continue
                self._unix_fds.extend(
                    ARRAY("i", data[: len(data) - (len(data) % MAX_UNIX_FDS_SIZE)])
                )
        if not msg:
            raise EOFError
        self._buf += msg
        self._buf_len = len(self._buf)
        if self._buf_len < pos:
            raise MARSHALL_STREAM_END_ERROR

    def _read_sock_without_fds(self, pos: _int) -> None:
        """reads from the socket and handling errors from the read itself.

        This function is greedy and will read as much data as possible
        from the underlying socket.
        """
        # This will raise BlockingIOError if there is no data to read
        # which we store in the MARSHALL_STREAM_END_ERROR object
        while True:
            try:
                data = self._sock_without_fds_reader(DEFAULT_BUFFER_SIZE)
            except OSError as e:
                errno = e.errno
                if errno == EAGAIN or errno == EWOULDBLOCK:
                    raise MARSHALL_STREAM_END_ERROR
                raise
            if not data:
                raise EOFError
            self._buf += data
            self._buf_len = len(self._buf)
            if self._buf_len >= pos:
                return

    def _read_stream(self, pos: _int, missing_bytes: _int) -> None:
        """Read from the stream."""
        data = self._stream_reader(missing_bytes)
        if data is None:
            raise MARSHALL_STREAM_END_ERROR
        if not data:
            raise EOFError
        self._buf += data
        self._buf_len = len(self._buf)
        if self._buf_len < pos:
            raise MARSHALL_STREAM_END_ERROR

    def _read_to_pos(self, pos: _int) -> None:
        """
        Read from underlying socket into buffer.

        Raises BlockingIOError if there is not enough data to be read.

        :arg pos:
            The pos to read to. If not enough bytes are available in the
            buffer, read more from it.

        :returns:
            None
        """
        missing_bytes = pos - self._buf_len
        if missing_bytes <= 0:
            return
        if self._sock is None:
            self._read_stream(pos, missing_bytes)
        elif self._negotiate_unix_fd:
            self._read_sock_with_fds(pos, missing_bytes)
        else:
            self._read_sock_without_fds(pos)
        self._buf_ustr = self._buf

    def read_uint32_unpack(self, type_: _SignatureType) -> int:
        return self._read_uint32_unpack()

    def _read_uint32_unpack(self) -> int:
        self._pos += UINT32_SIZE + (-self._pos & (UINT32_SIZE - 1))  # align
        if cython.compiled:
            if self._buf_len < self._pos:
                raise IndexError("Not enough data to read uint32")
            return _ustr_uint32(self._buf_ustr, self._pos - UINT32_SIZE, self._endian)
        return self._uint32_unpack(self._buf, self._pos - UINT32_SIZE)[0]

    def read_uint16_unpack(self, type_: _SignatureType) -> int:
        return self._read_uint16_unpack()

    def _read_uint16_unpack(self) -> int:
        self._pos += UINT16_SIZE + (-self._pos & (UINT16_SIZE - 1))  # align
        if cython.compiled:
            if self._buf_len < self._pos:
                raise IndexError("Not enough data to read uint16")
            return _ustr_uint16(self._buf_ustr, self._pos - UINT16_SIZE, self._endian)
        return self._uint16_unpack(self._buf, self._pos - UINT16_SIZE)[0]

    def read_int16_unpack(self, type_: _SignatureType) -> int:
        return self._read_int16_unpack()

    def _read_int16_unpack(self) -> int:
        self._pos += INT16_SIZE + (-self._pos & (INT16_SIZE - 1))  # align
        if cython.compiled:
            if self._buf_len < self._pos:
                raise IndexError("Not enough data to read int16")
            return _ustr_int16(self._buf_ustr, self._pos - INT16_SIZE, self._endian)
        return self._int16_unpack(self._buf, self._pos - INT16_SIZE)[0]

    def read_boolean(self, type_: _SignatureType) -> bool:
        return self._read_boolean()

    def _read_boolean(self) -> bool:
        return bool(self._read_uint32_unpack())

    def read_string_unpack(self, type_: _SignatureType) -> str:
        return self._read_string_unpack()

    def _read_string_unpack(self) -> str:
        """Read a string using unpack."""
        self._pos += UINT32_SIZE + (-self._pos & (UINT32_SIZE - 1))  # align
        str_start = self._pos
        # read terminating '\0' byte as well (str_length + 1)
        if cython.compiled:
            if self._buf_len < self._pos:
                raise IndexError("Not enough data to read uint32")
            self._pos += (
                _ustr_uint32(self._buf_ustr, str_start - UINT32_SIZE, self._endian) + 1
            )
            if self._buf_len < self._pos:
                raise IndexError("Not enough data to read string")
        else:
            self._pos += self._uint32_unpack(self._buf, str_start - UINT32_SIZE)[0] + 1
        return self._buf_ustr[str_start : self._pos - 1].decode()

    def read_signature(self, type_: _SignatureType) -> str:
        return self._read_signature()

    def _read_signature(self) -> str:
        if cython.compiled:
            if self._buf_len < self._pos:
                raise IndexError("Not enough data to read signature")
        signature_len = self._buf_ustr[self._pos]  # byte
        o = self._pos + 1
        # read terminating '\0' byte as well (str_length + 1)
        self._pos = o + signature_len + 1
        if cython.compiled:
            if self._buf_len < self._pos:
                raise IndexError("Not enough data to read signature")
        return self._buf_ustr[o : o + signature_len].decode()

    def read_variant(self, type_: _SignatureType) -> Variant:
        return self._read_variant()

    def _read_variant(self) -> Variant:
        signature = self._read_signature()
        token_as_int = ord(signature[0])
        # verify in Variant is only useful on construction not unmarshalling
        if len(signature) == 1:
            if token_as_int == TOKEN_N_AS_INT:
                return Variant._factory(SIGNATURE_TREE_N, self._read_int16_unpack())
            if token_as_int == TOKEN_S_AS_INT:
                return Variant._factory(SIGNATURE_TREE_S, self._read_string_unpack())
            if token_as_int == TOKEN_B_AS_INT:
                return VARIANT_BOOL_TRUE if self._read_boolean() else VARIANT_BOOL_FALSE
            if token_as_int == TOKEN_O_AS_INT:
                return Variant._factory(SIGNATURE_TREE_O, self._read_string_unpack())
            if token_as_int == TOKEN_U_AS_INT:
                return Variant._factory(SIGNATURE_TREE_U, self._read_uint32_unpack())
            if token_as_int == TOKEN_Y_AS_INT:
                if cython.compiled:
                    if self._buf_len < self._pos:
                        raise IndexError("Not enough data to read byte")
                self._pos += 1
                return Variant._factory(SIGNATURE_TREE_Y, self._buf_ustr[self._pos - 1])
        elif token_as_int == TOKEN_A_AS_INT:
            if signature == "ay":
                return Variant._factory(
                    SIGNATURE_TREE_AY, self.read_array(SIGNATURE_TREE_AY_TYPES_0)
                )
            if signature == "a{qv}":
                return Variant._factory(
                    SIGNATURE_TREE_A_QV, self.read_array(SIGNATURE_TREE_A_QV_TYPES_0)
                )
            if signature == "as":
                return Variant._factory(
                    SIGNATURE_TREE_AS, self.read_array(SIGNATURE_TREE_AS_TYPES_0)
                )
            if signature == "a{sv}":
                return Variant._factory(
                    SIGNATURE_TREE_A_SV, self.read_array(SIGNATURE_TREE_A_SV_TYPES_0)
                )
            if signature == "ao":
                return Variant._factory(
                    SIGNATURE_TREE_AO, self.read_array(SIGNATURE_TREE_AO_TYPES_0)
                )
        tree = get_signature_tree(signature)
        signature_type = tree.root_type
        return Variant._factory(
            tree, self._readers[signature_type.token](self, signature_type)
        )

    def read_struct(self, type_: _SignatureType) -> tuple[Any, ...]:
        self._pos += -self._pos & 7  # align 8
        readers = self._readers
        return tuple(
            readers[child_type.token](self, child_type) for child_type in type_.children
        )

    def read_dict_entry(self, type_: _SignatureType) -> tuple[Any, Any]:
        self._pos += -self._pos & 7  # align 8
        return self._readers[type_.children[0].token](
            self, type_.children[0]
        ), self._readers[type_.children[1].token](self, type_.children[1])

    def read_array(self, type_: _SignatureType) -> Iterable[Any]:
        self._pos += -self._pos & 3  # align 4 for the array
        self._pos += (
            -self._pos & (UINT32_SIZE - 1)
        ) + UINT32_SIZE  # align for the uint32
        if cython.compiled:
            if self._buf_len < self._pos:
                raise IndexError("Not enough data to read uint32")
            array_length = _ustr_uint32(
                self._buf_ustr, self._pos - UINT32_SIZE, self._endian
            )
        else:
            array_length = self._uint32_unpack(self._buf, self._pos - UINT32_SIZE)[0]
        child_type = type_._child_0
        token_as_int = child_type.token_as_int

        if token_as_int in {
            TOKEN_X_AS_INT,
            TOKEN_T_AS_INT,
            TOKEN_D_AS_INT,
            TOKEN_LEFT_CURLY_AS_INT,
            TOKEN_LEFT_PAREN_AS_INT,
        }:
            # the first alignment is not included in the array size
            self._pos += -self._pos & 7  # align 8

        if token_as_int == TOKEN_Y_AS_INT:
            self._pos += array_length
            if cython.compiled:
                if self._buf_len < self._pos:
                    raise IndexError("Not enough data to read byte")
            return self._buf_ustr[self._pos - array_length : self._pos]

        if token_as_int == TOKEN_LEFT_CURLY_AS_INT:
            result_dict: dict[Any, Any] = {}
            key: str | int
            beginning_pos = self._pos
            child_0 = child_type._child_0
            child_1 = child_type._child_1
            child_0_token_as_int = child_0.token_as_int
            child_1_token_as_int = child_1.token_as_int
            # Strings with variant values are the most common case
            # so we optimize for that by inlining the string reading
            # and the variant reading here
            if (
                child_0_token_as_int in {TOKEN_O_AS_INT, TOKEN_S_AS_INT}
                and child_1_token_as_int == TOKEN_V_AS_INT
            ):
                while self._pos - beginning_pos < array_length:
                    self._pos += -self._pos & 7  # align 8
                    key = self._read_string_unpack()
                    result_dict[key] = self._read_variant()
            elif (
                child_0_token_as_int == TOKEN_Q_AS_INT
                and child_1_token_as_int == TOKEN_V_AS_INT
            ):
                while self._pos - beginning_pos < array_length:
                    self._pos += -self._pos & 7  # align 8
                    key = self._read_uint16_unpack()
                    result_dict[key] = self._read_variant()
            elif (
                child_0_token_as_int in {TOKEN_O_AS_INT, TOKEN_S_AS_INT}
                and child_1_token_as_int == TOKEN_A_AS_INT
            ):
                while self._pos - beginning_pos < array_length:
                    self._pos += -self._pos & 7  # align 8
                    key = self._read_string_unpack()
                    result_dict[key] = self.read_array(child_1)
            else:
                reader_1 = self._readers[child_1.token]
                reader_0 = self._readers[child_0.token]
                while self._pos - beginning_pos < array_length:
                    self._pos += -self._pos & 7  # align 8
                    key = reader_0(self, child_0)
                    result_dict[key] = reader_1(self, child_1)

            return result_dict

        if array_length == 0:
            return []

        result_list = []
        beginning_pos = self._pos
        if token_as_int == TOKEN_O_AS_INT or token_as_int == TOKEN_S_AS_INT:
            while self._pos - beginning_pos < array_length:
                result_list.append(self._read_string_unpack())
            return result_list
        reader = self._readers[child_type.token]
        while self._pos - beginning_pos < array_length:
            result_list.append(reader(self, child_type))
        return result_list

    def _header_fields(self, header_length: _int) -> list[Any]:
        """Header fields are always a(yv)."""
        beginning_pos = self._pos
        headers = _EMPTY_HEADERS.copy()
        if cython.compiled:
            if self._buf_len < self._pos + header_length:
                raise IndexError("Not enough data to read header")
        while self._pos - beginning_pos < header_length:
            # Now read the y (byte) of struct (yv)
            self._pos += (-self._pos & 7) + 1  # align 8 + 1 for 'y' byte
            field_0 = self._buf_ustr[self._pos - 1]

            # Now read the v (variant) of struct (yv)
            # first we read the signature
            signature_len = self._buf_ustr[self._pos]  # byte
            o = self._pos + 1
            if cython.compiled:
                if self._buf_len < o + signature_len:
                    raise IndexError("Not enough data to read signature")
            self._pos += signature_len + 2  # one for the byte, one for the '\0'
            if field_0 == HEADER_UNIX_FDS_IDX:  # defined by self._unix_fds
                continue
            token_as_int = self._buf_ustr[o]
            # Now that we have the token we can read the variant value
            # Strings and signatures are the most common types
            # so we inline them for performance
            if token_as_int == TOKEN_O_AS_INT or token_as_int == TOKEN_S_AS_INT:
                headers[field_0] = self._read_string_unpack()
            elif token_as_int == TOKEN_G_AS_INT:
                headers[field_0] = self._read_signature()
            else:
                token = self._buf_ustr[o : o + signature_len].decode()
                # There shouldn't be any other types in the header
                # but just in case, we'll read it using the slow path
                headers[field_0] = self._readers[token](
                    self, get_signature_tree(token).root_type
                )
        return headers

    def _read_header(self) -> None:
        """Read the header of the message."""
        # Signature is of the header is
        # BYTE, BYTE, BYTE, BYTE, UINT32, UINT32, ARRAY of STRUCT of (BYTE,VARIANT)
        self._read_to_pos(HEADER_SIGNATURE_SIZE)
        endian = self._buf_ustr[0]
        self._message_type = self._buf_ustr[1]
        self._flag = self._buf_ustr[2]
        protocol_version = self._buf_ustr[3]

        if protocol_version != PROTOCOL_VERSION:
            raise InvalidMessageError(
                f"got unknown protocol version: {protocol_version}"
            )

        if endian != LITTLE_ENDIAN and endian != BIG_ENDIAN:
            raise InvalidMessageError(
                f"Expecting endianness as the first byte, got {endian} from {self._buf}"
            )

        if cython.compiled:
            self._body_len = _ustr_uint32(self._buf_ustr, 4, endian)
            self._serial = _ustr_uint32(self._buf_ustr, 8, endian)
            self._header_len = _ustr_uint32(self._buf_ustr, 12, endian)
        elif endian == LITTLE_ENDIAN:
            (
                self._body_len,
                self._serial,
                self._header_len,
            ) = UNPACK_HEADER_LITTLE_ENDIAN(self._buf, 4)
            self._uint32_unpack = UINT32_UNPACK_LITTLE_ENDIAN
            self._int16_unpack = INT16_UNPACK_LITTLE_ENDIAN
            self._uint16_unpack = UINT16_UNPACK_LITTLE_ENDIAN
        else:  # BIG_ENDIAN
            self._body_len, self._serial, self._header_len = UNPACK_HEADER_BIG_ENDIAN(
                self._buf, 4
            )
            self._uint32_unpack = UINT32_UNPACK_BIG_ENDIAN
            self._int16_unpack = INT16_UNPACK_BIG_ENDIAN
            self._uint16_unpack = UINT16_UNPACK_BIG_ENDIAN

        # align 8
        self._msg_len = self._header_len + (-self._header_len & 7) + self._body_len
        if self._endian != endian:
            self._readers = self._readers_by_type[endian]
            self._endian = endian

    def _read_body(self) -> None:
        """Read the body of the message."""
        self._read_to_pos(HEADER_SIGNATURE_SIZE + self._msg_len)
        self._pos = HEADER_ARRAY_OF_STRUCT_SIGNATURE_POSITION
        header_fields = self._header_fields(self._header_len)
        self._pos += -self._pos & 7  # align 8
        signature: str = header_fields[HEADER_SIGNATURE_IDX]
        if not self._body_len:
            tree = SIGNATURE_TREE_EMPTY
            body: list[Any] = []
        else:
            token_as_int = ord(signature[0])
            if len(signature) == 1:
                if token_as_int == TOKEN_O_AS_INT:
                    tree = SIGNATURE_TREE_O
                    body = [self._read_string_unpack()]
                elif token_as_int == TOKEN_S_AS_INT:
                    tree = SIGNATURE_TREE_S
                    body = [self._read_string_unpack()]
                else:
                    tree = get_signature_tree(signature)
                    body = [self._readers[t.token](self, t) for t in tree.types]
            elif token_as_int == TOKEN_S_AS_INT and signature == "sa{sv}as":
                tree = SIGNATURE_TREE_SA_SV_AS
                body = [
                    self._read_string_unpack(),
                    self.read_array(SIGNATURE_TREE_SA_SV_AS_TYPES_1),
                    self.read_array(SIGNATURE_TREE_SA_SV_AS_TYPES_2),
                ]
            elif token_as_int == TOKEN_O_AS_INT and signature == "oa{sa{sv}}":
                tree = SIGNATURE_TREE_OA_SA_SV
                body = [
                    self._read_string_unpack(),
                    self.read_array(SIGNATURE_TREE_OA_SA_SV_TYPES_1),
                ]
            elif token_as_int == TOKEN_O_AS_INT and signature == "oas":
                tree = SIGNATURE_TREE_OAS
                body = [
                    self._read_string_unpack(),
                    self.read_array(SIGNATURE_TREE_OAS_TYPES_1),
                ]
            elif token_as_int == TOKEN_A_AS_INT and signature == "a{oa{sa{sv}}}":
                tree = SIGNATURE_TREE_A_OA_SA_SV
                body = [self.read_array(SIGNATURE_TREE_A_OA_SA_SV_TYPES_0)]
            else:
                tree = get_signature_tree(signature)
                body = [self._readers[t.token](self, t) for t in tree.types]

        flags = MESSAGE_FLAG_MAP.get(self._flag)
        if flags is None:
            flags = MESSAGE_FLAG_INTENUM(self._flag)
        message = Message.__new__(Message)
        message._fast_init(
            header_fields[HEADER_DESTINATION_IDX],
            header_fields[HEADER_PATH_IDX],
            header_fields[HEADER_INTERFACE_IDX],
            header_fields[HEADER_MEMBER_IDX],
            MESSAGE_TYPE_MAP[self._message_type],
            flags,
            header_fields[HEADER_ERROR_NAME_IDX],
            header_fields[HEADER_REPLY_SERIAL_IDX] or 0,
            header_fields[HEADER_SENDER_IDX],
            self._unix_fds,
            tree,
            body,
            self._serial,
            # The D-Bus implementation already validates the message,
            # so we don't need to do it again.
            False,
        )
        self._message = message
        self._read_complete = True

    def unmarshall(self) -> Message | None:
        """Unmarshall the message.

        The underlying read function will raise BlockingIOError if the
        if there are not enough bytes in the buffer. This allows unmarshall
        to be resumed when more data comes in over the wire.
        """
        return self._unmarshall()

    def _unmarshall(self) -> Message | None:
        """Unmarshall the message.

        The underlying read function will raise BlockingIOError if the
        if there are not enough bytes in the buffer. This allows unmarshall
        to be resumed when more data comes in over the wire.
        """
        if self._read_complete:
            self._next_message()
        try:
            if not self._msg_len:
                self._read_header()
            self._read_body()
        except MARSHALL_STREAM_END_ERROR:
            return None
        return self._message

    _complex_parsers_unpack: dict[str, Callable[[Unmarshaller, SignatureType], Any]] = {
        "b": read_boolean,
        "o": read_string_unpack,
        "s": read_string_unpack,
        "g": read_signature,
        "a": read_array,
        "(": read_struct,
        "{": read_dict_entry,
        "v": read_variant,
        "h": read_uint32_unpack,
        UINT32_DBUS_TYPE: read_uint32_unpack,
        INT16_DBUS_TYPE: read_int16_unpack,
        UINT16_DBUS_TYPE: read_uint16_unpack,
    }

    _ctype_by_endian: dict[int, dict[str, READER_TYPE]] = {
        endian: build_simple_parsers(endian) for endian in (LITTLE_ENDIAN, BIG_ENDIAN)
    }

    _readers_by_type: dict[int, dict[str, READER_TYPE]] = {
        LITTLE_ENDIAN: {
            **_ctype_by_endian[LITTLE_ENDIAN],
            **_complex_parsers_unpack,
        },
        BIG_ENDIAN: {
            **_ctype_by_endian[BIG_ENDIAN],
            **_complex_parsers_unpack,
        },
    }
