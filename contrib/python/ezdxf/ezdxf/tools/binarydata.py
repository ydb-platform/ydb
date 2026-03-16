# Copyright (c) 2014-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Iterable, Any, Sequence, Union, overload, Optional
from array import array
import struct
from binascii import unhexlify, hexlify
from codecs import decode

Bytes = Union[bytes, bytearray, memoryview]


def hex_strings_to_bytes(data: Iterable[str]) -> bytes:
    """Returns multiple hex strings `data` as bytes."""
    byte_array = array("B")
    for hexstr in data:
        byte_array.extend(unhexlify(hexstr))
    return byte_array.tobytes()


def bytes_to_hexstr(data: bytes) -> str:
    """Returns `data` bytes as plain hex string."""
    return hexlify(data).upper().decode()


NULL_NULL = b"\x00\x00"


class EndOfBufferError(EOFError):
    pass


class ByteStream:
    """Process little endian binary data organized as bytes, data is padded to
    4 byte boundaries by default.
    """

    # Created for Proxy Entity Graphic decoding
    def __init__(self, buffer: Bytes, align: int = 4):
        self.buffer = memoryview(buffer)
        self.index: int = 0
        self._align: int = align

    @property
    def has_data(self) -> bool:
        return self.index < len(self.buffer)

    def align(self, index: int) -> int:
        modulo = index % self._align
        return index + self._align - modulo if modulo else index

    def read_struct(self, fmt: str) -> Any:
        """Read data defined by a struct format string. Insert little endian
        format character '<' as first character, if machine has native big
        endian byte order.
        """
        if not self.has_data:
            raise EndOfBufferError("Unexpected end of buffer.")

        result = struct.unpack_from(fmt, self.buffer, offset=self.index)
        self.index = self.align(self.index + struct.calcsize(fmt))
        return result

    def read_float(self) -> float:
        return self.read_struct("<d")[0]

    def read_long(self) -> int:
        return self.read_struct("<L")[0]

    def read_signed_long(self) -> int:
        return self.read_struct("<l")[0]

    def read_vertex(self) -> Sequence[float]:
        return self.read_struct("<3d")

    def read_padded_string(self, encoding: str = "utf_8") -> str:
        """PS: Padded String. This is a string, terminated with a zero byte.
        The file’s text encoding (code page) is used to encode/decode the bytes
        into a string.
        """
        buffer = self.buffer
        for end_index in range(self.index, len(buffer)):
            if buffer[end_index] == 0:
                start_index = self.index
                self.index = self.align(end_index + 1)
                # noinspection PyTypeChecker
                return decode(buffer[start_index:end_index], encoding=encoding)
        raise EndOfBufferError(
            "Unexpected end of buffer, did not detect terminating zero byte."
        )

    def read_padded_unicode_string(self) -> str:
        """PUS: Padded Unicode String. The bytes are encoded using Unicode
        encoding. The bytes consist of byte pairs and the string is terminated
        by 2 zero bytes.
        """
        buffer = self.buffer
        for end_index in range(self.index, len(buffer), 2):
            if buffer[end_index : end_index + 2] == NULL_NULL:
                start_index = self.index
                self.index = self.align(end_index + 2)
                # noinspection PyTypeChecker
                return decode(
                    buffer[start_index:end_index], encoding="utf_16_le"
                )
        raise EndOfBufferError(
            "Unexpected end of buffer, did not detect terminating zero bytes."
        )


class BitStream:
    """Process little endian binary data organized as bit stream."""

    # Created for Proxy Entity Graphic decoding and DWG bit stream decoding
    def __init__(
        self,
        buffer: Bytes,
        dxfversion: str = "AC1015",
        encoding: str = "cp1252",
    ):
        self.buffer = memoryview(buffer)
        self.bit_index: int = 0
        self.dxfversion = dxfversion
        self.encoding = encoding

    @property
    def has_data(self) -> bool:
        return self.bit_index >> 3 < len(self.buffer)

    def align(self, count: int) -> None:
        """Align to byte border."""
        byte_index = (self.bit_index >> 3) + bool(self.bit_index & 7)
        modulo = byte_index % count
        if modulo:
            byte_index += count - modulo
        self.bit_index = byte_index << 3

    def skip(self, count: int) -> None:
        """Skip `count` bits."""
        self.bit_index += count

    def read_bit(self) -> int:
        """Read one bit from buffer."""
        index = self.bit_index
        self.bit_index += 1
        try:
            return 1 if self.buffer[index >> 3] & (0x80 >> (index & 7)) else 0
        except IndexError:
            raise EndOfBufferError("Unexpected end of buffer.")

    def read_bits(self, count) -> int:
        """Read `count` bits from buffer."""
        index = self.bit_index
        buffer = self.buffer
        # index of next bit after reading `count` bits
        next_bit_index = index + count

        if (next_bit_index - 1) >> 3 > len(buffer):
            # not enough data to read all bits
            raise EndOfBufferError("Unexpected end of buffer.")
        self.bit_index = next_bit_index

        test_bit = 0x80 >> (index & 7)
        test_byte_index = index >> 3
        value = 0
        test_byte = buffer[test_byte_index]
        while count > 0:
            value <<= 1
            if test_byte & test_bit:
                value |= 1
            count -= 1
            test_bit >>= 1
            if not test_bit and count:
                test_bit = 0x80
                test_byte_index += 1
                test_byte = buffer[test_byte_index]
        return value

    def read_unsigned_byte(self) -> int:
        """Read an unsigned byte (8 bit) from buffer."""
        return self.read_bits(8)

    def read_signed_byte(self) -> int:
        """Read a signed byte (8 bit) from buffer."""
        value = self.read_bits(8)
        if value & 0x80:
            # 2er complement
            return -((~value & 0xFF) + 1)
        else:
            return value

    def read_aligned_bytes(self, count: int) -> Sequence[int]:
        buffer = self.buffer
        start_index = self.bit_index >> 3
        end_index = start_index + count
        if end_index <= len(buffer):
            self.bit_index += count << 3
            return buffer[start_index:end_index]
        else:
            raise EndOfBufferError("Unexpected end of buffer.")

    def read_unsigned_short(self) -> int:
        """Read an unsigned short (16 bit) from buffer."""
        if self.bit_index & 7:
            s1 = self.read_bits(8)
            s2 = self.read_bits(8)
        else:  # aligned data
            s1, s2 = self.read_aligned_bytes(2)
        return (s2 << 8) + s1

    def read_signed_short(self) -> int:
        """Read a signed short (16 bit) from buffer."""
        value = self.read_unsigned_short()
        if value & 0x8000:
            # 2er complement
            return -((~value & 0xFFFF) + 1)
        else:
            return value

    def read_unsigned_long(self) -> int:
        """Read an unsigned long (32 bit) from buffer."""
        if self.bit_index & 7:
            read_bits = self.read_bits
            l1 = read_bits(8)
            l2 = read_bits(8)
            l3 = read_bits(8)
            l4 = read_bits(8)
        else:  # aligned data
            l1, l2, l3, l4 = self.read_aligned_bytes(4)
        return (l4 << 24) + (l3 << 16) + (l2 << 8) + l1

    def read_signed_long(self) -> int:
        """Read a signed long (32 bit) from buffer."""
        value = self.read_unsigned_long()
        if value & 0x80000000:
            # 2er complement
            return -((~value & 0xFFFFFFFF) + 1)
        else:
            return value

    def read_float(self) -> float:
        if self.bit_index & 7:
            read_bits = self.read_bits
            data = bytes(read_bits(8) for _ in range(8))
        else:  # aligned data
            data = bytes(self.read_aligned_bytes(8))
        return struct.unpack("<d", data)[0]

    def read_3_bits(self) -> int:
        bit = self.read_bit()
        if bit:  # 1
            bit = self.read_bit()
            if bit:  # 11
                bit = self.read_bit()
                if bit:
                    return 7  # 111
                else:
                    return 6  # 110
            return 2  # 10
        else:
            return 0  # 0

    @overload
    def read_bit_short(self) -> int:
        ...

    @overload
    def read_bit_short(self, count: int) -> Sequence[int]:
        ...

    def read_bit_short(self, count: int = 1) -> Union[int, Sequence[int]]:
        def _read():
            bits = self.read_bits(2)
            if bits == 0:
                return self.read_signed_short()
            elif bits == 1:
                return self.read_unsigned_byte()
            elif bits == 2:
                return 0
            else:
                return 256

        if count == 1:
            return _read()
        else:
            return tuple(_read() for _ in range(count))

    @overload
    def read_bit_long(self) -> int:
        ...

    @overload
    def read_bit_long(self, count: int) -> Sequence[int]:
        ...

    def read_bit_long(self, count: int = 1) -> Union[int, Sequence[int]]:
        def _read():
            bits = self.read_bits(2)
            if bits == 0:
                return self.read_signed_long()
            elif bits == 1:
                return self.read_unsigned_byte()
            elif bits == 2:
                return 0
            else:  # not used!
                return 256  # ???

        if count == 1:
            return _read()
        else:
            return tuple(_read() for _ in range(count))

    # LibreDWG: https://github.com/LibreDWG/libredwg/blob/master/src/bits.c
    # Read 1 bitlonglong (compacted uint64_t) for REQUIREDVERSIONS, preview_size.
    # ODA doc bug. ODA say 1-3 bits until the first 0 bit. See 3BLL.
    # The first 3 bits indicate the length l (see paragraph 2.1). Then
    # l bytes follow, which represent the number (the least significant
    # byte is first).
    def read_bit_long_long(self) -> int:
        value = 0
        shifting = 0
        length = self.read_bits(3)  # or read_3_bits() ?
        while length > 0:
            value += self.read_unsigned_byte() << shifting
            length -= 1
            shifting += 8
        return value

    @overload
    def read_raw_double(self) -> float:
        ...

    @overload
    def read_raw_double(self, count: int) -> Sequence[float]:
        ...

    def read_raw_double(self, count: int = 1) -> Union[float, Sequence[float]]:
        if count == 1:
            return self.read_float()
        else:
            return tuple(self.read_float() for _ in range(count))

    @overload
    def read_bit_double(self) -> float:
        ...

    @overload
    def read_bit_double(self, count: int) -> Sequence[float]:
        ...

    def read_bit_double(self, count: int = 1) -> Union[float, Sequence[float]]:
        def _read():
            bits = self.read_bits(2)
            if bits == 0:
                return self.read_float()
            elif bits == 1:
                return 1.0
            elif bits == 2:
                return 0.0
            else:  # not used!
                return 0.0

        if count == 1:
            return _read()
        else:
            return tuple(_read() for _ in range(count))

    @overload
    def read_bit_double_default(self) -> float:
        ...

    @overload
    def read_bit_double_default(self, count: int) -> Sequence[float]:
        ...

    @overload
    def read_bit_double_default(
        self, count: int, default: float
    ) -> Sequence[float]:
        ...

    def read_bit_double_default(
        self, count: int = 1, default: float = 0.0
    ) -> Union[float, Sequence[float]]:
        data = struct.pack("<d", default)

        def _read():
            bits = self.read_bits(2)
            if bits == 0:
                return default
            elif bits == 1:
                _data = (
                    bytes(self.read_unsigned_byte() for _ in range(4))
                    + data[4:]
                )
                return struct.unpack("<d", _data)[0]
            elif bits == 2:
                _data = bytearray(data)
                _data[4] = self.read_unsigned_byte()
                _data[5] = self.read_unsigned_byte()
                _data[0] = self.read_unsigned_byte()
                _data[1] = self.read_unsigned_byte()
                _data[2] = self.read_unsigned_byte()
                _data[3] = self.read_unsigned_byte()
                return struct.unpack("<d", _data)[0]
            else:
                return self.read_float()

        if count == 1:
            return _read()
        else:
            return tuple(_read() for _ in range(count))

    def read_signed_modular_chars(self) -> int:
        """Modular characters are a method of storing compressed integer
        values. They consist of a stream of bytes, terminating when the high
        bit (8) of the byte is 0 else another byte follows. Negative numbers
        are indicated by bit 7 set in the last byte.

        """
        shifting = 0
        value = 0
        while True:
            char = self.read_unsigned_byte()
            if char & 0x80:
                # bit 8 set = another char follows
                value |= (char & 0x7F) << shifting
                shifting += 7
            else:
                # bit 8 clear = end of modular char
                # bit 7 set = negative number
                value |= (char & 0x3F) << shifting
                return -value if char & 0x40 else value

    def read_unsigned_modular_chars(self) -> int:
        """Modular characters are a method of storing compressed integer
        values. They consist of a stream of bytes, terminating when the high
        bit (8) of the byte is 0 else another byte follows.

        """
        shifting = 0
        value = 0
        while True:
            char = self.read_unsigned_byte()
            value |= (char & 0x7F) << shifting
            shifting += 7
            # bit 8 set = another char follows
            if not (char & 0x80):
                return value

    def read_modular_shorts(self) -> int:
        """Modular shorts are a method of storing compressed unsigned integer
        values. Only 1 or 2 shorts in practical usage (1GB), if the high
        bit (16) of the first short is set another short follows.

        """
        short = self.read_unsigned_short()
        if short & 0x8000:
            return (self.read_unsigned_short() << 15) | (short & 0x7FFF)
        else:
            return short

    def read_bit_extrusion(self) -> Sequence[float]:
        if self.read_bit():
            return 0.0, 0.0, 1.0
        else:
            return self.read_bit_double(3)

    def read_bit_thickness(self, dxfversion="AC1015") -> float:
        if dxfversion >= "AC1015":
            if self.read_bit():
                return 0.0
        return self.read_bit_double()

    def read_cm_color(self) -> int:
        return self.read_bit_short()

    def read_text(self) -> str:
        length = self.read_bit_short()
        data = bytes(self.read_unsigned_byte() for _ in range(length))
        return data.decode(encoding=self.encoding)

    def read_text_unicode(self) -> str:
        # Unicode text is read from the "string stream" within the object data,
        # see the main Object description section for details.
        length = self.read_bit_short()
        data = bytes(self.read_unsigned_byte() for _ in range(length * 2))
        return data.decode(encoding="utf16")

    def read_text_variable(self) -> str:
        if self.dxfversion < "AC1018":  # R2004
            return self.read_text()
        else:
            return self.read_text_unicode()

    def read_cm_color_cms(self) -> tuple[int, str, str]:
        """Returns tuple (rgb, color_name, book_name)."""
        _ = self.read_bit_short()  # index always 0
        color_name = ""
        book_name = ""
        rgb = self.read_bit_long()
        rc = self.read_unsigned_byte()
        if rc & 1:
            color_name = self.read_text_variable()
        if rc & 2:
            book_name = self.read_text_variable()
        return rgb, color_name, book_name

    def read_cm_color_enc(self) -> Union[int, Sequence[Optional[int]]]:
        """Returns color index as int or tuple (rgb, color_handle,
        transparency_type, transparency).
        """
        flags_and_index = self.read_bit_short()
        flags = flags_and_index >> 8
        index = flags_and_index & 0xFF
        if flags:
            rgb = None
            color_handle = None
            transparency_type = None
            transparency = None
            if flags & 0x80:
                rgb = self.read_bit_short() & 0x00FFFFFF
            if flags & 0x40:
                color_handle = self.read_handle()
            if flags & 0x20:
                data = self.read_bit_long()
                transparency_type = data >> 24
                transparency = data & 0xFF
            return rgb, color_handle, transparency_type, transparency
        else:
            return index

    def read_object_type(self) -> int:
        bits = self.read_bits(2)
        if bits == 0:
            return self.read_unsigned_byte()
        elif bits == 1:
            return self.read_unsigned_byte() + 0x1F0
        else:
            return self.read_unsigned_short()

    def read_handle(self, reference: int = 0) -> int:
        """Returns handle as integer value."""
        code = self.read_bits(4)
        length = self.read_bits(4)
        if code == 6:
            return reference + 1
        if code == 8:
            return reference - 1

        data = bytearray(b"\x00\x00\x00\x00\x00\x00\x00\x00")
        for index in range(length):
            data[index] = self.read_unsigned_byte()
        offset = struct.unpack("<Q", data)[0]

        if code < 6:
            return offset
        else:
            if code == 10:
                return reference + offset
            if code == 12:
                return reference - offset
        return 0

    def read_hex_handle(self, reference: int = 0) -> str:
        """Returns handle as hex string."""
        return "%X" % self.read_handle(reference)

    def read_code(self, code: str):
        """Read data from bit stream by data codes defined in the
        ODA reference.

        """
        if code == "B":
            return self.read_bit()
        elif code == "RC":
            return self.read_unsigned_byte()
        elif code == "RS":
            return self.read_signed_short()
        elif code == "BS":
            return self.read_bit_short()
        elif code == "RL":
            return self.read_signed_long()
        elif code == "BL":
            return self.read_bit_long()
        elif code == "RD":
            return self.read_raw_double()
        elif code == "2RD":
            return self.read_raw_double(2)
        elif code == "BD":
            return self.read_bit_double()
        elif code == "2BD":
            return self.read_bit_double(2)
        elif code == "3BD":
            return self.read_bit_double(3)
        elif code == "T":
            return self.read_text()
        elif code == "TV":
            return self.read_text_variable()
        elif code == "H":
            return self.read_hex_handle()
        elif code == "BLL":
            return self.read_bit_long_long()
        elif code == "CMC":
            return self.read_cm_color()
        raise ValueError(f"Unknown code: {code}")
