import struct

from clickhouse_connect.driver.types import ByteSource


class ByteArraySource(ByteSource):
    """
    ByteSource implementation for in-memory byte arrays.

    This class wraps a byte array and provides the ByteSource interface,
    allowing ClickHouse type decoders to read from in-memory data instead
    of a network stream.

    Used primarily for decoding variant-encoded values from JSON shared data
    where each value is a complete serialized type instance.
    """

    def __init__(self, data: bytes, encoding: str = "utf-8"):
        self.data = data
        self.pos = 0
        self.encoding = encoding

    def read_byte(self) -> int:
        if self.pos >= len(self.data):
            raise EOFError("Attempted to read past end of byte array")
        b = self.data[self.pos]
        self.pos += 1
        return b

    def read_bytes(self, sz: int) -> bytes:
        if self.pos + sz > len(self.data):
            raise EOFError(f"Attempted to read {sz} bytes, only {len(self.data) - self.pos} available")
        result = self.data[self.pos : self.pos + sz]
        self.pos += sz
        return result

    def read_leb128(self) -> int:
        sz = 0
        shift = 0
        while self.pos < len(self.data):
            b = self.read_byte()
            sz += (b & 0x7F) << shift
            if (b & 0x80) == 0:
                return sz
            shift += 7
        raise EOFError("Unexpected end while reading LEB128")

    def read_leb128_str(self) -> str:
        sz = self.read_leb128()
        return self.read_bytes(sz).decode(self.encoding)

    def read_uint64(self) -> int:
        return int.from_bytes(self.read_bytes(8), "little", signed=False)

    def read_int64(self) -> int:
        return int.from_bytes(self.read_bytes(8), "little", signed=True)

    def read_uint32(self) -> int:
        return int.from_bytes(self.read_bytes(4), "little", signed=False)

    def read_int32(self) -> int:
        return int.from_bytes(self.read_bytes(4), "little", signed=True)

    def read_uint16(self) -> int:
        return int.from_bytes(self.read_bytes(2), "little", signed=False)

    def read_int16(self) -> int:
        return int.from_bytes(self.read_bytes(2), "little", signed=True)

    def read_float32(self) -> float:
        return struct.unpack("<f", self.read_bytes(4))[0]

    def read_float64(self) -> float:
        return struct.unpack("<d", self.read_bytes(8))[0]

    # pylint: disable=too-many-return-statements
    def read_array(self, array_type: str, num_rows: int):  # type: ignore
        if array_type == "B":
            return [self.read_byte() for _ in range(num_rows)]
        if array_type == "H":
            return [self.read_uint16() for _ in range(num_rows)]
        if array_type == "I":
            return [self.read_uint32() for _ in range(num_rows)]
        if array_type == "Q":
            return [self.read_uint64() for _ in range(num_rows)]
        if array_type == "b":
            return [int.from_bytes([self.read_byte()], "little", signed=True) for _ in range(num_rows)]
        if array_type == "h":
            return [self.read_int16() for _ in range(num_rows)]
        if array_type == "i":
            return [self.read_int32() for _ in range(num_rows)]
        if array_type == "q":
            return [self.read_int64() for _ in range(num_rows)]
        if array_type == "f":
            return [self.read_float32() for _ in range(num_rows)]
        if array_type == "d":
            return [self.read_float64() for _ in range(num_rows)]
        raise NotImplementedError(f"Array type {array_type} not implemented for ByteArraySource")

    def read_str_col(self, num_rows, encoding, nullable=False, null_obj=None):  # type: ignore
        if num_rows != 1:
            raise NotImplementedError("read_str_col only supports num_rows=1 for single-value decoding")

        length = self.read_leb128()
        string_bytes = self.read_bytes(length)

        if encoding is None:
            return [string_bytes]

        return [string_bytes.decode(encoding)]

    def read_bytes_col(self, sz, num_rows):
        raise NotImplementedError("read_bytes_col not needed for single-value decoding")

    def read_fixed_str_col(self, sz, num_rows, encoding):
        raise NotImplementedError("read_fixed_str_col not needed for single-value decoding")

    def close(self):
        """No cleanup needed for byte arrays."""
