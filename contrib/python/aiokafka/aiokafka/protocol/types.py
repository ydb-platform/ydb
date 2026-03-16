import struct
from collections.abc import Callable, Sequence
from io import BytesIO
from struct import error
from typing import (
    Any,
    TypeAlias,
    TypeVar,
    Union,
    cast,
    overload,
)

from typing_extensions import Buffer

from .abstract import AbstractType

T = TypeVar("T")

ValueT: TypeAlias = Union[type[AbstractType[Any]], "String", "Array", "Schema"]


def _pack(f: Callable[[T], bytes], value: T) -> bytes:
    try:
        return f(value)
    except error as e:
        raise ValueError(
            "Error encountered when attempting to convert value: "
            f"{value!r} to struct format: '{f}', hit error: {e}"
        ) from e


def _unpack(f: Callable[[Buffer], tuple[T, ...]], data: Buffer) -> T:
    try:
        (value,) = f(data)
    except error as e:
        raise ValueError(
            "Error encountered when attempting to convert value: "
            f"{data!r} to struct format: '{f}', hit error: {e}"
        ) from e
    else:
        return value


class Int8(AbstractType[int]):
    _pack = struct.Struct(">b").pack
    _unpack = struct.Struct(">b").unpack

    @classmethod
    def encode(cls, value: int) -> bytes:
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data: BytesIO) -> int:
        return _unpack(cls._unpack, data.read(1))


class Int16(AbstractType[int]):
    _pack = struct.Struct(">h").pack
    _unpack = struct.Struct(">h").unpack

    @classmethod
    def encode(cls, value: int) -> bytes:
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data: BytesIO) -> int:
        return _unpack(cls._unpack, data.read(2))


class Int32(AbstractType[int]):
    _pack = struct.Struct(">i").pack
    _unpack = struct.Struct(">i").unpack

    @classmethod
    def encode(cls, value: int) -> bytes:
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data: BytesIO) -> int:
        return _unpack(cls._unpack, data.read(4))


class UInt32(AbstractType[int]):
    _pack = struct.Struct(">I").pack
    _unpack = struct.Struct(">I").unpack

    @classmethod
    def encode(cls, value: int) -> bytes:
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data: BytesIO) -> int:
        return _unpack(cls._unpack, data.read(4))


class Int64(AbstractType[int]):
    _pack = struct.Struct(">q").pack
    _unpack = struct.Struct(">q").unpack

    @classmethod
    def encode(cls, value: int) -> bytes:
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data: BytesIO) -> int:
        return _unpack(cls._unpack, data.read(8))


class Float64(AbstractType[float]):
    _pack = struct.Struct(">d").pack
    _unpack = struct.Struct(">d").unpack

    @classmethod
    def encode(cls, value: float) -> bytes:
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data: BytesIO) -> float:
        return _unpack(cls._unpack, data.read(8))


class String:
    def __init__(self, encoding: str = "utf-8"):
        self.encoding = encoding

    def encode(self, value: str | None) -> bytes:
        if value is None:
            return Int16.encode(-1)
        encoded_value = str(value).encode(self.encoding)
        return Int16.encode(len(encoded_value)) + encoded_value

    def decode(self, data: BytesIO) -> str | None:
        length = Int16.decode(data)
        if length < 0:
            return None
        value = data.read(length)
        if len(value) != length:
            raise ValueError("Buffer underrun decoding string")
        return value.decode(self.encoding)

    @classmethod
    def repr(cls, value: str) -> str:
        return repr(value)


class Bytes(AbstractType[bytes | None]):
    @classmethod
    def encode(cls, value: bytes | None) -> bytes:
        if value is None:
            return Int32.encode(-1)
        else:
            return Int32.encode(len(value)) + value

    @classmethod
    def decode(cls, data: BytesIO) -> bytes | None:
        length = Int32.decode(data)
        if length < 0:
            return None
        value = data.read(length)
        if len(value) != length:
            raise ValueError("Buffer underrun decoding Bytes")
        return value

    @classmethod
    def repr(cls, value: bytes | None) -> str:
        return repr(
            value[:100] + b"..." if value is not None and len(value) > 100 else value
        )


class Boolean(AbstractType[bool]):
    _pack = struct.Struct(">?").pack
    _unpack = struct.Struct(">?").unpack

    @classmethod
    def encode(cls, value: bool) -> bytes:
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data: BytesIO) -> bool:
        return _unpack(cls._unpack, data.read(1))


class Schema:
    names: tuple[str, ...]
    fields: tuple[ValueT, ...]

    def __init__(self, *fields: tuple[str, ValueT]):
        if fields:
            self.names, self.fields = zip(*fields, strict=False)
        else:
            self.names, self.fields = (), ()

    def encode(self, item: Sequence[Any]) -> bytes:
        if len(item) != len(self.fields):
            raise ValueError("Item field count does not match Schema")
        return b"".join(field.encode(item[i]) for i, field in enumerate(self.fields))

    def decode(
        self, data: BytesIO
    ) -> tuple[Any | str | None | list[Any | tuple[Any, ...]], ...]:
        return tuple(field.decode(data) for field in self.fields)

    def __len__(self) -> int:
        return len(self.fields)

    def repr(self, value: Any) -> str:
        key_vals: list[str] = []
        try:
            for i in range(len(self)):
                try:
                    field_val = getattr(value, self.names[i])
                except AttributeError:
                    field_val = value[i]
                key_vals.append(f"{self.names[i]}={self.fields[i].repr(field_val)}")
            return "(" + ", ".join(key_vals) + ")"
        except Exception:  # noqa: BLE001
            return repr(value)


class Array:
    array_of: ValueT

    @overload
    def __init__(self, array_of_0: ValueT): ...

    @overload
    def __init__(
        self, array_of_0: tuple[str, ValueT], *array_of: tuple[str, ValueT]
    ): ...

    def __init__(
        self,
        array_of_0: ValueT | tuple[str, ValueT],
        *array_of: tuple[str, ValueT],
    ) -> None:
        if array_of:
            array_of_0 = cast(tuple[str, ValueT], array_of_0)
            self.array_of = Schema(array_of_0, *array_of)
        else:
            array_of_0 = cast(ValueT, array_of_0)
            if isinstance(array_of_0, String | Array | Schema) or issubclass(
                array_of_0, AbstractType
            ):
                self.array_of = array_of_0
            else:
                raise ValueError("Array instantiated with no array_of type")

    def encode(self, items: Sequence[Any] | None) -> bytes:
        if items is None:
            return Int32.encode(-1)
        encoded_items = (self.array_of.encode(item) for item in items)
        return b"".join(
            (Int32.encode(len(items)), *encoded_items),
        )

    def decode(self, data: BytesIO) -> list[Any | tuple[Any, ...]] | None:
        length = Int32.decode(data)
        if length == -1:
            return None
        return [self.array_of.decode(data) for _ in range(length)]

    def repr(self, list_of_items: Sequence[Any] | None) -> str:
        if list_of_items is None:
            return "NULL"
        return "[" + ", ".join(self.array_of.repr(item) for item in list_of_items) + "]"


class UnsignedVarInt32(AbstractType[int]):
    @classmethod
    def decode(cls, data: BytesIO) -> int:
        value, i = 0, 0
        b: int
        while True:
            (b,) = struct.unpack("B", data.read(1))
            if not (b & 0x80):
                break
            value |= (b & 0x7F) << i
            i += 7
            if i > 28:
                raise ValueError(f"Invalid value {value}")
        value |= b << i
        return value

    @classmethod
    def encode(cls, value: int) -> bytes:
        value &= 0xFFFFFFFF
        ret = b""
        while (value & 0xFFFFFF80) != 0:
            b = (value & 0x7F) | 0x80
            ret += struct.pack("B", b)
            value >>= 7
        ret += struct.pack("B", value)
        return ret


class VarInt32(AbstractType[int]):
    @classmethod
    def decode(cls, data: BytesIO) -> int:
        value = UnsignedVarInt32.decode(data)
        return (value >> 1) ^ -(value & 1)

    @classmethod
    def encode(cls, value: int) -> bytes:
        # bring it in line with the java binary repr
        value &= 0xFFFFFFFF
        return UnsignedVarInt32.encode((value << 1) ^ (value >> 31))


class VarInt64(AbstractType[int]):
    @classmethod
    def decode(cls, data: BytesIO) -> int:
        value, i = 0, 0
        b: int
        while True:
            (b,) = struct.unpack("B", data.read(1))
            if not (b & 0x80):
                break
            value |= (b & 0x7F) << i
            i += 7
            if i > 63:
                raise ValueError(f"Invalid value {value}")
        value |= b << i
        return (value >> 1) ^ -(value & 1)

    @classmethod
    def encode(cls, value: int) -> bytes:
        # bring it in line with the java binary repr
        value &= 0xFFFFFFFFFFFFFFFF
        v = (value << 1) ^ (value >> 63)
        ret = b""
        while (v & 0xFFFFFFFFFFFFFF80) != 0:
            b = (value & 0x7F) | 0x80
            ret += struct.pack("B", b)
            v >>= 7
        ret += struct.pack("B", v)
        return ret


class CompactString(String):
    def decode(self, data: BytesIO) -> str | None:
        length = UnsignedVarInt32.decode(data) - 1
        if length < 0:
            return None
        value = data.read(length)
        if len(value) != length:
            raise ValueError("Buffer underrun decoding string")
        return value.decode(self.encoding)

    def encode(self, value: str | None) -> bytes:
        if value is None:
            return UnsignedVarInt32.encode(0)
        encoded_value = str(value).encode(self.encoding)
        return UnsignedVarInt32.encode(len(encoded_value) + 1) + encoded_value


class TaggedFields(AbstractType[dict[int, bytes]]):
    @classmethod
    def decode(cls, data: BytesIO) -> dict[int, bytes]:
        num_fields = UnsignedVarInt32.decode(data)
        ret: dict[int, bytes] = {}
        if not num_fields:
            return ret
        prev_tag = -1
        for _ in range(num_fields):
            tag = UnsignedVarInt32.decode(data)
            if tag <= prev_tag:
                raise ValueError(f"Invalid or out-of-order tag {tag}")
            prev_tag = tag
            size = UnsignedVarInt32.decode(data)
            val = data.read(size)
            ret[tag] = val
        return ret

    @classmethod
    def encode(cls, value: dict[int, bytes]) -> bytes:
        ret = UnsignedVarInt32.encode(len(value))
        for k, v in value.items():
            # do we allow for other data types ?? It could get complicated really fast
            assert isinstance(v, bytes), f"Value {v!r} is not a byte array"
            assert isinstance(k, int) and k > 0, f"Key {k} is not a positive integer"
            ret += UnsignedVarInt32.encode(k)
            ret += v
        return ret


class CompactBytes(AbstractType[bytes | None]):
    @classmethod
    def decode(cls, data: BytesIO) -> bytes | None:
        length = UnsignedVarInt32.decode(data) - 1
        if length < 0:
            return None
        value = data.read(length)
        if len(value) != length:
            raise ValueError("Buffer underrun decoding Bytes")
        return value

    @classmethod
    def encode(cls, value: bytes | None) -> bytes:
        if value is None:
            return UnsignedVarInt32.encode(0)
        else:
            return UnsignedVarInt32.encode(len(value) + 1) + value


class CompactArray(Array):
    def encode(self, items: Sequence[Any] | None) -> bytes:
        if items is None:
            return UnsignedVarInt32.encode(0)
        encoded_items = (self.array_of.encode(item) for item in items)
        return b"".join(
            (UnsignedVarInt32.encode(len(items) + 1), *encoded_items),
        )

    def decode(self, data: BytesIO) -> list[Any | tuple[Any, ...]] | None:
        length = UnsignedVarInt32.decode(data) - 1
        if length == -1:
            return None
        return [self.array_of.decode(data) for _ in range(length)]
