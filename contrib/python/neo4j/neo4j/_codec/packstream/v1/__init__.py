# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from codecs import decode
from contextlib import contextmanager
from struct import (
    pack as struct_pack,
    unpack as struct_unpack,
)

from ...hydration import DehydrationHooks
from .._common import Structure
from .types import (
    BYTES_TYPES,
    FALSE_VALUES,
    FLOAT_TYPES,
    INT_TYPES,
    MAPPING_TYPES,
    NONE_VALUES,
    SEQUENCE_TYPES,
    TRUE_VALUES,
)


try:
    from ...._rust.codec.packstream.v1 import (
        pack as _rust_pack,
        unpack as _rust_unpack,
    )
except ImportError:
    _rust_pack = None
    _rust_unpack = None


PACKED_UINT_8 = [struct_pack(">B", value) for value in range(0x100)]
PACKED_UINT_16 = [struct_pack(">H", value) for value in range(0x10000)]

UNPACKED_UINT_8 = {bytes(bytearray([x])): x for x in range(0x100)}
UNPACKED_UINT_16 = {struct_pack(">H", x): x for x in range(0x10000)}

INT64_MIN = -(2**63)
INT64_MAX = 2**63


class Packer:
    def __init__(self, stream):
        self.stream = stream
        self._write = self.stream.write

    def pack(self, data, dehydration_hooks=None):
        dehydration_hooks = self._inject_hooks(dehydration_hooks)
        self._pack(data, dehydration_hooks=dehydration_hooks)

    if _rust_pack:

        def _pack(self, data, dehydration_hooks=None):
            data = _rust_pack(data, dehydration_hooks)
            self._write(data)
    else:

        def _pack(self, data, dehydration_hooks=None):
            self._py_pack(data, dehydration_hooks)

    @classmethod
    def _inject_hooks(cls, dehydration_hooks=None):
        if dehydration_hooks is None:
            return DehydrationHooks(exact_types={tuple: list}, subtypes={})
        return dehydration_hooks.extend(exact_types={tuple: list}, subtypes={})

    def _py_pack(self, value, dehydration_hooks=None):
        write = self._write

        # None
        if any(value is v for v in NONE_VALUES):
            write(b"\xc0")  # NULL

        # Boolean
        elif any(value is v for v in TRUE_VALUES):
            write(b"\xc3")
        elif any(value is v for v in FALSE_VALUES):
            write(b"\xc2")

        # Float (only double precision is supported)
        elif isinstance(value, FLOAT_TYPES):
            write(b"\xc1")
            write(struct_pack(">d", value))

        # Integer
        elif isinstance(value, INT_TYPES):
            value = int(value)
            if -0x10 <= value < 0x80:
                write(PACKED_UINT_8[value % 0x100])
            elif -0x80 <= value < -0x10:
                write(b"\xc8")
                write(PACKED_UINT_8[value % 0x100])
            elif -0x8000 <= value < 0x8000:
                write(b"\xc9")
                write(PACKED_UINT_16[value % 0x10000])
            elif -0x80000000 <= value < 0x80000000:
                write(b"\xca")
                write(struct_pack(">i", value))
            elif INT64_MIN <= value < INT64_MAX:
                write(b"\xcb")
                write(struct_pack(">q", value))
            else:
                raise OverflowError(f"Integer {value} out of range")

        # String
        elif isinstance(value, str):
            encoded = value.encode("utf-8")
            self._pack_string_header(len(encoded))
            self._write(encoded)

        # Bytes
        elif isinstance(value, BYTES_TYPES):
            self._pack_bytes_header(len(value))
            self._write(value)

        # List
        elif isinstance(value, SEQUENCE_TYPES):
            self._pack_list_header(len(value))
            for item in value:
                self._py_pack(item, dehydration_hooks)

        # Map
        elif isinstance(value, MAPPING_TYPES):
            self._pack_map_header(len(value.keys()))
            for key, item in value.items():
                if not isinstance(key, str):
                    raise TypeError(
                        f"Map keys must be strings, not {type(key)}"
                    )
                self._py_pack(key, dehydration_hooks)
                self._py_pack(item, dehydration_hooks)

        # Structure
        elif isinstance(value, Structure):
            self.pack_struct(value.tag, value.fields)

        # Other if in dehydration hooks
        else:
            if dehydration_hooks:
                transformer = dehydration_hooks.get_transformer(value)
                if transformer is not None:
                    self._py_pack(transformer(value), dehydration_hooks)
                    return

            raise ValueError(f"Values of type {type(value)} are not supported")

    def _pack_bytes_header(self, size):
        write = self._write
        if size < 0x100:
            write(b"\xcc")
            write(PACKED_UINT_8[size])
        elif size < 0x10000:
            write(b"\xcd")
            write(PACKED_UINT_16[size])
        elif size < 0x100000000:
            write(b"\xce")
            write(struct_pack(">I", size))
        else:
            raise OverflowError("Bytes header size out of range")

    def _pack_string_header(self, size):
        write = self._write
        if size <= 0x0F:
            write(bytes((0x80 | size,)))
        elif size < 0x100:
            write(b"\xd0")
            write(PACKED_UINT_8[size])
        elif size < 0x10000:
            write(b"\xd1")
            write(PACKED_UINT_16[size])
        elif size < 0x100000000:
            write(b"\xd2")
            write(struct_pack(">I", size))
        else:
            raise OverflowError("String header size out of range")

    def _pack_list_header(self, size):
        write = self._write
        if size <= 0x0F:
            write(bytes((0x90 | size,)))
        elif size < 0x100:
            write(b"\xd4")
            write(PACKED_UINT_8[size])
        elif size < 0x10000:
            write(b"\xd5")
            write(PACKED_UINT_16[size])
        elif size < 0x100000000:
            write(b"\xd6")
            write(struct_pack(">I", size))
        else:
            raise OverflowError("List header size out of range")

    def _pack_map_header(self, size):
        write = self._write
        if size <= 0x0F:
            write(bytes((0xA0 | size,)))
        elif size < 0x100:
            write(b"\xd8")
            write(PACKED_UINT_8[size])
        elif size < 0x10000:
            write(b"\xd9")
            write(PACKED_UINT_16[size])
        elif size < 0x100000000:
            write(b"\xda")
            write(struct_pack(">I", size))
        else:
            raise OverflowError("Map header size out of range")

    def pack_struct(self, signature, fields, dehydration_hooks=None):
        self._pack_struct(
            signature,
            fields,
            dehydration_hooks=self._inject_hooks(dehydration_hooks),
        )

    def _pack_struct(self, signature, fields, dehydration_hooks=None):
        if len(signature) != 1 or not isinstance(signature, bytes):
            raise ValueError("Structure signature must be a single byte value")
        write = self._write
        size = len(fields)
        if size <= 0x0F:
            write(bytes((0xB0 | size,)))
        else:
            raise OverflowError("Structure size out of range")
        write(signature)
        for field in fields:
            self._pack(field, dehydration_hooks)

    @staticmethod
    def new_packable_buffer():
        return PackableBuffer()


class PackableBuffer:
    def __init__(self):
        self.data = bytearray()
        # export write method for packer; "inline" for performance
        self.write = self.data.extend
        self.clear = self.data.clear
        self._tmp_buffering = 0

    @contextmanager
    def tmp_buffer(self):
        self._tmp_buffering += 1
        old_len = len(self.data)
        try:
            yield
        except Exception:
            del self.data[old_len:]
            raise
        finally:
            self._tmp_buffering -= 1

    def is_tmp_buffering(self):
        return bool(self._tmp_buffering)


class Unpacker:
    def __init__(self, unpackable):
        self.unpackable = unpackable

    def reset(self):
        self.unpackable.reset()

    def read(self, n=1):
        return self.unpackable.read(n)

    def read_u8(self):
        return self.unpackable.read_u8()

    if _rust_unpack:

        def unpack(self, hydration_hooks=None):
            value, i = _rust_unpack(
                self.unpackable.data, self.unpackable.p, hydration_hooks
            )
            self.unpackable.p = i
            return value
    else:

        def unpack(self, hydration_hooks=None):
            return self._unpack(hydration_hooks=hydration_hooks)

    def _unpack(self, hydration_hooks=None):
        marker = self.read_u8()

        if marker == -1:
            raise ValueError("Nothing to unpack")

        # Tiny Integer
        if 0x00 <= marker <= 0x7F:
            return marker
        elif 0xF0 <= marker <= 0xFF:
            return marker - 0x100

        # Null
        elif marker == 0xC0:
            return None

        # Float
        elif marker == 0xC1:
            (value,) = struct_unpack(">d", self.read(8))
            return value

        # Boolean
        elif marker == 0xC2:
            return False
        elif marker == 0xC3:
            return True

        # Integer
        elif marker == 0xC8:
            return struct_unpack(">b", self.read(1))[0]
        elif marker == 0xC9:
            return struct_unpack(">h", self.read(2))[0]
        elif marker == 0xCA:
            return struct_unpack(">i", self.read(4))[0]
        elif marker == 0xCB:
            return struct_unpack(">q", self.read(8))[0]

        # Bytes
        elif marker == 0xCC:
            (size,) = struct_unpack(">B", self.read(1))
            return self.read(size).tobytes()
        elif marker == 0xCD:
            (size,) = struct_unpack(">H", self.read(2))
            return self.read(size).tobytes()
        elif marker == 0xCE:
            (size,) = struct_unpack(">I", self.read(4))
            return self.read(size).tobytes()

        else:
            marker_high = marker & 0xF0
            # String
            if marker_high == 0x80:  # TINY_STRING
                return decode(self.read(marker & 0x0F), "utf-8")
            elif marker == 0xD0:  # STRING_8:
                (size,) = struct_unpack(">B", self.read(1))
                return decode(self.read(size), "utf-8")
            elif marker == 0xD1:  # STRING_16:
                (size,) = struct_unpack(">H", self.read(2))
                return decode(self.read(size), "utf-8")
            elif marker == 0xD2:  # STRING_32:
                (size,) = struct_unpack(">I", self.read(4))
                return decode(self.read(size), "utf-8")

            # List
            elif 0x90 <= marker <= 0x9F or 0xD4 <= marker <= 0xD6:
                return list(
                    self._unpack_list_items(
                        marker, hydration_hooks=hydration_hooks
                    )
                )

            # Map
            elif 0xA0 <= marker <= 0xAF or 0xD8 <= marker <= 0xDA:
                return self._unpack_map(
                    marker, hydration_hooks=hydration_hooks
                )

            # Structure
            elif 0xB0 <= marker <= 0xBF:
                size, tag = self._unpack_structure_header(marker)
                value = Structure(tag, *([None] * size))
                for i in range(len(value)):
                    value[i] = self._unpack(hydration_hooks=hydration_hooks)
                if not hydration_hooks:
                    return value
                hydration_hook = hydration_hooks.get(type(value))
                if not hydration_hook:
                    return value
                return hydration_hook(value)

            else:
                raise ValueError(f"Unknown PackStream marker {marker:02X}")

    def _unpack_list_items(self, marker, hydration_hooks=None):
        marker_high = marker & 0xF0
        if marker_high == 0x90:
            size = marker & 0x0F
            if size == 0:
                return
            elif size == 1:
                yield self._unpack(hydration_hooks=hydration_hooks)
            else:
                for _ in range(size):
                    yield self._unpack(hydration_hooks=hydration_hooks)
        elif marker == 0xD4:  # LIST_8:
            (size,) = struct_unpack(">B", self.read(1))
            for _ in range(size):
                yield self._unpack(hydration_hooks=hydration_hooks)
        elif marker == 0xD5:  # LIST_16:
            (size,) = struct_unpack(">H", self.read(2))
            for _ in range(size):
                yield self._unpack(hydration_hooks=hydration_hooks)
        elif marker == 0xD6:  # LIST_32:
            (size,) = struct_unpack(">I", self.read(4))
            for _ in range(size):
                yield self._unpack(hydration_hooks=hydration_hooks)
        else:
            return

    def unpack_map(self, hydration_hooks=None):
        marker = self.read_u8()
        return self._unpack_map(marker, hydration_hooks=hydration_hooks)

    def _unpack_map(self, marker, hydration_hooks=None):
        marker_high = marker & 0xF0
        if marker_high == 0xA0:
            size = marker & 0x0F
            value = {}
            for _ in range(size):
                key = self._unpack(hydration_hooks=hydration_hooks)
                value[key] = self._unpack(hydration_hooks=hydration_hooks)
            return value
        elif marker == 0xD8:  # MAP_8:
            (size,) = struct_unpack(">B", self.read(1))
            value = {}
            for _ in range(size):
                key = self._unpack(hydration_hooks=hydration_hooks)
                value[key] = self._unpack(hydration_hooks=hydration_hooks)
            return value
        elif marker == 0xD9:  # MAP_16:
            (size,) = struct_unpack(">H", self.read(2))
            value = {}
            for _ in range(size):
                key = self._unpack(hydration_hooks=hydration_hooks)
                value[key] = self._unpack(hydration_hooks=hydration_hooks)
            return value
        elif marker == 0xDA:  # MAP_32:
            (size,) = struct_unpack(">I", self.read(4))
            value = {}
            for _ in range(size):
                key = self._unpack(hydration_hooks=hydration_hooks)
                value[key] = self._unpack(hydration_hooks=hydration_hooks)
            return value
        else:
            return None

    def unpack_structure_header(self):
        marker = self.read_u8()
        if marker == -1:
            return None, None
        else:
            return self._unpack_structure_header(marker)

    def _unpack_structure_header(self, marker):
        marker_high = marker & 0xF0
        if marker_high == 0xB0:  # TINY_STRUCT
            signature = self.read(1).tobytes()
            return marker & 0x0F, signature
        else:
            raise ValueError(f"Expected structure, found marker {marker:02X}")

    @staticmethod
    def new_unpackable_buffer():
        return UnpackableBuffer()


class UnpackableBuffer:
    initial_capacity = 8192

    def __init__(self, data=None):
        if data is None:
            self.data = bytearray(self.initial_capacity)
            self.used = 0
        else:
            self.data = bytearray(data)
            self.used = len(self.data)
        self.p = 0

    def reset(self):
        self.used = 0
        self.p = 0

    def read(self, n=1):
        view = memoryview(self.data)
        q = self.p + n
        subview = view[self.p : q]
        self.p = q
        return subview

    def read_u8(self):
        if self.used - self.p >= 1:
            value = self.data[self.p]
            self.p += 1
            return value
        else:
            return -1

    def pop_u16(self):
        """Pop last two bytes as a big-endian 16-bit unsigned integer."""
        if self.used >= 2:
            value = 0x100 * self.data[self.used - 2] + self.data[self.used - 1]
            self.used -= 2
            return value
        else:
            return -1
