import logging
from math import ceil, nan
from struct import pack, unpack
from typing import Any, Sequence

from clickhouse_connect.datatypes.base import ClickHouseType, TypeDef
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.ctypes import data_conv
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.options import np
from clickhouse_connect.driver.query import QueryContext
from clickhouse_connect.driver.types import ByteSource

logger = logging.getLogger(__name__)

if np is None:
    logger.info("NumPy not detected. Install NumPy to see an order of magnitude performance gain with QBit columns.")


class QBit(ClickHouseType):
    """
    QBit type - represents bit-transposed vectors for efficient vector search operations.

    Syntax: QBit(element_type, dimension)
    - element_type: BFloat16, Float32, or Float64
    - dimension: Number of elements per vector

    Over the Native protocol, ClickHouse transmits QBit columns as bit-transposed Tuples.

    Requires:
        - SET allow_experimental_qbit_type = 1
        - Server version >=25.10
    """

    __slots__ = (
        "element_type",
        "dimension",
        "_bits_per_element",
        "_bytes_per_fixedstring",
        "_tuple_type",
    )

    python_type = list
    _BIT_SHIFTS = [1 << i for i in range(8)]
    _ELEMENT_BITS = {"BFloat16": 16, "Float32": 32, "Float64": 64}

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)

        self.element_type = type_def.values[0]
        if self.element_type not in self._ELEMENT_BITS:
            raise ValueError(f"Unsupported QBit element type '{self.element_type}'. Supported types: BFloat16, Float32, Float64.")

        self.dimension = type_def.values[1]
        if self.dimension <= 0:
            raise ValueError(f"QBit dimension must be greater than 0. Got: {self.dimension}.")

        self._name_suffix = f"({self.element_type}, {self.dimension})"
        self._bits_per_element = self._ELEMENT_BITS.get(self.element_type, 32)
        self._bytes_per_fixedstring = ceil(self.dimension / 8)

        # Create the underlying Tuple type for bit-transposed representation
        # E.g., for Float32 with dim=8: Tuple(FixedString(1), FixedString(1), ... x32)
        fixedstring_type = f"FixedString({self._bytes_per_fixedstring})"
        tuple_types = ", ".join([fixedstring_type] * self._bits_per_element)
        tuple_type_name = f"Tuple({tuple_types})"
        self._tuple_type = get_from_name(tuple_type_name)
        self.byte_size = self._bits_per_element * self._bytes_per_fixedstring

    def read_column_prefix(self, source: ByteSource, ctx: QueryContext):
        return self._tuple_type.read_column_prefix(source, ctx)

    def read_column_data(self, source: ByteSource, num_rows: int, ctx: QueryContext, read_state: Any) -> Sequence:
        """Read bit-transposed Tuple data and convert to flat float vectors."""
        if num_rows == 0:
            return []

        null_map = None
        if self.nullable:
            null_map = source.read_bytes(num_rows)

        tuple_data = self._tuple_type.read_column_data(source, num_rows, ctx, read_state)
        vectors = [self._untranspose_row(t) for t in tuple_data]
        if self.nullable:
            return data_conv.build_nullable_column(vectors, null_map, self._active_null(ctx))
        return vectors

    def write_column_prefix(self, dest: bytearray):
        self._tuple_type.write_column_prefix(dest)

    def write_column_data(self, column: Sequence, dest: bytearray, ctx: InsertContext):
        """Convert flat float vectors to bit-transposed Tuple data and write."""
        if len(column) == 0:
            return

        if self.nullable:
            dest += bytes([1 if x is None else 0 for x in column])

        null_tuple = tuple(b"\x00" * self._bytes_per_fixedstring for _ in range(self._bits_per_element))
        tuple_column = [null_tuple if row is None else self._transpose_row(row) for row in column]

        self._tuple_type.write_column_data(tuple_column, dest, ctx)

    def _active_null(self, ctx: QueryContext):
        """Return context-appropriate null value for nullable QBit columns."""
        if ctx.use_none:
            return None
        if ctx.use_extended_dtypes:
            return nan
        return None

    def _values_to_words(self, values: list[float]) -> Sequence[int]:
        """Convert float values to integer words using batch struct processing."""
        count = len(values)

        if self.element_type == "BFloat16":
            # BFloat16 is the top 16 bits of a Float32 (truncate mantissa)
            raw_ints = unpack(f"<{count}I", pack(f"<{count}f", *values))
            return [(x >> 16) & 0xFFFF for x in raw_ints]

        fmt_char = "I" if self.element_type == "Float32" else "Q"
        float_char = "f" if self.element_type == "Float32" else "d"

        return unpack(f"<{count}{fmt_char}", pack(f"<{count}{float_char}", *values))

    def _words_to_values(self, words: list[int]) -> list[float]:
        """Convert integer words to float values using batch unpacking."""
        count = len(words)

        if self.element_type == "BFloat16":
            # Pad BFloat16 words with zeros to reconstruct valid Float32s
            shifted_words = [(w & 0xFFFF) << 16 for w in words]
            return list(unpack(f"<{count}f", pack(f"<{count}I", *shifted_words)))

        if self.element_type == "Float32":
            return list(unpack(f"<{count}f", pack(f"<{count}I", *words)))

        # Float64
        return list(unpack(f"<{count}d", pack(f"<{count}Q", *words)))

    def _untranspose_row(self, bit_planes: tuple):
        """Convert bit-transposed tuple to flat float vector."""
        if np is not None:
            return self._untranspose_row_numpy(bit_planes)

        words = [0] * self.dimension
        bit_shifts = self._BIT_SHIFTS
        dim = self.dimension

        # Iterate Planes (MSB -> LSB)
        for bit_idx, bit_plane_bytes in enumerate(bit_planes):
            bit_pos = self._bits_per_element - 1 - bit_idx
            mask = 1 << bit_pos

            # Iterate Bytes in Plane
            for byte_idx, byte_val in enumerate(bit_plane_bytes):
                # if byte is 0, skip processing 8 bits
                if byte_val == 0:
                    continue

                base_elem_idx = byte_idx << 3  # Each byte encodes 8 elements

                # Extract set bits from this byte
                for bit_in_byte in range(8):
                    if byte_val & bit_shifts[bit_in_byte]:
                        elem_idx = base_elem_idx + bit_in_byte
                        if elem_idx < dim:
                            words[elem_idx] |= mask  # Accumulate bit at position bit_pos

        return self._words_to_values(words)

    def _untranspose_row_numpy(self, bit_planes: tuple) -> list[float]:
        """Vectorized numpy operations version of _untranspose_row"""
        # 1. Convert tuple of bytes to a single uint8 array
        total_bytes = b"".join(bit_planes)
        planes_uint8 = np.frombuffer(total_bytes, dtype=np.uint8)
        planes_uint8 = planes_uint8.reshape(self._bits_per_element, -1)

        # 2. Unpack bits to get the boolean/integer matrix
        bits_matrix: "np.ndarray" = np.unpackbits(planes_uint8, axis=1, bitorder="little")

        # 3. Trim padding if necessary
        if bits_matrix.shape[1] != self.dimension:  # pylint: disable=no-member
            bits_matrix = bits_matrix[:, : self.dimension]  # pylint: disable=invalid-sequence-index

        # 4. Reconstruct the integer words
        if self.element_type == "Float64":
            int_dtype = np.uint64
            final_dtype = np.float64
        else:
            # Float32 and BFloat16 use 32-bit containers
            int_dtype = np.uint32
            final_dtype = np.float32

        # Accumulate bits into integers
        words = np.zeros(self.dimension, dtype=int_dtype)

        for i in range(self._bits_per_element):
            # MSB is at index 0
            shift = self._bits_per_element - 1 - i

            # If the bit row is 1, add 2^shift to the word
            # Cast bits to the target int type before shifting to avoid overflow
            words |= bits_matrix[i].astype(int_dtype) << shift

        # 5. Interpret as Floats
        if self.element_type == "BFloat16":
            # Shift back up to the top 16 bits of a Float32
            # Cast to uint32 first to ensure safe shifting
            words = words.astype(np.uint32) << 16
            return words.view(np.float32).tolist()

        return words.view(final_dtype).tolist()

    def _transpose_row(self, values: list[float]) -> tuple:
        """Convert flat float vector to bit-transposed tuple."""
        if len(values) != self.dimension:
            raise ValueError(f"Vector dimension mismatch: expected {self.dimension}, got {len(values)}")

        # If numpy is available, use the fast path
        if np is not None:
            if isinstance(values, np.ndarray):
                return self._transpose_row_numpy(values)

            # If numpy is available but user supplied python list, convert to np array anyway for
            #  huge performance gains.
            dtype = np.float64 if self.element_type == "Float64" else np.float32
            return self._transpose_row_numpy(np.array(values, dtype=dtype))

        words = self._values_to_words(values)
        bit_planes = []
        bit_shifts = self._BIT_SHIFTS
        bytes_per_fs = self._bytes_per_fixedstring

        for bit_idx in range(self._bits_per_element):
            bit_pos = self._bits_per_element - 1 - bit_idx
            mask = 1 << bit_pos
            plane = bytearray(bytes_per_fs)

            for elem_idx, word in enumerate(words):
                if word & mask:
                    plane[elem_idx >> 3] |= bit_shifts[elem_idx & 7]

            bit_planes.append(bytes(plane))

        return tuple(bit_planes)

    def _transpose_row_numpy(self, vector: "np.ndarray") -> tuple:
        """Fast path for numpy arrays using vectorized operations."""
        # Cast to int view
        if self.element_type == "BFloat16":
            # Numpy doesn't have bfloat16. Input is Float32 so just
            #  discard the bottom 16 bits.
            v_float = vector.astype(np.float32, copy=False)
            # View as uint32, shift right 16, cast to uint16
            v_int = (v_float.view(np.uint32) >> 16).astype(np.uint16)

        elif self.element_type == "Float32":
            # Ensure it is 32-bit float first (handles float64->32 downcast safely)
            v_float = vector.astype(np.float32, copy=False)
            v_int = v_float.view(np.uint32)

        else:  # Float64
            v_float = vector.astype(np.float64, copy=False)
            v_int = v_float.view(np.uint64)

        bits = self._bits_per_element
        masks = (1 << np.arange(bits - 1, -1, -1, dtype=v_int.dtype)).reshape(-1, 1)

        # Extract bits: (Bits, Dim)
        # v_int broadcasted to (1, Dim)
        bits_extracted = (v_int & masks) != 0

        packed = np.packbits(bits_extracted.view(np.uint8), axis=1, bitorder="little")

        return tuple(row.tobytes() for row in packed)
