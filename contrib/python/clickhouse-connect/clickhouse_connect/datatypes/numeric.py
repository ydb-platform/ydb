import decimal
from typing import Union, Type, Sequence, MutableSequence, Any
import struct
import array

from math import nan, isnan, isinf

from clickhouse_connect.datatypes.base import TypeDef, ArrayType, ClickHouseType
from clickhouse_connect.driver.common import array_type, write_array, decimal_size, decimal_prec, first_value
from clickhouse_connect.driver.ctypes import numpy_conv, data_conv
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.options import pd, np
from clickhouse_connect.driver.query import QueryContext
from clickhouse_connect.driver.types import ByteSource


class IntBase(ArrayType, registered=False):
    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: bytearray, ctx: InsertContext):
        if len(column) == 0:
            return
        if self.nullable:
            first = next((x for x in column if x is not None), None)
            if isinstance(first, int):
                column = [0 if x is None else x for x in column]
            elif isinstance(first, float):
                column = [0 if x is None or isnan(x) or isinf(x) else int(x) for x in column]
            else:
                column = [int(x) if x else 0 for x in column]
        elif isinstance(column[0], float):
            column = [0 if x is None or isnan(x) or isinf(x) else int(x) for x in column]
        elif not isinstance(column[0], int):
            column = [int(x) for x in column]
        write_array(self._array_type, column, dest)


class Int8(IntBase):
    _array_type = 'b'
    np_type = 'b'


class UInt8(IntBase):
    _array_type = 'B'
    np_type = 'B'


class Int16(IntBase):
    _array_type = 'h'
    np_type = '<i2'


class UInt16(IntBase):
    _array_type = 'H'
    np_type = '<u2'


class Int32(IntBase):
    _array_type = 'i'
    np_type = '<i4'


class UInt32(IntBase):
    _array_type = 'I'
    np_type = '<u4'


class Int64(IntBase):
    _array_type = 'q'
    np_type = '<i8'


class UInt64(IntBase):
    valid_formats = 'signed', 'native'
    _array_type = 'Q'
    np_type = '<u8'
    python_type = int

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext, _read_state: Any):
        fmt = self.read_format(ctx)
        if ctx.use_numpy:
            np_type = '<q' if fmt == 'signed' else '<u8'
            return numpy_conv.read_numpy_array(source, np_type, num_rows)
        arr_type = 'q' if fmt == 'signed' else 'Q'
        return source.read_array(arr_type, num_rows)

    def _read_nullable_column(self, source: ByteSource, num_rows: int, ctx: QueryContext, _read_state: Any) -> Sequence:
        return data_conv.read_nullable_array(source, 'q' if self.read_format(ctx) == 'signed' else 'Q',
                                             num_rows, self._active_null(ctx))

    def _finalize_column(self, column: Sequence, ctx: QueryContext) -> Sequence:
        fmt = self.read_format(ctx)
        if fmt == 'string':
            return [str(x) for x in column]
        if ctx.use_extended_dtypes and self.nullable:
            return pd.array(column, dtype='Int64' if fmt == 'signed' else 'UInt64')
        if ctx.use_numpy and self.nullable and (not ctx.use_none):
            return np.array(column, dtype='<q' if fmt == 'signed' else '<u8')
        return column


class BigInt(ClickHouseType, registered=False):
    _signed = True
    valid_formats = 'string', 'native'
    python_type = int

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext, _read_state: Any):
        signed = self._signed
        sz = self.byte_size
        column = []
        app = column.append
        ifb = int.from_bytes
        if self.read_format(ctx) == 'string':
            for _ in range(num_rows):
                app(str(ifb(source.read_bytes(sz), 'little', signed=signed)))
        else:
            for _ in range(num_rows):
                app(ifb(source.read_bytes(sz), 'little', signed=signed))
        return column

    # pylint: disable=too-many-branches
    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: bytearray, ctx: InsertContext):
        if len(column) == 0:
            return
        first = first_value(column, self.nullable)
        sz = self.byte_size
        signed = self._signed
        empty = bytes(b'\x00' * sz)
        ext = dest.extend
        if isinstance(first, str) or self.write_format(ctx) == 'string':
            if self.nullable:
                for x in column:
                    if x:
                        ext(int(x).to_bytes(sz, 'little', signed=signed))
                    else:
                        ext(empty)
            else:
                for x in column:
                    ext(int(x).to_bytes(sz, 'little', signed=signed))
        else:
            if self.nullable:
                for x in column:
                    if x:
                        ext(x.to_bytes(sz, 'little', signed=signed))
                    else:
                        ext(empty)
            else:
                for x in column:
                    ext(x.to_bytes(sz, 'little', signed=signed))


class Int128(BigInt):
    byte_size = 16
    _signed = True


class UInt128(BigInt):
    byte_size = 16
    _signed = False


class Int256(BigInt):
    byte_size = 32
    _signed = True


class UInt256(BigInt):
    byte_size = 32
    _signed = False


class Float(ArrayType, registered=False):
    _array_type = 'f'
    python_type = float

    def _finalize_column(self, column: Sequence, ctx: QueryContext) -> Sequence:
        if self.read_format(ctx) == 'string':
            return [str(x) for x in column]
        if ctx.use_numpy and self.nullable and (not ctx.use_none):
            return np.array(column, dtype=self.np_type)
        return column

    def _active_null(self, ctx: QueryContext):
        if ctx.use_extended_dtypes:
            return nan
        if ctx.use_none:
            return None
        if ctx.use_numpy:
            return nan
        return 0.0

    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: bytearray, ctx: InsertContext):
        if len(column) == 0:
            return
        if self.nullable:
            first = next((x for x in column if x is not None), None)
            if not isinstance(first, float):
                column = [0 if x is None else float(x) for x in column]
            else:
                column = [0 if x is None else x for x in column]
        elif not isinstance(column[0], float):
            column = [float(x) for x in column]
        write_array(self._array_type, column, dest)


class Float32(Float):
    np_type = '<f4'


class Float64(Float):
    _array_type = 'd'
    np_type = '<f8'


class BFloat16(ArrayType):
    _array_type = "H"
    python_type = float
    np_type = "<f4"

    def _write_column_binary(
        self,
        column: Sequence[Any],
        dest: bytearray,
        ctx: InsertContext,
    ):
        if not column:
            return

        if self.nullable:
            first = next((x for x in column if x is not None), None)
            if isinstance(first, float):
                column = [0 if (x is None or isnan(x) or isinf(x)) else x for x in column]
            else:
                column = [0 if x is None else float(x) for x in column]
        elif not isinstance(column[0], float):
            column = [float(x) for x in column]

        vals = array.array("H")
        extend = vals.extend
        for x in column:
            bits32 = struct.unpack("<I", struct.pack("<f", x))[0]
            extend([bits32 >> 16])

        write_array(self._array_type, vals, dest, ctx.column_name)

    def _read_column_binary(
        self, source: ByteSource, num_rows: int, ctx: QueryContext, _read_state: Any
    ):
        if ctx.use_numpy:
            arr16 = numpy_conv.read_numpy_array(source, "<u2", num_rows)
            return (arr16.astype(np.uint32) << np.uint32(16)).view(np.float32)

        raw = source.read_array(self._array_type, num_rows)
        return [struct.unpack("<f", struct.pack("<I", v << 16))[0] for v in raw]

    def _read_nullable_column(
        self, source: ByteSource, num_rows: int, ctx: QueryContext, _read_state: Any
    ):
        null_map = source.read_bytes(num_rows)

        if ctx.use_numpy:
            arr16 = numpy_conv.read_numpy_array(source, "<u2", num_rows)
            floats = (arr16.astype(np.uint32) << np.uint32(16)).view(np.float32)
            return data_conv.build_nullable_column(
                floats, null_map, self._active_null(ctx)
            )

        raw = source.read_array(self._array_type, num_rows)
        floats = [struct.unpack("<f", struct.pack("<I", v << 16))[0] for v in raw]
        return data_conv.build_nullable_column(floats, null_map, self._active_null(ctx))

    def _finalize_column(self, column, ctx: QueryContext):
        if ctx.use_extended_dtypes and self.nullable:
            return pd.array(column, dtype="Float32")
        if ctx.use_numpy and not isinstance(column, np.ndarray):
            return np.array(column, dtype=self.np_type)
        return column

    def _active_null(self, ctx: QueryContext):
        if ctx.use_extended_dtypes:
            return nan
        if ctx.use_none:
            return None
        if ctx.use_numpy:
            return nan
        return 0.0


class Bool(ClickHouseType):
    np_type = '?'
    python_type = bool
    byte_size = 1

    def _read_column_binary(self, source: ByteSource, num_rows: int, _ctx: QueryContext, _read_state: Any):
        column = source.read_bytes(num_rows)
        return [b != 0 for b in column]

    def _finalize_column(self, column: Sequence, ctx: QueryContext) -> Sequence:
        if ctx.use_numpy:
            return np.array(column)
        return column

    def _write_column_binary(self, column, dest, ctx):
        write_array('B', [1 if x else 0 for x in column], dest, ctx.column_name)


class Boolean(Bool):
    pass


class Enum(ClickHouseType):
    __slots__ = '_name_map', '_int_map'
    _array_type = 'b'
    valid_formats = 'native', 'int'
    python_type = str

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        escaped_keys = [key.replace("'", "\\'") for key in type_def.keys]
        self._name_map = dict(zip(type_def.keys, type_def.values))
        self._int_map = dict(zip(type_def.values, type_def.keys))
        val_str = ', '.join(f"'{key}' = {value}" for key, value in zip(escaped_keys, type_def.values))
        self._name_suffix = f'({val_str})'

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext, _read_state: Any):
        column = source.read_array(self._array_type, num_rows)
        if self.read_format(ctx) == 'int':
            return column
        lookup = self._int_map.get
        return [lookup(x, None) for x in column]

    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: bytearray, ctx:InsertContext):
        first = first_value(column, self.nullable)
        if first is None or not isinstance(first, str):
            if self.nullable:
                column = [0 if not x else x for x in column]
            write_array(self._array_type, column, dest, ctx.column_name)
        else:
            lookup = self._name_map.get
            write_array(self._array_type, [lookup(x, 0) for x in column], dest, ctx.column_name)


class Enum8(Enum):
    _array_type = 'b'
    byte_size = 1


class Enum16(Enum):
    _array_type = 'h'
    byte_size = 2


class Decimal(ClickHouseType):
    __slots__ = 'prec', 'scale', '_mult', '_zeros', 'byte_size', '_array_type'
    python_type = decimal.Decimal
    dec_size = 0

    @classmethod
    def build(cls: Type['Decimal'], type_def: TypeDef):
        size = cls.dec_size
        if size == 0:
            prec = type_def.values[0]
            scale = type_def.values[1]
            size = decimal_size(prec)
        else:
            prec = decimal_prec[size]
            scale = type_def.values[0]
        type_cls = BigDecimal if size > 64 else Decimal
        return type_cls(type_def, prec, size, scale)

    def __init__(self, type_def: TypeDef, prec, size, scale):
        super().__init__(type_def)
        self.prec = prec
        self.scale = scale
        self._mult = 10 ** scale
        self.byte_size = size // 8
        self._zeros = bytes([0] * self.byte_size)
        self._name_suffix = f'({prec}, {scale})'
        self._array_type = array_type(self.byte_size, True)

    def _read_column_binary(self, source: ByteSource, num_rows: int, _ctx: QueryContext, _read_state: Any):
        column = source.read_array(self._array_type, num_rows)
        dec = decimal.Decimal
        scale = self.scale
        prec = self.prec
        if scale == 0:
            return [dec(str(x)) for x in column]
        new_col = []
        app = new_col.append
        for x in column:
            if x >= 0:
                digits = str(x).rjust(prec, '0')
                app(dec(f'{digits[:-scale]}.{digits[-scale:]}'))
            else:
                digits = str(-x).rjust(prec, '0')
                app(dec(f'-{digits[:-scale]}.{digits[-scale:]}'))
        return new_col

    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: bytearray, ctx:InsertContext):
        with decimal.localcontext() as dec_ctx:
            dec_ctx.prec = self.prec
            dec = decimal.Decimal
            mult = self._mult
            if self.nullable:
                write_array(self._array_type, [int(dec(str(x)) * mult) if x else 0 for x in column], dest, ctx.column_name)
            else:
                write_array(self._array_type, [int(dec(str(x)) * mult) for x in column], dest, ctx.column_name)

    def _active_null(self, ctx: QueryContext):
        if ctx.use_none:
            return None
        digits = str('0').rjust(self.prec, '0')
        scale = self.scale
        return decimal.Decimal(f'{digits[:-scale]}.{digits[-scale:]}')


class BigDecimal(Decimal, registered=False):
    def _read_column_binary(self, source: ByteSource, num_rows: int, _ctx: QueryContext, _read_state: Any):
        dec = decimal.Decimal
        scale = self.scale
        prec = self.prec
        column = []
        app = column.append
        sz = self.byte_size
        ifb = int.from_bytes
        if scale == 0:
            for _ in range(num_rows):
                app(dec(str(ifb(source.read_bytes(sz), 'little', signed=True))))
            return column
        for _ in range(num_rows):
            x = ifb(source.read_bytes(sz), 'little', signed=True)
            if x >= 0:
                digits = str(x).rjust(prec, '0')
                app(dec(f'{digits[:-scale]}.{digits[-scale:]}'))
            else:
                digits = str(-x).rjust(prec, '0')
                app(dec(f'-{digits[:-scale]}.{digits[-scale:]}'))
        return column

    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: bytearray, _ctx):
        with decimal.localcontext() as ctx:
            ctx.prec = self.prec
            mult = decimal.Decimal(f"{self._mult}.{'0' * self.scale}")
            sz = self.byte_size
            itb = int.to_bytes
            if self.nullable:
                v = self._zeros
                for x in column:
                    dest += v if not x else itb(int(decimal.Decimal(str(x)) * mult), sz, 'little', signed=True)
            else:
                for x in column:
                    dest += itb(int(decimal.Decimal(str(x)) * mult), sz, 'little', signed=True)


class Decimal32(Decimal):
    dec_size = 32


class Decimal64(Decimal):
    dec_size = 64


class Decimal128(BigDecimal):
    dec_size = 128


class Decimal256(BigDecimal):
    dec_size = 256


class IntervalNanosecond(Int32):
    pass


class IntervalMicrosecond(Int32):
    pass


class IntervalMillisecond(Int32):
    pass


class IntervalSecond(Int32):
    pass


class IntervalMinute(Int32):
    pass


class IntervalHour(Int32):
    pass


class IntervalDay(Int32):
    pass


class IntervalWeek(Int32):
    pass


class IntervalMonth(Int32):
    pass


class IntervalQuarter(Int32):
    pass


class IntervalYear(Int32):
    pass
