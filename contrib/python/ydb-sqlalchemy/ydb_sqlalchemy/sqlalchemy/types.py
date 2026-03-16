import decimal
from typing import Any, Mapping, Type, Union

from sqlalchemy import __version__ as sa_version

if sa_version.startswith("2."):
    from sqlalchemy import ColumnElement
else:
    from sqlalchemy.sql.expression import ColumnElement

from sqlalchemy import ARRAY, exc, Table, types
from sqlalchemy.sql import type_api

from .datetime_types import YqlDate, YqlDateTime, YqlTimestamp, YqlDate32, YqlTimestamp64, YqlDateTime64  # noqa: F401
from .json import YqlJSON  # noqa: F401


class UInt64(types.Integer):
    __visit_name__ = "uint64"


class UInt32(types.Integer):
    __visit_name__ = "uint32"


class UInt16(types.Integer):
    __visit_name__ = "uint16"


class UInt8(types.Integer):
    __visit_name__ = "uint8"


class Int64(types.Integer):
    __visit_name__ = "int64"


class Int32(types.Integer):
    __visit_name__ = "int32"


class Int16(types.Integer):
    __visit_name__ = "int32"


class Int8(types.Integer):
    __visit_name__ = "int8"


class Decimal(types.DECIMAL):
    __visit_name__ = "DECIMAL"

    def __init__(self, precision=None, scale=None, asdecimal=True):
        # YDB supports Decimal(22,9) by default
        if precision is None:
            precision = 22
        if scale is None:
            scale = 9
        super().__init__(precision=precision, scale=scale, asdecimal=asdecimal)

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            # Convert float to Decimal if needed
            if isinstance(value, float):
                return decimal.Decimal(str(value))
            elif isinstance(value, str):
                return decimal.Decimal(value)
            elif not isinstance(value, decimal.Decimal):
                return decimal.Decimal(str(value))
            return value

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None

            # YDB always returns Decimal values as decimal.Decimal objects
            # But if asdecimal=False, we should convert to float
            if not self.asdecimal:
                return float(value)

            # For asdecimal=True (default), return as Decimal
            if not isinstance(value, decimal.Decimal):
                return decimal.Decimal(str(value))
            return value

        return process

    def literal_processor(self, dialect):
        def process(value):
            # Convert float to Decimal if needed
            if isinstance(value, float):
                value = decimal.Decimal(str(value))
            elif not isinstance(value, decimal.Decimal):
                value = decimal.Decimal(str(value))

            # Use default precision and scale if not specified
            precision = self.precision if self.precision is not None else 22
            scale = self.scale if self.scale is not None else 9

            return f'Decimal("{str(value)}", {precision}, {scale})'

        return process


class ListType(ARRAY):
    __visit_name__ = "list_type"

    def bind_processor(self, dialect):
        item_proc = self.item_type.bind_processor(dialect)

        def process(value):
            if value is None:
                return None
            return [item_proc(v) if v is not None else None for v in value]

        if item_proc:
            return process
        return None


class HashableDict(dict):
    def __hash__(self):
        return hash(tuple(self.items()))


class Optional(types.TypeEngine):
    """
    Wrapper for YDB Optional type.

    Used primarily within StructType to denote nullable fields.
    """

    __visit_name__ = "optional"

    def __init__(self, element_type: Union[Type[types.TypeEngine], types.TypeEngine]):
        self.element_type = element_type


class StructType(types.TypeEngine[Mapping[str, Any]]):
    """
    YDB Struct type.

    Represents a structured data type with named fields, mapped to a Python dictionary.
    """

    __visit_name__ = "struct_type"

    def __init__(
        self,
        fields_types: Mapping[
            str,
            Union[Type[types.TypeEngine], types.TypeEngine, Optional],
        ],
    ):
        self.fields_types = HashableDict(dict(sorted(fields_types.items())))

    @classmethod
    def from_table(cls, table: Table) -> "StructType":
        """
        Create a StructType definition from a SQLAlchemy Table.

        Automatically wraps nullable columns in Optional.

        :param table: SQLAlchemy Table object
        :return: StructType instance
        """
        fields = {}
        for col in table.columns:
            t = col.type
            if col.nullable:
                fields[col.name] = Optional(t)
            else:
                fields[col.name] = t
        return cls(fields)

    @property
    def python_type(self):
        return dict

    def compare_values(self, x, y):
        return x == y

    def bind_processor(self, dialect):
        processors = {}
        for name, type_ in self.fields_types.items():
            if isinstance(type_, Optional):
                type_ = type_.element_type

            type_ = type_api.to_instance(type_)
            proc = type_.bind_processor(dialect)
            if proc:
                processors[name] = proc

        if not processors:
            return None

        def process(value):
            if value is None:
                return None
            new_value = value.copy()
            for name, proc in processors.items():
                if name in new_value:
                    if new_value[name] is not None:
                        new_value[name] = proc(new_value[name])
            return new_value

        return process


class Lambda(ColumnElement):
    __visit_name__ = "lambda"

    def __init__(self, func):
        if not callable(func):
            raise exc.ArgumentError("func must be callable")

        self.type = type_api.NULLTYPE
        self.func = func


class Binary(types.LargeBinary):
    __visit_name__ = "BINARY"

    def bind_processor(self, dialect):
        return None
