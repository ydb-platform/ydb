# ruff: noqa: D101, D104, D105, D107, ANN401
from typing import Any

from duckdb.sqltypes import (
    BIGINT,
    BIT,
    BLOB,
    BOOLEAN,
    DATE,
    DOUBLE,
    FLOAT,
    HUGEINT,
    INTEGER,
    INTERVAL,
    SMALLINT,
    SQLNULL,
    TIME,
    TIME_TZ,
    TIMESTAMP,
    TIMESTAMP_MS,
    TIMESTAMP_NS,
    TIMESTAMP_S,
    TIMESTAMP_TZ,
    TINYINT,
    UBIGINT,
    UHUGEINT,
    UINTEGER,
    USMALLINT,
    UTINYINT,
    UUID,
    VARCHAR,
    DuckDBPyType,
)


class Value:
    def __init__(self, object: Any, type: DuckDBPyType) -> None:
        self.object = object
        self.type = type

    def __repr__(self) -> str:
        return str(self.object)


# Miscellaneous


class NullValue(Value):
    def __init__(self) -> None:
        super().__init__(None, SQLNULL)


class BooleanValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, BOOLEAN)


# Unsigned numerics


class UnsignedBinaryValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, UTINYINT)


class UnsignedShortValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, USMALLINT)


class UnsignedIntegerValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, UINTEGER)


class UnsignedLongValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, UBIGINT)


# Signed numerics


class BinaryValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, TINYINT)


class ShortValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, SMALLINT)


class IntegerValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, INTEGER)


class LongValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, BIGINT)


class HugeIntegerValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, HUGEINT)


class UnsignedHugeIntegerValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, UHUGEINT)


# Fractional


class FloatValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, FLOAT)


class DoubleValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, DOUBLE)


class DecimalValue(Value):
    def __init__(self, object: Any, width: int, scale: int) -> None:
        import duckdb

        decimal_type = duckdb.decimal_type(width, scale)
        super().__init__(object, decimal_type)


# String


class StringValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, VARCHAR)


class UUIDValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, UUID)


class BitValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, BIT)


class BlobValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, BLOB)


# Temporal


class DateValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, DATE)


class IntervalValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, INTERVAL)


class TimestampValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, TIMESTAMP)


class TimestampSecondValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, TIMESTAMP_S)


class TimestampMilisecondValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, TIMESTAMP_MS)


class TimestampNanosecondValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, TIMESTAMP_NS)


class TimestampTimeZoneValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, TIMESTAMP_TZ)


class TimeValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, TIME)


class TimeTimeZoneValue(Value):
    def __init__(self, object: Any) -> None:
        super().__init__(object, TIME_TZ)


class ListValue(Value):
    def __init__(self, object: Any, child_type: DuckDBPyType) -> None:
        import duckdb

        list_type = duckdb.list_type(child_type)
        super().__init__(object, list_type)


class StructValue(Value):
    def __init__(self, object: Any, children: dict[str, DuckDBPyType]) -> None:
        import duckdb

        struct_type = duckdb.struct_type(children)
        super().__init__(object, struct_type)


class MapValue(Value):
    def __init__(self, object: Any, key_type: DuckDBPyType, value_type: DuckDBPyType) -> None:
        import duckdb

        map_type = duckdb.map_type(key_type, value_type)
        super().__init__(object, map_type)


class UnionType(Value):
    def __init__(self, object: Any, members: dict[str, DuckDBPyType]) -> None:
        import duckdb

        union_type = duckdb.union_type(members)
        super().__init__(object, union_type)


# TODO: add EnumValue once `duckdb.enum_type` is added  # noqa: TD002, TD003

__all__ = [
    "BinaryValue",
    "BitValue",
    "BlobValue",
    "BooleanValue",
    "DateValue",
    "DecimalValue",
    "DoubleValue",
    "FloatValue",
    "HugeIntegerValue",
    "IntegerValue",
    "IntervalValue",
    "LongValue",
    "NullValue",
    "ShortValue",
    "StringValue",
    "TimeTimeZoneValue",
    "TimeValue",
    "TimestampMilisecondValue",
    "TimestampNanosecondValue",
    "TimestampSecondValue",
    "TimestampTimeZoneValue",
    "TimestampValue",
    "UUIDValue",
    "UnsignedBinaryValue",
    "UnsignedHugeIntegerValue",
    "UnsignedIntegerValue",
    "UnsignedLongValue",
    "UnsignedShortValue",
    "Value",
]
