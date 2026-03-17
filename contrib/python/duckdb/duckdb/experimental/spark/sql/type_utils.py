from typing import cast  # noqa: D100

from duckdb.sqltypes import DuckDBPyType

from ..exception import ContributionsAcceptedError
from .types import (
    ArrayType,
    BinaryType,
    BitstringType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DayTimeIntervalType,
    DecimalType,
    DoubleType,
    FloatType,
    HugeIntegerType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimeNTZType,
    TimestampMilisecondNTZType,
    TimestampNanosecondNTZType,
    TimestampNTZType,
    TimestampSecondNTZType,
    TimestampType,
    TimeType,
    UnsignedByteType,
    UnsignedHugeIntegerType,
    UnsignedIntegerType,
    UnsignedLongType,
    UnsignedShortType,
    UUIDType,
)

_sqltype_to_spark_class = {
    "boolean": BooleanType,
    "utinyint": UnsignedByteType,
    "tinyint": ByteType,
    "usmallint": UnsignedShortType,
    "smallint": ShortType,
    "uinteger": UnsignedIntegerType,
    "integer": IntegerType,
    "ubigint": UnsignedLongType,
    "bigint": LongType,
    "hugeint": HugeIntegerType,
    "uhugeint": UnsignedHugeIntegerType,
    "varchar": StringType,
    "blob": BinaryType,
    "bit": BitstringType,
    "uuid": UUIDType,
    "date": DateType,
    "time": TimeNTZType,
    "time with time zone": TimeType,
    "timestamp": TimestampNTZType,
    "timestamp with time zone": TimestampType,
    "timestamp_ms": TimestampNanosecondNTZType,
    "timestamp_ns": TimestampMilisecondNTZType,
    "timestamp_s": TimestampSecondNTZType,
    "interval": DayTimeIntervalType,
    "list": ArrayType,
    "struct": StructType,
    "map": MapType,
    # union
    # enum
    # null (???)
    "float": FloatType,
    "double": DoubleType,
    "decimal": DecimalType,
}


def convert_nested_type(dtype: DuckDBPyType) -> DataType:  # noqa: D103
    id = dtype.id
    if id == "list" or id == "array":
        children = dtype.children
        return ArrayType(convert_type(children[0][1]))
    if id == "union":
        msg = (
            "Union types are not supported in the PySpark interface. "
            "DuckDB union types cannot be directly mapped to PySpark types."
        )
        raise ContributionsAcceptedError(msg)
    if id == "struct":
        children: list[tuple[str, DuckDBPyType]] = dtype.children
        fields = [StructField(x[0], convert_type(x[1])) for x in children]
        return StructType(fields)
    if id == "map":
        return MapType(convert_type(dtype.key), convert_type(dtype.value))
    raise NotImplementedError


def convert_type(dtype: DuckDBPyType) -> DataType:  # noqa: D103
    id = dtype.id
    if id in ["list", "struct", "map", "array"]:
        return convert_nested_type(dtype)
    if id == "decimal":
        children: list[tuple[str, DuckDBPyType]] = dtype.children
        precision = cast("int", children[0][1])
        scale = cast("int", children[1][1])
        return DecimalType(precision, scale)
    spark_type = _sqltype_to_spark_class[id]
    return spark_type()


def duckdb_to_spark_schema(names: list[str], types: list[DuckDBPyType]) -> StructType:  # noqa: D103
    fields = [StructField(name, dtype) for name, dtype in zip(names, [convert_type(x) for x in types])]
    return StructType(fields)
