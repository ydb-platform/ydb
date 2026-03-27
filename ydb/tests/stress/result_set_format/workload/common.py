from dataclasses import dataclass
from typing import List, Any, Dict, Optional
from decimal import Decimal
from uuid import UUID
import pyarrow as pa
import random
import ydb

from ydb.tests.datashard.lib.types_of_variables import string_to_ydb_type

# Non-inclusive limits from: yql/essentials/public/udf/udf_data_type.h
MAX_DATE = 49673
MAX_DATETIME = 86400 * 49673
MAX_TIMESTAMP = 86400000000 * 49673
MAX_INTERVAL = MAX_TIMESTAMP
MIN_DATE32 = -53375810
MIN_DATETIME64 = -4611669897601
MIN_TIMESTAMP64 = -4611669897600000001
MAX_DATE32 = 53375808
MAX_DATETIME64 = 4611669811200
MAX_TIMESTAMP64 = 4611669811200000000
MAX_INTERVAL64 = MAX_TIMESTAMP64 - MIN_TIMESTAMP64 - 1

primitive_type_to_arrow_type: Dict[str, pa.DataType] = {
    "Int64": pa.int64(),
    "Uint64": pa.uint64(),
    "Int32": pa.int32(),
    "Uint32": pa.uint32(),
    "Int16": pa.int16(),
    "Uint16": pa.uint16(),
    "Int8": pa.int8(),
    "Uint8": pa.uint8(),
    "Bool": pa.uint8(),
    "Decimal(15,0)": pa.binary(16),
    "Decimal(22,9)": pa.binary(16),
    "Decimal(35,10)": pa.binary(16),
    "DyNumber": pa.string(),
    "String": pa.binary(),
    "Utf8": pa.string(),
    "UUID": pa.binary(16),
    "Date": pa.uint16(),
    "Datetime": pa.uint32(),
    "Timestamp": pa.uint64(),
    "Interval": pa.int64(),
    "Date32": pa.int32(),
    "Datetime64": pa.int64(),
    "Timestamp64": pa.int64(),
    "Interval64": pa.int64(),
    "Float": pa.float32(),
    "Double": pa.float64(),
    "Json": pa.string(),
    "JsonDocument": pa.string(),
    "Yson": pa.binary(),
}


@dataclass
class ColumnInfo:
    """Information about a table column including name, type, and nullability"""

    name: str
    type_name: str
    not_null: bool

    def ydb_type(self):
        """Convert column type to YDB type, wrapping in OptionalType if nullable"""

        result = string_to_ydb_type[self.type_name]
        if not self.not_null:
            result = ydb.OptionalType(result)
        return result

    def arrow_type(self):
        """Convert column type to PyArrow type for Arrow format result sets"""

        if self.type_name in primitive_type_to_arrow_type:
            return primitive_type_to_arrow_type[self.type_name]
        raise ValueError(f"Cannot convert column type to Arrow type: {self.type_name}")

    def to_string(self):
        """Generate SQL schema string for the column (e.g., 'col_name Type NOT NULL')"""

        result = f"{self.name} {self.type_name}"
        if self.not_null:
            result += " NOT NULL"
        return result


@dataclass
class TableInfo:
    """Complete table metadata including schema, primary keys, and partitioning"""

    path: str
    columns: List[ColumnInfo]
    primary_key_columns: List[str]
    min_partitions: int

    def get_column_by_name(self, name: str) -> Optional[ColumnInfo]:
        """Get column by name from table schema"""

        for col in self.columns:
            if col.name == name:
                return col
        return None

    def create_sql(self):
        """Generate CREATE TABLE SQL statement with schema and partitioning settings"""

        return f"""
            CREATE TABLE `{self.path}` (
                {", ".join([col.to_string() for col in self.columns])},
                PRIMARY KEY ({", ".join([f"{col}" for col in self.primary_key_columns])}),
            ) WITH (
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {self.min_partitions}
            );
        """

    def drop_sql(self):
        """Generate DROP TABLE SQL statement"""

        return f"DROP TABLE `{self.path}`;"


class ParameterBuilder:
    """Builder for creating YDB query parameters from column schemas and row data"""

    @staticmethod
    def create_list(name: str, columns: List[ColumnInfo], rows: List[List[Any]]) -> Dict[str, Any]:
        """Create a list parameter for operations like UPSERT with multiple rows"""

        struct_type = ydb.StructType()
        for column in columns:
            struct_type.add_member(column.name, column.ydb_type())
        list_type = ydb.ListType(struct_type)

        rows_list = []
        for row in rows:
            row_dict = {}
            for column, value in zip(columns, row):
                row_dict[column.name] = value
            rows_list.append(row_dict)

        return {name: ydb.TypedValue(rows_list, list_type)}

    @staticmethod
    def create_struct(
        name: str,
        columns: List[ColumnInfo],
        rows: List[List[Any]],
    ) -> Dict[str, Any]:
        """Create a struct parameter for single-row operations"""

        param_value = {}
        for column, value in zip(columns, rows):
            param_value[column.name] = value

        struct_type = ydb.StructType()
        for column in columns:
            struct_type.add_member(column.name, column.ydb_type())

        return {name: ydb.TypedValue(param_value, struct_type)}


def generate_value_by_type(column: ColumnInfo, max_value: int, null_probability: float) -> Any:
    """Generate a random value for the given column type respecting type constraints and null probability"""

    if not column.not_null and random.random() < null_probability:
        return None

    int_types = {
        "Uint8": (8, False),
        "Int8": (8, True),
        "Uint16": (16, False),
        "Int16": (16, True),
        "Uint32": (32, False),
        "Int32": (32, True),
        "Uint64": (64, False),
        "Int64": (64, True),
    }

    date_types = {
        "Date": MAX_DATE,
        "Datetime": MAX_DATETIME,
        "Timestamp": MAX_TIMESTAMP,
        "Interval": MAX_INTERVAL,
        # ydb-python-sdk returns datetime objects, but they are not supported for these types
        # so we use basic limits instead the correct (extended) limits
        "Date32": MAX_DATE,
        "Datetime64": MAX_DATETIME,
        "Timestamp64": MAX_TIMESTAMP,
        "Interval64": MAX_INTERVAL,
    }

    seed_value = random.randint(0, max_value)

    if column.type_name in int_types:
        bits, is_signed = int_types[column.type_name]
        limit = 2**bits
        if is_signed:
            min_val = max(-(limit // 2), -(max_value // 2))
            max_val = min((limit // 2) - 1, max_value // 2)
            return random.randint(min_val, max_val)
        else:
            return random.randint(0, max_value % limit)
    elif column.type_name == "Bool":
        return random.choice([True, False])
    elif column.type_name in ["String", "Utf8"]:
        length = random.randint(1, 100)
        chars = "".join(chr(i) for i in range(ord("a"), ord("z") + 1))
        return "".join(random.choice(chars) for _ in range(length)).encode()
    elif column.type_name == "Float":
        return float(seed_value)
    elif column.type_name == "Double":
        return float(seed_value)
    elif column.type_name in date_types:
        return seed_value % date_types[column.type_name]
    elif column.type_name == "Decimal(15,0)":
        return Decimal(seed_value % 1000000000000000)
    elif column.type_name == "Decimal(22,9)":
        return Decimal(f"{seed_value % 1000000000000}.{seed_value % 1000000000:09d}")
    elif column.type_name == "Decimal(35,10)":
        return Decimal(f"{seed_value % 1000000000000000000000000}.{seed_value % 10000000000:010d}")
    elif column.type_name == "DyNumber":
        return f"{seed_value % 1000000}e10"
    elif column.type_name == "UUID":
        return UUID(
            f"{seed_value % 0x100000000:08x}-{seed_value % 0x10000:04x}-4000-8000-{seed_value % 0x1000000000000:012x}"
        )
    elif column.type_name == "Json":
        return f'{{"key": {seed_value}}}'
    elif column.type_name == "JsonDocument":
        return f'{{"doc_key": {seed_value}}}'
    elif column.type_name == "Yson":
        return bytes(f"[{seed_value}]", "utf-8")
    else:
        raise ValueError(f"Unknown column type: {column.type_name}")


def generate_batch_rows(
    table_info: TableInfo, rows_count: int, max_pk_value: int, max_value: int, null_probability: float
) -> List[List[Any]]:
    """Generate multiple rows of random data for the given table schema"""

    batch_rows = []
    for _ in range(rows_count):
        row = []
        for col in table_info.columns:
            current_max_value = max_pk_value if col.name in table_info.primary_key_columns else max_value
            row.append(generate_value_by_type(col, current_max_value, null_probability))
        batch_rows.append(row)
    return batch_rows


def validate_arrow_result_set(result_set: ydb.convert.ResultSet, wait_schema: bool = True) -> None:
    """Validate that result set is properly formatted in Arrow format with expected fields populated"""

    assert result_set.format == ydb.QueryResultSetFormat.ARROW, "Result is not in ARROW format"

    # For arrow format, result must be filled to ResultSet.data and not to ResultSet.rows
    assert result_set.data is not None and len(result_set.data) != 0, "Arrow data is empty"

    # For arrow format, we wait for YQL and Arrow schemas to be filled
    if wait_schema:
        assert len(result_set.arrow_format_meta.schema) != 0, "Arrow schema is empty"
        assert result_set.columns is not None and len(result_set.columns) != 0, "YQL schema is empty"
    else:
        assert (
            result_set.arrow_format_meta is None
            or result_set.arrow_format_meta.schema is None
            or len(result_set.arrow_format_meta.schema) == 0
        ), "Arrow schema is not empty"
        assert result_set.columns is None or len(result_set.columns) == 0, "YQL schema is not empty"

    # Fields for other formats must be empty
    assert len(result_set.rows) == 0, "Some rows are present"


def validate_value_result_set(result_set: ydb.convert.ResultSet, wait_schema: bool = True) -> None:
    """Validate that result set is properly formatted in Value format with expected fields populated"""

    assert result_set.format == ydb.QueryResultSetFormat.VALUE, "Result is not in VALUE format"

    # For value format, we wait for YQL schema to be filled
    if wait_schema:
        assert result_set.columns is not None and len(result_set.columns) != 0, "YQL schema is empty"
    else:
        assert result_set.columns is None or len(result_set.columns) == 0, "YQL schema is not empty"

    # Fields for other formats must be empty
    assert result_set.data is None or len(result_set.data) == 0, "Some data is present"
    assert (
        result_set.arrow_format_meta is None
        or result_set.arrow_format_meta.schema is None
        or len(result_set.arrow_format_meta.schema) == 0
    )
