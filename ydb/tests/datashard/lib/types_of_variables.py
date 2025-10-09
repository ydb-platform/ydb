from uuid import UUID
from datetime import datetime
import ydb


def format_sql_value(value, type_name, unwrap_after_cast: bool = False):
    """
    Format a value for SQL insertion.

    Args:
        value: The value to format.
        type_name: The type of the value.
        unwrap_after_cast: Whether to unwrap the value after casting. Unwrap makes the value type not nullable.
    """
    if type_name == "Datetime64" or type_name == "Datetime":
        value = value.strftime("%Y-%m-%dT%H:%M:%SZ")
    if type_name == "String" or type_name == "Utf8":
        return f"'{value}'"

    casted_value = (
        # Use quoted values for types that require string representation
        f"CAST('{value}' AS {type_name})"
        if type_name in types_requiring_quotes_in_cast
        # Use unquoted values for numeric and other types
        else f"CAST({value} AS {type_name})"
    )
    return f"Unwrap({casted_value})" if unwrap_after_cast else casted_value


def generate_date_value(i):
    """
    Generate a Date value for YDB standard Date type.

    YDB Date range: 1970-01-01 to 2105-12-31 (inclusive)
    - MIN_YEAR: 1970 (inclusive)
    - MAX_YEAR: 2106 (non-inclusive, so max valid year is 2105)
    - Based on YDB constants: MIN_YEAR=1970, MAX_YEAR=2106, MAX_DATE=49673
    """
    year = min(2000 + i, 2105)  # Clamp to YDB's standard Date range
    return datetime.strptime(f"{year}-01-01", "%Y-%m-%d").date()


def generate_datetime_value(i):
    """
    Generate a Datetime value for YDB standard Datetime type.

    YDB Datetime range: 1970-01-01 to 2105-12-31 (inclusive)
    - MIN_YEAR: 1970 (inclusive)
    - MAX_YEAR: 2106 (non-inclusive, so max valid year is 2105)
    - Based on YDB constants: MIN_YEAR=1970, MAX_YEAR=2106, MAX_DATETIME=86400*49673
    """
    year = min(2000 + i, 2105)  # Clamp to YDB's standard Datetime range
    return datetime.strptime(f"{year}-10-02T11:00:00Z", "%Y-%m-%dT%H:%M:%SZ")


def generate_date32_value(i):
    """
    Generate a Date32 value for YDB extended Date32 type.

    YDB Date32 range: much larger than standard Date type
    - MIN_YEAR32: -144169 (inclusive)
    - MAX_YEAR32: 148108 (non-inclusive, so max valid year is 148107)
    - MIN_DATE32: -53375809, MAX_DATE32: 53375807 (both inclusive)

    For test purposes, we use a reasonable subset of this range.
    """
    year = min(2000 + i, 10000)  # Use reasonable range for testing (up to year 10000)
    return datetime.strptime(f"{year}-01-01", "%Y-%m-%d").date()


def generate_datetime64_value(i):
    """
    Generate a Datetime64 value for YDB extended Datetime64 type.

    YDB Datetime64 range: much larger than standard Datetime type
    - MIN_YEAR32: -144169 (inclusive)
    - MAX_YEAR32: 148108 (non-inclusive, so max valid year is 148107)
    - MIN_DATETIME64: -4611669897600, MAX_DATETIME64: 4611669811199

    For test purposes, we use a reasonable subset of this range.
    """
    year = min(2000 + i, 10000)  # Use reasonable range for testing (up to year 10000)
    return datetime.strptime(f"{year}-10-02T11:00:00Z", "%Y-%m-%dT%H:%M:%SZ")


def cleanup_type_name(type_name):
    return type_name.replace("(", "").replace(")", "").replace(",", "")


# Types that require quoted values in CAST operations
types_requiring_quotes_in_cast = {
    # String-based types
    "Json",
    "JsonDocument",
    "Yson",
    # Date/time types that need string formatting
    "Date",
    "Datetime",
    "Date32",
    "Datetime64",
    # Special types
    "UUID",
    "DyNumber",
    # Decimal types
    "Decimal(15,0)",
    "Decimal(22,9)",
    "Decimal(35,10)",
}

ttl_types = {
    "DyNumber": lambda i: float("3742656{:03}e10".format(i)),
    "Uint32": lambda i: 3742656000 + i,
    "Uint64": lambda i: 3742656000 + i,
    "Date": generate_date_value,
    "Datetime": generate_datetime_value,
    "Timestamp": lambda i: 2696200000000000 + i * 100000000,
}

index_zero_sync = {
    "Int64": lambda i: i,
    "Int32": lambda i: i,
    "Int16": lambda i: i,
}

index_first_sync = {
    "Uint64": lambda i: i,
    "Uint32": lambda i: i,
    "Uint16": lambda i: i,
}

index_second_sync = {
    "DyNumber": lambda i: float(f"{i}e1"),
    "String": lambda i: f"String {i}",
    "Utf8": lambda i: f"Utf8 {i}",
    "UUID": lambda i: UUID("3{:03}5678-e89b-12d3-a456-556642440000".format(i)),
    "Date": generate_date_value,
    "Datetime": generate_datetime_value,
}

index_three_sync = {
    "Bool": lambda i: bool(i),
    "Decimal(15,0)": lambda i: "{}".format(i),
    "Decimal(22,9)": lambda i: "{}.123".format(i),
    "Decimal(35,10)": lambda i: "{}.123456".format(i),
    "Date32": generate_date32_value,
    "Datetime64": generate_datetime64_value,
}

index_three_sync_not_Bool = {
    "Decimal(15,0)": lambda i: "{}".format(i),
    "Decimal(22,9)": lambda i: "{}.123".format(i),
    "Decimal(35,10)": lambda i: "{}.123456".format(i),
    "Date32": generate_date32_value,
    "Datetime64": generate_datetime64_value,
}

index_four_sync = {
    "Timestamp64": lambda i: 1696200000000000 + i * 100000000,
    "Interval64": lambda i: i,
    "Timestamp": lambda i: 1696200000000000 + i * 100000000,
    "Interval": lambda i: i,
    "Int8": lambda i: i,
    "Uint8": lambda i: i,
}

index_first_not_Bool = {
    "Int64": lambda i: i,
    "Uint64": lambda i: i,
    "Int32": lambda i: i,
    "Uint32": lambda i: i,
    "Int16": lambda i: i,
    "Uint16": lambda i: i,
    "Int8": lambda i: i,
    "Uint8": lambda i: i,
    "Decimal(15,0)": lambda i: "{}".format(i),
    "Decimal(22,9)": lambda i: "{}.123".format(i),
    "Decimal(35,10)": lambda i: "{}.123456".format(i),
}

index_first = {
    "Int64": lambda i: i,
    "Uint64": lambda i: i,
    "Int32": lambda i: i,
    "Uint32": lambda i: i,
    "Int16": lambda i: i,
    "Uint16": lambda i: i,
    "Int8": lambda i: i,
    "Uint8": lambda i: i,
    "Bool": lambda i: bool(i),
    "Decimal(15,0)": lambda i: "{}".format(i),
    "Decimal(22,9)": lambda i: "{}.123".format(i),
    "Decimal(35,10)": lambda i: "{}.123456".format(i),
}

index_second = {
    "DyNumber": lambda i: float(f"{i}e1"),
    "String": lambda i: f"String {i}",
    "Utf8": lambda i: f"Utf8 {i}",
    "UUID": lambda i: UUID("3{:03}5678-e89b-12d3-a456-556642440000".format(i)),
    "Date": generate_date_value,
    "Datetime": generate_datetime_value,
    "Timestamp": lambda i: 1696200000000000 + i * 100000000,
    "Interval": lambda i: i,
    "Date32": generate_date32_value,
    "Datetime64": generate_datetime64_value,
    "Timestamp64": lambda i: 1696200000000000 + i * 100000000,
    "Interval64": lambda i: i,
}

null_types = {
    "Int64": lambda i: i,
    "Decimal(22,9)": lambda i: "{}.123".format(i),
    "Decimal(35,10)": lambda i: "{}.123456".format(i),
    "String": lambda i: f"String {i}",
}

pk_types = {
    "Int64": lambda i: i,
    "Uint64": lambda i: i,
    "Int32": lambda i: i,
    "Uint32": lambda i: i,
    "Int16": lambda i: i,
    "Uint16": lambda i: i,
    "Int8": lambda i: i,
    "Uint8": lambda i: i,
    "Bool": lambda i: bool(i),
    "Decimal(15,0)": lambda i: "{}".format(i),
    "Decimal(22,9)": lambda i: "{}.123".format(i),
    "Decimal(35,10)": lambda i: "{}.123456".format(i),
    "DyNumber": lambda i: float(f"{i}e1"),
    "String": lambda i: f"String {i}",
    "Utf8": lambda i: f"Utf8 {i}",
    "UUID": lambda i: UUID("3{:03}5678-e89b-12d3-a456-556642440000".format(i)),
    "Date": generate_date_value,
    "Datetime": generate_datetime_value,
    "Timestamp": lambda i: 1696200000000000 + i * 100000000,
    "Interval": lambda i: i,
    "Date32": generate_date32_value,
    "Datetime64": generate_datetime64_value,
    "Timestamp64": lambda i: 1696200000000000 + i * 100000000,
    "Interval64": lambda i: i,
}

non_pk_types = {
    "Float": lambda i: i + 0.1,
    "Double": lambda i: i + 0.2,
    "Json": lambda i: '{{"another_key": {}}}'.format(i),
    "JsonDocument": lambda i: '{{"another_doc_key": {}}}'.format(i),
    "Yson": lambda i: "[{}]".format(i),
}

types_not_supported_yet_in_columnshard = {
    "Bool",
    "DyNumber",
    "UUID",
    "Interval"
}

non_comparable_types = {
    "Yson",
    "Json",
    "JsonDocument",
}

primitive_type = {
    "Int64": ydb.PrimitiveType.Int64,
    "Uint64": ydb.PrimitiveType.Uint64,
    "Int32": ydb.PrimitiveType.Int32,
    "Uint32": ydb.PrimitiveType.Uint32,
    "Int16": ydb.PrimitiveType.Int16,
    "Uint16": ydb.PrimitiveType.Uint16,
    "Int8": ydb.PrimitiveType.Int8,
    "Uint8": ydb.PrimitiveType.Uint8,
    "Bool": ydb.PrimitiveType.Bool,
    "DyNumber": ydb.PrimitiveType.DyNumber,
    "String": ydb.PrimitiveType.String,
    "Utf8": ydb.PrimitiveType.Utf8,
    "UUID": ydb.PrimitiveType.UUID,
    "Date": ydb.PrimitiveType.Date,
    "Datetime": ydb.PrimitiveType.Datetime,
    "Timestamp": ydb.PrimitiveType.Timestamp,
    "Interval": ydb.PrimitiveType.Interval,
    "Float": ydb.PrimitiveType.Float,
    "Double": ydb.PrimitiveType.Double,
    "Json": ydb.PrimitiveType.Json,
    "JsonDocument": ydb.PrimitiveType.JsonDocument,
    "Yson": ydb.PrimitiveType.Yson,
    "Date32": ydb.PrimitiveType.Date32,
    "Datetime64": ydb.PrimitiveType.Datetime64,
    "Timestamp64": ydb.PrimitiveType.Timestamp64,
    "Interval64": ydb.PrimitiveType.Interval64,
}

type_to_literal_lambda = {
    "Int64": lambda i: i,
    "Uint64": lambda i: i,
    "Int32": lambda i: i,
    "Uint32": lambda i: i,
    "Int16": lambda i: i,
    "Uint16": lambda i: i,
    "Int8": lambda i: i,
    "Uint8": lambda i: i,
    "Bool": lambda i: bool(i),
    "Decimal(15,0)": lambda i: f"Decimal('{i}', 15, 0)",
    "Decimal(22,9)": lambda i: f"Decimal('{i}.123', 22, 9)",
    "Decimal(35,10)": lambda i: f"Decimal('{i}.123456', 35, 10)",
    "DyNumber": lambda i: f"DyNumber('{float(str(i) + 'e1')}')",
    "String": lambda i: f"'String {i}'",
    "Utf8": lambda i: f"'Utf8 {i}'",
    "UUID": lambda i: f"Uuid('3{i:03}5678-e89b-12d3-a456-556642440000')",
    "Date": lambda i: f"Date('{generate_date_value(i).strftime("%Y-%m-%d")}')",
    "Datetime": lambda i: f"Datetime('{generate_datetime_value(i).strftime("%Y-%m-%dT%H:%M:%SZ")}')",
    "Timestamp": lambda i: f"Timestamp('{datetime.fromtimestamp(1696200000 + i * 100).strftime("%Y-%m-%dT%H:%M:%SZ")}')",
    "Interval": lambda i: f"Interval('P{i}D')",
    "Date32": lambda i: f"Date32('{generate_date32_value(i).strftime("%Y-%m-%d")}')",
    "Datetime64": lambda i: f"Datetime64('{generate_datetime64_value(i).strftime("%Y-%m-%dT%H:%M:%SZ")}')",
    "Timestamp64": lambda i: f"Timestamp64('{datetime.fromtimestamp(1696200000 + i * 100).strftime("%Y-%m-%dT%H:%M:%SZ")}')",
    "Interval64": lambda i: f"Interval64('P{i}D')",
    "Float": lambda i: f"{i + 0.1}f",
    "Double": lambda i: i + 0.2,
    "Json": lambda i: f"Json('{{\"another_key\": {i}}}')",
    "JsonDocument": lambda i: f"JsonDocument('{{\"another_doc_key\": {i}}}')",
    "Yson": lambda i: f"Yson('[{i}]')",
}
