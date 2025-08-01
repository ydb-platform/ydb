from uuid import UUID
from datetime import datetime


def cleanup_type_name(type_name):
    return type_name.replace('(', '').replace(')', '').replace(',', '')


def format_sql_value(value, type_name):
    if type_name == "Datetime64" or type_name == "Datetime":
        value = value.strftime("%Y-%m-%dT%H:%M:%SZ")
    if type_name == "String" or type_name == "Utf8":
        return f"'{value}'"
    if type_name in non_pk_types.keys() or type_name == "Datetime64" or type_name == "Date32" or type_name == "Datetime" or \
            type_name == "Date" or type_name == "UUID" or type_name == "DyNumber" or type_name == "Decimal(35,10)" or type_name == "Decimal(22,9)" or type_name == "Decimal(15,0)":
        return f"CAST('{value}' AS {type_name})"
    return f"CAST({value} AS {type_name})"


ttl_types = {
    "DyNumber": lambda i: float("3742656{:03}e10".format(i)),
    "Uint32": lambda i: 3742656000 + i,
    "Uint64": lambda i: 3742656000 + i,
    "Date": lambda i: datetime.strptime("2{:03}-01-01".format(i), "%Y-%m-%d").date(),
    "Datetime": lambda i: datetime.strptime("2{:03}-10-02T11:00:00Z".format(i), "%Y-%m-%dT%H:%M:%SZ"),
    "Timestamp": lambda i: 2696200000000000 + i * 100000000,
}

index_zero_sync = {
    "Int64": lambda i: i,
    "Int32": lambda i: i,
    # "Int16": lambda i: i, https://github.com/ydb-platform/ydb/issues/15842
}

index_first_sync = {
    "Uint64": lambda i: i,
    "Uint32": lambda i: i,
    # "Uint16": lambda i: i,  https://github.com/ydb-platform/ydb/issues/15842
}

index_second_sync = {
    "DyNumber": lambda i: float(f"{i}e1"),
    "String": lambda i: f"String {i}",
    "Utf8": lambda i: f"Utf8 {i}",
    "UUID": lambda i: UUID("3{:03}5678-e89b-12d3-a456-556642440000".format(i)),
    "Date": lambda i: datetime.strptime("2{:03}-01-01".format(i), "%Y-%m-%d").date(),
    "Datetime": lambda i: datetime.strptime("2{:03}-10-02T11:00:00Z".format(i), "%Y-%m-%dT%H:%M:%SZ"),
}

index_three_sync = {
    "Bool": lambda i: bool(i),
    "Decimal(15,0)": lambda i: "{}".format(i),
    "Decimal(22,9)": lambda i: "{}.123".format(i),
    "Decimal(35,10)": lambda i: "{}.123456".format(i),
    "Date32": lambda i: datetime.strptime("2{:03}-01-01".format(i), "%Y-%m-%d").date(),
    "Datetime64": lambda i: datetime.strptime("2{:03}-10-02T11:00:00Z".format(i), "%Y-%m-%dT%H:%M:%SZ"),
}

index_three_sync_not_Bool = {
    "Decimal(15,0)": lambda i: "{}".format(i),
    "Decimal(22,9)": lambda i: "{}.123".format(i),
    "Decimal(35,10)": lambda i: "{}.123456".format(i),
    "Date32": lambda i: datetime.strptime("2{:03}-01-01".format(i), "%Y-%m-%d").date(),
    "Datetime64": lambda i: datetime.strptime("2{:03}-10-02T11:00:00Z".format(i), "%Y-%m-%dT%H:%M:%SZ"),
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
    # "Int16": lambda i: i, https://github.com/ydb-platform/ydb/issues/15842
    # "Uint16": lambda i: i, https://github.com/ydb-platform/ydb/issues/15842
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
    # "Int16": lambda i: i, https://github.com/ydb-platform/ydb/issues/15842
    # "Uint16": lambda i: i, https://github.com/ydb-platform/ydb/issues/15842
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
    "Date": lambda i: datetime.strptime("2{:03}-01-01".format(i), "%Y-%m-%d").date(),
    "Datetime": lambda i: datetime.strptime("2{:03}-10-02T11:00:00Z".format(i), "%Y-%m-%dT%H:%M:%SZ"),
    "Timestamp": lambda i: 1696200000000000 + i * 100000000,
    "Interval": lambda i: i,
    "Date32": lambda i: datetime.strptime("2{:03}-01-01".format(i), "%Y-%m-%d").date(),
    "Datetime64": lambda i: datetime.strptime("2{:03}-10-02T11:00:00Z".format(i), "%Y-%m-%dT%H:%M:%SZ"),
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

    "Date": lambda i: datetime.strptime("2{:03}-01-01".format(i), "%Y-%m-%d").date(),
    "Datetime": lambda i: datetime.strptime("2{:03}-10-02T11:00:00Z".format(i), "%Y-%m-%dT%H:%M:%SZ"),
    "Timestamp": lambda i: 1696200000000000 + i * 100000000,
    "Interval": lambda i: i,
    "Date32": lambda i: datetime.strptime("2{:03}-01-01".format(i), "%Y-%m-%d").date(),
    "Datetime64": lambda i: datetime.strptime("2{:03}-10-02T11:00:00Z".format(i), "%Y-%m-%dT%H:%M:%SZ"),
    "Timestamp64": lambda i: 1696200000000000 + i * 100000000,
    "Interval64": lambda i: i,
}

non_pk_types = {
    "Float": lambda i: i + 0.1,
    "Double": lambda i: i + 0.2,
    "Json": lambda i: "{{\"another_key\": {}}}".format(i),
    "JsonDocument": lambda i: "{{\"another_doc_key\": {}}}".format(i),
    "Yson": lambda i: "[{}]".format(i)
}
