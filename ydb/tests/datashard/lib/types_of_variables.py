def cleanup_type_name(type_name):
    return type_name.replace('(', '').replace(')', '').replace(',', '')


ttl_types = {
    "DyNumber": "CAST('3742656{:03}' AS DyNumber)",
    "Uint32": "CAST(3742656{:03} AS Uint32)",
    "Uint64": "CAST(3742656{:03} AS Uint64)",
    "Date": "CAST('2{:03}-01-01' AS Date)",
    "Datetime": "CAST('2{:03}-10-02T11:00:00Z' AS Datetime)",
    "Timestamp": "CAST(26962{:03}00000000 AS Timestamp)",
}

index_zero_sync = {
    "Int64": "CAST({} AS Int64)",
    "Int32": "CAST({} AS Int32)",
    # "Int16": "CAST({} AS Int16)", https://github.com/ydb-platform/ydb/issues/15842
}

index_first_sync = {
    "Uint64": "CAST({} AS Uint64)",
    "Uint32": "CAST({} AS Uint32)",
    # "Uint16": "CAST({} AS Uint16)", https://github.com/ydb-platform/ydb/issues/15842
}

index_second_sync = {
    "DyNumber": "CAST('{}E1' AS DyNumber)",
    "String": "'String {}'",
    "Utf8": "'Utf8 {}'",
    "Uuid": "CAST('3{:03}5678-e89b-12d3-a456-556642440000' AS UUID)",
    "Date": "CAST('2{:03}-01-01' AS Date)",
    "Datetime": "CAST('2{:03}-10-02T11:00:00Z' AS Datetime)",
}

index_three_sync = {
    "Bool": "CAST({} AS Bool)",
    "Decimal(15,0)": "CAST('{}.0' AS Decimal(15,0))",
    "Decimal(22,9)": "CAST('{}.123' AS Decimal(22,9))",
    "Decimal(35,10)": "CAST('{}.123456' AS Decimal(35,10))",
    "Date32": "CAST('2{:03}-01-01' AS Date32)",
    "Datetime64": "CAST('2{:03}-10-02T11:00:00Z' AS Datetime64)",
}

index_three_sync_not_Bool = {
    "Decimal(15,0)": "CAST('{}.0' AS Decimal(15,0))",
    "Decimal(22,9)": "CAST('{}.123' AS Decimal(22,9))",
    "Decimal(35,10)": "CAST('{}.123456' AS Decimal(35,10))",
    "Date32": "CAST('2{:03}-01-01' AS Date32)",
    "Datetime64": "CAST('2{:03}-10-02T11:00:00Z' AS Datetime64)",
}

index_four_sync = {
    "Timestamp64": "CAST(16962{:03}00000000 AS Timestamp64)",
    "Interval64": "CAST({} AS Interval64)",
    "Timestamp": "CAST(16962{:03}00000000 AS Timestamp)",
    "Interval": "CAST({} AS Interval)",
    "Int8": "CAST({} AS Int8)",
    "Uint8": "CAST({} AS Uint8)",
}


index_first = {
    "Int64": "CAST({} AS Int64)",
    "Uint64": "CAST({} AS Uint64)",
    "Int32": "CAST({} AS Int32)",
    "Uint32": "CAST({} AS Uint32)",
    # "Int16": "CAST({} AS Int16)", https://github.com/ydb-platform/ydb/issues/15842
    # "Uint16": "CAST({} AS Uint16)", https://github.com/ydb-platform/ydb/issues/15842
    "Int8": "CAST({} AS Int8)",
    "Uint8": "CAST({} AS Uint8)",
    "Bool": "CAST({} AS Bool)",
    "Decimal(15,0)": "CAST('{}.0' AS Decimal(15,0))",
    "Decimal(22,9)": "CAST('{}.123' AS Decimal(22,9))",
    "Decimal(35,10)": "CAST('{}.123456' AS Decimal(35,10))",
}

index_second = {
    "DyNumber": "CAST('{}E1' AS DyNumber)",
    "String": "'String {}'",
    "Utf8": "'Utf8 {}'",
    "Uuid": "CAST('3{:03}5678-e89b-12d3-a456-556642440000' AS UUID)",
    "Date": "CAST('2{:03}-01-01' AS Date)",
    "Datetime": "CAST('2{:03}-10-02T11:00:00Z' AS Datetime)",
    "Timestamp": "CAST(16962{:03}00000000 AS Timestamp)",
    "Interval": "CAST({} AS Interval)",
    "Date32": "CAST('2{:03}-01-01' AS Date32)",
    "Datetime64": "CAST('2{:03}-10-02T11:00:00Z' AS Datetime64)",
    "Timestamp64": "CAST(16962{:03}00000000 AS Timestamp64)",
    "Interval64": "CAST({} AS Interval64)"
}

pk_types = {
    "Int64": "CAST({} AS Int64)",
    "Uint64": "CAST({} AS Uint64)",
    "Int32": "CAST({} AS Int32)",
    "Uint32": "CAST({} AS Uint32)",
    "Int16": "CAST({} AS Int16)",
    "Uint16": "CAST({} AS Uint16)",
    "Int8": "CAST({} AS Int8)",
    "Uint8": "CAST({} AS Uint8)",
    "Bool": "CAST({} AS Bool)",
    "Decimal(15,0)": "CAST('{}.0' AS Decimal(15,0))",
    "Decimal(22,9)": "CAST('{}.123' AS Decimal(22,9))",
    "Decimal(35,10)": "CAST('{}.123456' AS Decimal(35,10))",
    "DyNumber": "CAST('{}E1' AS DyNumber)",

    "String": "'String {}'",
    "Utf8": "'Utf8 {}'",
    "Uuid": "CAST('3{:03}5678-e89b-12d3-a456-556642440000' AS UUID)",

    "Date": "CAST('2{:03}-01-01' AS Date)",
    "Datetime": "CAST('2{:03}-10-02T11:00:00Z' AS Datetime)",
    "Timestamp": "CAST(16962{:03}00000000 AS Timestamp)",
    "Interval": "CAST({} AS Interval)",
    "Date32": "CAST('2{:03}-01-01' AS Date32)",
    "Datetime64": "CAST('2{:03}-10-02T11:00:00Z' AS Datetime64)",
    "Timestamp64": "CAST(16962{:03}00000000 AS Timestamp64)",
    "Interval64": "CAST({} AS Interval64)"
}
non_pk_types = {
    "Float": "CAST('{}.1' AS Float)",
    "Double": "CAST('{}.2' AS Double)",
    "Json": "CAST('{{\"another_key\":{}}}' AS Json)",
    "JsonDocument": "CAST('{{\"another_doc_key\":{}}}' AS JsonDocument)",
    "Yson": "CAST('[{}]' AS Yson)"
}

null_types = {
    "Int64": "CAST({} AS Int64)",
    "Decimal(22,9)": "CAST('{}.123' AS Decimal(22,9))",
    "Decimal(35,10)": "CAST('{}.123456' AS Decimal(35,10))",
    "String": "'{}'",
}
