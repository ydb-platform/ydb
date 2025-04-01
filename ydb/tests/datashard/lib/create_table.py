from ydb.tests.stress.oltp_workload.workload import cleanup_type_name

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


def create_table(table_name: str, columns: dict[str, dict[str]], pk_colums: dict[str, dict[str]], index_colums: dict[str, dict[str]], unique: str, sync: str) -> str:
    sql_create = f"CREATE TABLE {table_name} ("
    for prefix in columns.keys():
        for type_name in columns[prefix]:
            if prefix != "ttl_" or type_name != "":
                sql_create += f"{prefix}{cleanup_type_name(type_name)} {type_name}, "
    sql_create += "PRIMARY KEY( "
    for prefix in pk_colums.keys():
        for type_name in pk_colums[prefix]:
            if prefix != "ttl_" or type_name != "":
                sql_create += f"{prefix}{cleanup_type_name(type_name)},"
    sql_create = sql_create[:-1]
    sql_create += ")"
    for prefix in index_colums.keys():
        for type_name in index_colums[prefix]:
            if prefix != "ttl_" or type_name != "":
                sql_create += f", INDEX idx_{prefix}{cleanup_type_name(type_name)} GLOBAL {unique} {sync} ON ({prefix}{cleanup_type_name(type_name)})"
    sql_create += ")"
    return sql_create


def create_ttl(ttl: str, inteval: dict[str, str], time: str, table_name: str) -> str:
    sql_ttl = f" ALTER TABLE {table_name} SET ( TTL = "
    lenght_interval = len(inteval)
    count = 1
    for pt in inteval.keys():
        sql_ttl += f"""Interval("{pt}") {inteval[pt] if inteval[pt] == "" or inteval[pt] == "DELETE" else f"TO EXTERNAL DATA SOURCE {inteval[pt]}"}
                {", " if count != lenght_interval else " "}"""
        count += 1
    sql_ttl += f""" ON {ttl} {f"AS {time}" if time != "" else ""} ); """
    return sql_ttl
