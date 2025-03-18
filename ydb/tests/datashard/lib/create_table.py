from ydb.tests.stress.oltp_workload.workload import cleanup_type_name

ttl_types = {
    "Date": "CAST('20{:02}-01-01' AS Date)",
    "Datetime": "CAST('20{:02}-10-02T11:00:00Z' AS Datetime)",
    "Timestamp": "CAST(269624{:02}00000000 AS Timestamp)",
    "Uint32": "CAST({} AS Uint32)",
    "Uint64": "CAST({} AS Uint64)",
    "DyNumber": "CAST({} AS DyNumber)",
}


# Отсюда убраны uint16 и int16 потому что при поиске по данным индексам выдает ошибку
# ydb/core/kqp/provider/yql_kikimr_provider.cpp:685 FillLiteralProtoImpl(): requirement false failed, message: Unexpected type slot Int16 (или Uint16)
index_first = {
    "Int64": "CAST({} AS Int64)",
    "Uint64": "CAST({} AS Uint64)",
    "Int32": "CAST({} AS Int32)",
    "Uint32": "CAST({} AS Uint32)",
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
    "Utf8": "'Uft8 {}'",
    "Uuid": "CAST('{:2}345678-e89b-12d3-a456-556642440000' AS UUID)",
    "Date": "CAST('20{:02}-01-01' AS Date)",
    "Datetime": "CAST('20{:02}-10-02T11:00:00Z' AS Datetime)",
    "Timestamp": "CAST(169624{:02}00000000 AS Timestamp)",
    "Interval": "CAST({} AS Interval)",
    "Date32": "CAST('20{:02}-01-01' AS Date32)",
    "Datetime64": "CAST('20{:02}-10-02T11:00:00Z' AS Datetime64)",
    "Timestamp64": "CAST(169624{:02}00000000 AS Timestamp64)",
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
    "Utf8": "'Uft8 {}'",
    "Uuid": "CAST('{:2}345678-e89b-12d3-a456-556642440000' AS UUID)",

    "Date": "CAST('20{:02}-01-01' AS Date)",
    "Datetime": "CAST('20{:02}-10-02T11:00:00Z' AS Datetime)",
    "Timestamp": "CAST(169624{:02}00000000 AS Timestamp)",
    "Interval": "CAST({} AS Interval)",
    "Date32": "CAST('20{:02}-01-01' AS Date32)",
    "Datetime64": "CAST('20{:02}-10-02T11:00:00Z' AS Datetime64)",
    "Timestamp64": "CAST(169624{:02}00000000 AS Timestamp64)",
    "Interval64": "CAST({} AS Interval64)"
}
non_pk_types = {
    "Float": "CAST('{}.1' AS Float)",
    "Double": "CAST('{}.2' AS Double)",
    "Json": "CAST('{{\"another_key\":{}}}' AS Json)",
    "JsonDocument": "CAST('{{\"another_doc_key\":{}}}' AS JsonDocument)",
    "Yson": "CAST('[{}]' AS Yson)"
}
unique = ["", "UNIQUE"]
index_sync = ["SYNC", "ASYNC"]


class TestCreateTables():
    def create_table(self, ttl: str, table_name: str, index: dict[str, str], unique: str, sync: str) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                    {", ".join(["pk_" + cleanup_type_name(type_name) + " " + type_name for type_name in pk_types.keys()])},
                    {", ".join(["col_" + cleanup_type_name(type_name) + " " + type_name for type_name in pk_types.keys()])},
                    {", ".join(["col_" + cleanup_type_name(type_name) + " " + type_name for type_name in non_pk_types.keys()])},
                    {", ".join(["col_index_" + cleanup_type_name(type_name) + " " + type_name for type_name in index.keys()])},
                    ttl_{cleanup_type_name(ttl)} {ttl},
                    PRIMARY KEY(
                    {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])}),
                    {", ".join([f"INDEX idx_{cleanup_type_name(type_name)} GLOBAL {unique} {sync} ON (col_index_{cleanup_type_name(type_name)})" for type_name in index.keys()])},
                    ) WITH (
                    TTL = Interval("PT1H") ON ttl_{cleanup_type_name(ttl)} {"as SECONDS" if ttl == "Uint32" or ttl == "Uint64" or ttl == "DyNumber" else ""}
                );

        """
