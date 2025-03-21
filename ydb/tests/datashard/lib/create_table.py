from ydb.tests.stress.oltp_workload.workload import cleanup_type_name

ttl_types = {
    "Date": "CAST('2{:03}-01-01' AS Date)",
    "Datetime": "CAST('2{:03}-10-02T11:00:00Z' AS Datetime)",
    "Timestamp": "CAST(26962{:03}00000000 AS Timestamp)",
    "Uint32": "CAST({}0 AS Uint32)",
    "Uint64": "CAST({}0 AS Uint64)",
    "DyNumber": "CAST('{}E1' AS DyNumber)",
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
    "Utf8": "'Uft8 {}'",
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

unique = ["", "UNIQUE"]
index_sync = ["SYNC", "ASYNC"]


class TestCreateTables():
    def create_table(self, table_name: str, columns: dict[str, dict[str]], unique: str, sync: str) -> str:
        sql_create = f"CREATE TABLE {table_name} ("
        for prefix in columns.keys():
            for type_name in columns[prefix]:
                if prefix != "ttl_" and type_name != "":
                    sql_create += f"{prefix}{cleanup_type_name(type_name)} {type_name}, "
        sql_create += f"""PRIMARY KEY(
                    {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in columns["pk_"]])}),
                    {", ".join([f"INDEX idx_{cleanup_type_name(type_name)} GLOBAL {unique} {sync} ON (col_index_{cleanup_type_name(type_name)})" for type_name in columns["col_index_"]] if len(columns["col_index_"]) != 0 else "")}
            )"""
        return sql_create

    def create_ttl(self, ttl: str, inteval: dict[str, str], time: str, table_name: str) -> str:
        sql_ttl = f"""
        ALTER TABLE {table_name} SET ( TTL =
        """
        lenght_inteval = len(inteval)
        count = 1
        for pt in inteval.keys():
            sql_ttl += f"""
                Interval("{pt}" {inteval[pt] if inteval[pt] == "" or inteval[pt] == "DELETE" else f"TO EXTERNAL DATA SOURCE {inteval[pt]}"}
                {", " if count != lenght_inteval else " "}
            """
        sql_ttl += f"""
            ON {ttl} {f"AS {time}" if time != "" else ""} );
        """
        return sql_ttl
