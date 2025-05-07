import pytest
import ydb

from datetime import datetime
from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.datashard.lib.create_table import create_table_sql_request, create_ttl_sql_request
from ydb.tests.datashard.lib.types_of_variables import (
    cleanup_type_name,
    pk_types_parametrized_queries,
    non_pk_types,
    index_first_parametrized_queries,
    index_second_parametrized_queries,
    ttl_types,
    index_first_sync,
    index_second_sync,
    index_three_sync_parametrized_queries,
    index_three_sync_not_Bool_parametrized_queries,
    index_four_sync_parametrized_queries,
    index_zero_sync,
)

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
}


class TestParametrizedQueries(TestBase):
    @pytest.mark.parametrize(
        "table_name, pk_types, all_types, index, ttl, unique, sync",
        [
            (
                "table_index_4_UNIQUE_SYNC",
                pk_types_parametrized_queries,
                {},
                index_four_sync_parametrized_queries,
                "",
                "UNIQUE",
                "SYNC",
            ),
            (
                "table_index_3_UNIQUE_SYNC",
                pk_types_parametrized_queries,
                {},
                index_three_sync_not_Bool_parametrized_queries,
                "",
                "UNIQUE",
                "SYNC",
            ),
            ("table_index_2_UNIQUE_SYNC", pk_types_parametrized_queries, {}, index_second_sync, "", "UNIQUE", "SYNC"),
            ("table_index_1_UNIQUE_SYNC", pk_types_parametrized_queries, {}, index_first_sync, "", "UNIQUE", "SYNC"),
            ("table_index_0_UNIQUE_SYNC", pk_types_parametrized_queries, {}, index_zero_sync, "", "UNIQUE", "SYNC"),
            (
                "table_index_4__SYNC",
                pk_types_parametrized_queries,
                {},
                index_four_sync_parametrized_queries,
                "",
                "",
                "SYNC",
            ),
            (
                "table_index_3__SYNC",
                pk_types_parametrized_queries,
                {},
                index_three_sync_parametrized_queries,
                "",
                "",
                "SYNC",
            ),
            ("table_index_2__SYNC", pk_types_parametrized_queries, {}, index_second_sync, "", "", "SYNC"),
            ("table_index_1__SYNC", pk_types_parametrized_queries, {}, index_first_sync, "", "", "SYNC"),
            ("table_index_0__SYNC", pk_types_parametrized_queries, {}, index_zero_sync, "", "", "SYNC"),
            (
                "table_index_1__ASYNC",
                pk_types_parametrized_queries,
                {},
                index_second_parametrized_queries,
                "",
                "",
                "ASYNC",
            ),
            (
                "table_index_0__ASYNC",
                pk_types_parametrized_queries,
                {},
                index_first_parametrized_queries,
                "",
                "",
                "ASYNC",
            ),
            (
                "table_all_types",
                pk_types_parametrized_queries,
                {**pk_types_parametrized_queries, **non_pk_types},
                {},
                "",
                "",
                "",
            ),
            ("table_ttl_DyNumber", pk_types_parametrized_queries, {}, {}, "DyNumber", "", ""),
            ("table_ttl_Uint32", pk_types_parametrized_queries, {}, {}, "Uint32", "", ""),
            ("table_ttl_Uint64", pk_types_parametrized_queries, {}, {}, "Uint64", "", ""),
            ("table_ttl_Datetime", pk_types_parametrized_queries, {}, {}, "Datetime", "", ""),
            ("table_ttl_Timestamp", pk_types_parametrized_queries, {}, {}, "Timestamp", "", ""),
            ("table_ttl_Date", pk_types_parametrized_queries, {}, {}, "Date", "", ""),
        ],
    )
    def test_parametrized_queries(
        self,
        table_name: str,
        pk_types: dict[str, str],
        all_types: dict[str, str],
        index: dict[str, str],
        ttl: str,
        unique: str,
        sync: str,
    ):
        self.create_table(table_name, pk_types, all_types, index, ttl, unique, sync)
        self.insert(table_name, all_types, pk_types, index, ttl)
        self.select_after_insert(table_name, all_types, pk_types, index, ttl)

    def create_table(
        self,
        table_name: str,
        pk_types: dict[str, str],
        all_types: dict[str, str],
        index: dict[str, str],
        ttl: str,
        unique: str,
        sync: str,
    ):
        columns = {"pk_": pk_types.keys(), "col_": all_types.keys(), "col_index_": index.keys(), "ttl_": [ttl]}
        pk_columns = {"pk_": pk_types.keys()}
        index_columns = {"col_index_": index.keys()}
        sql_create_table = create_table_sql_request(table_name, columns, pk_columns, index_columns, unique, sync)
        self.query(sql_create_table)
        if ttl != "":
            sql_ttl = create_ttl_sql_request(
                f"ttl_{cleanup_type_name(ttl)}",
                {"P18262D": ""},
                "SECONDS" if ttl == "Uint32" or ttl == "Uint64" or ttl == "DyNumber" else "",
                table_name,
            )
            self.query(sql_ttl)

    def insert(
        self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str
    ):
        number_of_columns = len(pk_types) + len(all_types) + len(index)

        if ttl != "":
            number_of_columns += 1
        for count in range(1, number_of_columns + 1):
            self.create_insert(table_name, count, all_types, pk_types, index, ttl)

    def create_insert(
        self,
        table_name: str,
        value: int,
        all_types: dict[str, str],
        pk_types: dict[str, str],
        index: dict[str, str],
        ttl: str,
    ):
        insert_sql = f"""
            {" ".join([f"DECLARE $pk_{cleanup_type_name(type_name)} AS {type_name};" for type_name in pk_types.keys()])}
            {" ".join([f"DECLARE $col_{cleanup_type_name(type_name)} AS {type_name};" for type_name in all_types.keys()])}
            {" ".join([f"DECLARE $col_index_{cleanup_type_name(type_name)} AS {type_name};" for type_name in index.keys()])}
            {f"DECLARE $ttl_{ttl} AS {ttl};" if ttl != "" else ""}
        
            INSERT INTO {table_name}(
                {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])}{", " if len(all_types) != 0 else ""}
                {", ".join(["col_" + cleanup_type_name(type_name) for type_name in all_types.keys()])}{", " if len(index) != 0 else ""}
                {", ".join(["col_index_" + cleanup_type_name(type_name) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                {f"ttl_{ttl}" if ttl != "" else ""}
            )
            VALUES(
                {", ".join([f"$pk_{cleanup_type_name(type_name)}" for type_name in pk_types.keys()])}{", " if len(all_types) != 0 else ""}
                {", ".join([f"$col_{cleanup_type_name(type_name)}" for type_name in all_types.keys()])}{", " if len(index) != 0 else ""}
                {", ".join([f"$col_index_{cleanup_type_name(type_name)}" for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                {f"$ttl_{ttl}" if ttl != "" else ""}
            );
        """
        parameters = {}
        for type_name in pk_types.keys():
            parameters[f"$pk_{cleanup_type_name(type_name)}"] = self.create_type_value(pk_types, type_name, value)
        for type_name in all_types.keys():
            parameters[f"$col_{cleanup_type_name(type_name)}"] = self.create_type_value(all_types, type_name, value)
        for type_name in index.keys():
            parameters[f"$col_index_{cleanup_type_name(type_name)}"] = self.create_type_value(index, type_name, value)
        if ttl != "":
            parameters[f"$ttl_{cleanup_type_name(ttl)}"] = self.create_type_value(ttl_types, ttl, value)
        self.query(insert_sql, parameters=parameters)

    def select_after_insert(
        self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str
    ):
        number_of_columns = len(pk_types) + len(all_types) + len(index)

        if ttl != "":
            number_of_columns += 1

        for count in range(1, number_of_columns + 1):
            create_all_type = []
            create_all_type_declare = []
            for type_name in all_types.keys():
                if type_name != "Json" and type_name != "Yson" and type_name != "JsonDocument":
                    create_all_type.append(f"col_{cleanup_type_name(type_name)}=$col_{cleanup_type_name(type_name)}")
                    create_all_type_declare.append(f"DECLARE $col_{cleanup_type_name(type_name)} AS {type_name};")

            sql_select = f"""
                {" ".join([f"DECLARE $pk_{cleanup_type_name(type_name)} AS {type_name};" for type_name in pk_types.keys()])}
                {" ".join(create_all_type_declare)}
                {" ".join([f"DECLARE $col_index_{cleanup_type_name(type_name)} AS {type_name};" for type_name in index.keys()])}
                {f"DECLARE $ttl_{ttl} AS {ttl};" if ttl != "" else ""}
            
                SELECT COUNT(*) as count FROM `{table_name}` WHERE 
                {" and ".join([f"pk_{cleanup_type_name(type_name)}=$pk_{cleanup_type_name(type_name)}" for type_name in pk_types.keys()])}
                {" and " if len(index) != 0 else ""}
                {" and ".join([f"col_index_{cleanup_type_name(type_name)}=$col_index_{cleanup_type_name(type_name)}" for type_name in index.keys()])}
                {" and " if len(create_all_type) != 0 else ""}
                {" and ".join(create_all_type)}
                {f" and  ttl_{ttl}=$ttl_{ttl}" if ttl != "" else ""}
                """
            parameters = {}
            for type_name in pk_types.keys():
                parameters[f"$pk_{cleanup_type_name(type_name)}"] = self.create_type_value(pk_types, type_name, count)
            for type_name in all_types.keys():
                parameters[f"$col_{cleanup_type_name(type_name)}"] = self.create_type_value(all_types, type_name, count)
            for type_name in index.keys():
                parameters[f"$col_index_{cleanup_type_name(type_name)}"] = self.create_type_value(
                    index, type_name, count
                )
            if ttl != "":
                parameters[f"$ttl_{cleanup_type_name(ttl)}"] = self.create_type_value(ttl_types, ttl, count)
            rows = self.query(sql_select, parameters=parameters)
            assert (
                len(rows) == 1 and rows[0].count == 1
            ), f"Expected one rows, faild in {count} value, table {table_name}"

        rows = self.query(f"SELECT COUNT(*) as count FROM `{table_name}`")
        assert (
            len(rows) == 1 and rows[0].count == number_of_columns
        ), f"Expected {number_of_columns} rows, after select all line"

    def create_type_value(self, key, type_name, value):
        if "Decimal" in type_name:
            return key[type_name](value)
        if type_name == "String" or type_name == "Yson":
            return ydb.TypedValue(key[type_name](value).encode(), primitive_type[type_name])
        if type_name == "DyNumber":
            return ydb.TypedValue(str(key[type_name](value)), primitive_type[type_name])
        if type_name == "Datetime64" or type_name == "Datetime":
            return ydb.TypedValue(int(datetime.timestamp(key[type_name](value))), primitive_type[type_name])
        return ydb.TypedValue(key[type_name](value), primitive_type[type_name])
