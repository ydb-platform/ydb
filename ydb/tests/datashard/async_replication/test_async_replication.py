import pytest
import time
from ydb.tests.datashard.lib.base_async_replication import TestBase
from ydb.tests.datashard.lib.dml import DML
from ydb.tests.datashard.lib.types_of_variables import cleanup_type_name, format_sql_value, pk_types, non_pk_types, index_first, index_second, ttl_types, \
    index_first_sync, index_second_sync, index_three_sync, index_four_sync, index_zero_sync


class TestAsyncReplication(TestBase, DML):
    @pytest.mark.parametrize(
        "table_name, pk_types, all_types, index, ttl, unique, sync",
        [
            # ("table_index_4_UNIQUE_SYNC", pk_types, {},
            # index_four_sync, "", "UNIQUE", "SYNC"),
            # ("table_index_3_UNIQUE_SYNC", pk_types, {},
            # index_three_sync_not_Bool, "", "UNIQUE", "SYNC"),
            # ("table_index_2_UNIQUE_SYNC", pk_types, {},
            # index_second_sync, "", "UNIQUE", "SYNC"),
            # ("table_index_1_UNIQUE_SYNC", pk_types, {},
            # index_first_sync, "", "UNIQUE", "SYNC"),
            # ("table_index_0_UNIQUE_SYNC", pk_types, {},
            # index_zero_sync, "", "UNIQUE", "SYNC"),
            ("table_index_4__SYNC", pk_types, {},
             index_four_sync, "", "", "SYNC"),
            ("table_index_3__SYNC", pk_types, {},
             index_three_sync, "", "", "SYNC"),
            ("table_index_2__SYNC", pk_types, {},
             index_second_sync, "", "", "SYNC"),
            ("table_index_1__SYNC", pk_types, {},
             index_first_sync, "", "", "SYNC"),
            ("table_index_0__SYNC", pk_types, {},
             index_zero_sync, "", "", "SYNC"),
            ("table_index_1__ASYNC", pk_types, {}, index_second, "", "", "ASYNC"),
            ("table_index_0__ASYNC", pk_types, {}, index_first, "", "", "ASYNC"),
            ("table_all_types", pk_types, {
             **pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_ttl_DyNumber", pk_types, {}, {}, "DyNumber", "", ""),
            ("table_ttl_Uint32", pk_types, {}, {}, "Uint32", "", ""),
            ("table_ttl_Uint64", pk_types, {}, {}, "Uint64", "", ""),
            ("table_ttl_Datetime", pk_types, {}, {}, "Datetime", "", ""),
            ("table_ttl_Timestamp", pk_types, {}, {}, "Timestamp", "", ""),
            ("table_ttl_Date", pk_types, {}, {}, "Date", "", ""),
        ]
    )
    def test_async_replication(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str):
        self.create_table(table_name, pk_types, all_types,
                          index, ttl, unique, sync)
        self.insert(table_name, all_types, pk_types, index, ttl)
        self.query_async(f"""
                        CREATE ASYNC REPLICATION `replication_{table_name}`
                        FOR `{self.get_database()}/{table_name}` AS `{self.get_database()}/{table_name}`
                        WITH (
                        CONNECTION_STRING = 'grpc://{self.get_endpoint()}/?database={self.get_database()}'
                            )
                         """)
        self.select_async_after_insert(
            table_name, all_types, pk_types, index, ttl)
        self.query(f"delete from {table_name}")
        rows = self.query_async(f"select count(*) as count from {table_name}")
        for i in range(10):
            rows = self.query_async(
                f"select count(*) as count from {table_name}")
            if len(rows) == 1 and rows[0].count == 0:
                break
            time.sleep(1)

        assert len(
            rows) == 1 and rows[0].count == 0, "Expected zero rows after delete"
        self.insert(table_name, all_types, pk_types, index, ttl)
        self.select_async_after_insert(
            table_name, all_types, pk_types, index, ttl)

    def select_async_after_insert(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):

        number_of_columns = len(pk_types) + len(all_types) + len(index)

        if ttl != "":
            number_of_columns += 1

        for count in range(1, number_of_columns + 1):
            create_all_type = []
            for type_name in all_types.keys():
                if type_name != "Json" and type_name != "Yson" and type_name != "JsonDocument":
                    create_all_type.append(
                        f"col_{cleanup_type_name(type_name)}={format_sql_value(all_types[type_name](count), type_name)}")
            sql_select = f"""
                SELECT COUNT(*) as count FROM `{table_name}` WHERE 
                {" and ".join([f"pk_{cleanup_type_name(type_name)}={format_sql_value(pk_types[type_name](count), type_name)}" for type_name in pk_types.keys()])}
                {" and " if len(index) != 0 else ""}
                {" and ".join([f"col_index_{cleanup_type_name(type_name)}={format_sql_value(index[type_name](count), type_name)}" for type_name in index.keys()])}
                {" and " if len(create_all_type) != 0 else ""}
                {" and ".join(create_all_type)}
                {f" and  ttl_{ttl}={format_sql_value(ttl_types[ttl](count), ttl)}" if ttl != "" else ""}
                """
            rows = self.query(sql_select)
            assert len(
                rows) == 1 and rows[0].count == 1, f"Expected one rows, faild in {count} value, table {table_name}"

        rows = self.query_async(
            f"SELECT COUNT(*) as count FROM `{table_name}`")
        assert len(
            rows) == 1 and rows[0].count == number_of_columns, f"Expected {number_of_columns} rows, after select all line"
