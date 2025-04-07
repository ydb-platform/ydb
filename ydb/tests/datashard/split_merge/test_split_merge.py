import pytest
import time

from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.datashard.lib.create_table import create_table_sql_request, create_ttl_sql_request
from ydb.tests.datashard.lib.types_of_variables import cleanup_type_name, pk_types, non_pk_types, index_first, index_second, ttl_types, \
    index_first_sync, index_second_sync, index_three_sync, index_three_sync_not_Bool, index_four_sync, index_zero_sync


class TestSplitMerge(TestBase):
    def test_t(self):
        p = 30
        self.query(
            "create table a(pk Int64, big_line string, primary key(pk)) with (AUTO_PARTITIONING_PARTITION_SIZE_MB = 1)")
        big_line = "a" * 1_000_000
        for i in range(p):
            self.query(
                f"insert into a(pk, big_line) values({i}, 'String {big_line}')")
        rows = self.query("select count(*) as count from a")
        print((self.driver.scheme_client.list_directory(
            f"{self.get_database()}/a")))
        assert len(rows) == 1 and rows[0].count == p, "cdscscsc"
        for _ in range(50):
            rows = self.query("""SELECT
                count(*) as count
                FROM `.sys/partition_stats`
                """)
            if rows[0].count != 1:
                break
            time.sleep(1)
        print(rows)
        self.query("alter table a set(AUTO_PARTITIONING_PARTITION_SIZE_MB = 2000)")
        for _ in range(50):
            rows = self.query("""SELECT
                count(*) as count
                FROM `.sys/partition_stats`
                """)
            if rows[0].count == 1:
                break
            time.sleep(1)
        print(rows)
        
    @pytest.mark.parametrize(
        "table_name, pk_types, all_types, index, ttl, unique, sync",
        [
            ("table_index_4_UNIQUE_SYNC", pk_types, {},
             index_four_sync, "", "UNIQUE", "SYNC"),
            ("table_index_3_UNIQUE_SYNC", pk_types, {},
             index_three_sync_not_Bool, "", "UNIQUE", "SYNC"),
            ("table_index_2_UNIQUE_SYNC", pk_types, {},
             index_second_sync, "", "UNIQUE", "SYNC"),
            ("table_index_1_UNIQUE_SYNC", pk_types, {},
             index_first_sync, "", "UNIQUE", "SYNC"),
            ("table_index_0_UNIQUE_SYNC", pk_types, {},
             index_zero_sync, "", "UNIQUE", "SYNC"),
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
    def test_merge_split(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str):
        big_line = "a" * 100_000
        all_types["String"] = "'String " + big_line +  "{}'"
        self.create_table(table_name, pk_types, all_types,
                          index, ttl, unique, sync)
        self.query(f"alter table {table_name} set(AUTO_PARTITIONING_PARTITION_SIZE_MB = 1)")
        self.insert(table_name, all_types, pk_types, index, ttl)
        for _ in range(50):
            rows = self.query("""SELECT
                count(*) as count
                FROM `.sys/partition_stats`
                """)
            if rows[0].count != 1:
                break
            time.sleep(1)
        assert len(rows) == 1 and rows[0].count != 1, f"The table {table_name} is not split into partition"
        self.select_after_insert(table_name, all_types, pk_types, index, ttl)
        self.query(f"alter table {table_name} set(AUTO_PARTITIONING_PARTITION_SIZE_MB = 2000)")
        for _ in range(50):
            rows = self.query("""SELECT
                count(*) as count
                FROM `.sys/partition_stats`
                """)
            if rows[0].count == 1:
                break
            time.sleep(1)
        assert len(rows) == 1 and rows[0].count == 1, f"the table {table_name} is not merge into one partition"
        self.select_after_insert(table_name, all_types, pk_types, index, ttl)
        
    def create_table(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str):
        columns = {
            "pk_": pk_types.keys(),
            "col_": all_types.keys(),
            "col_index_": index.keys(),
            "ttl_": [ttl]
        }
        pk_columns = {
            "pk_": pk_types.keys()
        }
        index_columns = {
            "col_index_": index.keys()
        }
        sql_create_table = create_table_sql_request(
            table_name, columns, pk_columns, index_columns, unique, sync)
        self.query(sql_create_table)
        if ttl != "":
            sql_ttl = create_ttl_sql_request(f"ttl_{cleanup_type_name(ttl)}", {"P18262D": ""}, "SECONDS" if ttl ==
                                             "Uint32" or ttl == "Uint64" or ttl == "DyNumber" else "", table_name)
            self.query(sql_ttl)

    def insert(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        number_of_columns = len(pk_types) + len(all_types) + len(index)

        if ttl != "":
            number_of_columns += 1
        for count in range(1, number_of_columns + 1):
            self.create_insert(table_name, count, all_types,
                               pk_types, index, ttl)
            
    def create_insert(self, table_name: str, value: int, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        insert_sql = f"""
            INSERT INTO {table_name}(
                {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])}{", " if len(all_types) != 0 else ""}
                {", ".join(["col_" + cleanup_type_name(type_name) for type_name in all_types.keys()])}{", " if len(index) != 0 else ""}
                {", ".join(["col_index_" + cleanup_type_name(type_name) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                {f"ttl_{ttl}" if ttl != "" else ""}
            )
            VALUES(
                {", ".join([pk_types[type_name].format(value) for type_name in pk_types.keys()])}{", " if len(all_types) != 0 else ""}
                {", ".join([all_types[type_name].format(value) for type_name in all_types.keys()])}{", " if len(index) != 0 else ""}
                {", ".join([index[type_name].format(value) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                {ttl_types[ttl].format(value) if ttl != "" else ""}
            );
        """
        self.query(insert_sql)
    
    def select_after_insert(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):

        number_of_columns = len(pk_types) + len(all_types) + len(index)

        if ttl != "":
            number_of_columns += 1

        for count in range(1, number_of_columns + 1):
            create_all_type = []
            for type_name in all_types.keys():
                if type_name != "Json" and type_name != "Yson" and type_name != "JsonDocument":
                    create_all_type.append(
                        f"col_{cleanup_type_name(type_name)}={all_types[type_name].format(count)}")
            sql_select = f"""
                SELECT COUNT(*) as count FROM `{table_name}` WHERE 
                {" and ".join([f"pk_{cleanup_type_name(type_name)}={pk_types[type_name].format(count)}" for type_name in pk_types.keys()])}
                {" and " if len(index) != 0 else ""}
                {" and ".join([f"col_index_{cleanup_type_name(type_name)}={index[type_name].format(count)}" for type_name in index.keys()])}
                {" and " if len(create_all_type) != 0 else ""}
                {" and ".join(create_all_type)}
                {f" and  ttl_{ttl}={ttl_types[ttl].format(count)}" if ttl != "" else ""}
                """
            rows = self.query(sql_select)
            assert len(
                rows) == 1 and rows[0].count == 1, f"Expected one rows, faild in {count} value, table {table_name}"

        rows = self.query(f"SELECT COUNT(*) as count FROM `{table_name}`")
        assert len(
            rows) == 1 and rows[0].count == number_of_columns, f"Expected {number_of_columns} rows, after select all line"