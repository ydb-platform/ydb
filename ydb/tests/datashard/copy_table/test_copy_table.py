import os

from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.stress.oltp_workload.workload import cleanup_type_name
from ydb.tests.datashard.lib.create_table import create_table, pk_types, non_pk_types, index_first, index_second, unique, index_sync, index_first_not_Bool, ttl_types, create_ttl


class TestCopyTable(TestBase):
    def test_copy_table(self):
        # all index
        for i in range(2):
            for uniq in unique:
                for sync in index_sync:
                    if uniq != "UNIQUE" or sync != "ASYNC":
                        if i == 1:
                            index = index_second
                        elif uniq == "UNIQUE":
                            index = index_first_not_Bool
                        else:
                            index = index_first
                        self.copy_table(
                            f"table_index_{i}_{uniq}_{sync}", pk_types, {}, index, "", uniq, sync)

        # all ttl
        for ttl in ttl_types.keys():
            self.copy_table(f"table_ttl_{ttl}", pk_types, {}, {}, ttl, "", "")

        self.copy_table("table_all_types", pk_types, {
                        **pk_types, **non_pk_types}, {}, "", "", "")

    def copy_table(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str):
        self.create_table(table_name, pk_types, all_types,
                          index, ttl, unique, sync)
        self.insert(table_name, all_types, pk_types, index, ttl)
        os.system(f"""
                  ydb -e {self.get_endpoint()} -d {self.get_database()} tools copy --item destination=copy_{table_name},source={table_name}
                  """)
        self.select_after_insert(
            f"copy_{table_name}", all_types, pk_types, index, ttl)

    # После мерджа test_DML убрать эти методы https://github.com/ydb-platform/ydb/pull/16117/files
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
        sql_create_table = create_table(
            table_name, columns, pk_columns, index_columns, unique, sync)
        self.query(sql_create_table)
        if ttl != "":
            sql_ttl = create_ttl(f"ttl_{cleanup_type_name(ttl)}", {"P18262D": ""}, "SECONDS" if ttl ==
                                 "Uint32" or ttl == "Uint64" or ttl == "DyNumber" else "", table_name)
            self.query(sql_ttl)

    def insert(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        number_of_columns = len(pk_types) + len(all_types) + len(index)

        if ttl != "":
            number_of_columns += 1
        for count in range(1, number_of_columns + 1):
            self.create_insetr(table_name, count, all_types,
                               pk_types, index, ttl)

    def select_after_insert(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):

        number_of_columns = len(pk_types) + len(all_types) + len(index)

        if ttl != "":
            number_of_columns += 1

        for count in range(1, number_of_columns + 1):
            sql_select = f"""
                SELECT COUNT(*) as count FROM `{table_name}` WHERE """

            for type_name in pk_types.keys():
                sql_select += f"pk_{cleanup_type_name(type_name)}={pk_types[type_name].format(count)} and "
            for type_name in all_types.keys():
                if type_name != "Json" and type_name != "Yson" and type_name != "JsonDocument":
                    sql_select += f"col_{cleanup_type_name(type_name)}={all_types[type_name].format(count)} and "
            for type_name in index.keys():
                sql_select += f"col_index_{cleanup_type_name(type_name)}={index[type_name].format(count)} and "
            if ttl != "":
                sql_select += f"ttl_{ttl}={ttl_types[ttl].format(count)}"
            else:
                sql_select += f"""pk_Int64={pk_types["Int64"].format(count)}"""

            rows = self.query(sql_select)
            assert len(
                rows) == 1 and rows[0].count == 1, f"Expected one rows, faild in {count} value, table {table_name}"

    def create_insetr(self, table_name: str, value: int, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
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
