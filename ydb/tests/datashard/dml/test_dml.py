import pytest

from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.datashard.lib.create_table import create_table, create_ttl
from ydb.tests.datashard.lib.types_of_variables import cleanup_type_name, pk_types, non_pk_types, index_first, index_second, ttl_types, \
    index_first_sync, index_second_sync, index_three_sync, index_three_sync_not_Bool, index_four_sync, index_zero_sync


class TestDML(TestBase):
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
    def test_dml(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str):
        self.create_table(table_name, pk_types, all_types,
                          index, ttl, unique, sync)
        self.insert(table_name, all_types, pk_types, index, ttl)
        self.select_after_insert(table_name, all_types, pk_types, index, ttl)
        self.update(table_name, all_types, index, ttl, unique)
        self.upsert(table_name, all_types, pk_types, index, ttl)
        self.delete(table_name, all_types, pk_types, index, ttl)

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

    def update(self, table_name: str, all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str):
        count = 1

        if ttl != "":
            self.create_update(
                count, f"ttl_{ttl}", ttl_types[ttl], table_name)
            count += 1

        for type_name in all_types.keys():
            self.create_update(
                count, f"col_{type_name}", all_types[type_name], table_name)
            count += 1

        if unique == "":
            for type_name in index.keys():
                self.create_update(
                    count, f"col_index_{type_name}", index[type_name], table_name)
                count += 1
        else:
            number_of_columns = len(pk_types) + len(all_types) + len(index)+1
            if ttl != "":
                number_of_columns += 1
            for i in range(1, number_of_columns + 1):
                self.create_update_unique(
                    number_of_columns + i, i, index, table_name)

        count_assert = 1

        number_of_columns = len(pk_types) + len(all_types) + len(index)
        if ttl != "":
            number_of_columns += 1

        if ttl != "":
            rows = self.query(
                f"SELECT COUNT(*) as count FROM `{table_name}` WHERE ttl_{cleanup_type_name(ttl)}={ttl_types[ttl].format(count_assert)}")
            assert len(
                rows) == 1 and rows[0].count == number_of_columns, f"Expected {number_of_columns} rows after insert, faild in ttl_{cleanup_type_name(ttl)}, table {table_name}"
            count_assert += 1

        for type_name in all_types.keys():
            if type_name != "Json" and type_name != "Yson" and type_name != "JsonDocument":
                rows = self.query(
                    f"SELECT COUNT(*) as count FROM `{table_name}` WHERE col_{cleanup_type_name(type_name)}={all_types[type_name].format(count_assert)}")
                assert len(
                    rows) == 1 and rows[0].count == number_of_columns, f"Expected {number_of_columns} rows after insert, faild in col_{cleanup_type_name(type_name)}, table {table_name}"
            count_assert += 1
        if unique == "":
            for type_name in index.keys():
                rows = self.query(
                    f"SELECT COUNT(*) as count FROM `{table_name}` WHERE col_index_{cleanup_type_name(type_name)}={index[type_name].format(count_assert)}")
                assert len(
                    rows) == 1 and rows[0].count == number_of_columns, f"Expected {number_of_columns} rows after insert, faild in col_index_{cleanup_type_name(type_name)}, table {table_name}"
                count_assert += 1
        else:
            number_of_columns = len(pk_types) + len(all_types) + len(index) + 2
            if ttl != "":
                number_of_columns += 1
            for type_name in index.keys():
                rows = self.query(
                    f"SELECT COUNT(*) as count FROM `{table_name}` WHERE col_index_{cleanup_type_name(type_name)}={index[type_name].format(number_of_columns)}")
                assert len(
                    rows) == 1 and rows[0].count == 1, f"Expected {1} rows after insert, faild in col_index_{cleanup_type_name(type_name)}, table {table_name}"
                number_of_columns += 1

    def create_update(self, value: int, type_name: str, key: str, table_name: str):
        update_sql = f""" UPDATE `{table_name}` SET {cleanup_type_name(type_name)} = {key.format(value)} """
        self.query(update_sql)

    def create_update_unique(self, value: int, search: int, index: dict[str, str], table_name: str):
        update_sql = f" UPDATE `{table_name}` SET "
        count = 1
        for type_name in index.keys():
            update_sql += f"  col_index_{cleanup_type_name(type_name)} = {index[type_name].format(value)} "
            if count != len(index):
                update_sql += ", "
            count += 1
        update_sql += " WHERE "
        count = 1
        for type_name in index.keys():
            update_sql += f"  col_index_{cleanup_type_name(type_name)} = {index[type_name].format(search)} "
            if count != len(index):
                update_sql += " and "
            count += 1

        self.query(update_sql)

    def upsert(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        number_of_columns = len(pk_types) + len(all_types) + len(index)
        if ttl != "":
            number_of_columns += 1

        for count in range(1, number_of_columns+1):
            self.create_upsert(table_name, number_of_columns + 1 -
                               count, count, all_types, pk_types, index, ttl)

        for count in range(number_of_columns+1, 2*number_of_columns+1):
            self.create_upsert(table_name, count, count,
                               all_types, pk_types, index, ttl)

        for count in range(1, number_of_columns + 1):
            sql_select = f"""
                SELECT COUNT(*) as count FROM `{table_name}` WHERE """

            for type_name in pk_types.keys():
                sql_select += f"pk_{cleanup_type_name(type_name)}={pk_types[type_name].format(count)} and "
            for type_name in all_types.keys():
                if type_name != "Json" and type_name != "Yson" and type_name != "JsonDocument":
                    sql_select += f"col_{cleanup_type_name(type_name)}={all_types[type_name].format(number_of_columns - count + 1)} and "
            for type_name in index.keys():
                sql_select += f"col_index_{cleanup_type_name(type_name)}={index[type_name].format(number_of_columns - count + 1)} and "
            if ttl != "":
                sql_select += f"ttl_{ttl}={ttl_types[ttl].format(number_of_columns - count + 1)}"
            else:
                sql_select += f"""pk_Int64={pk_types["Int64"].format(count)}"""

            rows = self.query(sql_select)
            assert len(
                rows) == 1 and rows[0].count == 1, f"Expected one rows, faild in {count} value, table {table_name}"

        for count in range(number_of_columns + 1, 2*number_of_columns + 1):
            sql_select = f"""
                SELECT COUNT(*) as count FROM `{table_name}` WHERE """

            for type_name in pk_types.keys():
                if (type_name != "Date" and type_name != "Datetime") or count < 106:
                    sql_select += f"pk_{cleanup_type_name(type_name)}={pk_types[type_name].format(count)} and "
            for type_name in all_types.keys():
                if type_name != "Json" and type_name != "Yson" and type_name != "JsonDocument" and ((type_name != "Date" and type_name != "Datetime") or count < 106):
                    sql_select += f"col_{cleanup_type_name(type_name)}={all_types[type_name].format(count)} and "
            for type_name in index.keys():
                if (type_name != "Date" and type_name != "Datetime") or count < 106:
                    sql_select += f"col_index_{cleanup_type_name(type_name)}={index[type_name].format(count)} and "
            if ttl != "" and ((type_name != "Date" and type_name != "Datetime") or count < 106):
                sql_select += f"ttl_{ttl}={ttl_types[ttl].format(count)}"
            else:
                sql_select += f"""pk_Int64={pk_types["Int64"].format(count)}"""

            rows = self.query(sql_select)
            assert len(
                rows) == 1 and rows[0].count == 1, f"Expected one rows, faild in {count} value, table {table_name}"

    def create_upsert(self, table_name: str, value: int, search: int, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        upsert_sql = f"""
                    UPSERT INTO {table_name} (
                    {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])}{", " if len(all_types) != 0 else ""}
                    {", ".join(["col_" + cleanup_type_name(type_name) for type_name in all_types.keys()])}{", " if len(index) != 0 else ""}
                    {", ".join(["col_index_" + cleanup_type_name(type_name) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                    {f" ttl_{ttl}" if ttl != "" else ""}
                    )
                    VALUES
                    (
                    {", ".join([pk_types[type_name].format(search) for type_name in pk_types.keys()])}{", " if len(all_types) != 0 else ""}
                    {", ".join([all_types[type_name].format(value) for type_name in all_types.keys()])}{", " if len(index) != 0 else ""}
                    {", ".join([index[type_name].format(value) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                    {ttl_types[ttl].format(value) if ttl != "" else ""}
                    )
                    ;
                """
        self.query(upsert_sql)

    def delete(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        number_of_columns = len(pk_types) + len(all_types) + len(index) + 1

        if ttl != "":
            number_of_columns += 1

        if ttl != "":
            self.create_delete(number_of_columns,
                               f"ttl_{cleanup_type_name(ttl)}", ttl_types[ttl], table_name)
            number_of_columns += 1

        for type_name in pk_types.keys():
            if type_name != "Bool":
                self.create_delete(
                    number_of_columns, f"pk_{cleanup_type_name(type_name)}", pk_types[type_name], table_name)
            else:
                self.create_delete(
                    number_of_columns, "pk_Int64", pk_types["Int64"], table_name)
            number_of_columns += 1

        for type_name in all_types.keys():
            if type_name != "Bool" and type_name != "Json" and type_name != "Yson" and type_name != "JsonDocument":
                self.create_delete(
                    number_of_columns, f"col_{cleanup_type_name(type_name)}", all_types[type_name], table_name)
            else:
                self.create_delete(
                    number_of_columns, "pk_Int64", pk_types["Int64"], table_name)
            number_of_columns += 1

        for type_name in index.keys():
            if type_name != "Bool":
                self.create_delete(
                    number_of_columns, f"col_index_{cleanup_type_name(type_name)}", index[type_name], table_name)
            else:
                self.create_delete(
                    number_of_columns, "pk_Int64", pk_types["Int64"], table_name)
            number_of_columns += 1

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
                    sql_select += f"col_{cleanup_type_name(type_name)}={all_types[type_name].format(number_of_columns - count + 1)} and "
            for type_name in index.keys():
                sql_select += f"col_index_{cleanup_type_name(type_name)}={index[type_name].format(number_of_columns - count + 1)} and "
            if ttl != "":
                sql_select += f"ttl_{ttl}={ttl_types[ttl].format(number_of_columns - count + 1)}"
            else:
                sql_select += f"""pk_Int64={pk_types["Int64"].format(count)}"""

            rows = self.query(sql_select)
            assert len(
                rows) == 1 and rows[0].count == 1, f"Expected one rows, faild in {count} value, table {table_name}"

        for count in range(number_of_columns + 1, 2*number_of_columns + 1):
            sql_select = f"""
                SELECT COUNT(*) as count FROM `{table_name}` WHERE """

            for type_name in pk_types.keys():
                if (type_name != "Date" and type_name != "Datetime") or count < 106:
                    sql_select += f"pk_{cleanup_type_name(type_name)}={pk_types[type_name].format(count)} and "
            for type_name in all_types.keys():
                if type_name != "Json" and type_name != "Yson" and type_name != "JsonDocument" and ((type_name != "Date" and type_name != "Datetime") or count < 106):
                    sql_select += f"col_{cleanup_type_name(type_name)}={all_types[type_name].format(count)} and "
            for type_name in index.keys():
                if (type_name != "Date" and type_name != "Datetime") or count < 106:
                    sql_select += f"col_index_{cleanup_type_name(type_name)}={index[type_name].format(count)} and "
            if ttl != "" and ((type_name != "Date" and type_name != "Datetime") or count < 106):
                sql_select += f"ttl_{ttl}={ttl_types[ttl].format(count)}"
            else:
                sql_select += f"""pk_Int64={pk_types["Int64"].format(count)}"""

            rows = self.query(sql_select)
            assert len(
                rows) == 1 and rows[0].count == 0, f"Expected one rows, faild in {count} value, table {table_name}"

    def create_delete(self, value: int, type_name: str, key: str, table_name: str):
        delete_sql = f"""
            DELETE FROM {table_name} WHERE {type_name} = {key.format(value)};
        """
        self.query(delete_sql)
