import pytest
import math
from datetime import datetime, timedelta

from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.datashard.lib.dml_operations import DMLOperations
from ydb.tests.datashard.lib.types_of_variables import cleanup_type_name, format_sql_value, pk_types, non_pk_types, index_first, index_second, ttl_types, \
    index_first_sync, index_second_sync, index_three_sync, index_three_sync_not_Bool, index_four_sync, index_zero_sync

"""
FROM AS_TABLE +
FROM SELECT +
DISTINCT +
UNIQUE DISTINCT
UNION +
WITH
WITHOUT
WHERE DMLOperations+
ORDER BY
ASSUME ORDER BY
LIMIT OFFSET +
SAMPLE
TABLESAMPLE
"""

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
    def test_select(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str):
        dml = DMLOperations(self)
        dml.create_table(table_name, pk_types, all_types,
                          index, ttl, unique, sync)
        dml.insert(table_name, all_types, pk_types, index, ttl)
        self.limit(table_name, all_types, pk_types, index, ttl)
        self.from_select(table_name, all_types, pk_types, index, ttl)
        self.distinct(table_name, all_types, pk_types, index, ttl)
        self.union(table_name, all_types, pk_types, index, ttl)

    def limit(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        statements = self.create_types_for_all_select(
            all_types, pk_types, index, ttl)

        number_of_columns = self.get_number_of_columns(
            pk_types, all_types, index, ttl)

        for offset in range(number_of_columns):
            rows = self.query(f"select {", ".join(statements)} from {table_name} limit 1 OFFSET {offset}")
            self.assert_type_after_select(
                offset+1, 0, rows, all_types, pk_types, index, ttl)

    def create_types_for_all_select(self, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        statements = []
        # delete if after https://github.com/ydb-platform/ydb/issues/16930
        for type in all_types.keys():
            if type != "Date32" and type != "Datetime64" and type != "Timestamp64" and type != 'Interval64':
                statements.append(f"col_{cleanup_type_name(type)}")
        for type in pk_types.keys():
            if type != "Date32" and type != "Datetime64" and type != "Timestamp64" and type != 'Interval64':
                statements.append(f"pk_{cleanup_type_name(type)}")
        for type in index.keys():
            if type != "Date32" and type != "Datetime64" and type != "Timestamp64" and type != 'Interval64':
                statements.append(f"col_index_{cleanup_type_name(type)}")
        if ttl != "":
            statements.append(f"ttl_{cleanup_type_name(ttl)}")
        return statements

    def assert_type_after_select(self, value, line, rows, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        count = 0
        for type in all_types.keys():
            if type != "Date32" and type != "Datetime64" and type != "Timestamp64" and type != 'Interval64':
                self.assert_type(all_types, type, value, rows[line][count])
                count += 1
        for type in pk_types.keys():
            if type != "Date32" and type != "Datetime64" and type != "Timestamp64" and type != 'Interval64':
                self.assert_type(pk_types, type, value, rows[line][count])
                count += 1
        for type in index.keys():
            if type != "Date32" and type != "Datetime64" and type != "Timestamp64" and type != 'Interval64':
                self.assert_type(index, type, value, rows[line][count])
                count += 1
        if ttl != "":
            self.assert_type(ttl_types, ttl, value, rows[line][count])
            count += 1

    def from_select(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        statements = self.create_types_for_all_select(
            all_types, pk_types, index, ttl)

        rows = self.query(f"from {table_name} select {", ".join(statements)}")
        for i in range(len(rows)):
            self.assert_type_after_select(
                i+1, i, rows, all_types, pk_types, index, ttl)

    def assert_type(self, key, type: str, values: int, values_from_rows):
        if type == "String" or type == "Yson":
            assert values_from_rows.decode(
                "utf-8") == key[type](values), f"{type}"
        elif type == "Float" or type == "DyNumber":
            assert math.isclose(float(values_from_rows), float(
                key[type](values)), rel_tol=1e-3), f"{type}"
        elif type == "Interval" or type == "Interval64":
            assert values_from_rows == timedelta(
                microseconds=key[type](values)), f"{type}"
        elif type == "Timestamp" or type == "Timestamp64":
            assert values_from_rows == datetime.fromtimestamp(
                key[type](values)/1_000_000 - 3*60*60), f"{type}"
        elif type == "Json" or type == "JsonDocument":
            assert str(values_from_rows).replace(
                "'", "\"") == str(key[type](values)), f"{type}"
        else:
            assert str(values_from_rows) == str(key[type](values)), f"{type}"

    def distinct(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        for type in all_types.keys():
            if type != "Date32" and type != "Datetime64" and type != "Timestamp64" and type != 'Interval64'\
                    and type != "DyNumber" and type != "UUID" and type != "Json" and type != "JsonDocument" and type != "Yson":
                rows_distinct = self.query(
                    f"SELECT DISTINCT col_{cleanup_type_name(type)} from {table_name}")
                for i in range(len(rows_distinct)):
                    self.assert_type(all_types, type, i+1, rows_distinct[i][0])
        for type in pk_types.keys():
            if type != "Date32" and type != "Datetime64" and type != "Timestamp64" and type != 'Interval64' and type != "DyNumber" and type != "UUID":
                rows_distinct = self.query(
                    f"SELECT DISTINCT pk_{cleanup_type_name(type)} from {table_name}")
                for i in range(len(rows_distinct)):
                    self.assert_type(pk_types, type, i+1, rows_distinct[i][0])
        for type in index.keys():
            if type != "Date32" and type != "Datetime64" and type != "Timestamp64" and type != 'Interval64' and type != "DyNumber" and type != "UUID":
                rows_distinct = self.query(
                    f"SELECT DISTINCT col_index_{cleanup_type_name(type)} from {table_name}")
                for i in range(len(rows_distinct)):
                    self.assert_type(index, type, i+1, rows_distinct[i][0])
        if ttl != "" and ttl != "DyNumber":
            rows_distinct = self.query(
                f"SELECT DISTINCT ttl_{cleanup_type_name(ttl)} from {table_name}")
            for i in range(len(rows_distinct)):
                self.assert_type(ttl_types, ttl, i+1, rows_distinct[i][0])

    def union(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        statements = []
        for type in all_types.keys():
            if type != "Date32" and type != "Datetime64" and type != "Timestamp64" and type != 'Interval64'\
                    and type != "DyNumber" and type != "UUID" and type != "Json" and type != "JsonDocument" and type != "Yson":
                statements.append(f"col_{cleanup_type_name(type)}")
        for type in pk_types.keys():
            if type != "Date32" and type != "Datetime64" and type != "Timestamp64" and type != 'Interval64' and type != "DyNumber" and type != "UUID":
                statements.append(f"pk_{cleanup_type_name(type)}")
        for type in index.keys():
            if type != "Date32" and type != "Datetime64" and type != "Timestamp64" and type != 'Interval64' and type != "DyNumber" and type != "UUID":
                statements.append(f"col_index_{cleanup_type_name(type)}")
        if ttl != "" and ttl != "DyNumber":
            statements.append(f"ttl_{cleanup_type_name(ttl)}")
        rows = self.query(f"""
                          select {", ".join(statements)} from {table_name}
                          union
                          select {", ".join(statements)} from {table_name}
                          """)
        for type in all_types.keys():
            if type != "Date32" and type != "Datetime64" and type != "Timestamp64" and type != 'Interval64'\
                    and type != "DyNumber" and type != "UUID" and type != "Json" and type != "JsonDocument" and type != "Yson":
                for line in range(len(rows)):
                    self.assert_type(all_types, type, line+1,
                                     rows[line][f"col_{cleanup_type_name(type)}"])
        for type in pk_types.keys():
            if type != "Date32" and type != "Datetime64" and type != "Timestamp64" and type != 'Interval64' and type != "DyNumber" and type != "UUID":
                for line in range(len(rows)):
                    self.assert_type(pk_types, type, line+1,
                                     rows[line][f"pk_{cleanup_type_name(type)}"])
        for type in index.keys():
            if type != "Date32" and type != "Datetime64" and type != "Timestamp64" and type != 'Interval64' and type != "DyNumber" and type != "UUID":
                for line in range(len(rows)):
                    self.assert_type(
                        index, type, line+1, rows[line][f"col_index_{cleanup_type_name(type)}"])
        if ttl != "" and ttl != "DyNumber":
            for line in range(len(rows)):
                self.assert_type(ttl_types, ttl, line+1,
                                 rows[line][f"ttl_{cleanup_type_name(ttl)}"])

    def without(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        statements_without = []
        for type in all_types.keys():
            if type == "Date32" or type == "Datetime64" or type == "Timestamp64" or type == 'Interval64':
                statements_without.append(f"col_{cleanup_type_name(type)}")
        for type in pk_types.keys():
            if type == "Date32" or type == "Datetime64" or type == "Timestamp64" or type == 'Interval64':
                statements_without.append(f"pk_{cleanup_type_name(type)}")
        for type in index.keys():
            if type == "Date32" or type == "Datetime64" or type == "Timestamp64" or type == 'Interval64':
                statements_without.append(f"col_index_{cleanup_type_name(type)}")
                
        

    def get_number_of_columns(self, pk_types, all_types, index, ttl):
        number_of_columns = len(pk_types) + len(all_types) + len(index)

        if ttl != "":
            number_of_columns += 1
        return number_of_columns

    def test_as_table(self):
        all_types = {**pk_types, **non_pk_types}
        statements = []
        for type_name in all_types.keys():
            if type_name != "Date32" and type_name != "Datetime64" and type_name != "Timestamp64" and type_name != 'Interval64':
                statements.append(
                    f"{format_sql_value(all_types[type_name](1), type_name)} AS pk_{cleanup_type_name(type_name)}")
        list_sql = f"""
            $data = AsList(
                AsStruct({", ".join(statements)})
                );
        """
        rows = self.query(f"""
                   {list_sql}
                   select * from AS_TABLE($data);
                   """)
        print(f"""
                   {list_sql}
                   select * from AS_TABLE($data);
                   """)
        for type_name in all_types.keys():
            if type_name != "Date32" and type_name != "Datetime64" and type_name != "Timestamp64" and type_name != 'Interval64':
                if type_name == "Utf8":
                    self.assert_type(
                        all_types, type_name, 1, rows[0][f"pk_{cleanup_type_name(type_name)}"].decode("utf-8"))
                else:
                    self.assert_type(
                        all_types, type_name, 1, rows[0][f"pk_{cleanup_type_name(type_name)}"])
