import pytest

from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.datashard.lib.dml_operations import DMLOperations
from ydb.tests.datashard.lib.types_of_variables import cleanup_type_name, format_sql_value, pk_types, non_pk_types, index_first, index_second, ttl_types, \
    index_first_sync, index_second_sync, index_three_sync, index_three_sync_not_Bool, index_four_sync, index_zero_sync

"""
FROM AS_TABLE +
FROM SELECT +
DISTINCT +
UNIQUE DISTINCT does not work
UNION +
WITH -
WITHOUT +
WHERE DMLOperations+
ORDER BY +
ASSUME ORDER BY -
LIMIT OFFSET +
SAMPLE +
TABLESAMPLE +
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
        self.order_by(table_name, all_types, pk_types, index, ttl, dml)
        self.limit(table_name, all_types, pk_types, index, ttl, dml)
        self.from_select(table_name, all_types, pk_types, index, ttl, dml)
        self.distinct(table_name, all_types, pk_types, index, ttl, dml)
        self.union(table_name, all_types, pk_types, index, ttl, dml)
        self.without(table_name, all_types, pk_types, index, ttl)
        self.tablesample_sample(table_name, all_types,
                                pk_types, index, ttl, dml)

    def limit(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str, dml: DMLOperations):
        statements = self.create_types_for_all_select(
            all_types, pk_types, index, ttl)

        number_of_columns = self.get_number_of_columns(
            pk_types, all_types, index, ttl)

        for offset in range(number_of_columns):
            rows = self.query(f"select {", ".join(statements)} from {table_name} limit 1 OFFSET {offset}")
            self.assert_type_after_select(
                offset+1, 0, rows, all_types, pk_types, index, ttl, dml)

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

    def assert_type_after_select(self, value, line, rows, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str, dml: DMLOperations):
        count = 0
        for type in all_types.keys():
            if type != "Date32" and type != "Datetime64" and type != "Timestamp64" and type != 'Interval64':
                dml.assert_type(all_types, type, value, rows[line][count])
                count += 1
        for type in pk_types.keys():
            if type != "Date32" and type != "Datetime64" and type != "Timestamp64" and type != 'Interval64':
                dml.assert_type(pk_types, type, value, rows[line][count])
                count += 1
        for type in index.keys():
            if type != "Date32" and type != "Datetime64" and type != "Timestamp64" and type != 'Interval64':
                dml.assert_type(index, type, value, rows[line][count])
                count += 1
        if ttl != "":
            dml.assert_type(ttl_types, ttl, value, rows[line][count])
            count += 1

    def from_select(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str, dml: DMLOperations):
        statements = self.create_types_for_all_select(
            all_types, pk_types, index, ttl)

        rows = self.query(f"from {table_name} select {", ".join(statements)}")
        for i in range(len(rows)):
            self.assert_type_after_select(
                i+1, i, rows, all_types, pk_types, index, ttl, dml)

    def distinct(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str, dml: DMLOperations):
        # del type != "DyNumber" and type != "UUID" after https://github.com/ydb-platform/ydb/issues/17484
        for type in all_types.keys():
            if type != "Date32" and type != "Datetime64" and type != "Timestamp64" and type != 'Interval64'\
                    and type != "DyNumber" and type != "UUID" and type != "Json" and type != "JsonDocument" and type != "Yson":
                rows_distinct = self.query(
                    f"SELECT DISTINCT col_{cleanup_type_name(type)} from {table_name}")
                for i in range(len(rows_distinct)):
                    dml.assert_type(all_types, type, i+1, rows_distinct[i][0])
        for type in pk_types.keys():
            if type != "Date32" and type != "Datetime64" and type != "Timestamp64" and type != 'Interval64' and type != "DyNumber" and type != "UUID":
                rows_distinct = self.query(
                    f"SELECT DISTINCT pk_{cleanup_type_name(type)} from {table_name}")
                for i in range(len(rows_distinct)):
                    dml.assert_type(pk_types, type, i+1, rows_distinct[i][0])
        for type in index.keys():
            if type != "Date32" and type != "Datetime64" and type != "Timestamp64" and type != 'Interval64' and type != "DyNumber" and type != "UUID":
                rows_distinct = self.query(
                    f"SELECT DISTINCT col_index_{cleanup_type_name(type)} from {table_name}")
                for i in range(len(rows_distinct)):
                    dml.assert_type(index, type, i+1, rows_distinct[i][0])
        if ttl != "" and ttl != "DyNumber":
            rows_distinct = self.query(
                f"SELECT DISTINCT ttl_{cleanup_type_name(ttl)} from {table_name}")
            for i in range(len(rows_distinct)):
                dml.assert_type(ttl_types, ttl, i+1, rows_distinct[i][0])

    def union(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str, dml: DMLOperations):
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
                    dml.assert_type(all_types, type, line+1,
                                    rows[line][f"col_{cleanup_type_name(type)}"])
        for type in pk_types.keys():
            if type != "Date32" and type != "Datetime64" and type != "Timestamp64" and type != 'Interval64' and type != "DyNumber" and type != "UUID":
                for line in range(len(rows)):
                    dml.assert_type(pk_types, type, line+1,
                                    rows[line][f"pk_{cleanup_type_name(type)}"])
        for type in index.keys():
            if type != "Date32" and type != "Datetime64" and type != "Timestamp64" and type != 'Interval64' and type != "DyNumber" and type != "UUID":
                for line in range(len(rows)):
                    dml.assert_type(
                        index, type, line+1, rows[line][f"col_index_{cleanup_type_name(type)}"])
        if ttl != "" and ttl != "DyNumber":
            for line in range(len(rows)):
                dml.assert_type(ttl_types, ttl, line+1,
                                rows[line][f"ttl_{cleanup_type_name(ttl)}"])

    def without(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        statements_without = []
        for type_name in all_types.keys():
            if type_name == "Date32" or type_name == "Datetime64" or type_name == "Timestamp64" or type_name == 'Interval64':
                statements_without.append(
                    f"col_{cleanup_type_name(type_name)}")
        for type_name in pk_types.keys():
            if type_name == "Date32" or type_name == "Datetime64" or type_name == "Timestamp64" or type_name == 'Interval64':
                statements_without.append(f"pk_{cleanup_type_name(type_name)}")
        for type_name in index.keys():
            if type_name == "Date32" or type_name == "Datetime64" or type_name == "Timestamp64" or type_name == 'Interval64':
                statements_without.append(
                    f"col_index_{cleanup_type_name(type_name)}")

        for type_name in all_types.keys():
            if type_name != "Date32" and type_name != "Datetime64" and type_name != "Timestamp64" and type_name != 'Interval64':
                self.create_without(
                    table_name, statements_without, f"col_{cleanup_type_name(type_name)}")
        for type_name in pk_types.keys():
            if type_name != "Date32" and type_name != "Datetime64" and type_name != "Timestamp64" and type_name != 'Interval64':
                self.create_without(
                    table_name, statements_without, f"pk_{cleanup_type_name(type_name)}")
        for type_name in index.keys():
            if type_name != "Date32" and type_name != "Datetime64" and type_name != "Timestamp64" and type_name != 'Interval64':
                self.create_without(
                    table_name, statements_without, f"col_index_{cleanup_type_name(type_name)}")
        if ttl != "":
            self.create_without(table_name, statements_without,
                                f"ttl_{cleanup_type_name(ttl)}")

    def create_without(self, table_name, statements_without, without):
        rows = self.query(f"select * without {", ".join(statements_without)}, {without} from {table_name}")
        for col_name in rows[0].keys():
            assert col_name != without, f"a column {without} in the table {table_name} was not excluded"

    def tablesample_sample(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str, dml: DMLOperations):
        statements = self.create_types_for_all_select(
            all_types, pk_types, index, ttl)
        for i in range(1, 100):
            rows = self.query(f"select {", ".join(statements)} from {table_name} TABLESAMPLE SYSTEM({i}.0)")
            for line in range(len(rows)):
                numb = rows[line]["pk_Int64"]
                self.assert_type_after_select(
                    numb, line, rows, all_types, pk_types, index, ttl, dml)

            rows = self.query(f"select {", ".join(statements)} from {table_name} TABLESAMPLE BERNOULLI({i}.0) REPEATABLE({i})")
            for line in range(len(rows)):
                numb = rows[line]["pk_Int64"]
                self.assert_type_after_select(
                    numb, line, rows, all_types, pk_types, index, ttl, dml)

            rows = self.query(f"select {", ".join(statements)} from {table_name} SAMPLE 1.0 / {i}")
            for line in range(len(rows)):
                numb = rows[line]["pk_Int64"]
                self.assert_type_after_select(
                    numb, line, rows, all_types, pk_types, index, ttl, dml)

    def order_by(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str, dml: DMLOperations):
        statements = self.create_types_for_all_select(
            all_types, pk_types, index, ttl)
        for statement in statements:
            if "Json" not in statement and "JsonDocument" not in statement and "Yson" not in statement:
                rows = self.query(f"select {", ".join(statements)} from {table_name} ORDER BY {statement} ASC")
                if "String" not in statement and "Utf8" not in statement:
                    for line in range(len(rows)):
                        self.assert_type_after_select(
                            line + 1, line, rows, all_types, pk_types, index, ttl, dml)
                else:
                    line = 0
                    for i in range(len(rows)//10):
                        self.assert_type_after_select(
                            i + 1, line, rows, all_types, pk_types, index, ttl, dml)
                        line += 1
                        for j in range(10):
                            if (i+1) * 10 + j > len(rows):
                                break
                            self.assert_type_after_select(
                                (i + 1) * 10 + j, line, rows, all_types, pk_types, index, ttl, dml)
                            line += 1

                rows = self.query(f"select {", ".join(statements)} from {table_name} ORDER BY {statement} DESC")

                if "Bool" not in statement:
                    if "String" not in statement and "Utf8" not in statement:
                        for line in range(len(rows)):
                            self.assert_type_after_select(
                                len(rows) - line, line, rows, all_types, pk_types, index, ttl, dml)
                    else:
                        line = 0
                        for i in range(len(rows)//10, 0):
                            for j in range(10):
                                if i * 10 + (9 - j) > len(rows):
                                    break
                                self.assert_type_after_select(
                                    i * 10 + (9 - j), line, rows, all_types, pk_types, index, ttl, dml)
                                line += 1
                            self.assert_type_after_select(
                                i, line, rows, all_types, pk_types, index, ttl, dml)
                            line += 1

    def get_number_of_columns(self, pk_types, all_types, index, ttl):
        number_of_columns = len(pk_types) + len(all_types) + len(index)

        if ttl != "":
            number_of_columns += 1
        return number_of_columns

    def test_as_table(self):
        dml = DMLOperations(self)
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
        for type_name in all_types.keys():
            if type_name != "Date32" and type_name != "Datetime64" and type_name != "Timestamp64" and type_name != 'Interval64':
                if type_name == "Utf8":
                    dml.assert_type(
                        all_types, type_name, 1, rows[0][f"pk_{cleanup_type_name(type_name)}"].decode("utf-8"))
                else:
                    dml.assert_type(
                        all_types, type_name, 1, rows[0][f"pk_{cleanup_type_name(type_name)}"])

    @pytest.mark.parametrize(
        "table_name, pk_types, all_types, index, ttl, unique, sync",
        [
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
    def test_match_recognize(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str):
        dml = DMLOperations(self)
        dml.create_table(table_name, pk_types, all_types,
                         index, ttl, unique, sync)
        self.insert(table_name, all_types, pk_types, index, ttl)
        partition = []
        for type_name in all_types.keys():
            partition.append(f"col_{cleanup_type_name(type_name)}")
        for type_name in index.keys():
            partition.append(f"col_index_{cleanup_type_name(type_name)}")
        numb = 1
        for type_name in all_types.keys():
            self.update(table_name, type_name, "col_", all_types[type_name], "Int64", pk_types["Int64"])
            partition.remove(f"col_{cleanup_type_name(type_name)}")
            self.create_match_recognize(table_name, partition[numb], partition[numb+1], f"col_{cleanup_type_name(type_name)}")
            partition.append(f"col_{cleanup_type_name(type_name)}")
            self.update_back(table_name, type_name, "col_", all_types[type_name], "Int64", pk_types["Int64"])
            numb = (numb + 1) %len(all_types)
        numb = 1
        for type_name in index.keys():
            self.update(table_name, type_name, "col_index_", index[type_name], "Int64", pk_types["Int64"])
            partition.remove(f"col_index_{cleanup_type_name(type_name)}")
            self.create_match_recognize(table_name, partition[numb], partition[numb+1], f"col_index_{cleanup_type_name(type_name)}")
            partition.append(f"col_index_{cleanup_type_name(type_name)}")
            self.update_back(table_name, type_name, "col_index_", index[type_name], "Int64", pk_types["Int64"])
            numb = (numb + 1) %len(all_types)
            

    def update(self, table_name: str, type_name, prefix, key, type_name_pk, key_pk):
        n = 3
        m = 5
        a = 1
        c = 3
        e = 5
        f = 6
        g = 7
        h = 8
        i = 9
        j = 10
        k = 11
        l = 12
        pk_value = 1
        for _ in range(2):
            for _ in range(2):
                self.create_update(table_name, a, type_name,
                                prefix, key, pk_value, type_name_pk, key_pk)
                pk_value += 1
            for _ in range(2):
                self.create_update(table_name, c, type_name,
                                prefix, key, pk_value, type_name_pk, key_pk)
                pk_value += 1
            self.create_update(table_name, e, type_name,
                                prefix, key, pk_value, type_name_pk, key_pk)
            pk_value += 1
            for _ in range(n):
                self.create_update(table_name, f, type_name,
                                prefix, key, pk_value, type_name_pk, key_pk)
                pk_value += 1
            for _ in range((n+m)//2):
                self.create_update(table_name, g, type_name,
                                prefix, key, pk_value, type_name_pk, key_pk)
                pk_value += 1
            for _ in range(n+2):
                self.create_update(table_name, h, type_name,
                                prefix, key, pk_value, type_name_pk, key_pk)
                pk_value += 1
            self.create_update(table_name, i, type_name,
                                prefix, key, pk_value, type_name_pk, key_pk)
            pk_value += 1
            self.create_update(table_name, j, type_name,
                                prefix, key, pk_value, type_name_pk, key_pk)
            pk_value += 1
            for _ in range((n-1)//2):
                self.create_update(table_name, k, type_name,
                                prefix, key, pk_value, type_name_pk, key_pk)
                pk_value += 1
                self.create_update(table_name, l, type_name,
                                prefix, key, pk_value, type_name_pk, key_pk)
                pk_value += 1
    
    def update_back(self, table_name: str, type_name, prefix, key, type_name_pk, key_pk):
        for i in range(1,3):
            self.create_update(table_name, i, type_name, prefix, key, i, type_name_pk, key_pk)

    def create_update(self, table_name, value, type_name, prefix, key, value_pk, type_name_pk, key_pk):
        update_sql = f"""
            UPDATE `{table_name}` SET {prefix}{cleanup_type_name(type_name)} = {format_sql_value(key(value), type_name)}
            where pk_{cleanup_type_name(type_name_pk)} = {format_sql_value(key_pk(value_pk), type_name_pk)}
        """
        print(update_sql)
        self.query(update_sql)
        
    def insert(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        number_of_columns = 22
        for count in range(1, number_of_columns + 1):
            self.create_insert(table_name, 1, count, all_types,
                               pk_types, index, ttl)
        for count in range(number_of_columns + 1, 2*number_of_columns + 1):
            self.create_insert(table_name, 2, count, all_types,
                               pk_types, index, ttl)

    def create_insert(self, table_name: str, value: int, value_pk: int, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        insert_sql = f"""
            INSERT INTO {table_name}(
                {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])}{", " if len(all_types) != 0 else ""}
                {", ".join(["col_" + cleanup_type_name(type_name) for type_name in all_types.keys()])}{", " if len(index) != 0 else ""}
                {", ".join(["col_index_" + cleanup_type_name(type_name) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                {f"ttl_{ttl}" if ttl != "" else ""}
            )
            VALUES(
                {", ".join([format_sql_value(pk_types[type_name](value_pk), type_name) for type_name in pk_types.keys()])}{", " if len(all_types) != 0 else ""}
                {", ".join([format_sql_value(all_types[type_name](value), type_name) for type_name in all_types.keys()])}{", " if len(index) != 0 else ""}
                {", ".join([format_sql_value(index[type_name](value), type_name) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                {format_sql_value(ttl_types[ttl](value), ttl) if ttl != "" else ""}
            );
        """
        print(insert_sql)
        self.query(insert_sql)

    def create_match_recognize(self, table_name, partition, order, col):
        match_recognize_sql = f"""
            PRAGMA FeatureR010="prototype";
            select * from {table_name} MATCH_RECOGNIZE(
                PARTITION BY {partition}
                ORDER BY {order}
                MEASURES
                    LAST(A.{col}) AS a,
                    LAST(L.{col}) AS l
                ONE ROW PER MATCH          
                AFTER MATCH SKIP TO NEXT ROW 
                PATTERN (A+ B* C* D? E? F{{5}} G{{5, 10}} H{{5,}} (I|J) (I|J) (I|J){{,10}})  
                DEFINE
                A as A.{col}=1,
                B as B.{col}=2,
                C as C.{col}=3,
                D as D.{col}=4,
                E as E.{col}=5,
                F as F.{col}=6,
                G as G.{col}=7,
                H as H.{col}=8,
                I as I.{col}=9,
                J as J.{col}=10,
                K as K.{col}=11,
                L as L.{col}=12
            );
        """
        print(match_recognize_sql)
        rows = self.query(match_recognize_sql)
        print(rows)
