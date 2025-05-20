import pytest

from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.datashard.lib.dml_operations import DMLOperations
from ydb.tests.datashard.lib.types_of_variables import (
    cleanup_type_name,
    format_sql_value,
    pk_types,
    non_pk_types,
    index_first,
    index_second,
    ttl_types,
    index_first_sync,
    index_second_sync,
    index_three_sync,
    index_three_sync_not_Bool,
    index_four_sync,
    index_zero_sync,
)

unsuppored_time_types = [
    "Date32",
    "Datetime64",
    "Timestamp64",
    "Interval64",
]  # https://github.com/ydb-platform/ydb/issues/16930
unsuppored_distinct_types = [
    "DyNumber",
    "UUID",  # https://github.com/ydb-platform/ydb/issues/17484
]
uncomparable_types = ["Json", "JsonDocument", "Yson"]


class TestDML(TestBase):
    @pytest.mark.parametrize(
        "table_name, pk_types, all_types, index, ttl, unique, sync",
        [
            ("table_index_4_UNIQUE_SYNC", pk_types, {}, index_four_sync, "", "UNIQUE", "SYNC"),
            ("table_index_3_UNIQUE_SYNC", pk_types, {}, index_three_sync_not_Bool, "", "UNIQUE", "SYNC"),
            ("table_index_2_UNIQUE_SYNC", pk_types, {}, index_second_sync, "", "UNIQUE", "SYNC"),
            ("table_index_1_UNIQUE_SYNC", pk_types, {}, index_first_sync, "", "UNIQUE", "SYNC"),
            ("table_index_0_UNIQUE_SYNC", pk_types, {}, index_zero_sync, "", "UNIQUE", "SYNC"),
            ("table_index_4__SYNC", pk_types, {}, index_four_sync, "", "", "SYNC"),
            ("table_index_3__SYNC", pk_types, {}, index_three_sync, "", "", "SYNC"),
            ("table_index_2__SYNC", pk_types, {}, index_second_sync, "", "", "SYNC"),
            ("table_index_1__SYNC", pk_types, {}, index_first_sync, "", "", "SYNC"),
            ("table_index_0__SYNC", pk_types, {}, index_zero_sync, "", "", "SYNC"),
            ("table_index_1__ASYNC", pk_types, {}, index_second, "", "", "ASYNC"),
            ("table_index_0__ASYNC", pk_types, {}, index_first, "", "", "ASYNC"),
            ("table_all_types", pk_types, {**pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_ttl_DyNumber", pk_types, {}, {}, "DyNumber", "", ""),
            ("table_ttl_Uint32", pk_types, {}, {}, "Uint32", "", ""),
            ("table_ttl_Uint64", pk_types, {}, {}, "Uint64", "", ""),
            ("table_ttl_Datetime", pk_types, {}, {}, "Datetime", "", ""),
            ("table_ttl_Timestamp", pk_types, {}, {}, "Timestamp", "", ""),
            ("table_ttl_Date", pk_types, {}, {}, "Date", "", ""),
        ],
    )
    def test_select(
        self,
        table_name: str,
        pk_types: dict[str, str],
        all_types: dict[str, str],
        index: dict[str, str],
        ttl: str,
        unique: str,
        sync: str,
    ):
        dml = DMLOperations(self)
        dml.create_table(table_name, pk_types, all_types, index, ttl, unique, sync)
        dml.insert(table_name, all_types, pk_types, index, ttl)
        self.select_with(table_name, all_types, pk_types, index, ttl, dml)
        self.order_by(table_name, all_types, pk_types, index, ttl, dml)
        self.limit(table_name, all_types, pk_types, index, ttl, dml)
        self.from_select(table_name, all_types, pk_types, index, ttl, dml)
        self.distinct(table_name, all_types, pk_types, index, ttl, dml)
        self.union(table_name, all_types, pk_types, index, ttl, dml)
        self.without(table_name, all_types, pk_types, index, ttl)
        self.tablesample_sample(table_name, all_types, pk_types, index, ttl, dml)

    def limit(
        self,
        table_name: str,
        all_types: dict[str, str],
        pk_types: dict[str, str],
        index: dict[str, str],
        ttl: str,
        dml: DMLOperations,
    ):
        selected_columns = self.create_types_for_all_select(all_types, pk_types, index, ttl)

        number_of_columns = self.get_number_of_columns(pk_types, all_types, index, ttl)

        for offset in range(number_of_columns):
            rows = self.query(f"select {", ".join(selected_columns)} from {table_name} limit 1 OFFSET {offset}")
            self.assert_type_after_select(offset + 1, rows[0], all_types, pk_types, index, ttl, dml)

    def create_types_for_all_select(
        self, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str
    ):
        selected_columns = []
        for type in all_types.keys():
            if type not in unsuppored_time_types:
                selected_columns.append(f"col_{cleanup_type_name(type)}")
        for type in pk_types.keys():
            if type not in unsuppored_time_types:
                selected_columns.append(f"pk_{cleanup_type_name(type)}")
        for type in index.keys():
            if type not in unsuppored_time_types:
                selected_columns.append(f"col_index_{cleanup_type_name(type)}")
        if ttl != "":
            selected_columns.append(f"ttl_{cleanup_type_name(ttl)}")
        return selected_columns

    def assert_type_after_select(
        self,
        value,
        row,
        all_types: dict[str, str],
        pk_types: dict[str, str],
        index: dict[str, str],
        ttl: str,
        dml: DMLOperations,
    ):
        col_idx = 0
        for type in all_types.keys():
            if type not in unsuppored_time_types:
                dml.assert_type(all_types, type, value, row[col_idx])
                col_idx += 1
        for type in pk_types.keys():
            if type not in unsuppored_time_types:
                dml.assert_type(pk_types, type, value, row[col_idx])
                col_idx += 1
        for type in index.keys():
            if type not in unsuppored_time_types:
                dml.assert_type(index, type, value, row[col_idx])
                col_idx += 1
        if ttl != "":
            dml.assert_type(ttl_types, ttl, value, row[col_idx])
            col_idx += 1

    def from_select(
        self,
        table_name: str,
        all_types: dict[str, str],
        pk_types: dict[str, str],
        index: dict[str, str],
        ttl: str,
        dml: DMLOperations,
    ):
        selected_columns = self.create_types_for_all_select(all_types, pk_types, index, ttl)

        rows = self.query(f"from {table_name} select {", ".join(selected_columns)}")
        for i, row in enumerate(rows):
            self.assert_type_after_select(i + 1, row, all_types, pk_types, index, ttl, dml)

    def distinct(
        self,
        table_name: str,
        all_types: dict[str, str],
        pk_types: dict[str, str],
        index: dict[str, str],
        ttl: str,
        dml: DMLOperations,
    ):
        for type in all_types.keys():
            if (
                type not in unsuppored_time_types
                and type not in unsuppored_distinct_types
                and type not in uncomparable_types
            ):
                rows_distinct = self.query(f"SELECT DISTINCT col_{cleanup_type_name(type)} from {table_name}")
                for i in range(len(rows_distinct)):
                    dml.assert_type(all_types, type, i + 1, rows_distinct[i][0])
        for type in pk_types.keys():
            if type not in unsuppored_time_types and type not in unsuppored_distinct_types:
                rows_distinct = self.query(f"SELECT DISTINCT pk_{cleanup_type_name(type)} from {table_name}")
                for i in range(len(rows_distinct)):
                    dml.assert_type(pk_types, type, i + 1, rows_distinct[i][0])
        for type in index.keys():
            if type not in unsuppored_time_types and type not in unsuppored_distinct_types:
                rows_distinct = self.query(f"SELECT DISTINCT col_index_{cleanup_type_name(type)} from {table_name}")
                for i in range(len(rows_distinct)):
                    dml.assert_type(index, type, i + 1, rows_distinct[i][0])
        if ttl != "" and ttl != "DyNumber":
            rows_distinct = self.query(f"SELECT DISTINCT ttl_{cleanup_type_name(ttl)} from {table_name}")
            for i in range(len(rows_distinct)):
                dml.assert_type(ttl_types, ttl, i + 1, rows_distinct[i][0])

    def union(
        self,
        table_name: str,
        all_types: dict[str, str],
        pk_types: dict[str, str],
        index: dict[str, str],
        ttl: str,
        dml: DMLOperations,
    ):
        selected_columns = []
        for type in all_types.keys():
            if (
                type not in unsuppored_time_types
                and type not in unsuppored_distinct_types
                and type not in uncomparable_types
            ):
                selected_columns.append(f"col_{cleanup_type_name(type)}")
        for type in pk_types.keys():
            if type not in unsuppored_time_types and type not in unsuppored_distinct_types:
                selected_columns.append(f"pk_{cleanup_type_name(type)}")
        for type in index.keys():
            if type not in unsuppored_time_types and type not in unsuppored_distinct_types:
                selected_columns.append(f"col_index_{cleanup_type_name(type)}")
        if ttl != "" and ttl != "DyNumber":
            selected_columns.append(f"ttl_{cleanup_type_name(ttl)}")
        rows = self.query(
            f"""
                          select {", ".join(selected_columns)} from {table_name}
                          union
                          select {", ".join(selected_columns)} from {table_name}
                          """
        )
        for type in all_types.keys():
            if (
                type not in unsuppored_time_types
                and type not in unsuppored_distinct_types
                and type not in uncomparable_types
            ):
                for line in range(len(rows)):
                    dml.assert_type(all_types, type, line + 1, rows[line][f"col_{cleanup_type_name(type)}"])
        for type in pk_types.keys():
            if type not in unsuppored_time_types and type not in unsuppored_distinct_types:
                for line in range(len(rows)):
                    dml.assert_type(pk_types, type, line + 1, rows[line][f"pk_{cleanup_type_name(type)}"])
        for type in index.keys():
            if type not in unsuppored_time_types and type not in unsuppored_distinct_types:
                for line in range(len(rows)):
                    dml.assert_type(index, type, line + 1, rows[line][f"col_index_{cleanup_type_name(type)}"])
        if ttl != "" and ttl != "DyNumber":
            for line in range(len(rows)):
                dml.assert_type(ttl_types, ttl, line + 1, rows[line][f"ttl_{cleanup_type_name(ttl)}"])

    def select_with(
        self,
        table_name: str,
        all_types: dict[str, str],
        pk_types: dict[str, str],
        index: dict[str, str],
        ttl: str,
        dml: DMLOperations,
    ):
        select_with_parameters = [
            "INFER_SCHEMA",
            "FORCE_INFER_SCHEMA",
            "DIRECT_READ",
            "INLINE",
            "UNORDERED",
            "XLOCK",
            "IGNORETYPEV3",
        ]
        selected_columns = self.create_types_for_all_select(all_types, pk_types, index, ttl)
        for select_with_parameter in select_with_parameters:
            select_with_sql_request = f"""
                select {", ".join(selected_columns)}
                from {table_name}
                with {select_with_parameter}
            """
            rows = dml.query(select_with_sql_request)
            for i, row in enumerate(rows):
                self.assert_type_after_select(i + 1, row, all_types, pk_types, index, ttl, dml)

    def without(
        self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str
    ):
        selected_columns_without = []
        for type_name in all_types.keys():
            if type_name in unsuppored_time_types:
                selected_columns_without.append(f"col_{cleanup_type_name(type_name)}")
        for type_name in pk_types.keys():
            if type_name in unsuppored_time_types:
                selected_columns_without.append(f"pk_{cleanup_type_name(type_name)}")
        for type_name in index.keys():
            if type_name in unsuppored_time_types:
                selected_columns_without.append(f"col_index_{cleanup_type_name(type_name)}")

        for type_name in all_types.keys():
            if type_name not in unsuppored_time_types:
                self.create_without(table_name, selected_columns_without, f"col_{cleanup_type_name(type_name)}")
        for type_name in pk_types.keys():
            if type_name not in unsuppored_time_types:
                self.create_without(table_name, selected_columns_without, f"pk_{cleanup_type_name(type_name)}")
        for type_name in index.keys():
            if type_name not in unsuppored_time_types:
                self.create_without(table_name, selected_columns_without, f"col_index_{cleanup_type_name(type_name)}")
        if ttl != "":
            self.create_without(table_name, selected_columns_without, f"ttl_{cleanup_type_name(ttl)}")

    def create_without(self, table_name, selected_columns_without, without):
        rows = self.query(f"select * without {", ".join(selected_columns_without)}, {without} from {table_name}")
        for col_name in rows[0].keys():
            assert col_name != without, f"a column {without} in the table {table_name} was not excluded"

    def tablesample_sample(
        self,
        table_name: str,
        all_types: dict[str, str],
        pk_types: dict[str, str],
        index: dict[str, str],
        ttl: str,
        dml: DMLOperations,
    ):
        selected_columns = self.create_types_for_all_select(all_types, pk_types, index, ttl)
        for i in range(1, 100):
            rows = self.query(f"select {", ".join(selected_columns)} from {table_name} TABLESAMPLE SYSTEM({i}.0)")
            for _, row in enumerate(rows):
                numb = row["pk_Int64"]
                self.assert_type_after_select(numb, row, all_types, pk_types, index, ttl, dml)

            rows = self.query(
                f"select {", ".join(selected_columns)} from {table_name} TABLESAMPLE BERNOULLI({i}.0) REPEATABLE({i})"
            )
            for _, row in enumerate(rows):
                numb = row["pk_Int64"]
                self.assert_type_after_select(numb, row, all_types, pk_types, index, ttl, dml)

            rows = self.query(f"select {", ".join(selected_columns)} from {table_name} SAMPLE 1.0 / {i}")
            for _, row in enumerate(rows):
                numb = row["pk_Int64"]
                self.assert_type_after_select(numb, row, all_types, pk_types, index, ttl, dml)

    def order_by(
        self,
        table_name: str,
        all_types: dict[str, str],
        pk_types: dict[str, str],
        index: dict[str, str],
        ttl: str,
        dml: DMLOperations,
    ):
        count_rows = len(all_types) + len(pk_types) + len(index)
        if ttl != "":
            count_rows += 1
        string_order = [(pk_types["String"](i), i) for i in range(1, count_rows + 1)]
        string_order = sorted(string_order)
        selected_columns = self.create_types_for_all_select(all_types, pk_types, index, ttl)
        for statement in selected_columns:
            if "Json" not in statement and "JsonDocument" not in statement and "Yson" not in statement:
                rows = self.query(f"select {", ".join(selected_columns)} from {table_name} ORDER BY {statement} ASC")
                if "String" not in statement and "Utf8" not in statement:
                    for i, row in enumerate(rows):
                        self.assert_type_after_select(i + 1, row, all_types, pk_types, index, ttl, dml)
                else:
                    for i, row in enumerate(rows):
                        self.assert_type_after_select(string_order[i][1], row, all_types, pk_types, index, ttl, dml)

                rows = self.query(f"select {", ".join(selected_columns)} from {table_name} ORDER BY {statement} DESC")
                string_order.reverse()
                if "Bool" not in statement:
                    if "String" not in statement and "Utf8" not in statement:
                        for line, row in enumerate(rows):
                            self.assert_type_after_select(len(rows) - line, row, all_types, pk_types, index, ttl, dml)
                    else:
                        for i, row in enumerate(rows):
                            self.assert_type_after_select(string_order[i][1], row, all_types, pk_types, index, ttl, dml)
                else:
                    for i, row in enumerate(rows):
                        self.assert_type_after_select(i + 1, row, all_types, pk_types, index, ttl, dml)
                string_order.reverse()

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
            if type_name not in unsuppored_time_types:
                statements.append(
                    f"{format_sql_value(all_types[type_name](1), type_name)} AS pk_{cleanup_type_name(type_name)}"
                )
        list_sql = f"""
            $data = AsList(
                AsStruct({", ".join(statements)})
                );
        """
        rows = self.query(
            f"""
                   {list_sql}
                   select * from AS_TABLE($data);
                   """
        )
        for type_name in all_types.keys():
            if (
                type_name != "Date32"
                and type_name != "Datetime64"
                and type_name != "Timestamp64"
                and type_name != 'Interval64'
            ):
                if type_name == "Utf8":
                    dml.assert_type(
                        all_types, type_name, 1, rows[0][f"pk_{cleanup_type_name(type_name)}"].decode("utf-8")
                    )
                else:
                    dml.assert_type(all_types, type_name, 1, rows[0][f"pk_{cleanup_type_name(type_name)}"])
