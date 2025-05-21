import pytest
import ydb

from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.datashard.lib.dml_operations import DMLOperations
from ydb.tests.datashard.lib.types_of_variables import (
    cleanup_type_name,
    format_sql_value,
    pk_types,
    non_pk_types,
    index_first,
    index_second,
    index_first_sync,
    index_second_sync,
    index_three_sync,
    index_three_sync_not_Bool,
    index_four_sync,
    index_zero_sync,
)


class TestSecondaryIndex(TestBase):
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
        ],
    )
    def test_secondary_index(
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
        self.select_index(table_name, index, dml)

    def select_index(self, table_name: str, index: dict[str, str], dml: DMLOperations):
        rows = dml.query(f"select count(*) as count from {table_name}")
        count_col = rows[0].count
        for type_name in index.keys():
            for i in range(1, count_col + 1):

                def process(session):
                    rows = self.query(
                        f"""
                            select count(*) as count
                            from {table_name} VIEW idx_col_index_{cleanup_type_name(type_name)}
                            where col_index_{cleanup_type_name(type_name)} > {format_sql_value(pk_types[type_name](i), type_name)};
                            """,
                        tx=session.transaction(tx_mode=ydb.QueryStaleReadOnly()),
                    )
                    if type_name != "Bool" and type_name != "String" and type_name != "Utf8":
                        assert (
                            len(rows) == 1 and rows[0].count == count_col - i
                        ), f"there were not enough columns by type {type_name}"
                    elif type_name != "Bool":
                        if i < 10:
                            if i * 10 > count_col:
                                count = count_col - (9 - i)
                            else:
                                count = (i - 1) * 10 + i
                        else:
                            count = i - (9 - i // 10)
                        assert (
                            len(rows) == 1 and rows[0].count == count_col - count
                        ), f"there were not enough columns by type {type_name}, numb {i}"
                    else:
                        assert (
                            len(rows) == 1 and rows[0].count == 0
                        ), f"there were not enough columns by type {type_name}"

                dml.transactional(process)

    @pytest.mark.parametrize(
        "table_name, pk_types, all_types, index, ttl, unique, sync",
        [
            (
                "table_index_4_UNIQUE_SYNC",
                pk_types,
                {**pk_types, **non_pk_types},
                index_four_sync,
                "",
                "UNIQUE",
                "SYNC",
            ),
            (
                "table_index_3_UNIQUE_SYNC",
                pk_types,
                {**pk_types, **non_pk_types},
                index_three_sync_not_Bool,
                "",
                "UNIQUE",
                "SYNC",
            ),
            (
                "table_index_2_UNIQUE_SYNC",
                pk_types,
                {**pk_types, **non_pk_types},
                index_second_sync,
                "",
                "UNIQUE",
                "SYNC",
            ),
            (
                "table_index_1_UNIQUE_SYNC",
                pk_types,
                {**pk_types, **non_pk_types},
                index_first_sync,
                "",
                "UNIQUE",
                "SYNC",
            ),
            (
                "table_index_0_UNIQUE_SYNC",
                pk_types,
                {**pk_types, **non_pk_types},
                index_zero_sync,
                "",
                "UNIQUE",
                "SYNC",
            ),
            ("table_index_4__SYNC", pk_types, {**pk_types, **non_pk_types}, index_four_sync, "", "", "SYNC"),
            ("table_index_3__SYNC", pk_types, {**pk_types, **non_pk_types}, index_three_sync, "", "", "SYNC"),
            ("table_index_2__SYNC", pk_types, {**pk_types, **non_pk_types}, index_second_sync, "", "", "SYNC"),
            ("table_index_1__SYNC", pk_types, {**pk_types, **non_pk_types}, index_first_sync, "", "", "SYNC"),
            ("table_index_0__SYNC", pk_types, {**pk_types, **non_pk_types}, index_zero_sync, "", "", "SYNC"),
            ("table_index_1__ASYNC", pk_types, {**pk_types, **non_pk_types}, index_second, "", "", "ASYNC"),
            ("table_index_0__ASYNC", pk_types, {**pk_types, **non_pk_types}, index_first, "", "", "ASYNC"),
        ],
    )
    def test_secondary_index_cover(
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
        self.create_table(table_name, pk_types, all_types, index, ttl, unique, sync, dml)
        dml.insert(table_name, all_types, pk_types, index, ttl)
        self.select_all_types(table_name, index, all_types, dml)

    def select_all_types(self, table_name: str, index: dict[str, str], all_types, dml: DMLOperations):
        rows = dml.query(f"select count(*) as count from {table_name}")
        count_col = rows[0].count
        statements = []
        # delete if after https://github.com/ydb-platform/ydb/issues/16930
        for data_type in all_types.keys():
            if (
                data_type != "Date32"
                and data_type != "Datetime64"
                and data_type != "Timestamp64"
                and data_type != 'Interval64'
            ):
                statements.append(f"col_{cleanup_type_name(data_type)}")

        for type_name in index.keys():
            if type_name != "Bool":
                for i in range(1, count_col + 1):
                    select_sql = f"""
                        select {", ".join(statements)}
                        from {table_name} VIEW idx_col_index_{cleanup_type_name(type_name)}
                        WHERE col_index_{cleanup_type_name(type_name)} = {format_sql_value(pk_types[type_name](i), type_name)};
                    """

                    def process(session):
                        rows = self.query(select_sql, tx=session.transaction(tx_mode=ydb.QueryStaleReadOnly()))
                        count = 0
                        for data_type in all_types.keys():
                            if (
                                data_type != "Date32"
                                and data_type != "Datetime64"
                                and data_type != "Timestamp64"
                                and data_type != 'Interval64'
                            ):
                                dml.assert_type(all_types, data_type, i, rows[0][count])
                                count += 1

                    dml.transactional(process)
            else:
                select_sql = f"""
                    select {", ".join(statements)}
                    from {table_name} VIEW idx_col_index_{cleanup_type_name(type_name)}
                    WHERE col_index_{cleanup_type_name(type_name)} = {format_sql_value(pk_types[type_name](1), type_name)};
                    """

                def process(session):
                    rows = self.query(select_sql, tx=session.transaction(tx_mode=ydb.QueryStaleReadOnly()))
                    count = 0
                    for data_type in all_types.keys():
                        if (
                            data_type != "Date32"
                            and data_type != "Datetime64"
                            and data_type != "Timestamp64"
                            and data_type != 'Interval64'
                        ):
                            for i in range(len(rows)):
                                dml.assert_type(all_types, data_type, i + 1, rows[i][count])
                            count += 1

                dml.transactional(process)

    def create_table(
        self,
        table_name: str,
        pk_types: dict[str, str],
        all_types: dict[str, str],
        index: dict[str, str],
        ttl: str,
        unique: str,
        sync: str,
        dml: DMLOperations,
    ):
        columns = {"pk_": pk_types.keys(), "col_": all_types.keys(), "col_index_": index.keys(), "ttl_": [ttl]}
        pk_columns = {"pk_": pk_types.keys()}
        index_columns = {"col_index_": index.keys()}
        sql_create_table = self.create_table_sql_request(table_name, columns, pk_columns, index_columns, unique, sync)
        dml.query(sql_create_table)

    def create_table_sql_request(
        self,
        table_name: str,
        columns: dict[str, dict[str]],
        pk_colums: dict[str, dict[str]],
        index_colums: dict[str, dict[str]],
        unique: str,
        sync: str,
    ):
        create_columns = []
        for prefix in columns.keys():
            if (prefix != "ttl_" or columns[prefix][0] != "") and len(columns[prefix]) != 0:
                create_columns.append(
                    ", ".join(f"{prefix}{cleanup_type_name(type_name)} {type_name}" for type_name in columns[prefix])
                )
        create_primary_key = []
        for prefix in pk_colums.keys():
            if len(pk_colums[prefix]) != 0:
                create_primary_key.append(
                    ", ".join(f"{prefix}{cleanup_type_name(type_name)}" for type_name in pk_colums[prefix])
                )
        create_index = []
        for prefix in index_colums.keys():
            if len(index_colums[prefix]) != 0:
                create_index.append(
                    ", ".join(
                        f"""INDEX idx_{prefix}{cleanup_type_name(type_name)} GLOBAL {unique} {sync} ON 
                        ({prefix}{cleanup_type_name(type_name)}) 
                        COVER ({", ".join([f"col_{cleanup_type_name(type_name_all_types)}" for type_name_all_types in columns["col_"]])})"""
                        for type_name in index_colums[prefix]
                    )
                )
        sql_create = f"""
                CREATE TABLE `{table_name}` (
                    {", ".join(create_columns)},
                    PRIMARY KEY(
                        {", ".join(create_primary_key)}
                        ),
                    {", ".join(create_index)}
                    )
            """
        return sql_create
