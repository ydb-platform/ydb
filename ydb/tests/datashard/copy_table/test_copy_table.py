import os
import pytest
import yatest

from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.datashard.lib.test_pg_base import TestPgBase
from ydb.tests.datashard.lib.dml_operations import DMLOperations
from ydb.tests.datashard.lib.types_of_variables import pk_types, non_pk_types, index_first, index_second, \
    index_first_sync, index_second_sync, index_three_sync, index_three_sync_not_Bool, index_four_sync, \
    index_zero_sync, pk_pg_types, non_pk_pg_types, filter_dict


class TestCopyTableBase(TestBase):
    def do_test_copy_table(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str):
        dml = DMLOperations(self)
        dml.create_table(table_name, pk_types, all_types,
                         index, ttl, unique, sync)
        dml.insert(table_name, all_types, pk_types, index, ttl)
        yatest.common.execute([
            yatest.common.binary_path(os.getenv('YDB_CLI_BINARY')),
            '-e', 'grpc://'+self.get_endpoint(),
            f"--database={self.get_database()}",
            'tools', 'copy',
            '--item', f"destination=copy_{table_name},source={table_name}"
        ]).stdout.decode("utf-8")
        dml.select_after_insert(
            f"copy_{table_name}", all_types, pk_types, index, ttl)


class TestCopyTable(TestCopyTableBase):
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
    def test_copy_table(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str):
        self.do_test_copy_table(table_name, pk_types, all_types, index, ttl, unique, sync)


class TestPgCopyTable(TestPgBase, TestCopyTableBase):
    @pytest.mark.parametrize(
        "table_name, pk_types, all_types, index, ttl, unique, sync",
        [
            ("table_index_10_UNIQUE_SYNC", pk_pg_types, {}, filter_dict(pk_pg_types, "pginterval"), "", "UNIQUE", "SYNC"),
            ("table_index_9_UNIQUE_SYNC", pk_pg_types, {}, filter_dict(pk_pg_types, "pgtimestamp"), "", "UNIQUE", "SYNC"),
            ("table_index_8_UNIQUE_SYNC", pk_pg_types, {}, filter_dict(pk_pg_types, "pgdate"), "", "UNIQUE", "SYNC"),
            ("table_index_7_UNIQUE_SYNC", pk_pg_types, {}, filter_dict(pk_pg_types, "pguuid"), "", "UNIQUE", "SYNC"),
            ("table_index_6_UNIQUE_SYNC", pk_pg_types, {}, filter_dict(pk_pg_types, "pgvarchar"), "", "UNIQUE", "SYNC"),
            ("table_index_5_UNIQUE_SYNC", pk_pg_types, {}, filter_dict(pk_pg_types, "pgtext"), "", "UNIQUE", "SYNC"),
            ("table_index_4_UNIQUE_SYNC", pk_pg_types, {}, filter_dict(pk_pg_types, "pgbytea"), "", "UNIQUE", "SYNC"),
            ("table_index_3_UNIQUE_SYNC", pk_pg_types, {}, filter_dict(pk_pg_types, "pgnumeric"), "", "UNIQUE", "SYNC"),
            ("table_index_2_UNIQUE_SYNC", pk_pg_types, {}, filter_dict(pk_pg_types, "pgint8"), "", "UNIQUE", "SYNC"),
            ("table_index_1_UNIQUE_SYNC", pk_pg_types, {}, filter_dict(pk_pg_types, "pgint4"), "", "UNIQUE", "SYNC"),
            ("table_index_0_UNIQUE_SYNC", pk_pg_types, {}, filter_dict(pk_pg_types, "pgint2"), "", "UNIQUE", "SYNC"),
            ("table_index_0__SYNC", pk_pg_types, {},
             pk_pg_types, "", "", "SYNC"),
            ("table_index_0__ASYNC", pk_pg_types, {}, pk_pg_types, "", "", "ASYNC"),
            ("table_all_types", pk_pg_types, {
             **pk_pg_types, **non_pk_pg_types}, {}, "", "", ""),
            ("table_ttl_pgint4", pk_pg_types, {}, {}, "pgint4", "", ""),
            ("table_ttl_pgint8", pk_pg_types, {}, {}, "pgint8", "", ""),
            ("table_ttl_pgdate", pk_pg_types, {}, {}, "pgdate", "", ""),
            ("table_ttl_pgtimestamp", pk_pg_types, {}, {}, "pgtimestamp", "", ""),
        ]
    )
    def test_copy_table(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str):
        self.do_test_copy_table(table_name, pk_types, all_types, index, ttl, unique, sync)
