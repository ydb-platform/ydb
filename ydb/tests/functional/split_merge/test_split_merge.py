import logging
import struct

import pytest
from uuid import UUID
from datetime import datetime, timedelta
from decimal import Decimal

from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.datashard.lib.dml_operations import DMLOperations
from ydb.tests.datashard.lib.test_pg_base import TestPgBase
from ydb.tests.datashard.lib.types_of_variables import pk_types, non_pk_types, index_first, index_second, \
    index_first_sync, index_second_sync, index_three_sync, index_three_sync_not_Bool, index_four_sync, index_zero_sync, \
    filter_dict, pk_pg_types_mixed, pk_pg_types_no_bool_mixed, non_pk_pg_types_mixed, cleanup_type_name, ttl_types


class TestSplitMergeBase(TestBase):
    @classmethod
    def get_disabled_feature_flags(cls):
        # Shard size statistics remain enabled for automatic splitting.
        return super().get_disabled_feature_flags() + ["enable_column_statistics"]

    @pytest.fixture(autouse=True)
    def avoid_logging_payloads(self):
        # Avoid expensive protobuf formatting at DEBUG.
        logger = logging.getLogger("ydb.connection")
        previous_level = logger.level
        logger.setLevel(logging.INFO)
        try:
            yield
        finally:
            logger.setLevel(previous_level)

    @classmethod
    def get_datashard_config(cls):
        # Size-based splitting needs flushed data and fresh shard statistics.
        return {
            "stats_report_interval_seconds": 1,
            "idle_mem_compaction_interval_seconds": 1,
        }

    def do_test_merge_split(
        self,
        long_string_type: str,
        table_name: str,
        pk_types: dict[str, str],
        all_types: dict[str, str],
        index: dict[str, str],
        ttl: str,
        unique: str,
        sync: str
    ):
        long_string = "a" * 100_000
        all_types[long_string_type] = lambda i: f"long_string {long_string}{i}"
        dml = DMLOperations(self)
        dml.create_table(table_name, pk_types, all_types,
                         index, ttl, unique, sync)
        # One split is enough to check data preservation.
        dml.query(
            f"alter table {table_name} set(AUTO_PARTITIONING_PARTITION_SIZE_MB = 1, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 2)")
        dml.insert(table_name, all_types, pk_types, index, ttl, batch_size=10)
        is_split = wait_for(self.expected_split(
            True, table_name), timeout_seconds=150)
        assert is_split is True, f"The table {table_name} is not split into partition"
        self.check_table_data(table_name, pk_types, all_types, index, ttl, dml)
        dml.query(
            f"alter table {table_name} set(AUTO_PARTITIONING_PARTITION_SIZE_MB = 2000, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT=1)")
        is_merge = wait_for(self.expected_split(
            False, table_name), timeout_seconds=150)
        assert is_merge is True, f"the table {table_name} is not merge into one partition"
        self.check_table_data(table_name, pk_types, all_types, index, ttl, dml)

    def check_table_data(self, table_name, pk_types, all_types, index, ttl, dml):
        columns = [
            (prefix + cleanup_type_name(type_name), type_name, types)
            for prefix, types in (("pk_", pk_types), ("col_", all_types), ("col_index_", index))
            for type_name in types
        ]
        if ttl:
            columns.append((f"ttl_{cleanup_type_name(ttl)}", ttl, ttl_types))

        # Every schema has an Int64/pgint8 column identifying the generated row.
        row_number_column = next(name for name, type_name, _ in columns if type_name in ("Int64", "pgint8"))
        primary_key = ", ".join(f"pk_{cleanup_type_name(type_name)}" for type_name in pk_types)
        rows = self.query(f"SELECT {', '.join(name for name, _, _ in columns)} FROM `{table_name}` ORDER BY {primary_key}")
        row_numbers = [int(row[row_number_column]) for row in rows]
        assert sorted(row_numbers) == list(range(1, len(columns) + 1)), f"Unexpected rows in {table_name}: {row_numbers}"

        for row, number in zip(rows, row_numbers):
            for name, type_name, types in columns:
                actual = row[name]
                if type_name == "Float":
                    expected = struct.unpack("f", struct.pack("f", types[type_name](number)))[0]
                elif type_name == "DyNumber":
                    actual = Decimal(str(actual))
                    expected = Decimal(str(types[type_name](number)))
                elif type_name in ("Timestamp", "Timestamp64"):
                    expected = datetime(1970, 1, 1) + timedelta(microseconds=types[type_name](number))
                else:
                    dml.assert_type(types, type_name, number, actual)
                    continue
                assert actual == expected, f"{table_name}: {name}, row {number}: expected {expected}, got {actual}"

        # Check point reads across the key range after each transition.
        for position in (0, len(rows) // 2, len(rows) - 1):
            number = row_numbers[position]
            query = dml.create_select_sql_request(
                table_name, all_types, number, pk_types, number, index, number, ttl, number)
            result = dml.query(query)
            assert len(result) == 1 and result[0].count == 1, f"Point read failed in {table_name}, row {number}"

    def expected_split(self, is_split, table_name):
        def predicate():
            rows = self.query(f"""SELECT
                count(*) as count
                FROM `.sys/partition_stats`
                WHERE Path = "{self.get_database()}/{table_name}"
                """)
            if is_split:
                return rows[0].count > 1
            else:
                return rows[0].count == 1
        return predicate


class TestSplitMerge(TestSplitMergeBase):
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
            ("table_Int64", {"Int64": lambda i: i},
             {**pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_Int32", {"Int32": lambda i: i},
             {**pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_Int16", {"Int16": lambda i: i},
             {**pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_Int8", {"Int8": lambda i: i}, {
             **pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_Uint64", {"Uint64": lambda i: i},
             {**pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_Uint32", {"Uint32": lambda i: i},
             {**pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_Uint16", {"Uint16": lambda i: i},
             {**pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_Uint8", {"Uint8": lambda i: i},
             {**pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_Decimal150", {
                "Decimal(15,0)": lambda i: "{}".format(i)}, {**pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_Decimal229", {
             "Decimal(22,9)": lambda i: "{}.123".format(i)}, {**pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_Decimal3510", {
             "Decimal(35,10)": lambda i: "{}.123456".format(i)}, {**pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_DyNumber", {
             "DyNumber": lambda i: float(f"{i}e1")}, {**pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_String", {
             "String": lambda i: f"String {i}"}, {**pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_Utf8", {"Utf8": lambda i: f"Utf8 {i}"},
             {**pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_UUID", {"Utf8": lambda i: UUID("3{:03}5678-e89b-12d3-a456-556642440000".format(i))},
             {**pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_Date", {
             "Date": lambda i: datetime.strptime("2{:03}-01-01".format(i), "%Y-%m-%d").date()}, {**pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_Datetime", {
             "Datetime": lambda i: datetime.strptime("2{:03}-10-02T11:00:00Z".format(i), "%Y-%m-%dT%H:%M:%SZ")}, {**pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_Timestamp", {
             "Timestamp": lambda i: 1696200000000000 + i * 100000000}, {**pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_Interval", {
             "Interval": lambda i: i}, {**pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_Date32", {
             "Date32": lambda i: datetime.strptime("2{:03}-01-01".format(i), "%Y-%m-%d").date()}, {**pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_Datetime64", {
             "Datetime64": lambda i: datetime.strptime("2{:03}-10-02T11:00:00Z".format(i), "%Y-%m-%dT%H:%M:%SZ")}, {**pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_Timestamp64", {
             "Timestamp64": lambda i: 1696200000000000 + i * 100000000}, {**pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_Interval64", {
             "Interval64": lambda i: i}, {**pk_types, **non_pk_types}, {}, "", "", ""),
        ]
    )
    def test_merge_split(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str):
        self.do_test_merge_split("String", table_name, pk_types, all_types, index, ttl, unique, sync)


class TestPgSplitMerge(TestPgBase, TestSplitMergeBase):
    @pytest.mark.parametrize(
        "table_name, pk_types, all_types, index, ttl, unique, sync",
        [
            ("table_index_0_UNIQUE_SYNC", pk_pg_types_mixed, {}, pk_pg_types_no_bool_mixed, "", "UNIQUE", "SYNC"),
            ("table_index_0__SYNC", pk_pg_types_mixed, {}, pk_pg_types_mixed, "", "", "SYNC"),
            ("table_index_0__ASYNC", pk_pg_types_mixed, {}, pk_pg_types_mixed, "", "", "ASYNC"),
            ("table_all_types", pk_pg_types_mixed, {**pk_pg_types_mixed, **non_pk_pg_types_mixed}, {}, "", "", ""),
            ("table_ttl_pgint4", pk_pg_types_mixed, {}, {}, "pgint4", "", ""),
            ("table_ttl_pgint8", pk_pg_types_mixed, {}, {}, "pgint8", "", ""),
            ("table_ttl_pgdate", pk_pg_types_mixed, {}, {}, "pgdate", "", ""),
            ("table_ttl_pgtimestamp", pk_pg_types_mixed, {}, {}, "pgtimestamp", "", ""),
            ("table_pgint2", filter_dict(pk_pg_types_mixed, "pgint2"), {**pk_pg_types_mixed, **non_pk_pg_types_mixed}, {}, "", "", ""),
            ("table_pgint4", filter_dict(pk_pg_types_mixed, "pgint4"), {**pk_pg_types_mixed, **non_pk_pg_types_mixed}, {}, "", "", ""),
            ("table_pgint8", filter_dict(pk_pg_types_mixed, "pgint8"), {**pk_pg_types_mixed, **non_pk_pg_types_mixed}, {}, "", "", ""),
            ("table_pgnumeric", filter_dict(pk_pg_types_mixed, "pgnumeric"), {**pk_pg_types_mixed, **non_pk_pg_types_mixed}, {}, "", "", ""),
            ("table_pgbytea", filter_dict(pk_pg_types_mixed, "pgbytea"), {**pk_pg_types_mixed, **non_pk_pg_types_mixed}, {}, "", "", ""),
            ("table_pgtext", filter_dict(pk_pg_types_mixed, "pgtext"), {**pk_pg_types_mixed, **non_pk_pg_types_mixed}, {}, "", "", ""),
            ("table_pgvarchar", filter_dict(pk_pg_types_mixed, "pgvarchar"), {**pk_pg_types_mixed, **non_pk_pg_types_mixed}, {}, "", "", ""),
            ("table_pguuid", filter_dict(pk_pg_types_mixed, "pguuid"), {**pk_pg_types_mixed, **non_pk_pg_types_mixed}, {}, "", "", ""),
            ("table_pgdate", filter_dict(pk_pg_types_mixed, "pgdate"), {**pk_pg_types_mixed, **non_pk_pg_types_mixed}, {}, "", "", ""),
            ("table_pgtimestamp", filter_dict(pk_pg_types_mixed, "pgtimestamp"), {**pk_pg_types_mixed, **non_pk_pg_types_mixed}, {}, "", "", ""),
            ("table_pginterval", filter_dict(pk_pg_types_mixed, "pginterval"), {**pk_pg_types_mixed, **non_pk_pg_types_mixed}, {}, "", "", ""),
        ]
    )
    def test_merge_split(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str):
        self.do_test_merge_split("pgtext", table_name, pk_types, all_types, index, ttl, unique, sync)
