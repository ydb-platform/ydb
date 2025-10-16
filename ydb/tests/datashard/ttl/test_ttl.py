import os
import subprocess
import pytest

from datetime import datetime, timedelta
from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.datashard.lib.create_table import create_ttl_sql_request
from ydb.tests.datashard.lib.dml_operations import DMLOperations
from ydb.tests.datashard.lib.types_of_variables import (
    cleanup_type_name,
    format_sql_value,
    pk_types,
    non_pk_types,
    index_first,
    index_second,
    index_first_not_Bool,
)


ttl_types = {
    "Datetime": lambda i: i,
    "Timestamp": lambda i: i * 1000000,
    "DyNumber": lambda i: i,
    "Uint32": lambda i: i,
    "Uint64": lambda i: i,
    "Date": lambda i: i,
}


class TestTTL(TestBase):
    @pytest.mark.parametrize(
        "table_name, pk_types, all_types, index, ttl, unique, sync",
        [
            ("table_Datetime_0__SYNC", pk_types, {**pk_types, **non_pk_types}, index_first, "Datetime", "", "SYNC"),
            ("table_Datetime_0__ASYNC", pk_types, {**pk_types, **non_pk_types}, index_first, "Datetime", "", "ASYNC"),
            (
                "table_Datetime_0_UNIQUE_SYNC",
                pk_types,
                {**pk_types, **non_pk_types},
                index_first_not_Bool,
                "Datetime",
                "UNIQUE",
                "SYNC",
            ),
            ("table_Datetime_1__SYNC", pk_types, {**pk_types, **non_pk_types}, index_second, "Datetime", "", "SYNC"),
            ("table_Datetime_1__ASYNC", pk_types, {**pk_types, **non_pk_types}, index_second, "Datetime", "", "ASYNC"),
            (
                "table_Datetime_1_UNIQUE_SYNC",
                pk_types,
                {**pk_types, **non_pk_types},
                index_second,
                "Datetime",
                "UNIQUE",
                "SYNC",
            ),
            ("table_Timestamp_0__SYNC", pk_types, {**pk_types, **non_pk_types}, index_first, "Timestamp", "", "SYNC"),
            ("table_Timestamp_0__ASYNC", pk_types, {**pk_types, **non_pk_types}, index_first, "Timestamp", "", "ASYNC"),
            (
                "table_Timestamp_0_UNIQUE_SYNC",
                pk_types,
                {**pk_types, **non_pk_types},
                index_first_not_Bool,
                "Timestamp",
                "UNIQUE",
                "SYNC",
            ),
            ("table_Timestamp_1__SYNC", pk_types, {**pk_types, **non_pk_types}, index_second, "Timestamp", "", "SYNC"),
            (
                "table_Timestamp_1__ASYNC",
                pk_types,
                {**pk_types, **non_pk_types},
                index_second,
                "Timestamp",
                "",
                "ASYNC",
            ),
            (
                "table_Timestamp_1_UNIQUE_SYNC",
                pk_types,
                {**pk_types, **non_pk_types},
                index_second,
                "Timestamp",
                "UNIQUE",
                "SYNC",
            ),
            ("table_DyNumber_0__SYNC", pk_types, {**pk_types, **non_pk_types}, index_first, "DyNumber", "", "SYNC"),
            ("table_DyNumber_0__ASYNC", pk_types, {**pk_types, **non_pk_types}, index_first, "DyNumber", "", "ASYNC"),
            (
                "table_DyNumber_0_UNIQUE_SYNC",
                pk_types,
                {**pk_types, **non_pk_types},
                index_first_not_Bool,
                "DyNumber",
                "UNIQUE",
                "SYNC",
            ),
            ("table_DyNumber_1__SYNC", pk_types, {**pk_types, **non_pk_types}, index_second, "DyNumber", "", "SYNC"),
            ("table_DyNumber_1__ASYNC", pk_types, {**pk_types, **non_pk_types}, index_second, "DyNumber", "", "ASYNC"),
            (
                "table_DyNumber_1_UNIQUE_SYNC",
                pk_types,
                {**pk_types, **non_pk_types},
                index_second,
                "DyNumber",
                "UNIQUE",
                "SYNC",
            ),
            ("table_Uint32_0__SYNC", pk_types, {**pk_types, **non_pk_types}, index_first, "Uint32", "", "SYNC"),
            ("table_Uint32_0__ASYNC", pk_types, {**pk_types, **non_pk_types}, index_first, "Uint32", "", "ASYNC"),
            (
                "table_Uint32_0_UNIQUE_SYNC",
                pk_types,
                {**pk_types, **non_pk_types},
                index_first_not_Bool,
                "Uint32",
                "UNIQUE",
                "SYNC",
            ),
            ("table_Uint32_1__SYNC", pk_types, {**pk_types, **non_pk_types}, index_second, "Uint32", "", "SYNC"),
            ("table_Uint32_1__ASYNC", pk_types, {**pk_types, **non_pk_types}, index_second, "Uint32", "", "ASYNC"),
            (
                "table_Uint32_1_UNIQUE_SYNC",
                pk_types,
                {**pk_types, **non_pk_types},
                index_second,
                "Uint32",
                "UNIQUE",
                "SYNC",
            ),
            ("table_Uint64_0__SYNC", pk_types, {**pk_types, **non_pk_types}, index_first, "Uint64", "", "SYNC"),
            ("table_Uint64_0__ASYNC", pk_types, {**pk_types, **non_pk_types}, index_first, "Uint64", "", "ASYNC"),
            (
                "table_Uint64_0_UNIQUE_SYNC",
                pk_types,
                {**pk_types, **non_pk_types},
                index_first_not_Bool,
                "Uint64",
                "UNIQUE",
                "SYNC",
            ),
            ("table_Uint64_1__SYNC", pk_types, {**pk_types, **non_pk_types}, index_second, "Uint64", "", "SYNC"),
            ("table_Uint64_1__ASYNC", pk_types, {**pk_types, **non_pk_types}, index_second, "Uint64", "", "ASYNC"),
            (
                "table_Uint64_1_UNIQUE_SYNC",
                pk_types,
                {**pk_types, **non_pk_types},
                index_second,
                "Uint64",
                "UNIQUE",
                "SYNC",
            ),
            ("table_Date_0__SYNC", pk_types, {**pk_types, **non_pk_types}, index_first, "Date", "", "SYNC"),
            ("table_Date_0__ASYNC", pk_types, {**pk_types, **non_pk_types}, index_first, "Date", "", "ASYNC"),
            (
                "table_Date_0_UNIQUE_SYNC",
                pk_types,
                {**pk_types, **non_pk_types},
                index_first_not_Bool,
                "Date",
                "UNIQUE",
                "SYNC",
            ),
            ("table_Date_1__SYNC", pk_types, {**pk_types, **non_pk_types}, index_second, "Date", "", "SYNC"),
            ("table_Date_1__ASYNC", pk_types, {**pk_types, **non_pk_types}, index_second, "Date", "", "ASYNC"),
            (
                "table_Date_1_UNIQUE_SYNC",
                pk_types,
                {**pk_types, **non_pk_types},
                index_second,
                "Date",
                "UNIQUE",
                "SYNC",
            ),
        ],
    )
    def test_ttl(
        self,
        table_name: str,
        pk_types: dict[str, str],
        all_types: dict[str, str],
        index: dict[str, str],
        ttl: str,
        unique: str,
        sync: str,
    ):
        self.ttl_time_sec(1)
        dml = DMLOperations(self)
        dml.create_table(table_name, pk_types, all_types, index, ttl, unique, sync)
        dml.query(f"ALTER TABLE `{table_name}` RESET (TTL)")
        dml.query(
            create_ttl_sql_request(
                f"ttl_{ttl}",
                {"PT0S": ""},
                "SECONDS" if ttl == "Uint32" or ttl == "Uint64" or ttl == "DyNumber" else "",
                table_name,
            )
        )
        self.insert(table_name, pk_types, all_types, index, ttl)
        self.select(table_name, pk_types, all_types, index, dml)

    def insert(
        self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str
    ):
        now_date = datetime.now()
        old_date = now_date - timedelta(weeks=1)
        future_time_5_sec = now_date + timedelta(seconds=5)
        future_time = now_date + timedelta(weeks=2)
        if ttl != "Date" and ttl != "Datetime":
            now_date = int(datetime.timestamp(now_date))
            old_date = int(datetime.timestamp(old_date))
            future_time_5_sec = int(datetime.timestamp(future_time_5_sec))
            future_time = int(datetime.timestamp(future_time))
        elif ttl != "Datetime":
            now_date = str(now_date.date())
            old_date = str(old_date.date())
            future_time_5_sec = str(future_time_5_sec.date())
            future_time = str(future_time.date())
        for i in range(1, 3):
            self.create_insert(table_name, pk_types, all_types, index, ttl, i, old_date)
        for i in range(3, 5):
            self.create_insert(table_name, pk_types, all_types, index, ttl, i, now_date)
        for i in range(5, 7):
            self.create_insert(table_name, pk_types, all_types, index, ttl, i, future_time_5_sec)
        for i in range(7, 9):
            self.create_insert(table_name, pk_types, all_types, index, ttl, i, future_time)

    def create_insert(
        self,
        table_name: str,
        pk_types: dict[str, str],
        all_types: dict[str, str],
        index: dict[str, str],
        ttl: str,
        value: int,
        date,
    ):
        insert_sql = f"""
            INSERT INTO {table_name}(
                {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])}{", " if len(all_types) != 0 else ""}
                {", ".join(["col_" + cleanup_type_name(type_name) for type_name in all_types.keys()])}{", " if len(index) != 0 else ""}
                {", ".join(["col_index_" + cleanup_type_name(type_name) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                {f"ttl_{ttl}" if ttl != "" else ""}
            )
            VALUES(
                {", ".join([format_sql_value(pk_types[type_name](value), type_name) for type_name in pk_types.keys()])}{", " if len(all_types) != 0 else ""}
                {", ".join([format_sql_value(all_types[type_name](value), type_name) for type_name in all_types.keys()])}{", " if len(index) != 0 else ""}
                {", ".join([format_sql_value(index[type_name](value), type_name) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                {format_sql_value(ttl_types[ttl](date), ttl) if ttl != "" else ""}
            );
        """
        self.query(insert_sql)

    def select(
        self,
        table_name: str,
        pk_types: dict[str, str],
        all_types: dict[str, str],
        index: dict[str, str],
        dml: DMLOperations,
    ):
        for i in range(1, 7):
            self.create_select(table_name, pk_types, all_types, index, i, 0, dml)

        for i in range(7, 9):
            self.create_select(table_name, pk_types, all_types, index, i, 1, dml)

    def create_select(
        self,
        table_name: str,
        pk_types: dict[str, str],
        all_types: dict[str, str],
        index: dict[str, str],
        value: int,
        expected_count_rows: int,
        dml: DMLOperations,
    ):
        sql_select = dml.create_select_sql_request(
            table_name, all_types, value, pk_types, value, index, value, "", value
        )
        wait_for(self.create_predicate(sql_select, expected_count_rows), timeout_seconds=200)
        rows = dml.query(sql_select)
        assert (
            len(rows) == 1 and rows[0].count == expected_count_rows
        ), f"Expected {expected_count_rows} rows, error when deleting {value} lines, table {table_name}"

    def alter_database_quotas(self, node, database_path, database_quotas):
        alter_proto = """ModifyScheme {
            OperationType: ESchemeOpAlterSubDomain
            WorkingDir: "%s"
            SubDomain {
                Name: "%s"
                DatabaseQuotas {
                    %s
                }
            }
        }""" % (
            os.path.dirname(database_path),
            os.path.basename(database_path),
            database_quotas,
        )

        self.ydbcli_db_schema_exec(node, alter_proto)

    def ttl_time_sec(self, time: int):
        self.alter_database_quotas(
            self.cluster.nodes[1],
            '/Root',
            f"""
            ttl_min_run_internal_seconds: {time}
        """,
        )

    def ydbcli_db_schema_exec(self, node, operation_proto):
        endpoint = f"{node.host}:{node.port}"
        args = [
            node.binary_path,
            f"--server=grpc://{endpoint}",
            "db",
            "schema",
            "exec",
            operation_proto,
        ]
        command = subprocess.run(args, capture_output=True)
        assert command.returncode == 0, command.stderr.decode("utf-8")

    def create_predicate(self, sql_select, expected_count_rows):
        def predicate():
            rows = self.query(sql_select)
            return len(rows) == 1 and rows[0].count == expected_count_rows

        return predicate
