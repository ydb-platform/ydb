from datetime import datetime, timedelta
import time
import os
import subprocess
import pytest

from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.stress.oltp_workload.workload import cleanup_type_name
from ydb.tests.datashard.lib.create_table import create_table, pk_types, non_pk_types, index_first, index_second, index_first_not_Bool


ttl_types = {
    "Datetime": "CAST({} AS Datetime)",
    "Timestamp": "CAST({}000000 AS Timestamp)",
    "DyNumber": "CAST('{}' AS DyNumber)",
    "Uint32": "CAST({} AS Uint32)",
    "Uint64": "CAST({} AS Uint64)",
    "Date": "CAST('{}' AS Date)",
}


class TestTTL(TestBase):
    @pytest.mark.parametrize(
        "table_name, pk_types, all_types, index, ttl, unique, sync",
        [
            ("table_Datetime_0__SYNC", pk_types, {
             **pk_types, **non_pk_types}, index_first, "Datetime", "", "SYNC"),
            ("table_Datetime_0__ASYNC", pk_types, {
             **pk_types, **non_pk_types}, index_first, "Datetime", "", "ASYNC"),
            ("table_Datetime_0_UNIQUE_SYNC", pk_types, {
             **pk_types, **non_pk_types}, index_first_not_Bool, "Datetime", "UNIQUE", "SYNC"),
            ("table_Datetime_1__SYNC", pk_types, {
             **pk_types, **non_pk_types}, index_second, "Datetime", "", "SYNC"),
            ("table_Datetime_1__ASYNC", pk_types, {
             **pk_types, **non_pk_types}, index_second, "Datetime", "", "ASYNC"),
            ("table_Datetime_1_UNIQUE_SYNC", pk_types, {
             **pk_types, **non_pk_types}, index_second, "Datetime", "UNIQUE", "SYNC"),
            ("table_Timestamp_0__SYNC", pk_types, {
             **pk_types, **non_pk_types}, index_first, "Timestamp", "", "SYNC"),
            ("table_Timestamp_0__ASYNC", pk_types, {
             **pk_types, **non_pk_types}, index_first, "Timestamp", "", "ASYNC"),
            ("table_Timestamp_0_UNIQUE_SYNC", pk_types, {
             **pk_types, **non_pk_types}, index_first_not_Bool, "Timestamp", "UNIQUE", "SYNC"),
            ("table_Timestamp_1__SYNC", pk_types, {
             **pk_types, **non_pk_types}, index_second, "Timestamp", "", "SYNC"),
            ("table_Timestamp_1__ASYNC", pk_types, {
             **pk_types, **non_pk_types}, index_second, "Timestamp", "", "ASYNC"),
            ("table_Timestamp_1_UNIQUE_SYNC", pk_types, {
             **pk_types, **non_pk_types}, index_second, "Timestamp", "UNIQUE", "SYNC"),
            ("table_DyNumber_0__SYNC", pk_types, {
             **pk_types, **non_pk_types}, index_first, "DyNumber", "", "SYNC"),
            ("table_DyNumber_0__ASYNC", pk_types, {
             **pk_types, **non_pk_types}, index_first, "DyNumber", "", "ASYNC"),
            ("table_DyNumber_0_UNIQUE_SYNC", pk_types, {
             **pk_types, **non_pk_types}, index_first_not_Bool, "DyNumber", "UNIQUE", "SYNC"),
            ("table_DyNumber_1__SYNC", pk_types, {
             **pk_types, **non_pk_types}, index_second, "DyNumber", "", "SYNC"),
            ("table_DyNumber_1__ASYNC", pk_types, {
             **pk_types, **non_pk_types}, index_second, "DyNumber", "", "ASYNC"),
            ("table_DyNumber_1_UNIQUE_SYNC", pk_types, {
             **pk_types, **non_pk_types}, index_second, "DyNumber", "UNIQUE", "SYNC"),
            ("table_Uint32_0__SYNC", pk_types, {
             **pk_types, **non_pk_types}, index_first, "Uint32", "", "SYNC"),
            ("table_Uint32_0__ASYNC", pk_types, {
             **pk_types, **non_pk_types}, index_first, "Uint32", "", "ASYNC"),
            ("table_Uint32_0_UNIQUE_SYNC", pk_types, {
             **pk_types, **non_pk_types}, index_first_not_Bool, "Uint32", "UNIQUE", "SYNC"),
            ("table_Uint32_1__SYNC", pk_types, {
             **pk_types, **non_pk_types}, index_second, "Uint32", "", "SYNC"),
            ("table_Uint32_1__ASYNC", pk_types, {
             **pk_types, **non_pk_types}, index_second, "Uint32", "", "ASYNC"),
            ("table_Uint32_1_UNIQUE_SYNC", pk_types, {
             **pk_types, **non_pk_types}, index_second, "Uint32", "UNIQUE", "SYNC"),
            ("table_Uint64_0__SYNC", pk_types, {
             **pk_types, **non_pk_types}, index_first, "Uint64", "", "SYNC"),
            ("table_Uint64_0__ASYNC", pk_types, {
             **pk_types, **non_pk_types}, index_first, "Uint64", "", "ASYNC"),
            ("table_Uint64_0_UNIQUE_SYNC", pk_types, {
             **pk_types, **non_pk_types}, index_first_not_Bool, "Uint64", "UNIQUE", "SYNC"),
            ("table_Uint64_1__SYNC", pk_types, {
             **pk_types, **non_pk_types}, index_second, "Uint64", "", "SYNC"),
            ("table_Uint64_1__ASYNC", pk_types, {
             **pk_types, **non_pk_types}, index_second, "Uint64", "", "ASYNC"),
            ("table_Uint64_1_UNIQUE_SYNC", pk_types, {
             **pk_types, **non_pk_types}, index_second, "Uint64", "UNIQUE", "SYNC"),
            ("table_Date_0__SYNC", pk_types, {
             **pk_types, **non_pk_types}, index_first, "Date", "", "SYNC"),
            ("table_Date_0__ASYNC", pk_types, {
             **pk_types, **non_pk_types}, index_first, "Date", "", "ASYNC"),
            ("table_Date_0_UNIQUE_SYNC", pk_types, {
             **pk_types, **non_pk_types}, index_first_not_Bool, "Date", "UNIQUE", "SYNC"),
            ("table_Date_1__SYNC", pk_types, {
             **pk_types, **non_pk_types}, index_second, "Date", "", "SYNC"),
            ("table_Date_1__ASYNC", pk_types, {
             **pk_types, **non_pk_types}, index_second, "Date", "", "ASYNC"),
            ("table_Date_1_UNIQUE_SYNC", pk_types, {
             **pk_types, **non_pk_types}, index_second, "Date", "UNIQUE", "SYNC"),
        ]
    )
    def test_TTL(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str):
        self.ttl_time_sec(1)
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
        if ttl == "Uint32" or ttl == "Uint64" or ttl == "DyNumber":
            os.system(
                f"ydb -e {self.get_endpoint()} -d {self.get_database()} table ttl set --column ttl_{ttl} --expire-after 1 --unit seconds {table_name}")
        else:
            os.system(
                f"ydb -e {self.get_endpoint()} -d {self.get_database()} table ttl set --column ttl_{ttl} --expire-after 1 {table_name}")
        self.insert(table_name, pk_types, all_types, index, ttl)
        self.select(table_name, pk_types, all_types, index)

    def insert(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str):
        now_date = datetime.now()
        old_date = now_date - timedelta(weeks=1)
        future_time_5_sec = now_date + timedelta(seconds=5)
        future_time = now_date + timedelta(weeks=2)
        if ttl != "Date":
            now_date = int(datetime.timestamp(now_date))
            old_date = int(datetime.timestamp(old_date))
            future_time_5_sec = int(datetime.timestamp(future_time_5_sec))
            future_time = int(datetime.timestamp(future_time))
        else:
            now_date = now_date.date()
            old_date = old_date.date()
            future_time_5_sec = future_time_5_sec.date()
            future_time = future_time.date()
        print(f"{table_name} insert")
        for i in range(1, 3):
            self.create_insert(table_name, pk_types,
                               all_types, index, ttl, i, old_date)
        for i in range(3, 5):
            self.create_insert(table_name, pk_types,
                               all_types, index, ttl, i, now_date)
        for i in range(5, 7):
            self.create_insert(table_name, pk_types, all_types,
                               index, ttl, i, future_time_5_sec)
        for i in range(7, 9):
            self.create_insert(table_name, pk_types,
                               all_types, index, ttl, i, future_time)
        print(f"{table_name} insert ok")

    def create_insert(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, value: int, date):
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
                {ttl_types[ttl].format(date) if ttl != "" else ""}
            );
        """
        self.query(insert_sql)

    def select(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str]):
        print(f"{table_name} select")
        for i in range(1, 7):
            self.create_select(table_name, pk_types, all_types, index, i, 0)

        for i in range(7, 9):
            self.create_select(table_name, pk_types, all_types, index, i, 1)
        print(f"{table_name} select ok")

    def create_select(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], value: int, expected_count_rows: int):
        sql_select = f"""
                SELECT COUNT(*) as count FROM `{table_name}` WHERE """

        for type_name in pk_types.keys():
            sql_select += f"pk_{cleanup_type_name(type_name)}={pk_types[type_name].format(value)} and "
        for type_name in all_types.keys():
            if type_name != "Json" and type_name != "Yson" and type_name != "JsonDocument":
                sql_select += f"col_{cleanup_type_name(type_name)}={all_types[type_name].format(value)} and "
        for type_name in index.keys():
            sql_select += f"col_index_{cleanup_type_name(type_name)}={index[type_name].format(value)} and "
        sql_select += f"""pk_Int64={pk_types["Int64"].format(value)}"""
        start_time = time.time()
        max_wait_time = 300
        while True:
            rows = self.query(sql_select)
            if (len(rows) == 1 and rows[0].count == expected_count_rows) or expected_count_rows == 1:
                print(time.time() - start_time)
                break
            elapsed_time = time.time() - start_time
            if elapsed_time >= max_wait_time:
                break
            time.sleep(1)
        rows = self.query(sql_select)
        assert len(
            rows) == 1 and rows[0].count == expected_count_rows, f"Expected {expected_count_rows} rows, error when deleting {value} lines, table {table_name}"

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
        self.alter_database_quotas(self.cluster.nodes[1], '/Root', f"""
            ttl_min_run_internal_seconds: {time}
        """)

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
