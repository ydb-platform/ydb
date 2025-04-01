import yatest
import pytest

from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.sql.lib.test_s3 import S3Base
from ydb.tests.stress.oltp_workload.workload import cleanup_type_name
from ydb.tests.datashard.lib.create_table import create_table, create_ttl, pk_types, non_pk_types, index_first, index_second, ttl_types, \
    index_first_sync, index_second_sync, index_three_sync, index_three_sync_not_Bool, index_four_sync, index_zero_sync
import os
import time


class TestYdbS3TTL(TestBase, S3Base):
    @pytest.mark.parametrize(
        "table_name, pk_types, all_types, index, ttl, unique, sync",
        [
            #("table_index_4_UNIQUE_SYNC", pk_types, {},        Issues:
            # index_four_sync, "", "UNIQUE", "SYNC"),           <main>: Error: Failed item check: unsupported index type to build
            #("table_index_3_UNIQUE_SYNC", pk_types, {},
            # index_three_sync_not_Bool, "", "UNIQUE", "SYNC"),
            #("table_index_2_UNIQUE_SYNC", pk_types, {},
            # index_second_sync, "", "UNIQUE", "SYNC"),
            #("table_index_1_UNIQUE_SYNC", pk_types, {},
            # index_first_sync, "", "UNIQUE", "SYNC"),
            #("table_index_0_UNIQUE_SYNC", pk_types, {},
            # index_zero_sync, "", "UNIQUE", "SYNC"),
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
    def test_S3_t(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str):
        s3_client = self.s3_session_client()

        bucket_name = self.bucket_name()
        s3_client.create_bucket(Bucket=self.bucket_name())
        print(self.bucket_name())
        self.create_external_datasource_and_secrets(bucket_name)
        self.create_table(table_name, pk_types, all_types,
                          index, ttl, unique, sync)
        self.insert(table_name, all_types, pk_types, index, ttl)
        self.is_import_or_export(False, table_name)
        self.query(f"drop table {table_name}")
        self.is_import_or_export(True, table_name)
        self.select_after_insert(table_name, all_types, pk_types, index, ttl)

    def is_import_or_export(self, is_import: bool, table_name: str):
        cmd_test = yatest.common.execute([
            yatest.common.binary_path(os.getenv('YDB_CLI_BINARY')),
            '-e', 'grpc://'+self.get_endpoint(),
            "--database", self.get_database(),
            'import' if is_import else 'export',
            's3',
            '--s3-endpoint', self.s3_endpoint(),
            '--access-key', self.s3_access_key(),
            '--secret-key', self.s3_secret_access_key(),
            '--item',
            f"destination=/Root,source={table_name}" if is_import else f"destination={table_name},source=/Root",
            '--bucket', self.bucket_name(),
        ]).stdout.decode("utf-8")

        position = cmd_test.find("ydb://")
        url = ""
        while cmd_test[position] != " ":
            url += cmd_test[position]
            position += 1

        i = 0
        while True:
            time.sleep(0.1)
            i += 1
            if i == 10:
                break
            cmd = yatest.common.execute([
                yatest.common.binary_path(os.getenv('YDB_CLI_BINARY')),
                '-e', 'grpc://'+self.get_endpoint(),
                "--database", self.get_database(),
                "operation", "get",
                url,
            ])
            if "Done" in cmd._std_out.decode('utf-8'):
                break

        yatest.common.execute([
            yatest.common.binary_path(os.getenv('YDB_CLI_BINARY')),
            '-e', 'grpc://'+self.get_endpoint(),
            "--database", self.get_database(),
            "operation", "forget",
            url,
        ])

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
