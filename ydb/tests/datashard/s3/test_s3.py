import yatest
import pytest
import os

from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.sql.lib.test_s3 import S3Base
from ydb.tests.datashard.lib.types_of_variables import pk_types, non_pk_types, index_first, index_second, \
    index_first_sync, index_second_sync, index_three_sync, index_four_sync, index_zero_sync
from ydb.tests.datashard.lib.dml_operations import DMLOperations


class TestYdbS3TTL(TestBase, S3Base):
    @pytest.mark.parametrize(
        "table_name, pk_types, all_types, index, ttl, unique, sync",
        [
            # ("table_index_4_UNIQUE_SYNC", pk_types, {},        Issues:
            # index_four_sync, "", "UNIQUE", "SYNC"),           <main>: Error: Failed item check: unsupported index type to build
            # ("table_index_3_UNIQUE_SYNC", pk_types, {},       https://github.com/ydb-platform/ydb/issues/16594
            # index_three_sync_not_Bool, "", "UNIQUE", "SYNC"),
            # ("table_index_2_UNIQUE_SYNC", pk_types, {},
            # index_second_sync, "", "UNIQUE", "SYNC"),
            # ("table_index_1_UNIQUE_SYNC", pk_types, {},
            # index_first_sync, "", "UNIQUE", "SYNC"),
            # ("table_index_0_UNIQUE_SYNC", pk_types, {},
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
    def test_s3(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str):
        s3_client = self.s3_session_client()
        s3_client.create_bucket(Bucket=self.bucket_name())
        dml = DMLOperations(self)
        dml.create_table(table_name, pk_types, all_types,
                         index, ttl, unique, sync)
        dml.insert(table_name, all_types, pk_types, index, ttl)
        self.export_s3(table_name)
        dml.query(f"drop table {table_name}")
        self.import_s3(table_name)
        dml.select_after_insert(table_name, all_types, pk_types, index, ttl)

    def export_s3(self, table_name: str):
        cmd_test = yatest.common.execute([
            yatest.common.binary_path(os.getenv('YDB_CLI_BINARY')),
            '-e', 'grpc://'+self.get_endpoint(),
            "--database", self.get_database(),
            'export',
            's3',
            '--s3-endpoint', self.s3_endpoint(),
            '--access-key', self.s3_access_key(),
            '--secret-key', self.s3_secret_access_key(),
            '--item',
            f"destination={table_name},source=/Root",
            '--bucket', self.bucket_name(),
        ]).stdout.decode("utf-8")

        self.wait(cmd_test)

    def import_s3(self, table_name: str):
        cmd_test = yatest.common.execute([
            yatest.common.binary_path(os.getenv('YDB_CLI_BINARY')),
            '-e', 'grpc://'+self.get_endpoint(),
            "--database", self.get_database(),
            'import',
            's3',
            '--s3-endpoint', self.s3_endpoint(),
            '--access-key', self.s3_access_key(),
            '--secret-key', self.s3_secret_access_key(),
            '--item',
            f"destination=/Root,source={table_name}",
            '--bucket', self.bucket_name(),
        ]).stdout.decode("utf-8")

        self.wait(cmd_test)

    def wait(self, cmd_test: str):
        position = cmd_test.find("ydb://")
        url = ""
        while cmd_test[position] != " ":
            url += cmd_test[position]
            position += 1

        wait_for(self.create_predicate(url), timeout_seconds=100)
        yatest.common.execute([
            yatest.common.binary_path(os.getenv('YDB_CLI_BINARY')),
            '-e', 'grpc://'+self.get_endpoint(),
            "--database", self.get_database(),
            "operation", "forget",
            url,
        ])

    def create_predicate(self, url):
        def predicate():
            cmd = yatest.common.execute([
                yatest.common.binary_path(os.getenv('YDB_CLI_BINARY')),
                '-e', 'grpc://'+self.get_endpoint(),
                "--database", self.get_database(),
                "operation", "get",
                url,
            ])
            return "Done" in cmd._std_out.decode('utf-8')
        return predicate
