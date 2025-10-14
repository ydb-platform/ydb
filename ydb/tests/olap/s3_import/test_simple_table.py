import grpc
import logging
import signal
import time
import threading
import os

import ydb

from math import ceil

from ydb.tests.library.test_meta import link_test_case
from ydb.tests.olap.s3_import.base import S3ImportTestBase

logger = logging.getLogger(__name__)


def _stop_s3(pid):
    os.kill(pid, signal.SIGSTOP)


def _cont_s3(pid):
    os.kill(pid, signal.SIGCONT)


def _corrupt_connection(stop, s3_mock_pid):
    while True:
        time.sleep(1)
        _stop_s3(s3_mock_pid)
        time.sleep(1)
        _cont_s3(s3_mock_pid)
        if stop():
            break


class TestSimpleTable(S3ImportTestBase):
    def _setup_olap_table(self):
        super(TestSimpleTable, self).setup_class()

        self.olap_table = f"{self.ydb_client.database}/olap_table"

        self.ydb_client.query(f"""
            CREATE TABLE `{self.olap_table}` (
                key Int32 NOT NULL,
                data string,
                PRIMARY KEY (key)
            ) WITH (
                STORE = COLUMN
            );
        """)

        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("key", ydb.PrimitiveType.Int32)
        column_types.add_column("data", ydb.PrimitiveType.String)

        self.table_size = 100 * 1000
        self.max_batch_size = 50 * 1000
        batch_count = ceil(self.table_size // self.max_batch_size)
        data = []
        for batch_n in range(batch_count):
            data.append([
                {
                    "key": i,
                    "data": b" " * 1000
                }
                for i in range(self.table_size * batch_n // batch_count, self.table_size * (batch_n + 1) // batch_count)
            ])

        for batch in data:
            self.ydb_client.bulk_upsert(
                self.olap_table,
                column_types,
                batch
            )

    def _drop_olap_table(self):
        self.ydb_client.query(f"""
            DROP TABLE `{self.olap_table}`;
        """)

    def _check_tables_hash(self, original_table_name, exported_table_name):
        result_sets = self.ydb_client.query(f"""
            SELECT
                String::Hex(Sum(Digest::MurMurHash32(Pickle(TableRow())))) AS check_hash,
                COUNT(*) AS check_size
            FROM {exported_table_name};

            SELECT
                String::Hex(Sum(Digest::MurMurHash32(Pickle(TableRow())))) AS olap_hash,
                COUNT(*) AS olap_size
            FROM {original_table_name};
        """)

        check_result = result_sets[0].rows[0]
        assert check_result.check_size > 0

        original_table_result = result_sets[1].rows[0]
        assert check_result.check_size == original_table_result.olap_size
        assert check_result.check_hash == original_table_result.olap_hash

    @link_test_case("#18786")
    def test_unstable_connection(self):
        self._setup_olap_table()

        corrupter_stop = False
        connection_corrupter = threading.Thread(target=_corrupt_connection, args=(lambda : corrupter_stop, self.s3_mock.s3_pid))
        connection_corrupter.start()

        test_bucket = "test_unstable_connection_bucket"
        s3_source = f"{test_bucket}_s3_source"
        s3_table = f"{test_bucket}_s3_table"
        from_s3 = f"{test_bucket}_from_s3"
        self.s3_client.create_bucket(test_bucket)

        access_key_id_secret_name = f"{test_bucket}_key_id"
        access_key_secret_secret_name = f"{test_bucket}_key_secret"
        self.ydb_client.query(f"CREATE OBJECT {access_key_id_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_id}'")
        self.ydb_client.query(f"CREATE OBJECT {access_key_secret_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_secret}'")

        self.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE {s3_source} WITH (
                SOURCE_TYPE = "ObjectStorage",
                LOCATION = "{self.s3_mock.endpoint}/{test_bucket}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_id_secret_name}",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="{access_key_secret_secret_name}",
                AWS_REGION="{self.s3_client.region}"
            );

            CREATE EXTERNAL TABLE {s3_table} (
                key Int32 NOT NULL,
                data string
            ) WITH (
                DATA_SOURCE="{s3_source}",
                LOCATION="/test_folder/",
                FORMAT="parquet"
            );
        """)

        logger.info("Exporting into s3...")
        self.ydb_client.query(f"INSERT INTO {s3_table} SELECT * FROM `{self.olap_table}`")
        logger.info(f"Exporting finished, bucket stats: {self.s3_client.get_bucket_stat(test_bucket)}")

        logger.info("Importing into ydb...")
        self.ydb_client.query(f"""
            CREATE TABLE {from_s3} (
                PRIMARY KEY (key)
            ) WITH (
                STORE = COLUMN
            ) AS SELECT * FROM {s3_table}
        """)

        self._check_tables_hash(f"`{self.olap_table}`", from_s3)

        corrupter_stop = True
        connection_corrupter.join()

        self._drop_olap_table()

    @link_test_case("#18787")
    def test_unavailable_during_import(self):
        self._setup_olap_table()

        request_settings = ydb.BaseRequestSettings().with_timeout(10)

        s3_pid = self.s3_mock.s3_pid

        test_bucket = "test_unavailable_during_import_bucket"
        s3_source = f"{test_bucket}_s3_source"
        s3_table = f"{test_bucket}_s3_table"
        from_s3 = f"{test_bucket}_from_s3"
        self.s3_client.create_bucket(test_bucket)

        access_key_id_secret_name = f"{test_bucket}_key_id"
        access_key_secret_secret_name = f"{test_bucket}_key_secret"
        self.ydb_client.query(f"CREATE OBJECT {access_key_id_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_id}'")
        self.ydb_client.query(f"CREATE OBJECT {access_key_secret_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_secret}'")

        self.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE {s3_source} WITH (
                SOURCE_TYPE = "ObjectStorage",
                LOCATION = "{self.s3_mock.endpoint}/{test_bucket}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_id_secret_name}",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="{access_key_secret_secret_name}",
                AWS_REGION="{self.s3_client.region}"
            );

            CREATE EXTERNAL TABLE {s3_table} (
                key Int32 NOT NULL,
                data string
            ) WITH (
                DATA_SOURCE="{s3_source}",
                LOCATION="/test_folder/",
                FORMAT="parquet"
            );
        """)

        logger.info("Exporting into s3...")
        _stop_s3(s3_pid)
        query_failed = False
        try:
            self.ydb_client.query(f"INSERT INTO {s3_table} SELECT * FROM `{self.olap_table}`", request_settings=request_settings)
        except grpc.RpcError as rpc_error:
            query_failed = True
            assert rpc_error.code() == grpc.StatusCode.DEADLINE_EXCEEDED

        assert query_failed

        logger.info("Exporting into s3...")
        self.ydb_client.query(f"INSERT INTO {s3_table} SELECT * FROM `{self.olap_table}`")
        logger.info(f"Exporting finished, bucket stats: {self.s3_client.get_bucket_stat(test_bucket)}")

        logger.info("Importing into ydb...")
        _stop_s3(s3_pid)
        query_failed = False
        try:
            self.ydb_client.query(f"""
                CREATE TABLE {from_s3} (
                    PRIMARY KEY (key)
                ) WITH (
                    STORE = COLUMN
                ) AS SELECT * FROM {s3_table}
            """, request_settings=request_settings)
        except grpc.RpcError as rpc_error:
            query_failed = True
            assert rpc_error.code() == grpc.StatusCode.DEADLINE_EXCEEDED

        assert query_failed

        _cont_s3(s3_pid)
        self._drop_olap_table()

    @link_test_case("#19148")
    def test_parallel_import_export(self):
        self._setup_olap_table()

        test_bucket = "test_parallel_import_export_bucket"
        s3_source = f"{test_bucket}_s3_source"
        self.s3_client.create_bucket(test_bucket)

        access_key_id_secret_name = f"{test_bucket}_key_id"
        access_key_secret_secret_name = f"{test_bucket}_key_secret"
        self.ydb_client.query(f"CREATE OBJECT {access_key_id_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_id}'")
        self.ydb_client.query(f"CREATE OBJECT {access_key_secret_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_secret}'")

        self.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE {s3_source} WITH (
                SOURCE_TYPE = "ObjectStorage",
                LOCATION = "{self.s3_mock.endpoint}/{test_bucket}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_id_secret_name}",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="{access_key_secret_secret_name}",
                AWS_REGION="{self.s3_client.region}"
            );

            CREATE EXTERNAL TABLE s3_table_1 (
                key Int32 NOT NULL,
                data string
            ) WITH (
                DATA_SOURCE="{s3_source}",
                LOCATION="/test_folder/first_import/",
                FORMAT="parquet"
            );

            CREATE EXTERNAL TABLE s3_table_2 (
                key Int32 NOT NULL,
                data string
            ) WITH (
                DATA_SOURCE="{s3_source}",
                LOCATION="/test_folder/second_import/",
                FORMAT="parquet"
            );
        """)

        logger.info("Exporting into s3...")
        future1 = self.ydb_client.query_async(f"INSERT INTO s3_table_1 SELECT * FROM `{self.olap_table}`")
        future2 = self.ydb_client.query_async(f"INSERT INTO s3_table_2 SELECT * FROM `{self.olap_table}`")

        future1.result()
        future2.result()
        logger.info(f"Exporting finished, bucket stats: {self.s3_client.get_bucket_stat(test_bucket)}")

        logger.info("Importing into ydb...")
        future1 = self.ydb_client.query_async("""
            CREATE TABLE from_s3_1 (
                PRIMARY KEY (key)
            ) WITH (
                STORE = COLUMN
            ) AS SELECT * FROM s3_table_1
        """)
        future2 = self.ydb_client.query_async("""
            CREATE TABLE from_s3_2 (
                PRIMARY KEY (key)
            ) WITH (
                STORE = COLUMN
            ) AS SELECT * FROM s3_table_2
        """)

        future1.result()
        future2.result()

        self._check_tables_hash(f"`{self.olap_table}`", "from_s3_1")
        self._check_tables_hash(f"`{self.olap_table}`", "from_s3_2")

        self._drop_olap_table()

    @link_test_case("#19153")
    def test_change_data_during_execution(self):
        self._setup_olap_table()

        test_bucket = "test_change_data_during_execution_bucket"
        s3_source = f"{test_bucket}_s3_source"
        s3_table = f"{test_bucket}_s3_table"
        self.s3_client.create_bucket(test_bucket)

        access_key_id_secret_name = f"{test_bucket}_key_id"
        access_key_secret_secret_name = f"{test_bucket}_key_secret"
        self.ydb_client.query(f"CREATE OBJECT {access_key_id_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_id}'")
        self.ydb_client.query(f"CREATE OBJECT {access_key_secret_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_secret}'")

        self.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE {s3_source} WITH (
                SOURCE_TYPE = "ObjectStorage",
                LOCATION = "{self.s3_mock.endpoint}/{test_bucket}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_id_secret_name}",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="{access_key_secret_secret_name}",
                AWS_REGION="{self.s3_client.region}"
            );

            CREATE EXTERNAL TABLE {s3_table} (
                key Int32 NOT NULL,
                data string
            ) WITH (
                DATA_SOURCE="{s3_source}",
                LOCATION="/test_folder/",
                FORMAT="parquet"
            );
        """)

        logger.info("Exporting into s3...")
        import_future = self.ydb_client.query_async(f"INSERT INTO {s3_table} SELECT * FROM `{self.olap_table}`")

        self.ydb_client.query(f"""
            INSERT INTO `{self.olap_table}` (key, data) VALUES ({self.table_size}, "inserted data");
            UPSERT INTO `{self.olap_table}` (key, data) VALUES ({self.table_size + 1}, "upserted data");
            UPDATE `{self.olap_table}` SET data = "updated data" WHERE key = {self.table_size};
        """)

        import_future.result()
        logger.info(f"Exporting finished, bucket stats: {self.s3_client.get_bucket_stat(test_bucket)}")

        results = self.ydb_client.query(f"SELECT count(*) FROM {s3_table};")
        assert len(results) > 0 and len(results[0].rows) > 0

        self._drop_olap_table()

    @link_test_case("#19155")
    def test_change_schema_during_execution(self):
        self._setup_olap_table()

        test_bucket = "test_change_schema_during_execution_bucket"
        s3_source = f"{test_bucket}_s3_source"
        s3_table = f"{test_bucket}_s3_table"
        self.s3_client.create_bucket(test_bucket)

        access_key_id_secret_name = f"{test_bucket}_key_id"
        access_key_secret_secret_name = f"{test_bucket}_key_secret"
        self.ydb_client.query(f"CREATE OBJECT {access_key_id_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_id}'")
        self.ydb_client.query(f"CREATE OBJECT {access_key_secret_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_secret}'")

        self.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE {s3_source} WITH (
                SOURCE_TYPE = "ObjectStorage",
                LOCATION = "{self.s3_mock.endpoint}/{test_bucket}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_id_secret_name}",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="{access_key_secret_secret_name}",
                AWS_REGION="{self.s3_client.region}"
            );

            CREATE EXTERNAL TABLE {s3_table} (
                key Int32 NOT NULL,
                data string
            ) WITH (
                DATA_SOURCE="{s3_source}",
                LOCATION="/test_folder/",
                FORMAT="parquet"
            );
        """)

        logger.info("Exporting into s3...")
        import_future = self.ydb_client.query_async(f"INSERT INTO {s3_table} SELECT * FROM `{self.olap_table}`")

        time.sleep(1)

        self.ydb_client.query(f"""
            ALTER TABLE `{self.olap_table}` DROP COLUMN data;
        """)

        self.ydb_client.query(f"""
            ALTER TABLE `{self.olap_table}` ADD COLUMN data String;
        """)

        import_future.result()
        logger.info(f"Exporting finished, bucket stats: {self.s3_client.get_bucket_stat(test_bucket)}")

        results = self.ydb_client.query(f"SELECT data FROM {s3_table} WHERE data IS NOT NULL;")
        assert len(results) > 0 and len(results[0].rows) > 0

        self._drop_olap_table()

    @link_test_case("#19156")
    def test_drop_table_during_export(self):
        self._setup_olap_table()

        test_bucket = "test_drop_table_during_export_bucket"
        s3_source = f"{test_bucket}_s3_source"
        s3_table = f"{test_bucket}_s3_table"
        self.s3_client.create_bucket(test_bucket)

        access_key_id_secret_name = f"{test_bucket}_key_id"
        access_key_secret_secret_name = f"{test_bucket}_key_secret"
        self.ydb_client.query(f"CREATE OBJECT {access_key_id_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_id}'")
        self.ydb_client.query(f"CREATE OBJECT {access_key_secret_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_secret}'")

        self.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE {s3_source} WITH (
                SOURCE_TYPE = "ObjectStorage",
                LOCATION = "{self.s3_mock.endpoint}/{test_bucket}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_id_secret_name}",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="{access_key_secret_secret_name}",
                AWS_REGION="{self.s3_client.region}"
            );

            CREATE EXTERNAL TABLE {s3_table} (
                key Int32 NOT NULL,
                data string
            ) WITH (
                DATA_SOURCE="{s3_source}",
                LOCATION="/test_folder/",
                FORMAT="json_each_row",
                COMPRESSION="gzip"
            );
        """)

        logger.info("Exporting into s3...")
        import_future = self.ydb_client.query_async(f"INSERT INTO {s3_table} SELECT * FROM `{self.olap_table}`")

        time.sleep(1)

        self.ydb_client.query(f"""
            DROP TABLE `{self.olap_table}`;
        """)

        try:
            import_future.result()
        except ydb.issues.SchemeError as schema_error:
            assert f"Cannot find table \'db.[{self.olap_table}]\' because it does not exist or you do not have access permissions." in schema_error.message

            self._drop_olap_table()
            return

        assert "query should have failed"

    @link_test_case("#19157")
    def test_rename_table_during_export(self):
        self._setup_olap_table()

        test_bucket = "test_rename_table_during_export_bucket"
        s3_source = f"{test_bucket}_s3_source"
        s3_table = f"{test_bucket}_s3_table"
        self.s3_client.create_bucket(test_bucket)

        access_key_id_secret_name = f"{test_bucket}_key_id"
        access_key_secret_secret_name = f"{test_bucket}_key_secret"
        self.ydb_client.query(f"CREATE OBJECT {access_key_id_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_id}'")
        self.ydb_client.query(f"CREATE OBJECT {access_key_secret_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_secret}'")

        self.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE {s3_source} WITH (
                SOURCE_TYPE = "ObjectStorage",
                LOCATION = "{self.s3_mock.endpoint}/{test_bucket}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_id_secret_name}",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="{access_key_secret_secret_name}",
                AWS_REGION="{self.s3_client.region}"
            );

            CREATE EXTERNAL TABLE {s3_table} (
                key Int32 NOT NULL,
                data string
            ) WITH (
                DATA_SOURCE="{s3_source}",
                LOCATION="/test_folder/",
                FORMAT="json_each_row",
                COMPRESSION="gzip"
            );
        """)

        logger.info("Exporting into s3...")
        import_future = self.ydb_client.query_async(f"INSERT INTO {s3_table} SELECT * FROM `{self.olap_table}`")

        time.sleep(1)

        self.ydb_client.query(f"""
            ALTER TABLE `{self.olap_table}` RENAME TO `{self.olap_table}_2`;
        """)

        try:
            import_future.result()
        except ydb.issues.SchemeError as schema_error:
            assert f"Cannot find table \'db.[{self.olap_table}]\' because it does not exist or you do not have access permissions." in schema_error.message

            self.ydb_client.query(f"DROP TABLE `{self.olap_table}_2`")
            return

        assert "query should have failed"
