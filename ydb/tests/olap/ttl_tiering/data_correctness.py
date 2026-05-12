import time
import logging
import json
import os
import yatest.common
from .base import TllTieringTestBase
from ydb.tests.olap.common.column_table_helper import ColumnTableHelper
import ydb
import concurrent
import random
import datetime

from ydb.tests.library.test_meta import link_test_case

logger = logging.getLogger(__name__)


class TestDataCorrectness(TllTieringTestBase):
    test_name = "data_correctness"
    cold_bucket = "cold"
    n_shards = 4

    @classmethod
    def setup_class(cls):
        super(TestDataCorrectness, cls).setup_class()
        cls.s3_client.create_bucket(cls.cold_bucket)

    def write_data(
        self,
        table: str,
        timestamp_from_ms: int,
        rows: int,
        value: int = 1,
    ):
        chunk_size = 100
        while rows:
            current_chunk_size = min(chunk_size, rows)
            data = [
                {
                    "ts": timestamp_from_ms + i,
                    "s": random.randbytes(1024 * 10),
                    "val": value,
                    "flag": (i % 2) == 0,
                }
                for i in range(current_chunk_size)
            ]
            self.ydb_client.bulk_upsert(
                table,
                self.column_types,
                data,
            )
            timestamp_from_ms += current_chunk_size
            rows -= current_chunk_size
            assert rows >= 0

    def total_values(self, table: str) -> int:
        return (
            self.ydb_client.query(f"select sum(val) as Values from `{table}`")[0].rows[
                0
            ]["Values"]
            or 0
        )

    def _create_column_types(self):
        """Create column types for table with ts, s, val, flag columns"""
        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("ts", ydb.PrimitiveType.Timestamp)
        column_types.add_column("s", ydb.PrimitiveType.String)
        column_types.add_column("val", ydb.PrimitiveType.Uint64)
        column_types.add_column("flag", ydb.PrimitiveType.Bool)
        return column_types

    def _create_table_schema(self, table_path: str):
        """Create table with standard schema (ts, s, val, flag)"""
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                ts Timestamp NOT NULL,
                s String,
                val Uint64,
                flag Bool,
                PRIMARY KEY(ts),
            ) WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {self.n_shards}
            )
            """
        )

    def wait_eviction(self, table: ColumnTableHelper):
        deadline = datetime.datetime.now() + datetime.timedelta(seconds=120)
        while (
            table.get_portion_stat_by_tier().get("__DEFAULT", {}).get("Portions", 0)
            > self.n_shards
        ):
            assert (
                datetime.datetime.now() < deadline
            ), "Timeout for wait eviction is exceeded"

            logger.info(
                f"Waiting for data eviction: {table.get_portion_stat_by_tier()}"
            )
            time.sleep(1)

        stats = table.get_portion_stat_by_tier()
        assert len(stats) > 1 or '__DEFAULT' not in stats

    @link_test_case("#13465")
    def test(self):
        test_dir = f"{self.ydb_client.database}/{self.test_name}"
        table_path = f"{test_dir}/table"
        secret_prefix = self.test_name
        access_key_id_secret_name = f"{secret_prefix}_key_id"
        access_key_secret_secret_name = f"{secret_prefix}_key_secret"
        eds_path = f"{test_dir}/{self.cold_bucket}"

        # Expect empty buckets to avoid unintentional data deletion/modification
        if self.s3_client.get_bucket_stat(self.cold_bucket) != (0, 0):
            raise Exception("Bucket for cold data is not empty")

        self._create_table_schema(table_path)

        table = ColumnTableHelper(self.ydb_client, table_path)
        table.set_fast_compaction()

        self.column_types = self._create_column_types()

        logger.info(f"Table {table_path} created")

        self.ydb_client.query(
            f"CREATE OBJECT {access_key_id_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_id}'"
        )
        self.ydb_client.query(
            f"CREATE OBJECT {access_key_secret_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_secret}'"
        )

        self.ydb_client.query(
            f"""
            CREATE EXTERNAL DATA SOURCE `{eds_path}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{self.s3_client.endpoint}/{self.cold_bucket}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_id_secret_name}",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="{access_key_secret_secret_name}",
                AWS_REGION="{self.s3_client.region}"
            )
        """
        )

        stmt = f"""
            ALTER TABLE `{table_path}` SET (TTL =
                    Interval("PT1S") TO EXTERNAL DATA SOURCE `{eds_path}`
                ON ts
            )
        """
        logger.info(stmt)
        self.ydb_client.query(stmt)

        ts_start = int(datetime.datetime.now().timestamp() * 1000000)
        rows = 10000
        num_threads = 10
        assert rows % num_threads == 0
        chunk_size = rows // num_threads

        # Write data
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            insert_futures = [
                executor.submit(
                    self.write_data,
                    table_path,
                    ts_start + i * chunk_size,
                    chunk_size,
                    1,
                )
                for i in range(num_threads)
            ]

            concurrent.futures.wait(insert_futures)
            for future in insert_futures:
                future.result()

        self.wait_eviction(table)
        assert self.total_values(table_path) == rows

        # Update data
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            insert_futures = [
                executor.submit(
                    self.write_data,
                    table_path,
                    ts_start + i * chunk_size,
                    chunk_size,
                    2,
                )
                for i in range(num_threads)
            ]

            concurrent.futures.wait(insert_futures)
            for future in insert_futures:
                future.result()

        self.wait_eviction(table)
        assert self.total_values(table_path) == rows * 2

        # Delete data
        self.ydb_client.query(f"delete from `{table_path}`")

        assert not self.total_values(table_path)

    @link_test_case("#27040")
    def test_export_import_formats(self):
        test_dir = f"{self.ydb_client.database}/{self.test_name}_export_import"
        source_table_path = f"{test_dir}/source_table"

        self._create_table_schema(source_table_path)
        self.column_types = self._create_column_types()

        ts_start = int(datetime.datetime.now().timestamp() * 1000000)
        rows = 100
        self.write_data(source_table_path, ts_start, rows, value=1)

        source_total = self.total_values(source_table_path)
        assert source_total == rows, f"Expected {rows}, got {source_total}"

        result = self.ydb_client.query(f"SELECT COUNT(*) as cnt FROM `{source_table_path}`")
        actual_rows = result[0].rows[0]["cnt"]
        assert actual_rows == rows, f"Expected {rows} rows in table, got {actual_rows}"

        formats = ["csv", "json"]

        for format_type in formats:
            logger.info(f"Testing export for format: {format_type}")

            query = f"SELECT `ts`, `s`, `val`, `flag` FROM `{source_table_path}` ORDER BY `ts`"
            export_format = format_type if format_type != "json" else "json-unicode"

            if not os.getenv("YDB_CLI_BINARY"):
                raise RuntimeError("YDB_CLI_BINARY environment variable is not specified")

            ydb_cli_path = yatest.common.binary_path(os.getenv("YDB_CLI_BINARY"))
            cmd = [
                ydb_cli_path,
                "-e", self.ydb_client.endpoint,
                "-d", self.ydb_client.database,
                "sql", "-s", query, "--format", export_format
            ]

            execution = yatest.common.execute(cmd)
            exported_data = execution.std_out.decode('utf-8') if execution.std_out else ""

            assert exported_data, f"Export in format {format_type} returned empty data"

            exported_lines = exported_data.strip().split('\n')
            logger.info(f"Format {format_type}: Total exported lines: {len(exported_lines)}")

            if format_type == "csv":
                data_lines = [line for line in exported_lines[1:] if line.strip()]
                logger.info(f"Format {format_type}: Data lines after filtering: {len(data_lines)}, header: {exported_lines[0] if exported_lines else 'none'}")
            else:
                data_lines = [line for line in exported_lines if line.strip()]

            assert len(data_lines) >= actual_rows - 1, f"Format {format_type}: Expected at least {actual_rows - 1} data lines, got {len(data_lines)}"
            assert len(data_lines) <= actual_rows, f"Format {format_type}: Expected at most {actual_rows} data lines, got {len(data_lines)}"

            if format_type == "csv":
                sample_line = data_lines[0] if data_lines else ""
                assert "true" in sample_line.lower() or "false" in sample_line.lower() or "1" in sample_line or "0" in sample_line, \
                    f"Format {format_type}: Boolean values not found in exported data"
            else:
                sample_line = data_lines[0] if data_lines else ""
                try:
                    sample_data = json.loads(sample_line)
                    assert "flag" in sample_data, f"Format {format_type}: Boolean column 'flag' not found in exported data"
                    assert isinstance(sample_data["flag"], bool), f"Format {format_type}: Boolean value is not of type bool, got {type(sample_data['flag'])}"
                except json.JSONDecodeError:
                    assert False, f"Format {format_type}: Failed to parse JSON line: {sample_line}"

            logger.info(f"Format {format_type}: Successfully exported {len(data_lines)} rows with boolean values")
