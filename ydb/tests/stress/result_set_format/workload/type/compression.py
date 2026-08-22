# -*- coding: utf-8 -*-
import threading
import logging
import random
import time
import ydb
import pyarrow as pa

from dataclasses import dataclass, field
from typing import Dict, List, Any

from ydb.tests.stress.common.common import WorkloadBase
from ydb.tests.stress.result_set_format.workload.common import (
    ColumnInfo,
    TableInfo,
    ParameterBuilder,
    generate_batch_rows,
)

logger = logging.getLogger(__name__)


class WorkloadStats:
    """Thread-safe statistics tracking for the workload"""

    def __init__(self):
        self.tables = 0
        self.select_queries = 0
        self.total_rows_selected = 0
        self.compression_unspecified = 0
        self.compression_none = 0
        self.compression_zstd = 0
        self.compression_lz4_frame = 0
        self._lock = threading.Lock()

    def increment_tables(self) -> None:
        """Increment the tables counter in a thread-safe manner"""
        with self._lock:
            self.tables += 1

    def increment_select_queries(self, rows_count: int = 0) -> None:
        """Increment the select queries counter in a thread-safe manner"""
        with self._lock:
            self.select_queries += 1
            self.total_rows_selected += rows_count

    def increment_compression_type(self, codec_type: ydb.ArrowCompressionCodecType) -> None:
        """Increment compression type counter in a thread-safe manner"""
        with self._lock:
            if codec_type == ydb.ArrowCompressionCodecType.UNSPECIFIED:
                self.compression_unspecified += 1
            elif codec_type == ydb.ArrowCompressionCodecType.NONE:
                self.compression_none += 1
            elif codec_type == ydb.ArrowCompressionCodecType.ZSTD:
                self.compression_zstd += 1
            elif codec_type == ydb.ArrowCompressionCodecType.LZ4_FRAME:
                self.compression_lz4_frame += 1

    def get_stats(self) -> str:
        """Get current statistics as a formatted string"""
        with self._lock:
            avg_rows_selected = self.total_rows_selected / self.select_queries if self.select_queries > 0 else 0
            return (
                f"Tables: {self.tables}, "
                f"Selects: {self.select_queries}, "
                f"AvgRows: {avg_rows_selected:.2f}, "
                f"UNSPEC: {self.compression_unspecified}, "
                f"NONE: {self.compression_none}, "
                f"ZSTD: {self.compression_zstd}, "
                f"LZ4: {self.compression_lz4_frame}"
            )


@dataclass
class WorkloadConfig:
    """Configuration parameters for the workload execution and data generation"""

    # Table structure constants
    column_types: List[str] = field(
        default_factory=lambda: ["Uint32", "Uint64", "Int32", "Int64", "String", "Utf8", "Bool", "Uint8"]
    )
    min_columns: int = 5
    max_columns: int = 10
    min_partitions_per_table: int = 2
    max_partitions_per_table: int = 8

    # Data generation constants
    min_rows_count: int = 1000
    max_rows_count: int = 5000
    max_pk_value: int = 1000000000
    max_value: int = 1000000000
    null_probability: float = 0.1
    upserts_per_table: int = 5

    # Workload execution constants
    tables_inflight: int = 2
    jobs_per_table: int = 4
    selects_per_job: int = 50

    # Compression settings
    zstd_min_level: int = 1
    zstd_max_level: int = 22
    zstd_no_level_probability: float = 0.2  # 20% chance to use ZSTD without level
    limit_probability: float = 0.3  # 30% chance to add LIMIT
    min_limit: int = 100
    max_limit: int = 1000

    # Retry configuration
    max_retries: int = 5
    retry_delay_seconds: float = 0.5


class WorkloadCompression(WorkloadBase):
    """
    Stress test for Arrow format compression.
    Tests compression codecs with different compression levels.
    """

    def __init__(self, client, prefix, format, stop):
        """Initialize the workload with client connection and format configuration"""

        super().__init__(client, prefix, f"compression_{format}", stop)

        self._config = WorkloadConfig()
        self._param_builder = ParameterBuilder()
        self._stats = WorkloadStats()

        match format:
            case "value":
                self._format = ydb.QueryResultSetFormat.VALUE
            case "arrow":
                self._format = ydb.QueryResultSetFormat.ARROW
            case _:
                raise ValueError(f"Invalid format: {format}")

    def get_stat(self):
        """Return formatted statistics string with current workload metrics"""

        return self._stats.get_stats()

    def run_job(self, table_info: TableInfo, job_key: int) -> None:
        """Execute multiple SELECT operations with different compression settings"""

        logger.info(f"Starting job {job_key} for table {table_info.path}")
        selects = 0
        while selects < self._config.selects_per_job and not self.is_stop_requested():
            try:
                self._run_select_with_compression(table_info)
                selects += 1
            except Exception as e:
                logger.error(
                    f"Job {job_key} (select {selects}) for {table_info.path} encountered unexpected error: {e}"
                )
                raise

    def run_for_table(self, table_name: str) -> None:
        """Create table, fill with data, run concurrent jobs, and cleanup"""

        table_info = self._generate_table(table_name)
        self._create_table(table_info)
        self._stats.increment_tables()

        self._fill_table(table_info)

        threads = []
        exception_holder = []

        def thread_wrapper(func, *args, **kwargs):
            try:
                func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Exception in thread '{threading.current_thread().name}': {e}")
                exception_holder.append(e)

        for i in range(self._config.jobs_per_table):
            threads.append(
                threading.Thread(
                    target=thread_wrapper,
                    args=(self.run_job, table_info, i),
                    name=f"{table_name} job:{i}",
                )
            )

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self._drop_table(table_info)

        if exception_holder:
            raise Exception(f"Exceptions for table {table_name}: {', '.join([str(e) for e in exception_holder])}")

    def loop(self):
        """Main workload loop running multiple tables concurrently until stop is requested"""

        # Value format does not support compressions
        # Currently, this test is only for Arrow format
        if self._format == ydb.QueryResultSetFormat.VALUE:
            return

        exception_holder = []

        def thread_wrapper(func, *args, **kwargs):
            try:
                func(*args, **kwargs)
            except Exception as e:
                exception_holder.append(e)

        while not self.is_stop_requested():
            threads = []
            for table_name in [f'test{i}' for i in range(self._config.tables_inflight)]:
                threads.append(
                    threading.Thread(target=thread_wrapper, args=(self.run_for_table, table_name), name=table_name)
                )

            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            if len(exception_holder) > 0:
                raise exception_holder[0]

    def get_workload_thread_funcs(self):
        """Return list of workload thread functions to be executed"""

        return [self.loop]

    def _generate_table(self, table_name: str) -> TableInfo:
        """Generate table schema with primary key and various data types for compression testing"""

        columns = [ColumnInfo(name="pk", type_name="Uint64", not_null=True)]

        num_columns = random.randint(self._config.min_columns, self._config.max_columns)
        for i in range(num_columns):
            type_name = random.choice(self._config.column_types)
            columns.append(ColumnInfo(name=f"col_{i}", type_name=type_name, not_null=False))

        min_partitions = random.randint(self._config.min_partitions_per_table, self._config.max_partitions_per_table)

        return TableInfo(
            path=self.get_table_path(table_name),
            columns=columns,
            primary_key_columns=["pk"],
            min_partitions=min_partitions,
        )

    def _create_table(self, table_info: TableInfo) -> None:
        """Execute CREATE TABLE query using table schema"""

        create_sql = table_info.create_sql()
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Creating table {table_info.path} with SQL: {create_sql}")
        else:
            logger.info(f"Creating table {table_info.path}")
        self._execute_query_with_retry(create_sql)

    def _drop_table(self, table_info: TableInfo) -> None:
        """Execute DROP TABLE query to remove the table"""

        drop_sql = table_info.drop_sql()
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Dropping table {table_info.path} with SQL: {drop_sql}")
        else:
            logger.info(f"Dropping table {table_info.path}")
        self._execute_query_with_retry(drop_sql)

    def _fill_table(self, table_info: TableInfo) -> None:
        """Populate table with large amounts of random data for compression testing"""

        for _ in range(self._config.upserts_per_table):
            rows_count = random.randint(self._config.min_rows_count, self._config.max_rows_count)
            batch_rows = generate_batch_rows(
                table_info,
                rows_count=rows_count,
                max_pk_value=self._config.max_pk_value,
                max_value=self._config.max_value,
                null_probability=self._config.null_probability,
            )

            param_name = "$rows"
            parameters = self._param_builder.create_list(param_name, table_info.columns, batch_rows)

            upsert_sql = f"""
                UPSERT INTO `{table_info.path}`
                SELECT * FROM AS_TABLE({param_name});
            """

            logger.info(f"Filling table {table_info.path} with {rows_count} rows")
            self._execute_query_with_retry(upsert_sql, parameters=parameters)

    def _generate_compression_codec(self) -> ydb.ArrowCompressionCodec:
        """Generate random compression codec configuration"""

        codec_types = [
            ydb.ArrowCompressionCodecType.UNSPECIFIED,
            ydb.ArrowCompressionCodecType.NONE,
            ydb.ArrowCompressionCodecType.ZSTD,
            ydb.ArrowCompressionCodecType.LZ4_FRAME,
        ]
        codec_type = random.choice(codec_types)

        if codec_type == ydb.ArrowCompressionCodecType.ZSTD:
            # Sometimes use ZSTD without level
            if random.random() < self._config.zstd_no_level_probability:
                return ydb.ArrowCompressionCodec(codec_type)
            else:
                level = random.randint(self._config.zstd_min_level, self._config.zstd_max_level)
                return ydb.ArrowCompressionCodec(codec_type, level)
        else:
            return ydb.ArrowCompressionCodec(codec_type)

    def _run_select_with_compression(self, table_info: TableInfo) -> None:
        """Execute SELECT query with random compression codec and validate results"""

        codec = self._generate_compression_codec()
        self._stats.increment_compression_type(codec.type)

        select_sql = f"SELECT * FROM `{table_info.path}`"
        expected_limit = None
        if random.random() < self._config.limit_probability:
            limit = random.randint(self._config.min_limit, self._config.max_limit)
            select_sql += f" LIMIT {limit}"
            expected_limit = limit
        select_sql += ";"

        codec_desc = f"{codec.type.name}"
        if codec.type == ydb.ArrowCompressionCodecType.ZSTD and codec.level is not None:
            codec_desc += f"(level={codec.level})"
        logger.debug(f"Executing SELECT on {table_info.path} with codec: {codec_desc}, LIMIT: {expected_limit}")

        arrow_format_settings = ydb.ArrowFormatSettings(compression_codec=codec)
        result = self._execute_query_with_retry(
            select_sql,
            arrow_format_settings=arrow_format_settings,
        )

        if not result:
            raise Exception(f"SELECT query returned no result sets for {table_info.path}")

        total_rows = 0
        for result_set in result:
            assert (
                result_set.format == ydb.QueryResultSetFormat.ARROW
            ), f"result_set.format != ARROW (index: {result_set.index})"
            assert len(result_set.rows) == 0, f"rows != 0 for Arrow format (index: {result_set.index})"
            assert (
                result_set.data is not None and len(result_set.data) != 0
            ), f"data is empty (index: {result_set.index})"

            schema = pa.ipc.read_schema(pa.py_buffer(result_set.arrow_format_meta.schema))
            batch = pa.ipc.read_record_batch(pa.py_buffer(result_set.data), schema)
            batch.validate()

            assert batch.num_columns == len(
                table_info.columns
            ), f"Column count mismatch: {batch.num_columns} != {len(table_info.columns)}"

            total_rows += batch.num_rows

        if expected_limit is not None:
            assert (
                total_rows <= expected_limit
            ), f"Row count {total_rows} exceeds LIMIT {expected_limit} for {table_info.path}"

        logger.debug(
            f"SELECT on {table_info.path} with {codec_desc} returned {len(result)} result sets, {total_rows} rows"
        )
        self._stats.increment_select_queries(total_rows)

    def _execute_query_with_retry(
        self,
        query: str,
        parameters: Dict[str, Any] = None,
        arrow_format_settings: ydb.ArrowFormatSettings = None,
    ) -> Any:
        """Execute YDB query with exponential backoff retry logic for transient errors"""

        last_exception = None
        for attempt in range(self._config.max_retries):
            try:
                return self.client.session_pool.execute_with_retries(
                    query=query,
                    parameters=parameters,
                    result_set_format=self._format,
                    arrow_format_settings=arrow_format_settings,
                )
            except (ydb.issues.Aborted, ydb.issues.Unavailable, ydb.issues.Undetermined) as e:
                last_exception = e
                if attempt < self._config.max_retries - 1:
                    logger.warning(f"Query failed (attempt {attempt + 1}/{self._config.max_retries}): {e}")
                    time.sleep(self._config.retry_delay_seconds * (2**attempt))
                else:
                    logger.error(f"Query failed after {self._config.max_retries} attempts: {e}")
            except Exception:
                raise

        raise last_exception
