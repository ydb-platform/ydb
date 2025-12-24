from typing import Dict, List, Any
from dataclasses import dataclass, field

import pyarrow as pa
import threading
import logging
import random
import time
import ydb

from ydb.tests.datashard.lib.types_of_variables import pk_types, non_pk_types
from ydb.tests.stress.common.common import WorkloadBase
from ydb.tests.stress.result_set_format.workload.common import (
    ColumnInfo,
    TableInfo,
    ParameterBuilder,
    generate_batch_rows,
    validate_arrow_result_set,
    validate_value_result_set,
)

logger = logging.getLogger(__name__)


class WorkloadStats:
    """Thread-safe statistics tracking for the workload"""

    def __init__(self):
        self.tables = 0
        self.select_queries = 0
        self.upsert_queries = 0
        self.total_rows_inserted = 0
        self.total_rows_selected = 0
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

    def increment_upsert_queries(self, rows_count: int) -> None:
        """Increment the upsert queries counter in a thread-safe manner"""

        with self._lock:
            self.upsert_queries += 1
            self.total_rows_inserted += rows_count

    def get_stats(self) -> str:
        """Get current statistics as a formatted string"""

        with self._lock:
            avg_rows_selected = self.total_rows_selected / self.select_queries if self.select_queries > 0 else 0
            avg_rows_inserted = self.total_rows_inserted / self.upsert_queries if self.upsert_queries > 0 else 0
            return (
                f"Tables: {self.tables}, "
                f"Selects: {self.select_queries}, "
                f"Upserts: {self.upsert_queries}, "
                f"AvgRowsSelected: {avg_rows_selected:.2f}, "
                f"AvgRowsInserted: {avg_rows_inserted:.2f}"
            )


@dataclass
class WorkloadConfig:
    """Configuration parameters for the workload execution and data generation"""

    # Table structure constants
    pk_types: List[str] = field(default_factory=lambda: list(pk_types.keys()))
    all_types: List[str] = field(default_factory=lambda: [*pk_types.keys(), *non_pk_types.keys()])

    # Data generation constants
    min_rows_count: int = 100
    max_rows_count: int = 500
    max_pk_value: int = 100000000
    max_value: int = 100000000
    null_probability: float = 0.1
    min_upserts_count: int = 10
    max_upserts_count: int = 20
    min_partitions_per_table: int = 2
    max_partitions_per_table: int = 8

    # Workload execution constants
    tables_inflight: int = 8
    jobs_per_table: int = 2
    selects_per_job: int = 32
    max_columns_per_select: int = 16

    # Retry configuration
    max_retries: int = 5
    retry_delay_seconds: float = 0.5


class WorkloadDataTypes(WorkloadBase):
    """
    Stress test for result set formats with all supported data types.
    Creates tables with all types, fills them with data, and performs SELECT queries.
    """

    def __init__(self, client, prefix, format, stop):
        """Initialize the workload with client connection and format configuration"""

        super().__init__(client, prefix, f"data_types_{format}", stop)

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

        return "Format: " + self._format.name + ", " + self._stats.get_stats()

    def run_job(self, table_info: TableInfo, job_key: int) -> None:
        """Execute multiple SELECT operations on the table until quota is reached"""

        logger.info(f"Starting job {job_key} for table {table_info.path}")
        selects = 0
        while selects < self._config.selects_per_job and not self.is_stop_requested():
            try:
                self._run_select_operation(table_info)
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
        """Generate table schema with primary key and all supported data types"""

        pk_type = random.choice(self._config.pk_types)
        columns = [ColumnInfo(name="pk", type_name=pk_type, not_null=True)]

        for i, type_name in enumerate(self._config.all_types):
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
        """Populate table with random data using multiple UPSERT operations"""

        upserts_count = random.randint(self._config.min_upserts_count, self._config.max_upserts_count)
        for _ in range(upserts_count):
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
            self._stats.increment_upsert_queries(rows_count=rows_count)

    def _run_select_operation(self, table_info: TableInfo) -> None:
        """Execute SELECT query with random columns and validate result set format"""

        column_names = [col.name for col in table_info.columns]
        num_columns = random.randint(1, min(self._config.max_columns_per_select, len(column_names)))
        selected_columns = random.sample(column_names, num_columns)

        select_sql = f"""
            SELECT {', '.join(selected_columns)}
            FROM `{table_info.path}`;
        """

        logger.debug(f"Executing SELECT on {table_info.path} with columns: {selected_columns}")
        result = self._execute_query_with_retry(select_sql)

        if not result:
            raise Exception(f"SELECT query returned no result sets for {table_info.path}")

        rows_count = 0
        for result_set in result:
            if self._format == ydb.QueryResultSetFormat.ARROW:
                validate_arrow_result_set(result_set)
                schema = pa.ipc.read_schema(pa.py_buffer(result_set.arrow_format_meta.schema))
                batch = pa.ipc.read_record_batch(pa.py_buffer(result_set.data), schema)
                rows_count += batch.num_rows

                for column_name in selected_columns:
                    column_info = table_info.get_column_by_name(column_name)
                    if column_info is None:
                        raise Exception(f"Column {column_name} not found in table {table_info.path}")

                    assert (
                        batch.schema.field(column_name).type == column_info.arrow_type()
                    ), f"Column `{column_name}` with type {column_info.type_name} has incorrect Arrow type"

            elif self._format == ydb.QueryResultSetFormat.VALUE:
                validate_value_result_set(result_set)
                rows_count += len(result_set.rows)

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"SELECT query on {table_info.path} returned {len(result)} result sets with {rows_count} rows")
        else:
            logger.info(f"SELECT query on {table_info.path} returned {len(result)} result sets")

        self._stats.increment_select_queries(rows_count)

    def _execute_query_with_retry(self, query: str, parameters: Dict[str, Any] = None) -> Any:
        """Execute YDB query with exponential backoff retry logic for transient errors"""

        last_exception = None
        for attempt in range(self._config.max_retries):
            try:
                return self.client.session_pool.execute_with_retries(
                    query=query,
                    parameters=parameters,
                    result_set_format=self._format,
                    schema_inclusion_mode=ydb.QuerySchemaInclusionMode.ALWAYS,
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
