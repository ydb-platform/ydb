from typing import Dict, List, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum

import pyarrow as pa
import threading
import logging
import random
import time
import ydb

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
        self.operations = 0
        self.precondition_failed = 0
        self.tables = 0
        self.queries_executed = 0
        self.total_result_sets = 0
        self.total_statements = 0
        self.schema_inclusion_always = 0
        self.schema_inclusion_first_only = 0
        self._lock = threading.Lock()

    def increment_operations(self) -> None:
        """Increment the operations counter in a thread-safe manner"""

        with self._lock:
            self.operations += 1

    def increment_precondition_failed(self) -> None:
        """Increment the precondition failed counter in a thread-safe manner"""

        with self._lock:
            self.precondition_failed += 1

    def increment_tables(self) -> None:
        """Increment the tables counter in a thread-safe manner"""

        with self._lock:
            self.tables += 1

    def increment_queries_executed(self) -> None:
        """Increment the queries executed counter in a thread-safe manner"""

        with self._lock:
            self.queries_executed += 1

    def add_result_sets(self, count: int) -> None:
        """Add result sets count in a thread-safe manner"""

        with self._lock:
            self.total_result_sets += count

    def add_statements(self, count: int) -> None:
        """Add statements count in a thread-safe manner"""

        with self._lock:
            self.total_statements += count

    def increment_schema_inclusion_mode(self, mode: ydb.QuerySchemaInclusionMode) -> None:
        """Increment schema inclusion mode counter in a thread-safe manner"""

        with self._lock:
            if mode == ydb.QuerySchemaInclusionMode.ALWAYS:
                self.schema_inclusion_always += 1
            elif mode == ydb.QuerySchemaInclusionMode.FIRST_ONLY:
                self.schema_inclusion_first_only += 1

    def get_stats(self) -> str:
        """Get current statistics as a formatted string"""

        with self._lock:
            avg_result_sets = self.total_result_sets / self.queries_executed if self.queries_executed > 0 else 0
            avg_statements = self.total_statements / self.queries_executed if self.queries_executed > 0 else 0
            return (
                f"Tables: {self.tables}, "
                f"Operations: {self.operations}, "
                f"Queries: {self.queries_executed}, "
                f"PreconditionFailed: {self.precondition_failed}, "
                f"AvgResultSets: {avg_result_sets:.2f}, "
                f"AvgStatements: {avg_statements:.2f}, "
                f"Always: {self.schema_inclusion_always}, "
                f"FirstOnly: {self.schema_inclusion_first_only}"
            )


@dataclass
class WorkloadConfig:
    """Configuration parameters for the workload execution and data generation"""

    # Table structure constants
    max_columns: int = 16
    primary_key_min_columns: int = 1
    primary_key_max_columns: int = 4
    not_null_columns_probability: float = 0.25
    min_partitions_per_table: int = 2
    max_partitions_per_table: int = 8
    column_types: List[str] = field(
        default_factory=lambda: ["Uint32", "Uint64", "Int32", "Int64", "Uint8", "Bool", "Int8", "String", "Utf8"]
    )

    # Data generation constants
    max_primary_key_value: int = 100000000
    max_value: int = 100000000
    max_rows_per_query: int = 100
    null_probability: float = 0.05

    # Workload execution constants
    tables_inflight: int = 4
    jobs_per_table: int = 4
    operations_per_job: int = 32
    max_statements_per_operation: int = 10

    # Retry configuration
    max_retries: int = 5
    retry_delay: float = 0.5  # seconds


class OperationType(Enum):
    """Enumeration of supported database operations"""

    INSERT = 0
    REPLACE = 1
    UPSERT = 2
    UPDATE = 3
    DELETE = 4
    SELECT = 5


class WorkloadSchemaInclusion(WorkloadBase):
    """
    Stress test for result set formats with different schema_inclusion_mode settings.
    Tests ALWAYS and FIRST_ONLY modes with multiple result sets.
    """

    def __init__(self, client, prefix, format, stop):
        """Initialize the workload with client connection and format configuration"""

        super().__init__(client, prefix, f"schema_inclusion_{format}", stop)

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
        """Execute multiple database operations on the table until quota is reached"""

        logger.info(f"Starting job {job_key} for table {table_info.path}")
        operations = 0
        while operations < self._config.operations_per_job and not self.is_stop_requested():
            try:
                self._run_operations(table_info, operations)
                self._stats.increment_operations()
                operations += 1
            except Exception as e:
                logger.error(
                    f"Job {job_key} (operation id: {operations}) for {table_info.path} encountered unexpected error: {e}"
                )
                raise

    def run_for_table(self, table_name: str) -> None:
        """Create table, fill with data, run concurrent jobs, and cleanup"""

        table_info = self._generate_table(table_name)
        self._create_table(table_info)
        self._stats.increment_tables()

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
                    target=thread_wrapper, args=(self.run_job, table_info, i), name=f"{table_name} job:{i}"
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

        pk_size = random.randint(self._config.primary_key_min_columns, self._config.primary_key_max_columns)
        columns: List[ColumnInfo] = []
        for i in range(random.randint(pk_size, self._config.max_columns)):
            columns.append(
                ColumnInfo(
                    name=f"c{i + 1}",
                    type_name=random.choice(self._config.column_types),
                    not_null=random.random() < self._config.not_null_columns_probability,
                )
            )

        primary_key_columns: List[str] = []
        for i in random.sample(range(len(columns)), pk_size):
            primary_key_columns.append(columns[i].name)

        min_partitions = random.randint(self._config.min_partitions_per_table, self._config.max_partitions_per_table)
        return TableInfo(
            path=self.get_table_path(table_name),
            columns=columns,
            primary_key_columns=primary_key_columns,
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

    def _build_operation(self, operation_type: OperationType, *args, **kwargs) -> Tuple[str, Dict[str, Any]]:
        """Build database operation query and parameters"""

        match operation_type:
            case OperationType.INSERT:
                return self._insert_operation(*args, **kwargs)
            case OperationType.REPLACE:
                return self._replace_operation(*args, **kwargs)
            case OperationType.UPSERT:
                return self._upsert_operation(*args, **kwargs)
            case OperationType.UPDATE:
                return self._update_operation(*args, **kwargs)
            case OperationType.DELETE:
                return self._delete_operation(*args, **kwargs)
            case OperationType.SELECT:
                return self._select_operation(*args, **kwargs)
            case _:
                raise ValueError(f"Invalid operation type: {operation_type}")

    def _run_operations(self, table_info: TableInfo, operation_id: int) -> None:
        """Execute multiple database operations on the table until quota is reached"""

        operations = list(OperationType)
        operations_count = random.randint(1, self._config.max_statements_per_operation)
        selected_operations = [random.choice(operations) for _ in range(operations_count)]

        query = ""
        all_parameters = {}

        idx_to_return_columns = {}
        for query_id, operation_type in enumerate(selected_operations):
            return_columns_count = random.randint(1, len(table_info.columns))
            return_columns = random.sample(range(len(table_info.columns)), return_columns_count)
            return_columns = [table_info.columns[i].name for i in return_columns]
            idx_to_return_columns[query_id] = return_columns

            stmt, parameters = self._build_operation(operation_type, query_id, table_info, return_columns)
            query += stmt
            all_parameters.update(parameters)

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Executing operation {operation_id} for table {table_info.path} with query: {query}")
        else:
            logger.info(f"Executing operation {operation_id} for table {table_info.path}")

        schema_inclusion_mode = (
            ydb.QuerySchemaInclusionMode.ALWAYS if random.random() < 0.5 else ydb.QuerySchemaInclusionMode.FIRST_ONLY
        )
        self._stats.increment_schema_inclusion_mode(schema_inclusion_mode)
        self._stats.add_statements(len(selected_operations))

        result = None
        try:
            result = self._execute_query_with_retry(
                query, parameters=all_parameters, schema_inclusion_mode=schema_inclusion_mode
            )
            self._stats.increment_queries_executed()
            assert result is not None, f"Result is None for operation {operation_id}"
        except ydb.issues.PreconditionFailed as e:
            self._stats.increment_precondition_failed()
            result = []
            log_msg = f"Operation {operation_id} with {len(selected_operations)} statements returned precondition failed error: {e}"
            if OperationType.INSERT in selected_operations:
                logger.warning(log_msg)
            else:
                logger.error(log_msg)
                raise e
        except Exception:
            raise

        logger.info(
            f"Operation {operation_id} with {len(selected_operations)} statements returned {len(result)} result sets"
        )

        self._stats.add_result_sets(len(result))

        if self._format == ydb.QueryResultSetFormat.ARROW:
            self._validate_arrow_schema(result, schema_inclusion_mode, idx_to_return_columns)
        elif self._format == ydb.QueryResultSetFormat.VALUE:
            self._validate_value_schema(result, schema_inclusion_mode)
        else:
            raise ValueError(f"Invalid format: {self._format}")

    def _validate_arrow_schema(
        self,
        result: ydb.convert.ResultSets,
        schema_inclusion_mode: ydb.QuerySchemaInclusionMode,
        idx_to_return_columns: Dict[int, List[str]],
    ) -> None:
        """Validate that result set is properly formatted in Arrow format"""

        result_set_schemas = {}
        for result_set in result:
            wait_schema = False
            match schema_inclusion_mode:
                case ydb.QuerySchemaInclusionMode.ALWAYS:
                    wait_schema = True
                    result_set_schemas[result_set.index] = result_set.arrow_format_meta.schema
                case ydb.QuerySchemaInclusionMode.FIRST_ONLY:
                    if result_set.index not in result_set_schemas:
                        wait_schema = True
                        result_set_schemas[result_set.index] = result_set.arrow_format_meta.schema
                case _:
                    raise ValueError(f"Invalid schema inclusion mode: {schema_inclusion_mode}")

            validate_arrow_result_set(result_set, wait_schema)

            schema = pa.ipc.read_schema(pa.py_buffer(result_set_schemas[result_set.index]))
            batch = pa.ipc.read_record_batch(pa.py_buffer(result_set.data), schema)
            batch.validate()

            return_columns = idx_to_return_columns[result_set.index]
            assert batch.num_columns == len(
                return_columns
            ), f"Columns count: {batch.num_columns} != {len(return_columns)} (index: {result_set.index})"

    def _validate_value_schema(
        self, result: ydb.convert.ResultSets, schema_inclusion_mode: ydb.QuerySchemaInclusionMode
    ) -> None:
        """Validate that result set is properly formatted in Value format"""

        result_set_indexes = set()
        for result_set in result:
            wait_schema = False
            match schema_inclusion_mode:
                case ydb.QuerySchemaInclusionMode.ALWAYS:
                    wait_schema = True
                    result_set_indexes.add(result_set.index)
                case ydb.QuerySchemaInclusionMode.FIRST_ONLY:
                    if result_set.index not in result_set_indexes:
                        wait_schema = True
                        result_set_indexes.add(result_set.index)
                case _:
                    raise ValueError(f"Invalid schema inclusion mode: {schema_inclusion_mode}")

            validate_value_result_set(result_set, wait_schema)

    def _insert_operation(
        self, query_id: int, table_info: TableInfo, return_columns: List[str]
    ) -> Tuple[str, Dict[str, Any]]:
        """Build INSERT operation query and parameters"""

        batch_rows = generate_batch_rows(
            table_info,
            rows_count=random.randint(1, self._config.max_rows_per_query),
            max_pk_value=self._config.max_primary_key_value,
            max_value=self._config.max_value,
            null_probability=self._config.null_probability,
        )

        param_name = f"$p{query_id}_rows"
        parameters = self._param_builder.create_list(param_name, table_info.columns, batch_rows)

        query = f"""
            INSERT INTO `{table_info.path}`
            SELECT * FROM AS_TABLE({param_name})
            RETURNING {', '.join(return_columns)};
        """

        return query, parameters

    def _upsert_operation(
        self, query_id: int, table_info: TableInfo, return_columns: List[str]
    ) -> Tuple[str, Dict[str, Any]]:
        """Build UPSERT operation query and parameters"""

        batch_rows = generate_batch_rows(
            table_info,
            rows_count=random.randint(1, self._config.max_rows_per_query),
            max_pk_value=self._config.max_primary_key_value,
            max_value=self._config.max_value,
            null_probability=self._config.null_probability,
        )

        param_name = f"$p{query_id}_rows"
        parameters = self._param_builder.create_list(param_name, table_info.columns, batch_rows)

        query = f"""
            UPSERT INTO `{table_info.path}`
            SELECT * FROM AS_TABLE({param_name})
            RETURNING {', '.join(return_columns)};
        """

        return query, parameters

    def _replace_operation(
        self, query_id: int, table_info: TableInfo, return_columns: List[str]
    ) -> Tuple[str, Dict[str, Any]]:
        """Build REPLACE operation query and parameters"""

        batch_rows = generate_batch_rows(
            table_info,
            rows_count=random.randint(1, self._config.max_rows_per_query),
            max_pk_value=self._config.max_primary_key_value,
            max_value=self._config.max_value,
            null_probability=self._config.null_probability,
        )

        param_name = f"$p{query_id}_rows"
        parameters = self._param_builder.create_list(param_name, table_info.columns, batch_rows)

        query = f"""
            REPLACE INTO `{table_info.path}`
            SELECT * FROM AS_TABLE({param_name})
            RETURNING {', '.join(return_columns)};
        """

        return query, parameters

    def _update_operation(
        self, query_id: int, table_info: TableInfo, return_columns: List[str]
    ) -> Tuple[str, Dict[str, Any]]:
        """Build UPDATE operation query and parameters"""

        batch_rows = generate_batch_rows(
            table_info,
            rows_count=random.randint(1, self._config.max_rows_per_query),
            max_pk_value=self._config.max_primary_key_value,
            max_value=self._config.max_value,
            null_probability=self._config.null_probability,
        )

        param_name = f"$p{query_id}_rows"
        parameters = self._param_builder.create_list(param_name, table_info.columns, batch_rows)

        query = f"""
            UPDATE `{table_info.path}` ON
            SELECT * FROM AS_TABLE({param_name})
            RETURNING {', '.join(return_columns)};
        """

        return query, parameters

    def _delete_operation(
        self, query_id: int, table_info: TableInfo, return_columns: List[str]
    ) -> Tuple[str, Dict[str, Any]]:
        """Build DELETE operation query and parameters"""

        batch_rows = generate_batch_rows(
            table_info,
            rows_count=random.randint(1, self._config.max_rows_per_query),
            max_pk_value=self._config.max_primary_key_value,
            max_value=self._config.max_value,
            null_probability=self._config.null_probability,
        )

        param_name = f"$p{query_id}_rows"
        parameters = self._param_builder.create_list(param_name, table_info.columns, batch_rows)

        query = f"""
            DELETE FROM `{table_info.path}` ON
            SELECT * FROM AS_TABLE({param_name})
            RETURNING {', '.join(return_columns)};
        """

        return query, parameters

    def _select_operation(
        self, query_id: int, table_info: TableInfo, return_columns: List[str]
    ) -> Tuple[str, Dict[str, Any]]:
        """Build SELECT operation query and parameters"""

        query = f"""
            SELECT {', '.join(return_columns)} FROM `{table_info.path}`;
        """

        return query, {}

    def _execute_query_with_retry(
        self, query: str, parameters: Dict[str, Any] = None, schema_inclusion_mode: ydb.QuerySchemaInclusionMode = None
    ) -> Any:
        """Execute YDB query with exponential backoff retry logic for transient errors"""

        last_exception = None
        for attempt in range(self._config.max_retries):
            try:
                return self.client.session_pool.execute_with_retries(
                    query=query,
                    parameters=parameters,
                    result_set_format=self._format,
                    schema_inclusion_mode=schema_inclusion_mode,
                )
            except (ydb.issues.Aborted, ydb.issues.Unavailable, ydb.issues.Undetermined) as e:
                last_exception = e
                if attempt < self._config.max_retries - 1:
                    logger.warning(f"Query failed (attempt {attempt + 1}/{self._config.max_retries}): {e}")
                    time.sleep(self._config.retry_delay * (2**attempt))
                else:
                    logger.error(f"Query failed after {self._config.max_retries} attempts: {e}")
            except Exception:
                raise

        raise last_exception
