# -*- coding: utf-8 -*-
import threading
import logging
import random
import time
import ydb
import pyarrow as pa

from typing import Dict, List, Any
from dataclasses import dataclass, field

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
    """Thread-safe statistics tracking for the mixed workload"""

    def __init__(self):
        self.select_queries = 0
        self.select_all_queries = 0
        self.upsert_queries = 0
        self.replace_queries = 0
        self.update_queries = 0
        self.delete_queries = 0
        self.total_rows_selected = 0
        self.total_rows_modified = 0
        self.compression_used = {
            "UNSPECIFIED": 0,
            "NONE": 0,
            "ZSTD": 0,
            "LZ4_FRAME": 0,
        }
        self.schema_inclusion_used = {
            "ALWAYS": 0,
            "FIRST_ONLY": 0,
        }
        self._lock = threading.Lock()

    def increment_select(self, rows_count: int = 0) -> None:
        """Increment the select queries counter in a thread-safe manner"""

        with self._lock:
            self.select_queries += 1
            self.total_rows_selected += rows_count

    def increment_select_all(self, rows_count: int = 0) -> None:
        """Increment the select all queries counter in a thread-safe manner"""

        with self._lock:
            self.select_all_queries += 1
            self.total_rows_selected += rows_count

    def increment_upsert(self, rows_count: int = 0) -> None:
        """Increment the upsert queries counter in a thread-safe manner"""

        with self._lock:
            self.upsert_queries += 1
            self.total_rows_modified += rows_count

    def increment_replace(self, rows_count: int = 0) -> None:
        """Increment the replace queries counter in a thread-safe manner"""

        with self._lock:
            self.replace_queries += 1
            self.total_rows_modified += rows_count

    def increment_update(self, rows_count: int = 0) -> None:
        """Increment the update queries counter in a thread-safe manner"""

        with self._lock:
            self.update_queries += 1
            self.total_rows_modified += rows_count

    def increment_delete(self, rows_count: int = 0) -> None:
        """Increment the delete queries counter in a thread-safe manner"""

        with self._lock:
            self.delete_queries += 1
            self.total_rows_modified += rows_count

    def increment_compression(self, codec_type: ydb.ArrowCompressionCodecType) -> None:
        """Increment the compression counter in a thread-safe manner"""

        with self._lock:
            self.compression_used[codec_type.name] += 1

    def increment_schema_inclusion(self, mode: ydb.QuerySchemaInclusionMode) -> None:
        """Increment the schema inclusion counter in a thread-safe manner"""

        with self._lock:
            self.schema_inclusion_used[mode.name] += 1

    def get_stats(self) -> str:
        """Get current statistics as a formatted string"""

        with self._lock:
            return (
                f"Selects: {self.select_queries}, "
                f"SelectAll: {self.select_all_queries}, "
                f"Upserts: {self.upsert_queries}, "
                f"Replaces: {self.replace_queries}, "
                f"Updates: {self.update_queries}, "
                f"Deletes: {self.delete_queries}, "
                f"RowsSelected: {self.total_rows_selected}, "
                f"RowsModified: {self.total_rows_modified}, "
                f"Compression(U:{self.compression_used['UNSPECIFIED']},N:{self.compression_used['NONE']},"
                f"Z:{self.compression_used['ZSTD']},L:{self.compression_used['LZ4_FRAME']}), "
                f"Schema(A:{self.schema_inclusion_used['ALWAYS']},F:{self.schema_inclusion_used['FIRST_ONLY']})"
            )


@dataclass
class WorkloadConfig:
    """Configuration parameters for the mixed workload"""

    # Table structure
    pk_types: List[str] = field(default_factory=lambda: list(pk_types.keys()))
    all_types: List[str] = field(default_factory=lambda: [*pk_types.keys(), *non_pk_types.keys()])
    min_partitions: int = 4
    max_partitions: int = 8

    # Data generation
    min_rows_count: int = 50
    max_rows_count: int = 200
    max_pk_value: int = 10000000
    max_value: int = 10000000
    null_probability: float = 0.1

    # Operation probabilities
    limit_probability: float = 0.3
    min_limit: int = 10
    max_limit: int = 100

    # Compression settings
    zstd_min_level: int = 1
    zstd_max_level: int = 22
    zstd_no_level_probability: float = 0.2

    # Retry configuration
    max_retries: int = 5
    retry_delay_seconds: float = 0.5


class WorkloadMixed(WorkloadBase):
    """
    Comprehensive mixed workload for result set formats.
    Combines compression, data types, and schema inclusion testing.
    Creates a single table and runs multiple concurrent threads performing different operations.
    """

    def __init__(self, client, prefix, format, stop):
        """Initialize the mixed workload with client connection and format configuration"""

        super().__init__(client, prefix, f"mixed_{format}", stop)

        self._config = WorkloadConfig()
        self._param_builder = ParameterBuilder()
        self._stats = WorkloadStats()
        self._table_info = None
        self._table_created = threading.Event()

        match format:
            case "value":
                self._format = ydb.QueryResultSetFormat.VALUE
            case "arrow":
                self._format = ydb.QueryResultSetFormat.ARROW
            case _:
                raise ValueError(f"Invalid format: {format}")

        # Create table on initialization
        self._initialize_table()

    def _initialize_table(self):
        """Create the shared table for all threads"""

        try:
            self._table_info = self._generate_table("mixed_workload_table")
            self._create_table(self._table_info)
            self._table_created.set()
            logger.info(f"Table {self._table_info.path} created successfully")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise

    def get_stat(self):
        """Return formatted statistics string with current workload metrics"""

        return self._stats.get_stats()

    def _generate_table(self, table_name: str) -> TableInfo:
        """Generate table schema with all data types (both nullable and NOT NULL)"""

        pk_type = random.choice(self._config.pk_types)
        columns = [ColumnInfo(name="pk", type_name=pk_type, not_null=True)]

        # Add each type twice: once nullable, once NOT NULL
        for i, type_name in enumerate(self._config.all_types):
            columns.append(ColumnInfo(name=f"col_{i}_nullable", type_name=type_name, not_null=False))
            columns.append(ColumnInfo(name=f"col_{i}_not_null", type_name=type_name, not_null=True))

        min_partitions = random.randint(self._config.min_partitions, self._config.max_partitions)

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
            if random.random() < self._config.zstd_no_level_probability:
                return ydb.ArrowCompressionCodec(codec_type)
            else:
                level = random.randint(self._config.zstd_min_level, self._config.zstd_max_level)
                return ydb.ArrowCompressionCodec(codec_type, level)
        else:
            return ydb.ArrowCompressionCodec(codec_type)

    def _generate_schema_inclusion_mode(self) -> ydb.QuerySchemaInclusionMode:
        """Generate random schema inclusion mode"""

        return random.choice([ydb.QuerySchemaInclusionMode.ALWAYS, ydb.QuerySchemaInclusionMode.FIRST_ONLY])

    def _loop_select(self):
        """Thread function: Regular SELECT queries with random LIMIT, compression, and schema inclusion"""

        self._table_created.wait()
        logger.info("Starting SELECT loop")

        while not self.is_stop_requested():
            try:
                # Generate random column selection
                column_names = [col.name for col in self._table_info.columns]
                num_columns = random.randint(1, min(10, len(column_names)))
                selected_columns = random.sample(column_names, num_columns)

                select_sql = f"SELECT {', '.join(selected_columns)} FROM `{self._table_info.path}`"

                expected_limit = None
                if random.random() < self._config.limit_probability:
                    limit = random.randint(self._config.min_limit, self._config.max_limit)
                    select_sql += f" LIMIT {limit}"
                    expected_limit = limit
                select_sql += ";"

                codec = self._generate_compression_codec()
                schema_inclusion_mode = self._generate_schema_inclusion_mode()

                self._stats.increment_compression(codec.type)
                self._stats.increment_schema_inclusion(schema_inclusion_mode)

                arrow_format_settings = ydb.ArrowFormatSettings(compression_codec=codec)
                result = self._execute_query_with_retry(
                    select_sql,
                    arrow_format_settings=arrow_format_settings,
                    schema_inclusion_mode=schema_inclusion_mode,
                )

                logger.info(f"For SELECT with validation, result sets count: {len(result)}")

                total_rows = self._validate_result(result, schema_inclusion_mode, num_columns)

                if expected_limit is not None:
                    assert total_rows <= expected_limit, f"Row count {total_rows} exceeds LIMIT {expected_limit}"

                self._stats.increment_select(total_rows)

            except Exception as e:
                logger.error(f"Error in SELECT loop: {e}")
                raise

    def _loop_select_all_with_validation(self):
        """Thread function: SELECT all columns with full type validation"""

        self._table_created.wait()
        logger.info("Starting SELECT ALL with validation loop")

        while not self.is_stop_requested():
            try:
                select_sql = f"SELECT * FROM `{self._table_info.path}` LIMIT {self._config.min_limit};"

                codec = self._generate_compression_codec()
                schema_inclusion_mode = self._generate_schema_inclusion_mode()

                self._stats.increment_compression(codec.type)
                self._stats.increment_schema_inclusion(schema_inclusion_mode)

                arrow_format_settings = ydb.ArrowFormatSettings(compression_codec=codec)
                result = self._execute_query_with_retry(
                    select_sql,
                    arrow_format_settings=arrow_format_settings,
                    schema_inclusion_mode=schema_inclusion_mode,
                )

                result_set_indexes = set()
                arrow_schema = None

                logger.info(f"For SELECT ALL with validation, result sets count: {len(result)}")

                total_rows = 0
                for result_set in result:
                    wait_schema = False
                    match schema_inclusion_mode:
                        case ydb.QuerySchemaInclusionMode.ALWAYS:
                            wait_schema = True
                        case ydb.QuerySchemaInclusionMode.FIRST_ONLY:
                            if result_set.index not in result_set_indexes:
                                wait_schema = True
                                result_set_indexes.add(result_set.index)
                        case _:
                            raise ValueError(f"Invalid schema inclusion mode: {schema_inclusion_mode}")

                    if self._format == ydb.QueryResultSetFormat.ARROW:
                        arrow_schema = result_set.arrow_format_meta.schema if wait_schema else arrow_schema
                        rows = self._validate_arrow_result_with_types(result_set, arrow_schema, wait_schema)
                        total_rows += rows
                    elif self._format == ydb.QueryResultSetFormat.VALUE:
                        validate_value_result_set(result_set, wait_schema)
                        total_rows += len(result_set.rows)

                self._stats.increment_select_all(total_rows)

            except Exception as e:
                logger.error(f"Error in SELECT ALL loop: {e}")
                raise

    def _loop_modify_operations(self):
        """Thread function: UPSERT, REPLACE, UPDATE, DELETE operations with RETURNING clause"""

        self._table_created.wait()
        logger.info("Starting MODIFY operations loop")

        while not self.is_stop_requested():
            try:
                operation_type = random.choice(["UPSERT", "REPLACE", "UPDATE", "DELETE"])

                rows_count = random.randint(self._config.min_rows_count, self._config.max_rows_count)
                batch_rows = generate_batch_rows(
                    self._table_info,
                    rows_count=rows_count,
                    max_pk_value=self._config.max_pk_value,
                    max_value=self._config.max_value,
                    null_probability=self._config.null_probability,
                )

                param_name = "$rows"
                parameters = self._param_builder.create_list(param_name, self._table_info.columns, batch_rows)

                # Random RETURNING columns
                return_columns_count = random.randint(1, len(self._table_info.columns))
                return_columns = random.sample([col.name for col in self._table_info.columns], return_columns_count)

                if operation_type == "UPSERT":
                    query = f"""
                        UPSERT INTO `{self._table_info.path}`
                        SELECT * FROM AS_TABLE({param_name})
                        RETURNING {', '.join(return_columns)};
                    """
                elif operation_type == "REPLACE":
                    query = f"""
                        REPLACE INTO `{self._table_info.path}`
                        SELECT * FROM AS_TABLE({param_name})
                        RETURNING {', '.join(return_columns)};
                    """
                elif operation_type == "UPDATE":
                    query = f"""
                        UPDATE `{self._table_info.path}` ON
                        SELECT * FROM AS_TABLE({param_name})
                        RETURNING {', '.join(return_columns)};
                    """
                elif operation_type == "DELETE":
                    query = f"""
                        DELETE FROM `{self._table_info.path}` ON
                        SELECT * FROM AS_TABLE({param_name})
                        RETURNING {', '.join(return_columns)};
                    """
                else:
                    raise ValueError(f"Invalid operation type: {operation_type}")

                codec = self._generate_compression_codec()
                schema_inclusion_mode = self._generate_schema_inclusion_mode()

                self._stats.increment_compression(codec.type)
                self._stats.increment_schema_inclusion(schema_inclusion_mode)

                arrow_format_settings = ydb.ArrowFormatSettings(compression_codec=codec)
                result = self._execute_query_with_retry(
                    query,
                    parameters=parameters,
                    arrow_format_settings=arrow_format_settings,
                    schema_inclusion_mode=schema_inclusion_mode,
                )

                logger.info(f"For {operation_type} operation with RETURNING, result sets count: {len(result)}")

                total_rows = self._validate_result(result, schema_inclusion_mode, return_columns_count)

                if operation_type == "UPSERT":
                    self._stats.increment_upsert(total_rows)
                elif operation_type == "REPLACE":
                    self._stats.increment_replace(total_rows)
                elif operation_type == "UPDATE":
                    self._stats.increment_update(total_rows)
                elif operation_type == "DELETE":
                    self._stats.increment_delete(total_rows)
                else:
                    raise ValueError(f"Invalid operation type: {operation_type}")

            except Exception as e:
                logger.error(f"Error in MODIFY loop: {e}")
                raise

    def _validate_result(
        self, result: ydb.convert.ResultSets, schema_inclusion_mode: ydb.QuerySchemaInclusionMode, expected_columns: int
    ) -> int:
        """Validate result set and return total row count"""

        total_rows = 0
        result_set_indexes = {}

        for result_set in result:
            wait_schema = False
            match schema_inclusion_mode:
                case ydb.QuerySchemaInclusionMode.ALWAYS:
                    wait_schema = True
                    result_set_indexes[result_set.index] = (
                        result_set.arrow_format_meta.schema if self._format == ydb.QueryResultSetFormat.ARROW else True
                    )
                case ydb.QuerySchemaInclusionMode.FIRST_ONLY:
                    if result_set.index not in result_set_indexes:
                        wait_schema = True
                        result_set_indexes[result_set.index] = (
                            result_set.arrow_format_meta.schema
                            if self._format == ydb.QueryResultSetFormat.ARROW
                            else True
                        )
                case _:
                    raise ValueError(f"Invalid schema inclusion mode: {schema_inclusion_mode}")

            if self._format == ydb.QueryResultSetFormat.ARROW:
                validate_arrow_result_set(result_set, wait_schema)

                schema = pa.ipc.read_schema(pa.py_buffer(result_set_indexes[result_set.index]))
                batch = pa.ipc.read_record_batch(pa.py_buffer(result_set.data), schema)
                batch.validate()

                assert (
                    batch.num_columns == expected_columns
                ), f"Column count mismatch: {batch.num_columns} != {expected_columns}"
                total_rows += batch.num_rows

            elif self._format == ydb.QueryResultSetFormat.VALUE:
                validate_value_result_set(result_set, wait_schema)
                total_rows += len(result_set.rows)

        return total_rows

    def _validate_arrow_result_with_types(
        self, result_set: ydb.convert.ResultSet, arrow_schema: bytes, wait_schema: bool
    ) -> int:
        """Validate Arrow result set with full type checking"""

        validate_arrow_result_set(result_set, wait_schema)

        schema = pa.ipc.read_schema(pa.py_buffer(arrow_schema))
        batch = pa.ipc.read_record_batch(pa.py_buffer(result_set.data), schema)
        batch.validate()

        # Validate all column types
        for column_info in self._table_info.columns:
            column_name = column_info.name
            if column_name in schema.names:
                expected_type = column_info.arrow_type()
                actual_type = batch.schema.field(column_name).type
                assert (
                    actual_type == expected_type
                ), f"Column `{column_name}` type mismatch: {actual_type} != {expected_type}"

        return batch.num_rows

    def _execute_query_with_retry(
        self,
        query: str,
        parameters: Dict[str, Any] = None,
        arrow_format_settings: ydb.ArrowFormatSettings = None,
        schema_inclusion_mode: ydb.QuerySchemaInclusionMode = None,
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
                    schema_inclusion_mode=schema_inclusion_mode,
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

    def get_workload_thread_funcs(self):
        """Return list of workload thread functions to be executed concurrently"""

        return [
            self._loop_select,
            self._loop_select_all_with_validation,
            self._loop_modify_operations,
        ]
