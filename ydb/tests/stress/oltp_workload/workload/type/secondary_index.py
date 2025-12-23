import threading
import random
import time
import logging
from typing import Dict, List, Tuple, Any, Optional, Union
from enum import Enum
from dataclasses import dataclass, field
import ydb
from ydb.tests.stress.common.common import WorkloadBase

# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class WorkloadConfig:
    """Configuration constants for the secondary index workload"""

    # Table structure constants
    columns: int = 8
    primary_key_max_columns: int = 2
    secondary_key_max_columns: int = 2
    cover_max_columns: int = 2
    allowed_secondary_indexes_count: List[int] = field(default_factory=lambda: [1, 2, 3, 8, 16])
    unique_indexes_allowed: bool = True
    unique_index_probability: float = 0.05
    null_probability: float = 0.05

    # Column types
    allowed_column_types: List[str] = field(
        default_factory=lambda: ["Uint32", "Uint64", "Int32", "Int64", "Uint8", "Bool", "Int8", "String", "Utf8"]
    )

    # Data generation constants
    max_primary_key_value: int = 100  # update the same rows
    max_value: int = 100000
    max_operations: int = 4
    max_rows_per_operation: int = 4
    inserts_allowed: bool = True

    # Workload execution constants
    tables_inflight: int = 32
    jobs_per_table: int = 1
    operations_per_job: int = 128
    check_operations_period: int = 1

    # Retry configuration
    max_retries: int = 3
    retry_delay: float = 0.5  # seconds


class OperationType(Enum):
    """Enumeration of supported database operations"""

    INSERT = 0
    REPLACE = 1
    UPSERT = 2
    UPDATE = 3
    DELETE = 4
    UPDATE_ON = 5
    DELETE_ON = 6


class YdbTypeError(Exception):
    """Exception raised for unsupported YDB types"""

    pass


class WorkloadError(Exception):
    """Base exception for workload-related errors"""

    pass


class TableVerificationError(WorkloadError):
    """Exception raised during table verification"""

    pass


@dataclass
class IndexInfo:
    """Index information"""

    unique: bool
    columns: List[int]
    cover: List[int] = field(default_factory=list)


@dataclass
class TableInfo:
    """Table information"""

    primary_key_size: int
    indexes: List[IndexInfo]
    column_types: List[str]


class TypeConverter:
    """Handles conversion between Python types and YDB types"""

    # Mapping from YDB type strings to YDB primitive types
    TYPE_MAPPING = {
        "Uint8": ydb.PrimitiveType.Uint8,
        "Uint32": ydb.PrimitiveType.Uint32,
        "Uint64": ydb.PrimitiveType.Uint64,
        "Int8": ydb.PrimitiveType.Int8,
        "Int32": ydb.PrimitiveType.Int32,
        "Int64": ydb.PrimitiveType.Int64,
        "Bool": ydb.PrimitiveType.Bool,
        "Utf8": ydb.PrimitiveType.Utf8,
        "String": ydb.PrimitiveType.String,
    }

    @classmethod
    def get_ydb_type(cls, type_str: str) -> ydb.PrimitiveType:
        """Get YDB primitive type from type string"""
        if type_str not in cls.TYPE_MAPPING:
            raise YdbTypeError(f"Unknown type: {type_str}")
        return cls.TYPE_MAPPING[type_str]

    @classmethod
    def convert_value(cls, value: Any, type_str: str) -> Any:
        """Convert a Python value to the appropriate YDB format"""
        if value is None:
            return None

        if type_str in ["Uint8", "Uint32", "Uint64", "Int8", "Int32", "Int64"]:
            return int(value)
        elif type_str == "Bool":
            return bool(value)
        elif type_str in ["Utf8", "String"]:
            if isinstance(value, str):
                return value.encode()
            return value
        else:
            raise YdbTypeError(f"Unknown type: {type_str}")

    @classmethod
    def create_optional_type(cls, type_str: str) -> ydb.OptionalType:
        """Create an optional YDB type from type string"""
        return ydb.OptionalType(cls.get_ydb_type(type_str))

    @classmethod
    def create_struct_type(cls, column_types: List[str], field_names: List[str]) -> ydb.StructType:
        """Create a YDB struct type from column types and field names"""
        struct_type = ydb.StructType()
        for field_name, col_type in zip(field_names, column_types):
            struct_type.add_member(field_name, cls.create_optional_type(col_type))
        return struct_type


class WorkloadStats:
    """Thread-safe statistics tracking for the workload"""

    def __init__(self):
        self.operations = 0
        self.precondition_failed = 0
        self.tables = 0
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

    def get_stats(self) -> str:
        """Get current statistics as a formatted string"""
        with self._lock:
            return (
                f"Tables: {self.tables}, Operations: {self.operations}, PreconditionFailed: {self.precondition_failed}"
            )


class ParameterBuilder:
    """Helper class for building YDB query parameters"""

    @staticmethod
    def create_list_parameter(
        operation_id: int, rows: List[List[Any]], column_types: List[str], prefix: str = ""
    ) -> Tuple[List[str], Dict[str, Any]]:
        """
        Create a list parameter for batch operations

        Args:
            operation_id: ID of the operation
            rows: List of rows, each row is a list of values
            column_types: List of column types
            prefix: Optional prefix for parameter names

        Returns:
            Tuple of (parameter_declarations, parameters_dict)
        """
        param_declarations = []
        parameters = {}

        # Create a single parameter that is a List of Structs
        param_name = f"$p{operation_id}_{prefix}rows"

        # Build the struct type definition with optional types
        struct_fields = [f"c{i}: Optional<{col_type}>" for i, col_type in enumerate(column_types)]
        param_declarations.append(f"DECLARE {param_name} AS List<Struct<{', '.join(struct_fields)}>>;")

        # Convert rows to list of dicts for the parameter
        rows_list = []
        for row in rows:
            row_dict = {}
            for i, value in enumerate(row):
                if value is not None:
                    row_dict[f"c{i}"] = TypeConverter.convert_value(value, column_types[i])
                else:
                    row_dict[f"c{i}"] = None
            rows_list.append(row_dict)

        # Create the parameter with the list of rows and its type
        field_names = [f"c{i}" for i in range(len(column_types))]
        struct_type = TypeConverter.create_struct_type(column_types, field_names)
        list_type = ydb.ListType(struct_type)

        parameters[param_name] = ydb.TypedValue(rows_list, list_type)

        return param_declarations, parameters

    @staticmethod
    def create_struct_parameter(
        operation_id: int, values: List[Any], column_types: List[str], param_name_suffix: str
    ) -> Tuple[List[str], Dict[str, Any]]:
        """
        Create a struct parameter for single row operations

        Args:
            operation_id: ID of the operation
            values: List of values
            column_types: List of column types
            param_name_suffix: Suffix for the parameter name

        Returns:
            Tuple of (parameter_declarations, parameters_dict)
        """
        param_name = f"$p{operation_id}_{param_name_suffix}"

        # Build the struct type definition with optional types
        struct_fields = [f"{param_name_suffix}_{i}: Optional<{col_type}>" for i, col_type in enumerate(column_types)]
        param_declarations = [f"DECLARE {param_name} AS Struct<{', '.join(struct_fields)}>;"]

        # Create parameter values
        param_value = {}
        for i, value in enumerate(values):
            if value is not None:
                param_value[f"{param_name_suffix}_{i}"] = TypeConverter.convert_value(value, column_types[i])
            else:
                param_value[f"{param_name_suffix}_{i}"] = None

        # Create the parameter with its type
        field_names = [f"{param_name_suffix}_{i}" for i in range(len(column_types))]
        struct_type = TypeConverter.create_struct_type(column_types, field_names)

        parameters = {param_name: ydb.TypedValue(param_value, struct_type)}

        return param_declarations, parameters


class SqlBuilder:
    """Helper class for building SQL queries"""

    @staticmethod
    def build_index_description(index_id: int, index_info: IndexInfo, column_names: List[str]) -> str:
        """Build the SQL description for a secondary index"""
        parts = [f"INDEX idx{index_id} GLOBAL"]

        if index_info.unique:
            parts.append("UNIQUE")

        parts.append("SYNC ON")
        parts.append(f"({','.join(column_names[col] for col in index_info.columns)})")

        if index_info.cover:
            parts.append(f"COVER ({','.join(column_names[col] for col in index_info.cover)})")

        return " ".join(parts)

    @staticmethod
    def build_where_clause(column_names: List[str], values: List[Any], param_name: str, field_name: str) -> List[str]:
        """Build a WHERE clause with parameters"""
        where_clause = []
        for i, (col, value) in enumerate(zip(column_names, values)):
            if value is None:
                where_clause.append(f"{col} IS NULL")
            else:
                where_clause.append(f"{col} = {param_name}.{field_name}_{i}")
        return where_clause

    @staticmethod
    def build_set_clause(column_names: List[str], param_name: str, field_name: str) -> List[str]:
        """Build a SET clause with parameters"""
        return [f"{col} = {param_name}.{field_name}_{i}" for i, col in enumerate(column_names)]


class WorkloadSecondaryIndex(WorkloadBase):
    """
    Workload for testing secondary indexes in YDB.

    This workload generates tables with various secondary index configurations
    and performs different operations to test index functionality.
    """

    def __init__(self, client, prefix, stop, config: Optional[WorkloadConfig] = None):
        super().__init__(client, prefix, "secondary_index", stop)

        # Configuration
        self.config = config or WorkloadConfig()

        # Statistics tracking
        self.stats = WorkloadStats()

        # Column names cache
        self._column_names = [f"c{i}" for i in range(self.config.columns)]

        # Type converter
        self._type_converter = TypeConverter()

        # SQL builder
        self._sql_builder = SqlBuilder()

        # Parameter builder
        self._param_builder = ParameterBuilder()

    def get_stat(self) -> str:
        """Get workload statistics"""
        return self.stats.get_stats()

    def generate_table(self) -> TableInfo:
        """Generate a table with different types of secondary indexes"""
        primary_key_size = random.randint(1, self.config.primary_key_max_columns)
        indexes = []

        # Generate random column types
        column_types = [random.choice(self.config.allowed_column_types) for _ in range(self.config.columns)]

        # Generate secondary indexes
        for _ in range(random.choice(self.config.allowed_secondary_indexes_count)):
            columns = self._generate_index_columns(primary_key_size)
            cover = self._generate_cover_columns(primary_key_size, columns)

            indexes.append(
                IndexInfo(
                    unique=(
                        (random.random() < self.config.unique_index_probability)
                        if self.config.unique_indexes_allowed
                        else False
                    ),
                    columns=columns,
                    cover=cover,
                )
            )

        return TableInfo(primary_key_size, indexes, column_types)

    def _generate_index_columns(self, primary_key_size: int) -> List[int]:
        """Generate columns for a secondary index"""
        columns = []

        # Ensure we don't create an index that exactly matches the primary key
        while len(columns) == 0 or (
            primary_key_size == len(columns) and all(col < primary_key_size for col in columns)
        ):
            columns = random.sample(
                range(self.config.columns), random.randint(1, self.config.secondary_key_max_columns)
            )

        return columns

    def _generate_cover_columns(self, primary_key_size: int, index_columns: List[int]) -> List[int]:
        """Generate cover columns for a secondary index"""
        if not random.choice([True, False]):
            return []

        available_columns = [x for x in range(self.config.columns) if x >= primary_key_size and x not in index_columns]

        if not available_columns:
            return []

        return random.sample(
            available_columns, random.randint(0, min(self.config.cover_max_columns, len(available_columns)))
        )

    def create_table(self, table_name: str, table_info: TableInfo) -> None:
        """Create a single table with its secondary indexes"""
        table_path = self.get_table_path(table_name)

        # Generate column definitions with random types
        columns = [f"{col} {table_info.column_types[i]}" for i, col in enumerate(self._column_names)]

        # Generate primary key columns
        pk_columns = [self._column_names[i] for i in range(table_info.primary_key_size)]

        # Generate index definitions
        indexes = []
        for i, index_info in enumerate(table_info.indexes):
            idx_desc = self._sql_builder.build_index_description(i, index_info, self._column_names)
            indexes.append(idx_desc)

        create_sql = f"""
            CREATE TABLE `{table_path}` (
                {', '.join(columns)},
                PRIMARY KEY ({', '.join(pk_columns)}),
                {', '.join(indexes)}
            );
        """

        logger.debug(f"Creating table with SQL: {create_sql}")
        self._execute_query_with_retry(create_sql, is_ddl=True)

    def drop_table(self, table_name: str) -> None:
        """Drop a table with its secondary indexes"""
        table_path = self.get_table_path(table_name)

        drop_sql = f"DROP TABLE `{table_path}`"

        self._execute_query_with_retry(drop_sql, is_ddl=True)

    def run_job(self, table: str, table_info: TableInfo, job_key: int) -> None:
        """Perform various operations on tables with secondary indexes"""
        logger.info(f"Starting job {job_key} for table {table}")

        operations = 0

        while operations < self.config.operations_per_job and not self.is_stop_requested():
            try:
                self._run_operations(table, table_info)

                self.stats.increment_operations()
                operations += 1

                if operations % self.config.check_operations_period == 0:
                    logger.info(f"Job {job_key} for {table} completed {operations} operations. Run verify.")
                    self.verify_table(table, table_info)
            except ydb.issues.PreconditionFailed as e:
                self.stats.increment_precondition_failed()
                logger.info(f"Job {job_key} for {table} operation failed with PreconditionFailed: {e}")
            except Exception as e:
                logger.error(f"Job {job_key} for {table} encountered unexpected error: {e}")
                raise

    def run_for_table(self, table_name: str) -> None:
        """Run the workload for a single table"""
        table_info = self.generate_table()
        self.create_table(table_name, table_info)

        threads = []
        for i in range(self.config.jobs_per_table):
            thread = threading.Thread(
                target=self.run_job, args=(table_name, table_info, i), name=f"{table_name} job:{i}"
            )
            threads.append(thread)

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        self.stats.increment_tables()

        self.verify_table(table_name, table_info)
        self.drop_table(table_name)

        print(f"Workload for table {table_name} completed.")

    def _get_batch(self, pk_size: int, pk_only: bool, table_info: TableInfo = None) -> List[List[Any]]:
        """Generate a batch of rows for operations"""
        batch_rows = []
        rows_count = random.randint(1, self.config.max_rows_per_operation)

        for _ in range(rows_count):
            row = []
            for i in range(self.config.columns if not pk_only else pk_size):
                row.append(self._generate_value_by_type(table_info.column_types[i], i < pk_size))
            batch_rows.append(row)

        return batch_rows

    def _generate_value_by_type(self, column_type: str, is_pk: bool) -> Union[int, bool, str, None]:
        """Generate a value based on column type"""
        if random.random() < self.config.null_probability:
            return None
        elif column_type in ["Uint8", "Uint32", "Uint64", "Int8", "Int32", "Int64"]:
            return random.randint(1, self.config.max_primary_key_value if is_pk else self.config.max_value)
        elif column_type == "Bool":
            return random.choice([True, False])
        elif column_type in ["Utf8", "String"]:
            # Generate a random string of lowercase latin letters
            length = random.randint(1, 100)
            return ''.join(random.choice([chr(i) for i in range(ord('a'), ord('z'))]) for _ in range(length))
        else:
            raise YdbTypeError(f"Unknown type {column_type}")

    def _run_operations(self, table: str, table_info: TableInfo) -> None:
        """Execute a batch of operations on the specified table"""
        operations_count = random.randint(1, self.config.max_operations)
        declare_statements = []
        operation_queries = []
        all_parameters = {}

        # Get available operation types based on configuration
        available_operations = list(OperationType)
        if not self.config.inserts_allowed:
            available_operations = [op for op in available_operations if op != OperationType.INSERT]

        for operation_id in range(operations_count):
            operation_type = random.choice(available_operations)

            # Use a dispatch dictionary to avoid repetitive if-elif chains
            operation_handlers = {
                OperationType.INSERT: self._insert_operation,
                OperationType.REPLACE: self._replace_operation,
                OperationType.UPSERT: self._upsert_operation,
                OperationType.UPDATE: self._update_operation,
                OperationType.DELETE: self._delete_operation,
                OperationType.UPDATE_ON: self._update_on_operation,
                OperationType.DELETE_ON: self._delete_on_operation,
            }

            handler = operation_handlers.get(operation_type)
            if handler:
                declare, query, parameters = handler(operation_id, table, table_info)
                declare_statements.extend(declare)
                operation_queries.append(query)
                all_parameters.update(parameters)

        # Combine all DECLARE statements at the beginning, followed by all operations
        query = '\n'.join(declare_statements + operation_queries)
        logger.debug("Executing operations")
        self._execute_query_with_retry(query, is_ddl=False, parameters=all_parameters)

    def _execute_query_with_retry(self, query: str, is_ddl: bool, parameters: Dict[str, Any] = None) -> Any:
        """Execute a query with retry logic for handling transient failures"""
        last_exception = None

        for attempt in range(self.config.max_retries):
            try:
                if parameters:
                    logger.debug(f"Query: {query} Params: {parameters}")
                    return self.client.query(query, is_ddl, parameters, log_error=False)
                else:
                    return self.client.query(query, is_ddl, log_error=False)
            except (ydb.issues.Aborted, ydb.issues.Unavailable, ydb.issues.Undetermined) as e:
                last_exception = e
                if attempt < self.config.max_retries - 1:
                    logger.warning(f"Query failed (attempt {attempt + 1}/{self.config.max_retries}): {e}")
                    time.sleep(self.config.retry_delay * (2**attempt))  # Exponential backoff
                else:
                    logger.error(f"Query failed after {self.config.max_retries} attempts: {e}")
            except Exception:
                # Don't retry on non-transient errors
                raise

        # If we get here, all retries failed
        raise last_exception

    def _insert_operation(self, operation_id: int, table: str, table_info: TableInfo) -> Tuple[str, Dict[str, Any]]:
        """Generate INSERT operation SQL with parameters"""
        table_path = self.get_table_path(table)
        batch_rows = self._get_batch(table_info.primary_key_size, False, table_info)

        # Create parameter declarations and parameters
        param_declarations, parameters = self._param_builder.create_list_parameter(
            operation_id, batch_rows, table_info.column_types
        )

        query = f"""
            INSERT INTO `{table_path}`
            SELECT * FROM AS_TABLE($p{operation_id}_rows);
        """

        return param_declarations, query, parameters

    def _upsert_operation(self, operation_id: int, table: str, table_info: TableInfo) -> Tuple[str, Dict[str, Any]]:
        """Generate UPSERT operation SQL with parameters"""
        table_path = self.get_table_path(table)
        batch_rows = self._get_batch(table_info.primary_key_size, False, table_info)

        # Create parameter declarations and parameters
        param_declarations, parameters = self._param_builder.create_list_parameter(
            operation_id, batch_rows, table_info.column_types
        )

        query = f"""
            UPSERT INTO `{table_path}`
            SELECT * FROM AS_TABLE($p{operation_id}_rows);
        """

        return param_declarations, query, parameters

    def _replace_operation(self, operation_id: int, table: str, table_info: TableInfo) -> Tuple[str, Dict[str, Any]]:
        """Generate REPLACE operation SQL with parameters"""
        table_path = self.get_table_path(table)
        batch_rows = self._get_batch(table_info.primary_key_size, False, table_info)

        # Create parameter declarations and parameters
        param_declarations, parameters = self._param_builder.create_list_parameter(
            operation_id, batch_rows, table_info.column_types
        )

        query = f"""
            REPLACE INTO `{table_path}`
            SELECT * FROM AS_TABLE($p{operation_id}_rows);
        """

        return param_declarations, query, parameters

    def _update_operation(self, operation_id: int, table: str, table_info: TableInfo) -> Tuple[str, Dict[str, Any]]:
        """Generate UPDATE operation SQL with parameters"""
        table_path = self.get_table_path(table)
        batch = self._get_batch(table_info.primary_key_size, False, table_info)
        pk_size = table_info.primary_key_size

        # Generate primary key values
        pk_values = batch[0][:pk_size]
        pk_types = table_info.column_types[:pk_size]

        # Generate update values for non-primary key columns
        update_columns = self._column_names[pk_size:]
        update_values = batch[0][pk_size:]
        update_types = table_info.column_types[pk_size:]

        # Create parameters for SET and WHERE clauses
        set_param_decls, set_params = self._param_builder.create_struct_parameter(
            operation_id, update_values, update_types, "set_values"
        )

        where_param_decls, where_params = self._param_builder.create_struct_parameter(
            operation_id, pk_values, pk_types, "where_values"
        )

        # Combine all parameter declarations and parameters
        param_declarations = set_param_decls + where_param_decls
        parameters = {**set_params, **where_params}

        # Generate SET clause with parameters
        set_clause = self._sql_builder.build_set_clause(update_columns, f"$p{operation_id}_set_values", "set_values")

        # Generate WHERE clause for primary key with parameters
        where_clause = self._sql_builder.build_where_clause(
            self._column_names[:pk_size], pk_values, f"$p{operation_id}_where_values", "where_values"
        )

        query = f"""
            UPDATE `{table_path}`
            SET {', '.join(set_clause)}
            WHERE {' AND '.join(where_clause)};
        """

        return param_declarations, query, parameters

    def _delete_operation(self, operation_id: int, table: str, table_info: TableInfo) -> Tuple[str, Dict[str, Any]]:
        """Generate DELETE operation SQL with parameters"""
        table_path = self.get_table_path(table)
        pk_size = table_info.primary_key_size
        pk_values = self._get_batch(pk_size, True, table_info)[0]
        pk_types = table_info.column_types[:pk_size]

        # Create parameter for WHERE clause
        where_param_decls, where_params = self._param_builder.create_struct_parameter(
            operation_id, pk_values, pk_types, "where_values"
        )

        # Generate WHERE clause for primary key with parameters
        where_clause = self._sql_builder.build_where_clause(
            self._column_names[:pk_size], pk_values, f"$p{operation_id}_where_values", "where_values"
        )

        query = f"""
            DELETE FROM `{table_path}`
            WHERE {' AND '.join(where_clause)};
        """

        return where_param_decls, query, where_params

    def _update_on_operation(self, operation_id: int, table: str, table_info: TableInfo) -> Tuple[str, Dict[str, Any]]:
        """Generate UPDATE ON operation SQL with parameters"""
        table_path = self.get_table_path(table)
        batch_rows = self._get_batch(table_info.primary_key_size, False, table_info)

        param_declarations, parameters = self._param_builder.create_list_parameter(
            operation_id, batch_rows, table_info.column_types
        )

        query = f"""
            UPDATE `{table_path}` ON
            SELECT * FROM AS_TABLE($p{operation_id}_rows);
        """

        return param_declarations, query, parameters

    def _delete_on_operation(self, operation_id: int, table: str, table_info: TableInfo) -> Tuple[str, Dict[str, Any]]:
        """Generate DELETE ON operation SQL with parameters"""
        table_path = self.get_table_path(table)
        pk_size = table_info.primary_key_size
        batch_rows = self._get_batch(pk_size, True, table_info)

        pk_column_types = table_info.column_types[:pk_size]
        pk_batch_rows = [[row[i] for i in range(pk_size)] for row in batch_rows]

        param_declarations, parameters = self._param_builder.create_list_parameter(
            operation_id, pk_batch_rows, pk_column_types
        )

        query = f"""
            DELETE FROM `{table_path}` ON
            SELECT * FROM AS_TABLE($p{operation_id}_rows);
        """

        return param_declarations, query, parameters

    def verify_table(self, table_name: str, table_info: TableInfo) -> None:
        """Verify data consistency between main table and its index tables"""
        table_path = self.get_table_path(table_name)
        logger.info(f"Verifying table {table_path} and its index tables...")

        indexes = table_info.indexes

        # Build query to fetch data from main table and all index tables
        queries = [f"SELECT * FROM `{table_path}`;"]

        for index, _ in enumerate(indexes):
            index_table_path = f"{table_path}/idx{index}/indexImplTable"
            queries.append(f"SELECT * FROM `{index_table_path}`;")

        # Execute all queries in one batch
        combined_query = '\n'.join(queries)
        result = self._execute_query_with_retry(combined_query, is_ddl=False)

        # Process main table data
        main_data = self._process_main_table_data(result[0].rows, table_info.primary_key_size)
        logger.info(f"Main table {table_name} has {len(main_data)} rows")

        # Verify each index table
        for index, index_desc in enumerate(indexes):
            self._verify_index_table(
                result[index + 1].rows, main_data, index_desc, index, table_name, table_info.primary_key_size
            )

        logger.info(f"All index tables for {table_name} are consistent with the main table")

    def _process_main_table_data(self, rows: List[Any], pk_size: int) -> Dict[Tuple, Any]:
        """Process main table rows into a dictionary keyed by primary key"""
        main_data = {}

        for row in rows:
            key = tuple(row[f'c{col}'] for col in range(pk_size))
            main_data[key] = row

        return main_data

    def _verify_index_table(
        self,
        index_rows: List[Any],
        main_data: Dict[Tuple, Any],
        index_desc: IndexInfo,
        index_id: int,
        table_name: str,
        pk_size: int,
    ) -> None:
        """Verify a single index table against the main table"""
        # Process index table data
        index_data = {}
        for row in index_rows:
            key = tuple(row[f'c{col}'] for col in range(pk_size))
            index_data[key] = row

        try:
            logger.info(f"Index idx{index_id} has {len(index_data)} rows")

            # Check data size consistency
            if len(main_data) != len(index_data):
                raise TableVerificationError(
                    f"Data size mismatch between main table {table_name} and index {index_id}: "
                    f"main={len(main_data)}, index={len(index_data)}"
                )

            # Check data consistency
            for key, main_row in main_data.items():
                if key not in index_data:
                    raise TableVerificationError(f"Key {key} not found in index {index_id} for table {table_name}")

                index_row = index_data[key]
                for column in index_row.keys():
                    if main_row[column] != index_row[column]:
                        raise TableVerificationError(
                            f"Data mismatch between main table {table_name} and index {index_id} "
                            f"for key {key} and column {column}: "
                            f"main={main_row[column]}, index={index_row[column]}"
                        )

            # Check uniqueness constraint if applicable
            if index_desc.unique:
                self._verify_index_uniqueness(index_rows, index_desc, index_id, table_name)
        except TableVerificationError:
            logger.error(f"Main: {main_data}")
            logger.error(f"Index: {index_data}")
            raise

    def _verify_index_uniqueness(
        self, index_rows: List[Any], index_desc: IndexInfo, index_id: int, table_name: str
    ) -> None:
        """Verify uniqueness constraint for an index"""
        index_keys = set()

        for row in index_rows:
            key = tuple(row[f'c{col}'] for col in index_desc.columns)
            # NULL != NULL for secondary index
            if None not in key and key in index_keys:
                raise TableVerificationError(f"Duplicate key {key} found in index {index_id} for table {table_name}")
            index_keys.add(key)

    def _loop(self):
        """Main loop for the workload"""
        iteration = 0
        while not self.is_stop_requested():
            threads = []
            for table_name in [f'test_{iteration}_{i}' for i in range(self.config.tables_inflight)]:
                threads.append(
                    threading.Thread(target=lambda t=table_name: self.run_for_table(t), name=f'{table_name}')
                )

            for thread in threads:
                thread.start()

            for thread in threads:
                thread.join()

            print(f"Iteration {iteration} finished.")
            print(self.stats.get_stats())
            iteration += 1

    def get_workload_thread_funcs(self):
        """Get the thread functions for the workload"""
        return [self._loop]
