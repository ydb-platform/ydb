import threading
import random
import time
import logging
from typing import Dict, List, Tuple, Any, Optional, Callable, Union
from enum import Enum
from contextlib import contextmanager
import ydb
from ydb.tests.stress.common.common import WorkloadBase

# Configure logging
logger = logging.getLogger(__name__)


class WorkloadConfig:
    """Configuration constants for the secondary index workload"""
    # Table structure constants
    COLUMNS = 8
    PRIMARY_KEY_MAX_COLUMNS = 2
    SECONDARY_KEY_MAX_COLUMNS = 2
    COVER_MAX_COLUMNS = 2
    ALLOWED_SECONDARY_INDEXES_COUNT = [1, 2, 3, 8, 16]
    UNIQUE_INDEXES_ALLOWED = True
    UNIQUE_INDEX_PROBALITY = 0.1
    NULL_PROBABILITY = 0.05
    
    # Column types
    ALLOWED_COLUMN_TYPES = ["Uint32", "Uint64", "Int32", "Int64", "Uint8", "Bool", "Int8", "String", "Utf8"]
    
    # Data generation constants
    MAX_PRIMARY_KEY_VALUE = 100  # update the same rows
    MAX_VALUE = 100000
    MAX_OPERATIONS = 4
    MAX_ROWS_PER_OPERATION = 4
    INSERTS_ALLOWED = True
    
    # Workload execution constants
    TABLES_INFLIGHT = 32
    JOBS_PER_TABLE = 1
    OPERATIONS_PER_JOB = 128
    CHECK_OPERATIONS_PERIOD = 1
    
    # Retry configuration
    MAX_RETRIES = 3
    RETRY_DELAY = 0.5  # seconds


class OperationType(Enum):
    """Enumeration of supported database operations"""
    INSERT = 0
    REPLACE = 1
    UPSERT = 2
    UPDATE = 3
    DELETE = 4
    UPDATE_ON = 5
    DELETE_ON = 6


class IndexInfo:
    """Index information"""
    def __init__(self, unique: bool, columns: List[int], cover: Optional[List[int]] = None):
        self.unique = unique
        self.columns = columns
        self.cover = cover if cover is not None else []


class TableInfo:
    """Table information"""
    def __init__(self, primary_key_size: int, indexes: List[IndexInfo], column_types: List[str] = None):
        self.primary_key_size = primary_key_size
        self.indexes = indexes
        self.column_types = column_types


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
            return f"Tables: {self.tables}, Operations: {self.operations}, PreconditionFailed: {self.precondition_failed}"


class WorkloadSecondaryIndex(WorkloadBase):
    """
    Workload for testing secondary indexes in YDB.
    
    This workload generates tables with various secondary index configurations
    and performs different operations to test index functionality.
    """
    
    def __init__(self, client, prefix, stop):
        super().__init__(client, prefix, "secondary_index", stop)
        
        # Statistics tracking
        self.stats = WorkloadStats()
        
        # Column names cache
        self._column_names = [f"c{i}" for i in range(WorkloadConfig.COLUMNS)]
    
    def get_stat(self) -> str:
        """Get workload statistics"""
        return self.stats.get_stats()
    
    def generate_table(self) -> TableInfo:
        """Generate a table with different types of secondary indexes"""
        primary_key_size = random.randint(1, WorkloadConfig.PRIMARY_KEY_MAX_COLUMNS)
        indexes = []
        
        # Generate random column types
        column_types = [random.choice(WorkloadConfig.ALLOWED_COLUMN_TYPES) for _ in range(WorkloadConfig.COLUMNS)]
        
        # Generate secondary indexes
        for _ in range(random.choice(WorkloadConfig.ALLOWED_SECONDARY_INDEXES_COUNT)):
            columns = self._generate_index_columns(primary_key_size)
            cover = self._generate_cover_columns(primary_key_size, columns)
            
            indexes.append(IndexInfo(
                unique=(random.randint(0, 1000000) < WorkloadConfig.UNIQUE_INDEX_PROBALITY * 1000000) if WorkloadConfig.UNIQUE_INDEXES_ALLOWED else False,
                columns=columns,
                cover=cover
            ))
        
        return TableInfo(primary_key_size, indexes, column_types)
    
    def _generate_index_columns(self, primary_key_size: int) -> List[int]:
        """Generate columns for a secondary index"""
        columns = []
        
        # Ensure we don't create an index that exactly matches the primary key
        while (len(columns) == 0 or
               (primary_key_size == len(columns) and
                all(col < primary_key_size for col in columns))):
            columns = random.sample(
                range(WorkloadConfig.COLUMNS),
                random.randint(1, WorkloadConfig.SECONDARY_KEY_MAX_COLUMNS)
            )
        
        return columns
    
    def _generate_cover_columns(self, primary_key_size: int, index_columns: List[int]) -> List[int]:
        """Generate cover columns for a secondary index"""
        if not random.choice([True, False]):
            return []
        
        available_columns = [
            x for x in range(WorkloadConfig.COLUMNS) 
            if x >= primary_key_size and x not in index_columns
        ]
        
        if not available_columns:
            return []
        
        return random.sample(
            available_columns, 
            random.randint(0, min(WorkloadConfig.COVER_MAX_COLUMNS, len(available_columns)))
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
            idx_desc = self._build_index_description(i, index_info)
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
    
    def _build_index_description(self, index_id: int, index_info: IndexInfo) -> str:
        """Build the SQL description for a secondary index"""
        idx_desc = f"INDEX idx{index_id} GLOBAL "
        
        if index_info.unique:
            idx_desc += "UNIQUE "
        
        idx_desc += "SYNC ON ("
        idx_desc += ','.join(self._column_names[col] for col in index_info.columns)
        idx_desc += ") "
        
        if index_info.cover:
            idx_desc += "COVER ("
            idx_desc += ','.join(self._column_names[col] for col in index_info.cover)
            idx_desc += ") "
        
        return idx_desc
    
    def run_job(self, table: str, table_info: TableInfo, job_key: int) -> None:
        """Perform various operations on tables with secondary indexes"""
        logger.info(f"Starting job {job_key} for table {table}")

        operations = 0
        
        while operations < WorkloadConfig.OPERATIONS_PER_JOB and not self.is_stop_requested():
            try:
                self._run_operations(table, table_info)
                
                self.stats.increment_operations()
                operations += 1

                if operations % WorkloadConfig.CHECK_OPERATIONS_PERIOD == 0:
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
        for i in range(WorkloadConfig.JOBS_PER_TABLE):
            thread = threading.Thread(
                target=self.run_job,
                args=(table_name, table_info, i),
                name=f"{table_name} job:{i}"
            )
            threads.append(thread)

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        self.stats.increment_tables()

        self.verify_table(table_name, table_info)
        self.drop_table(table_name)

    def _get_batch(self, pk_size: int, pk_only: bool, table_info: TableInfo = None) -> List[List[Any]]:
        """Generate a batch of rows for operations"""
        batch_rows = []
        rows_count = random.randint(1, WorkloadConfig.MAX_ROWS_PER_OPERATION)
        
        for _ in range(rows_count):
            row = []
            for i in range(WorkloadConfig.COLUMNS if not pk_only else pk_size):
                row.append(self._generate_value_by_type(table_info.column_types[i], i < pk_size))
            batch_rows.append(row)
        
        return batch_rows
    
    def _generate_value_by_type(self, column_type: str, is_pk: bool) -> Union[int, bool, str, None]:
        """Generate a value based on column type"""
        if random.random() < WorkloadConfig.NULL_PROBABILITY:
            return None
        elif column_type in ["Uint8", "Uint32", "Uint64", "Int8", "Int32", "Int64"]:
            return random.randint(1, WorkloadConfig.MAX_PRIMARY_KEY_VALUE if is_pk else WorkloadConfig.MAX_VALUE)
        elif column_type == "Bool":
            return random.choice([True, False])
        elif column_type in ["Utf8", "String"]:
            # Generate a random string of lowercase latin letters
            length = random.randint(1, 100)
            return ''.join(random.choice([chr(i) for i in range(ord('a'), ord('z'))]) for _ in range(length))
        else:
            raise Exception(f"Unknown type {column_type}")
    
    def _run_operations(self, table: str, table_info: TableInfo) -> None:
        """Execute a batch of operations on the specified table"""
        operations_count = random.randint(1, WorkloadConfig.MAX_OPERATIONS)
        declare_statements = []
        operation_queries = []
        all_parameters = {}
        
        for operation_id in range(operations_count):
            operation_type = random.choice(
                list(
                    filter(
                        lambda x: True if WorkloadConfig.INSERTS_ALLOWED else x != OperationType.INSERT,
                        OperationType)))
            
            if operation_type == OperationType.INSERT:
                declare, query, parameters = self._insert_operation(operation_id, table, table_info)
                declare_statements.extend(declare)
                operation_queries.append(query)
                all_parameters.update(parameters)
            elif operation_type == OperationType.REPLACE:
                declare, query, parameters = self._replace_operation(operation_id, table, table_info)
                declare_statements.extend(declare)
                operation_queries.append(query)
                all_parameters.update(parameters)
            elif operation_type == OperationType.UPSERT:
                declare, query, parameters = self._upsert_operation(operation_id, table, table_info)
                declare_statements.extend(declare)
                operation_queries.append(query)
                all_parameters.update(parameters)
            elif operation_type == OperationType.UPDATE:
                declare, query, parameters = self._update_operation(operation_id, table, table_info)
                declare_statements.extend(declare)
                operation_queries.append(query)
                all_parameters.update(parameters)
            elif operation_type == OperationType.DELETE:
                declare, query, parameters = self._delete_operation(operation_id, table, table_info)
                declare_statements.extend(declare)
                operation_queries.append(query)
                all_parameters.update(parameters)
            elif operation_type == OperationType.UPDATE_ON:
                declare, query, parameters = self._update_on_operation(operation_id, table, table_info)
                declare_statements.extend(declare)
                operation_queries.append(query)
                all_parameters.update(parameters)
            elif operation_type == OperationType.DELETE_ON:
                declare, query, parameters = self._delete_on_operation(operation_id, table, table_info)
                declare_statements.extend(declare)
                operation_queries.append(query)
                all_parameters.update(parameters)
        
        # Combine all DECLARE statements at the beginning, followed by all operations
        query = '\n'.join(declare_statements + operation_queries)
        logger.debug(f"Executing operations")
        self._execute_query_with_retry(query, is_ddl=False, parameters=all_parameters)
    
    def _execute_query_with_retry(self, query: str, is_ddl: bool, parameters: Dict[str, Any] = None) -> Any:
        """Execute a query with retry logic for handling transient failures"""
        last_exception = None
        
        for attempt in range(WorkloadConfig.MAX_RETRIES):
            try:
                if parameters:
                    logger.debug(f"Query: {query} Params: {parameters}")
                    return self.client.query(query, is_ddl, parameters)
                else:
                    return self.client.query(query, is_ddl)
            except (ydb.issues.Aborted, ydb.issues.Unavailable, ydb.issues.Undetermined) as e:
                last_exception = e
                if attempt < WorkloadConfig.MAX_RETRIES - 1:
                    logger.warning(f"Query failed (attempt {attempt + 1}/{WorkloadConfig.MAX_RETRIES}): {e}")
                    time.sleep(WorkloadConfig.RETRY_DELAY * (2 ** attempt))  # Exponential backoff
                else:
                    logger.error(f"Query failed after {WorkloadConfig.MAX_RETRIES} attempts: {e}")
            except Exception as e:
                # Don't retry on non-transient errors
                raise
        
        # If we get here, all retries failed
        raise last_exception
    
    def _create_parameters(self, operation_id: int, rows: List[List[Any]], column_types: List[str],
                          prefix: str = "") -> Tuple[List[str], Dict[str, Any]]:
        """
        Create parameter declarations and parameter dictionary for the given rows and column types.
        
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
        struct_fields = []
        for i, col_type in enumerate(column_types):
            struct_fields.append(f"c{i}: Optional<{col_type}>")
        
        param_declarations.append(f"DECLARE {param_name} AS List<Struct<{', '.join(struct_fields)}>>;")
        
        # Convert rows to list of dicts for the parameter
        rows_list = []
        for row in rows:
            row_dict = {}
            for i, value in enumerate(row):
                if value is None:
                    row_dict[f"c{i}"] = None
                elif column_types[i] == "Uint8":
                    row_dict[f"c{i}"] = int(value)
                elif column_types[i] == "Uint32":
                    row_dict[f"c{i}"] = int(value)
                elif column_types[i] == "Uint64":
                    row_dict[f"c{i}"] = int(value)
                elif column_types[i] == "Int8":
                    row_dict[f"c{i}"] = int(value)
                elif column_types[i] == "Int32":
                    row_dict[f"c{i}"] = int(value)
                elif column_types[i] == "Int64":
                    row_dict[f"c{i}"] = int(value)
                elif column_types[i] == "Bool":
                    row_dict[f"c{i}"] = value
                elif column_types[i] == "Utf8":
                    row_dict[f"c{i}"] = value.encode()
                elif column_types[i] == "String":
                    row_dict[f"c{i}"] = value.encode()
            rows_list.append(row_dict)
        
        # Create the parameter with the list of rows and its type
        # Define the struct type for a row
        struct_type = ydb.StructType()
        for i, column_type in enumerate(column_types):
            if column_type == "Uint8":
                struct_type.add_member(f"c{i}", ydb.OptionalType(ydb.PrimitiveType.Uint8))
            elif column_type == "Uint32":
                struct_type.add_member(f"c{i}", ydb.OptionalType(ydb.PrimitiveType.Uint32))
            elif column_type == "Uint64":
                struct_type.add_member(f"c{i}", ydb.OptionalType(ydb.PrimitiveType.Uint64))
            elif column_type == "Int8":
                struct_type.add_member(f"c{i}", ydb.OptionalType(ydb.PrimitiveType.Int8))
            elif column_type == "Int32":
                struct_type.add_member(f"c{i}", ydb.OptionalType(ydb.PrimitiveType.Int32))
            elif column_type == "Int64":
                struct_type.add_member(f"c{i}", ydb.OptionalType(ydb.PrimitiveType.Int64))
            elif column_type == "Bool":
                struct_type.add_member(f"c{i}", ydb.OptionalType(ydb.PrimitiveType.Bool))
            elif column_type == "Utf8":
                struct_type.add_member(f"c{i}", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            elif column_type == "String":
                struct_type.add_member(f"c{i}", ydb.OptionalType(ydb.PrimitiveType.String))
        
        # Create a list type of the struct
        list_type = ydb.ListType(struct_type)
        
        # Create the parameter with the list of rows and its type
        parameters[param_name] = ydb.TypedValue(rows_list, list_type)
        
        return param_declarations, parameters
    
    def _insert_operation(self, operation_id: int, table: str, table_info: TableInfo) -> Tuple[str, Dict[str, Any]]:
        """Generate INSERT operation SQL with parameters"""
        table_path = self.get_table_path(table)
        batch_rows = self._get_batch(table_info.primary_key_size, False, table_info)
        
        # Create parameter declarations and parameters
        param_declarations, parameters = self._create_parameters(
            operation_id, batch_rows, table_info.column_types)
        
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
        param_declarations, parameters = self._create_parameters(
            operation_id, batch_rows, table_info.column_types)
        
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
        param_declarations, parameters = self._create_parameters(
            operation_id, batch_rows, table_info.column_types)
        
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
        
        # Create a single parameter for the update values
        set_param_name = f"$p{operation_id}_set_values"
        set_struct_fields = []
        for i, col_type in enumerate(table_info.column_types[pk_size:]):
            set_struct_fields.append(f"set_{i}: Optional<{col_type}>")
        
        set_param_declarations = [f"DECLARE {set_param_name} AS Struct<{', '.join(set_struct_fields)}>;"]
        
        # Create a single parameter for the where values
        where_param_name = f"$p{operation_id}_where_values"
        where_struct_fields = []
        for i, col_type in enumerate(table_info.column_types[:pk_size]):
            where_struct_fields.append(f"where_{i}: Optional<{col_type}>")
        
        where_param_declarations = [f"DECLARE {where_param_name} AS Struct<{', '.join(where_struct_fields)}>;"]
        
        # Combine all parameter declarations
        param_declarations = set_param_declarations + where_param_declarations
        
        # Create parameter values - just the values, not tuples
        set_param_value = {}
        for i, value in enumerate(update_values):
            if value is None:
                set_param_value[f"set_{i}"] = None
            elif update_types[i] == "Uint8":
                set_param_value[f"set_{i}"] = int(value)
            elif update_types[i] == "Uint32":
                set_param_value[f"set_{i}"] = int(value)
            elif update_types[i] == "Uint64":
                set_param_value[f"set_{i}"] = int(value)
            elif update_types[i] == "Int8":
                set_param_value[f"set_{i}"] = int(value)
            elif update_types[i] == "Int32":
                set_param_value[f"set_{i}"] = int(value)
            elif update_types[i] == "Int64":
                set_param_value[f"set_{i}"] = int(value)
            elif update_types[i] == "Bool":
                set_param_value[f"set_{i}"] = value
            elif update_types[i] == "Utf8":
                set_param_value[f"set_{i}"] = value.encode()
            elif update_types[i] == "String":
                set_param_value[f"set_{i}"] = value.encode()
        
        where_param_value = {}
        for i, value in enumerate(pk_values):
            if value is None:
                where_param_value[f"where_{i}"] = None
            elif pk_types[i] == "Uint8":
                where_param_value[f"where_{i}"] = int(value)
            elif pk_types[i] == "Uint32":
                where_param_value[f"where_{i}"] = int(value)
            elif pk_types[i] == "Uint64":
                where_param_value[f"where_{i}"] = int(value)
            elif pk_types[i] == "Int8":
                where_param_value[f"where_{i}"] = int(value)
            elif pk_types[i] == "Int32":
                where_param_value[f"where_{i}"] = int(value)
            elif pk_types[i] == "Int64":
                where_param_value[f"where_{i}"] = int(value)
            elif pk_types[i] == "Bool":
                where_param_value[f"where_{i}"] = value
            elif pk_types[i] == "Utf8":
                where_param_value[f"where_{i}"] = value.encode()
            elif pk_types[i] == "String":
                where_param_value[f"where_{i}"] = value.encode()
        
        # Create TypedValue objects for parameters
        # Create struct type for set values
        set_struct_type = ydb.StructType()
        for i, column_type in enumerate(table_info.column_types[pk_size:]):
            if column_type == "Uint8":
                set_struct_type.add_member(f"set_{i}", ydb.OptionalType(ydb.PrimitiveType.Uint8))
            elif column_type == "Uint32":
                set_struct_type.add_member(f"set_{i}", ydb.OptionalType(ydb.PrimitiveType.Uint32))
            elif column_type == "Uint64":
                set_struct_type.add_member(f"set_{i}", ydb.OptionalType(ydb.PrimitiveType.Uint64))
            elif column_type == "Int8":
                set_struct_type.add_member(f"set_{i}", ydb.OptionalType(ydb.PrimitiveType.Int8))
            elif column_type == "Int32":
                set_struct_type.add_member(f"set_{i}", ydb.OptionalType(ydb.PrimitiveType.Int32))
            elif column_type == "Int64":
                set_struct_type.add_member(f"set_{i}", ydb.OptionalType(ydb.PrimitiveType.Int64))
            elif column_type == "Bool":
                set_struct_type.add_member(f"set_{i}", ydb.OptionalType(ydb.PrimitiveType.Bool))
            elif column_type == "Utf8":
                set_struct_type.add_member(f"set_{i}", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            elif column_type == "String":
                set_struct_type.add_member(f"set_{i}", ydb.OptionalType(ydb.PrimitiveType.String))
        
        # Create struct type for where values
        where_struct_type = ydb.StructType()
        for i, column_type in enumerate(table_info.column_types[:pk_size]):
            if column_type == "Uint8":
                where_struct_type.add_member(f"where_{i}", ydb.OptionalType(ydb.PrimitiveType.Uint8))
            elif column_type == "Uint32":
                where_struct_type.add_member(f"where_{i}", ydb.OptionalType(ydb.PrimitiveType.Uint32))
            elif column_type == "Uint64":
                where_struct_type.add_member(f"where_{i}", ydb.OptionalType(ydb.PrimitiveType.Uint64))
            elif column_type == "Int8":
                where_struct_type.add_member(f"where_{i}", ydb.OptionalType(ydb.PrimitiveType.Int8))
            elif column_type == "Int32":
                where_struct_type.add_member(f"where_{i}", ydb.OptionalType(ydb.PrimitiveType.Int32))
            elif column_type == "Int64":
                where_struct_type.add_member(f"where_{i}", ydb.OptionalType(ydb.PrimitiveType.Int64))
            elif column_type == "Bool":
                where_struct_type.add_member(f"where_{i}", ydb.OptionalType(ydb.PrimitiveType.Bool))
            elif column_type == "Utf8":
                where_struct_type.add_member(f"where_{i}", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            elif column_type == "String":
                where_struct_type.add_member(f"where_{i}", ydb.OptionalType(ydb.PrimitiveType.String))
        
        parameters = {
            set_param_name: ydb.TypedValue(set_param_value, set_struct_type),
            where_param_name: ydb.TypedValue(where_param_value, where_struct_type)
        }
        
        # Generate SET clause with parameters
        set_clause = []
        for i, col in enumerate(update_columns):
            set_clause.append(f"{col} = {set_param_name}.set_{i}")
        
        # Generate WHERE clause for primary key with parameters
        where_clause = []
        for i in range(pk_size):
            if pk_values[i] is None:
                where_clause.append(f"{self._column_names[i]} IS NULL")
            else:
                where_clause.append(f"{self._column_names[i]} = {where_param_name}.where_{i}")
        
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
        
        # Create a single parameter for the where values
        where_param_name = f"$p{operation_id}_where_values"
        where_struct_fields = []
        for i, col_type in enumerate(table_info.column_types[:pk_size]):
            where_struct_fields.append(f"where_{i}: Optional<{col_type}>")
        
        param_declarations = [f"DECLARE {where_param_name} AS Struct<{', '.join(where_struct_fields)}>;"]
        
        # Create parameter values with explicit type specification
        where_param_value = {}
        for i, value in enumerate(pk_values):
            if value is None:
                where_param_value[f"where_{i}"] = None
            elif pk_types[i] == "Uint8":
                where_param_value[f"where_{i}"] = int(value)
            elif pk_types[i] == "Uint32":
                where_param_value[f"where_{i}"] = int(value)
            elif pk_types[i] == "Uint64":
                where_param_value[f"where_{i}"] = int(value)
            elif pk_types[i] == "Int8":
                where_param_value[f"where_{i}"] = int(value)
            elif pk_types[i] == "Int32":
                where_param_value[f"where_{i}"] = int(value)
            elif pk_types[i] == "Int64":
                where_param_value[f"where_{i}"] = int(value)
            elif pk_types[i] == "Bool":
                where_param_value[f"where_{i}"] = value
            elif pk_types[i] == "Utf8":
                where_param_value[f"where_{i}"] = value.encode()
            elif pk_types[i] == "String":
                where_param_value[f"where_{i}"] = value.encode()
        
        # Create struct type for where values
        where_struct_type = ydb.StructType()
        for i, column_type in enumerate(table_info.column_types[:pk_size]):
            if column_type == "Uint8":
                where_struct_type.add_member(f"where_{i}", ydb.OptionalType(ydb.PrimitiveType.Uint8))
            elif column_type == "Uint32":
                where_struct_type.add_member(f"where_{i}", ydb.OptionalType(ydb.PrimitiveType.Uint32))
            elif column_type == "Uint64":
                where_struct_type.add_member(f"where_{i}", ydb.OptionalType(ydb.PrimitiveType.Uint64))
            elif column_type == "Int8":
                where_struct_type.add_member(f"where_{i}", ydb.OptionalType(ydb.PrimitiveType.Int8))
            elif column_type == "Int32":
                where_struct_type.add_member(f"where_{i}", ydb.OptionalType(ydb.PrimitiveType.Int32))
            elif column_type == "Int64":
                where_struct_type.add_member(f"where_{i}", ydb.OptionalType(ydb.PrimitiveType.Int64))
            elif column_type == "Bool":
                where_struct_type.add_member(f"where_{i}", ydb.OptionalType(ydb.PrimitiveType.Bool))
            elif column_type == "Utf8":
                where_struct_type.add_member(f"where_{i}", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            elif column_type == "String":
                where_struct_type.add_member(f"where_{i}", ydb.OptionalType(ydb.PrimitiveType.String))
        
        parameters = {where_param_name: ydb.TypedValue(where_param_value, where_struct_type)}
        
        # Generate WHERE clause for primary key with parameters
        where_clause = []
        for i in range(pk_size):
            if pk_values[i] is None:
                where_clause.append(f"{self._column_names[i]} IS NULL")
            else:
                where_clause.append(f"{self._column_names[i]} = {where_param_name}.where_{i}")
        
        query = f"""
            DELETE FROM `{table_path}`
            WHERE {' AND '.join(where_clause)};
        """
        
        return param_declarations, query, parameters
    
    def _update_on_operation(self, operation_id: int, table: str, table_info: TableInfo) -> Tuple[str, Dict[str, Any]]:
        """Generate UPDATE ON operation SQL with parameters"""
        table_path = self.get_table_path(table)
        batch_rows = self._get_batch(table_info.primary_key_size, False, table_info)
        
        param_declarations, parameters = self._create_parameters(
            operation_id, batch_rows, table_info.column_types)
        
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
        
        pk_columns = self._column_names[:pk_size]
        pk_column_types = table_info.column_types[:pk_size]
        pk_batch_rows = [[row[i] for i in range(pk_size)] for row in batch_rows]

        param_declarations, parameters = self._create_parameters(
            operation_id, pk_batch_rows, pk_column_types)
        
        query = f"""
            DELETE FROM `{table_path}` ON
            SELECT * FROM AS_TABLE($p{operation_id}_rows);
        """
        
        return param_declarations, query, parameters
    
    def verify_table(self, table_name: str, table_info: TableInfo) -> None:
        """Verify data consistency between main table and its index tables"""
        table_path = self.get_table_path(table_name)
        logger.info(f"Verifying table {table_path} and its index tables...")
        
        table_info = table_info
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
                result[index + 1].rows,
                main_data,
                index_desc,
                index,
                table_name,
                table_info.primary_key_size
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
        pk_size: int
    ) -> None:
        """Verify a single index table against the main table"""
        # Process index table data
        index_data = {}
        for row in index_rows:
            key = tuple(row[f'c{col}'] for col in range(pk_size))
            index_data[key] = row
        
        logger.info(f"Index idx{index_id} has {len(index_data)} rows")
        
        # Check data size consistency
        if len(main_data) != len(index_data):
            raise Exception(
                f"Data size mismatch between main table {table_name} and index {index_id}: "
                f"main={len(main_data)}, index={len(index_data)}"
            )
        
        # Check data consistency
        for key, main_row in main_data.items():
            if key not in index_data:
                raise Exception(f"Key {key} not found in index {index_id} for table {table_name}")
            
            index_row = index_data[key]
            for column in index_row.keys():
                if main_row[column] != index_row[column]:
                    raise Exception(
                        f"Data mismatch between main table {table_name} and index {index_id} "
                        f"for key {key} and column {column}: "
                        f"main={main_row[column]}, index={index_row[column]}"
                    )
        
        # Check uniqueness constraint if applicable
        if index_desc.unique:
            self._verify_index_uniqueness(index_rows, index_desc, index_id, table_name)
    
    def _verify_index_uniqueness(
        self, 
        index_rows: List[Any], 
        index_desc: IndexInfo, 
        index_id: int, 
        table_name: str
    ) -> None:
        """Verify uniqueness constraint for an index"""
        index_keys = set()
        
        for row in index_rows:
            key = tuple(row[f'c{col}'] for col in index_desc.columns)
            # NULL != NULL for secondary index
            if None not in key and key in index_keys:
                raise Exception(f"Duplicate key {key} found in index {index_id} for table {table_name}")
            index_keys.add(key)
    
    def _loop(self):
        """Main loop for the workload"""
        while not self.is_stop_requested():
            threads = []
            for table_name in [f'test{i}' for i in range(WorkloadConfig.TABLES_INFLIGHT)]:
                threads.append(threading.Thread(
                    target=lambda t=table_name: self.run_for_table(t),
                    name=f'{table_name}'))

            for thread in threads:
                thread.start()

            for thread in threads:
                thread.join()

    def get_workload_thread_funcs(self):
        """Get the thread functions for the workload"""
        return [self._loop]