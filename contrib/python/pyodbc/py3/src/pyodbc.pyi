from __future__ import annotations
from typing import (
    Any, Callable, Dict, Final, Generator, Iterable, Iterator,
    List, Optional, Sequence, Tuple, Union,
)


# SQLSetConnectAttr attributes
# ref: https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetconnectattr-function
SQL_ATTR_ACCESS_MODE: int
SQL_ATTR_AUTOCOMMIT: int
SQL_ATTR_CURRENT_CATALOG: int
SQL_ATTR_LOGIN_TIMEOUT: int
SQL_ATTR_ODBC_CURSORS: int
SQL_ATTR_QUIET_MODE: int
SQL_ATTR_TRACE: int
SQL_ATTR_TRACEFILE: int
SQL_ATTR_TRANSLATE_LIB: int
SQL_ATTR_TRANSLATE_OPTION: int
SQL_ATTR_TXN_ISOLATION: int
# other (e.g. specific to certain RDBMSs)
SQL_ACCESS_MODE: int
SQL_AUTOCOMMIT: int
SQL_CURRENT_QUALIFIER: int
SQL_LOGIN_TIMEOUT: int
SQL_ODBC_CURSORS: int
SQL_OPT_TRACE: int
SQL_OPT_TRACEFILE: int
SQL_PACKET_SIZE: int
SQL_QUIET_MODE: int
SQL_TRANSLATE_DLL: int
SQL_TRANSLATE_OPTION: int
SQL_TXN_ISOLATION: int
# Unicode
SQL_ATTR_ANSI_APP: int

# ODBC column data types
# https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/appendix-d-data-types
SQL_UNKNOWN_TYPE: int
SQL_CHAR: int
SQL_VARCHAR: int
SQL_LONGVARCHAR: int
SQL_WCHAR: int
SQL_WVARCHAR: int
SQL_WLONGVARCHAR: int
SQL_DECIMAL: int
SQL_NUMERIC: int
SQL_SMALLINT: int
SQL_INTEGER: int
SQL_REAL: int
SQL_FLOAT: int
SQL_DOUBLE: int
SQL_BIT: int
SQL_TINYINT: int
SQL_BIGINT: int
SQL_BINARY: int
SQL_VARBINARY: int
SQL_LONGVARBINARY: int
SQL_TYPE_DATE: int
SQL_TYPE_TIME: int
SQL_TYPE_TIMESTAMP: int
SQL_SS_TIME2: int
SQL_SS_VARIANT: int
SQL_SS_XML: int
SQL_INTERVAL_MONTH: int
SQL_INTERVAL_YEAR: int
SQL_INTERVAL_YEAR_TO_MONTH: int
SQL_INTERVAL_DAY: int
SQL_INTERVAL_HOUR: int
SQL_INTERVAL_MINUTE: int
SQL_INTERVAL_SECOND: int
SQL_INTERVAL_DAY_TO_HOUR: int
SQL_INTERVAL_DAY_TO_MINUTE: int
SQL_INTERVAL_DAY_TO_SECOND: int
SQL_INTERVAL_HOUR_TO_MINUTE: int
SQL_INTERVAL_HOUR_TO_SECOND: int
SQL_INTERVAL_MINUTE_TO_SECOND: int
SQL_GUID: int
# SQLDescribeCol
SQL_NO_NULLS: int
SQL_NULLABLE: int
SQL_NULLABLE_UNKNOWN: int
# specific to pyodbc
SQL_WMETADATA: int

# SQL_CONVERT_X
SQL_CONVERT_FUNCTIONS: int
SQL_CONVERT_BIGINT: int
SQL_CONVERT_BINARY: int
SQL_CONVERT_BIT: int
SQL_CONVERT_CHAR: int
SQL_CONVERT_DATE: int
SQL_CONVERT_DECIMAL: int
SQL_CONVERT_DOUBLE: int
SQL_CONVERT_FLOAT: int
SQL_CONVERT_GUID: int
SQL_CONVERT_INTEGER: int
SQL_CONVERT_INTERVAL_DAY_TIME: int
SQL_CONVERT_INTERVAL_YEAR_MONTH: int
SQL_CONVERT_LONGVARBINARY: int
SQL_CONVERT_LONGVARCHAR: int
SQL_CONVERT_NUMERIC: int
SQL_CONVERT_REAL: int
SQL_CONVERT_SMALLINT: int
SQL_CONVERT_TIME: int
SQL_CONVERT_TIMESTAMP: int
SQL_CONVERT_TINYINT: int
SQL_CONVERT_VARBINARY: int
SQL_CONVERT_VARCHAR: int
SQL_CONVERT_WCHAR: int
SQL_CONVERT_WLONGVARCHAR: int
SQL_CONVERT_WVARCHAR: int

# transaction isolation
# ref: https://docs.microsoft.com/en-us/sql/relational-databases/native-client-odbc-cursors/properties/cursor-transaction-isolation-level
SQL_TXN_READ_COMMITTED: int
SQL_TXN_READ_UNCOMMITTED: int
SQL_TXN_REPEATABLE_READ: int
SQL_TXN_SERIALIZABLE: int

# outer join capabilities
SQL_OJ_LEFT: int
SQL_OJ_RIGHT: int
SQL_OJ_FULL: int
SQL_OJ_NESTED: int
SQL_OJ_NOT_ORDERED: int
SQL_OJ_INNER: int
SQL_OJ_ALL_COMPARISON_OPS: int

# other ODBC database constants
SQL_SCOPE_CURROW: int
SQL_SCOPE_TRANSACTION: int
SQL_SCOPE_SESSION: int
SQL_PC_UNKNOWN: int
SQL_PC_NOT_PSEUDO: int
SQL_PC_PSEUDO: int
# SQL_INDEX_BTREE: int
# SQL_INDEX_CLUSTERED: int
# SQL_INDEX_CONTENT: int
# SQL_INDEX_HASHED: int
# SQL_INDEX_OTHER: int

# attributes for the ODBC SQLGetInfo function
# https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetinfo-function
SQL_ACCESSIBLE_PROCEDURES: int
SQL_ACCESSIBLE_TABLES: int
SQL_ACTIVE_ENVIRONMENTS: int
SQL_AGGREGATE_FUNCTIONS: int
SQL_ALTER_DOMAIN: int
SQL_ALTER_TABLE: int
SQL_ASYNC_MODE: int
SQL_BATCH_ROW_COUNT: int
SQL_BATCH_SUPPORT: int
SQL_BOOKMARK_PERSISTENCE: int
SQL_CATALOG_LOCATION: int
SQL_CATALOG_NAME: int
SQL_CATALOG_NAME_SEPARATOR: int
SQL_CATALOG_TERM: int
SQL_CATALOG_USAGE: int
SQL_COLLATION_SEQ: int
SQL_COLUMN_ALIAS: int
SQL_CONCAT_NULL_BEHAVIOR: int
SQL_CORRELATION_NAME: int
SQL_CREATE_ASSERTION: int
SQL_CREATE_CHARACTER_SET: int
SQL_CREATE_COLLATION: int
SQL_CREATE_DOMAIN: int
SQL_CREATE_SCHEMA: int
SQL_CREATE_TABLE: int
SQL_CREATE_TRANSLATION: int
SQL_CREATE_VIEW: int
SQL_CURSOR_COMMIT_BEHAVIOR: int
SQL_CURSOR_ROLLBACK_BEHAVIOR: int
# SQL_CURSOR_ROLLBACK_SQL_CURSOR_SENSITIVITY: int
SQL_DATABASE_NAME: int
SQL_DATA_SOURCE_NAME: int
SQL_DATA_SOURCE_READ_ONLY: int
SQL_DATETIME_LITERALS: int
SQL_DBMS_NAME: int
SQL_DBMS_VER: int
SQL_DDL_INDEX: int
SQL_DEFAULT_TXN_ISOLATION: int
SQL_DESCRIBE_PARAMETER: int
SQL_DM_VER: int
SQL_DRIVER_HDESC: int
SQL_DRIVER_HENV: int
SQL_DRIVER_HLIB: int
SQL_DRIVER_HSTMT: int
SQL_DRIVER_NAME: int
SQL_DRIVER_ODBC_VER: int
SQL_DRIVER_VER: int
SQL_DROP_ASSERTION: int
SQL_DROP_CHARACTER_SET: int
SQL_DROP_COLLATION: int
SQL_DROP_DOMAIN: int
SQL_DROP_SCHEMA: int
SQL_DROP_TABLE: int
SQL_DROP_TRANSLATION: int
SQL_DROP_VIEW: int
SQL_DYNAMIC_CURSOR_ATTRIBUTES1: int
SQL_DYNAMIC_CURSOR_ATTRIBUTES2: int
SQL_EXPRESSIONS_IN_ORDERBY: int
SQL_FILE_USAGE: int
SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1: int
SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2: int
SQL_GETDATA_EXTENSIONS: int
SQL_GROUP_BY: int
SQL_IDENTIFIER_CASE: int
SQL_IDENTIFIER_QUOTE_CHAR: int
SQL_INDEX_KEYWORDS: int
SQL_INFO_SCHEMA_VIEWS: int
SQL_INSERT_STATEMENT: int
SQL_INTEGRITY: int
SQL_KEYSET_CURSOR_ATTRIBUTES1: int
SQL_KEYSET_CURSOR_ATTRIBUTES2: int
SQL_KEYWORDS: int
SQL_LIKE_ESCAPE_CLAUSE: int
SQL_MAX_ASYNC_CONCURRENT_STATEMENTS: int
SQL_MAX_BINARY_LITERAL_LEN: int
SQL_MAX_CATALOG_NAME_LEN: int
SQL_MAX_CHAR_LITERAL_LEN: int
SQL_MAX_COLUMNS_IN_GROUP_BY: int
SQL_MAX_COLUMNS_IN_INDEX: int
SQL_MAX_COLUMNS_IN_ORDER_BY: int
SQL_MAX_COLUMNS_IN_SELECT: int
SQL_MAX_COLUMNS_IN_TABLE: int
SQL_MAX_COLUMN_NAME_LEN: int
SQL_MAX_CONCURRENT_ACTIVITIES: int
SQL_MAX_CURSOR_NAME_LEN: int
SQL_MAX_DRIVER_CONNECTIONS: int
SQL_MAX_IDENTIFIER_LEN: int
SQL_MAX_INDEX_SIZE: int
SQL_MAX_PROCEDURE_NAME_LEN: int
SQL_MAX_ROW_SIZE: int
SQL_MAX_ROW_SIZE_INCLUDES_LONG: int
SQL_MAX_SCHEMA_NAME_LEN: int
SQL_MAX_STATEMENT_LEN: int
SQL_MAX_TABLES_IN_SELECT: int
SQL_MAX_TABLE_NAME_LEN: int
SQL_MAX_USER_NAME_LEN: int
SQL_MULTIPLE_ACTIVE_TXN: int
SQL_MULT_RESULT_SETS: int
SQL_NEED_LONG_DATA_LEN: int
SQL_NON_NULLABLE_COLUMNS: int
SQL_NULL_COLLATION: int
SQL_NUMERIC_FUNCTIONS: int
SQL_ODBC_INTERFACE_CONFORMANCE: int
SQL_ODBC_VER: int
SQL_OJ_CAPABILITIES: int
SQL_ORDER_BY_COLUMNS_IN_SELECT: int
SQL_PARAM_ARRAY_ROW_COUNTS: int
SQL_PARAM_ARRAY_SELECTS: int
SQL_PARAM_TYPE_UNKNOWN: int
SQL_PARAM_INPUT: int
SQL_PARAM_INPUT_OUTPUT: int
SQL_PARAM_OUTPUT: int
SQL_RETURN_VALUE: int
SQL_RESULT_COL: int
SQL_PROCEDURES: int
SQL_PROCEDURE_TERM: int
SQL_QUOTED_IDENTIFIER_CASE: int
SQL_ROW_UPDATES: int
SQL_SCHEMA_TERM: int
SQL_SCHEMA_USAGE: int
SQL_SCROLL_OPTIONS: int
SQL_SEARCH_PATTERN_ESCAPE: int
SQL_SERVER_NAME: int
SQL_SPECIAL_CHARACTERS: int
SQL_SQL92_DATETIME_FUNCTIONS: int
SQL_SQL92_FOREIGN_KEY_DELETE_RULE: int
SQL_SQL92_FOREIGN_KEY_UPDATE_RULE: int
SQL_SQL92_GRANT: int
SQL_SQL92_NUMERIC_VALUE_FUNCTIONS: int
SQL_SQL92_PREDICATES: int
SQL_SQL92_RELATIONAL_JOIN_OPERATORS: int
SQL_SQL92_REVOKE: int
SQL_SQL92_ROW_VALUE_CONSTRUCTOR: int
SQL_SQL92_STRING_FUNCTIONS: int
SQL_SQL92_VALUE_EXPRESSIONS: int
SQL_SQL_CONFORMANCE: int
SQL_STANDARD_CLI_CONFORMANCE: int
SQL_STATIC_CURSOR_ATTRIBUTES1: int
SQL_STATIC_CURSOR_ATTRIBUTES2: int
SQL_STRING_FUNCTIONS: int
SQL_SUBQUERIES: int
SQL_SYSTEM_FUNCTIONS: int
SQL_TABLE_TERM: int
SQL_TIMEDATE_ADD_INTERVALS: int
SQL_TIMEDATE_DIFF_INTERVALS: int
SQL_TIMEDATE_FUNCTIONS: int
SQL_TXN_CAPABLE: int
SQL_TXN_ISOLATION_OPTION: int
SQL_UNION: int
SQL_USER_NAME: int
SQL_XOPEN_CLI_YEAR: int


# pyodbc-specific constants
BinaryNull: Any  # to distinguish binary NULL values from char NULL values
SQLWCHAR_SIZE: int


# module attributes
# https://www.python.org/dev/peps/pep-0249/#globals

# read-only
apilevel: Final[str] = '2.0'
paramstyle: Final[str] = 'qmark'
threadsafety: Final[int] = 1
version: Final[str]  # not pep-0249

# read-write (not pep-0249)
lowercase: bool = False
native_uuid: bool = False
odbcversion: str = '3.X'
pooling: bool = True


# exceptions
# https://www.python.org/dev/peps/pep-0249/#exceptions
class Warning(Exception): ...
class Error(Exception): ...
class InterfaceError(Error): ...
class DatabaseError(Error): ...
class DataError(DatabaseError): ...
class OperationalError(DatabaseError): ...
class IntegrityError(DatabaseError): ...
class InternalError(DatabaseError): ...
class ProgrammingError(DatabaseError): ...
class NotSupportedError(DatabaseError): ...


class Connection:
    """The ODBC connection class representing an ODBC connection to a database, for
    managing database transactions and creating cursors.
    https://www.python.org/dev/peps/pep-0249/#connection-objects

    This class should not be instantiated directly, instead call pyodbc.connect() to
    create a Connection object.
    """

    @property
    def autocommit(self) -> bool:
        """Whether the database automatically executes a commit after every successful transaction.
        Default is False.
        """
        ...

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        ...

    @property
    def closed(self) -> bool:
        """Returns True if the connection is closed, False otherwise."""
        ...

    @property
    def maxwrite(self) -> int:
        """The maximum bytes to write before using SQLPutData, default is zero for no maximum."""
        ...

    @maxwrite.setter
    def maxwrite(self, value: int) -> None:
        ...

    @property
    def searchescape(self) -> str:
        """The character for escaping search pattern characters like "%" and "_".
        This is typically the backslash character but can be driver-specific."""
        ...

    @property
    def timeout(self) -> int:
        """The timeout in seconds for SQL queries, use zero (the default) for no timeout limit."""
        ...

    @timeout.setter
    def timeout(self, value: int) -> None:
        ...


    # implemented dunder methods
    def __enter__(self) -> Connection: ...
    def __exit__(self, exc_type, exc_value, traceback) -> None: ...


    # functions for defining the text encoding used for data, metadata, sql, parameters, etc.

    def setencoding(self,
                    encoding: Optional[str] = None,
                    ctype: Optional[int] = None) -> None:
        """Set the text encoding for SQL statements and textual parameters sent to the database.

        Args:
            encoding: Text encoding codec, e.g. "utf-8".
            ctype: The C data type when passing data - either pyodbc.SQL_CHAR or pyodbc.SQL_WCHAR.  More
                relevant for Python 2.7.
        """
        ...

    def setdecoding(self,
                    sqltype: int,
                    encoding: Optional[str] = None,
                    ctype: Optional[int] = None) -> None:
        """Set the text decoding used when reading SQL_CHAR or SQL_WCHAR data from the database.

        Args:
            sqltype: pyodbc.SQL_CHAR, pyodbc.SQL_WCHAR, or pyodbc.SQL_WMETADATA.
            encoding: Text encoding codec, e.g. "utf-8".
            ctype: The C data type to request from SQLGetData - either pyodbc.SQL_CHAR or
                pyodbc.SQL_WCHAR.  More relevant for Python 2.7.
        """
        ...


    # functions for getting/setting connection attributes

    def getinfo(self, infotype: int, /) -> Any:
        """Retrieve general information about the driver and the data source, via SQLGetInfo.

        Args:
            infotype: Id of the information to retrieve.

        Returns:
            The value of the requested information.
        """
        ...

    def set_attr(self, attr_id: int, value: int, /) -> None:
        """Set an attribute on the connection, via SQLSetConnectAttr.

        Args:
            attr_id: Id for the attribute, as defined by ODBC or the driver.
            value: The value of the attribute.
        """
        ...


    # functions to handle non-standard database data types

    def add_output_converter(self, sqltype: int, func: Optional[Callable], /) -> None:
        """Register an output converter function that will be called whenever a value
        with the given SQL type is read from the database.  See the Wiki for details:
        https://github.com/mkleehammer/pyodbc/wiki/Using-an-Output-Converter-function

        Args:
            sqltype: The SQL type for the values to convert.
            func: The converter function.
        """
        ...

    def get_output_converter(self, sqltype: int, /) -> Optional[Callable]:
        """Retrieve the (previously registered) converter function for the SQL type.

        Args:
            sqltype: The SQL type.

        Returns:
            The converter function if it exists, None otherwise.
        """
        ...

    def remove_output_converter(self, sqltype: int, /) -> None:
        """Delete a previously registered output converter function.

        Args:
            sqltype: The SQL type.
        """
        ...

    def clear_output_converters(self) -> None:
        """Delete all previously registered converter functions."""
        ...


    # functions for managing database transactions (in typical order of use)

    def cursor(self) -> Cursor:
        """Create a new cursor on the connection.

        Returns:
            A new cursor.
        """
        ...

    def execute(self, sql: str, *params: Any) -> Cursor:
        """A convenience function for running queries directly from a Connection object.
        Creates a new cursor, runs the SQL query, and returns the new cursor.

        Args:
            sql: The SQL query.
            *params: Any parameter values for the SQL query.

        Returns:
            A new cursor.
        """
        ...

    def commit(self) -> None:
        """Commit all pending transactions since the last commit/rollback.  This includes
        all SQL statements executed from ALL cursors created on this connection."""
        ...

    def rollback(self) -> None:
        """Rollback all pending transactions since the last commit/rollback.  This includes
        all SQL statements executed from ALL cursors created on this connection."""
        ...

    def close(self) -> None:
        """Close the connection.  Any uncommitted SQL statements will be rolled back."""
        ...


class Cursor:
    """The class representing database cursors.  Cursors are vehicles for executing SQL
    statements and returning their results.
    https://www.python.org/dev/peps/pep-0249/#cursor-objects

    This class should not be instantiated directly, instead call cursor() from a Connection
    object to create a Cursor object.
    """

    @property
    def arraysize(self) -> int:
        """The number of rows at a time to fetch with fetchmany(), default is 1."""
        ...

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        ...

    @property
    def connection(self) -> Connection:
        """The parent Connection object for the cursor."""
        ...

    @property
    def description(self) -> Tuple[Tuple[str, Any, int, int, int, int, bool]]:
        """The metadata for the columns returned in the last SQL SELECT statement, in
        the form of a list of tuples.  Each tuple contains seven fields:

        0. name of the column (or column alias)
        1. type code, the Python-equivalent class of the column, e.g. str for VARCHAR
        2. display size (pyodbc does not set this value)
        3. internal size (in bytes)
        4. precision
        5. scale
        6. nullable (True/False)

        ref: https://peps.python.org/pep-0249/#description
        """
        ...

    @property
    def fast_executemany(self) -> bool:
        """When this cursor property is False (the default), calls to executemany() do
        nothing more than iterate over the provided list of parameters and calls execute()
        on each set of parameters.  This is typically slow.  When fast_executemany is
        True, the parameters are sent to the database in one bundle (with the SQL).  This
        is usually much faster, but there are limitations.  Check the Wiki for details.
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#executemanysql-params-with-fast_executemanytrue
        """
        ...

    @fast_executemany.setter
    def fast_executemany(self, value: bool) -> None:
        ...

    @property
    def messages(self) -> Optional[List[Tuple[str, Union[str, bytes]]]]:
        """Any descriptive messages returned by the last call to execute(), e.g. PRINT
        statements, or None."""
        ...

    @property
    def noscan(self) -> bool:
        """Whether the driver should scan SQL strings for escape sequences, default is True."""
        ...

    @noscan.setter
    def noscan(self, value: bool) -> None:
        ...

    @property
    def rowcount(self) -> int:
        """The number of rows modified by the last SQL statement.  Has the value of -1
        if the number of rows is unknown or unavailable.
        """
        ...


    # implemented dunder methods
    def __enter__(self) -> Cursor: ...
    def __exit__(self, exc_type, exc_value, traceback) -> None: ...
    def __iter__(self, /) -> Cursor: ...
    def __next__(self, /) -> Row: ...


    # functions for running SQL queries (in rough order of use)

    def setinputsizes(self, sizes: Optional[Iterable[Tuple[int, int, int]]], /) -> None:
        """Explicitly declare the types and sizes of the parameters in a query.  Set
        to None to clear any previously registered input sizes.

        Args:
            sizes: A list of tuples, one tuple for each query parameter, where each
                tuple contains:

                    1. the column datatype
                    2. the column size (char length or decimal precision)
                    3. the decimal scale.

                For example: [(pyodbc.SQL_WVARCHAR, 50, 0), (pyodbc.SQL_DECIMAL, 18, 4)]
        """
        ...

    def setoutputsize(self) -> None:
        """Not supported."""
        ...

    def execute(self, sql: str, *params: Any) -> Cursor:
        """Run a SQL query and return the cursor.

        Args:
            sql: The SQL query.
            *params: Any parameters for the SQL query, as positional arguments or a single iterable.

        Returns:
            The cursor, so that calls on the cursor can be chained.
        """
        ...

    def executemany(self, sql: str, params: Union[Sequence, Iterator, Generator], /) -> None:
        """Run the SQL query against an iterable of parameters.  The behavior of this
        function depends heavily on the setting of the fast_executemany cursor property.
        See the Wiki for details.
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#executemanysql-params-with-fast_executemanyfalse-the-default
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#executemanysql-params-with-fast_executemanytrue

        Args:
            sql: The SQL query.
            *params: Any parameters for the SQL query, as an iterable of parameter sets.
        """
        ...

    def fetchone(self) -> Optional[Row]:
        """Retrieve the next row in the current result set for the query.

        Returns:
            A row of results, or None if there is no more data to return.
        """
        ...

    def fetchmany(self, size: int, /) -> List[Row]:
        """Retrieve the next rows in the current result set for the query, as a list.

        Args:
            size: The number of rows to return.

        Returns:
            A list of rows, or an empty list if there is no more data to return.
        """
        ...

    def fetchall(self) -> List[Row]:
        """Retrieve all the remaining rows in the current result set for the query, as a list.

        Returns:
            A list of rows, or an empty list if there is no more data to return.
        """
        ...

    def fetchval(self) -> Any:
        """A convenience function for returning the first column of the first row from
        the query.

        Returns:
            The value in the first column of the first row, or None if there is no data.
        """
        ...

    def skip(self, count: int, /) -> None:
        """Skip over rows in the current result set of a query.

        Args:
            count: The number of rows to skip.
        """
        ...

    def nextset(self) -> bool:
        """Switch to the next result set in the SQL query (e.g. if there are
        multiple SELECT statements in the SQL script).

        Returns:
            True if there are more result sets, False otherwise.
        """
        ...

    def commit(self) -> None:
        """A convenience function for executing a commit on the parent connection.
        Commits all pending transactions on the parent connection since the last
        commit/rollback.  This includes all SQL statements executed from ALL cursors
        created on the parent connection, not just this cursor."""
        ...

    def rollback(self) -> None:
        """A convenience function for executing a rollback on the parent connection.
        Rolls back all pending transactions on the parent connection since the last
        commit/rollback.  This includes all SQL statements executed from ALL cursors
        created on the parent connection, not just this cursor."""
        ...

    def cancel(self) -> None:
        """Cancel the processing of the current query.  Typically this has to be called
        from a separate thread.
        """
        ...

    def close(self) -> None:
        """Close the cursor, discarding any remaining result sets and/or messages."""
        ...


    # functions to retrieve database metadata

    def tables(self,
               table: Optional[str] = None,
               catalog: Optional[str] = None,
               schema: Optional[str] = None,
               tableType: Optional[str] = None) -> Cursor:
        """Return information about tables in the database, typically from the
        INFORMATION_SCHEMA.TABLES metadata view.  Parameter values can include
        wildcard characters.

        Args:
            table: Name of the database table.
            catalog: Name of the catalog (database).
            schema: Name of the table schema.
            tableType: Kind of table, e.g. "BASE TABLE".

        Returns:
            The cursor object, containing table information in the result set.
        """
        ...

    def columns(self,
                table: Optional[str] = None,
                catalog: Optional[str] = None,
                schema: Optional[str] = None,
                column: Optional[str] = None) -> Cursor:
        """Return information about columns in database tables, typically from the
        INFORMATION_SCHEMA.COLUMNS metadata view.  Parameter values can include
        wildcard characters.

        Args:
            table: Name of the database table.
            catalog: Name of the catalog (database).
            schema: Name of the table schema.
            column: Name of the column in the table.

        Returns:
            The cursor object, containing column information in the result set.
        """
        ...

    def statistics(self,
                   table: str,
                   catalog: Optional[str] = None,
                   schema: Optional[str] = None,
                   unique: bool = False,
                   quick: bool = True) -> Cursor:
        """Return statistical information about database tables.  Parameter values
        can include wildcard characters.

        Args:
            table: Name of the database table.
            catalog: Name of the catalog (database).
            schema: Name of the table schema.
            unique: If True, include information about unique indexes only, not all indexes.
            quick: If True, CARDINALITY and PAGES are returned only if they are readily
                available, otherwise None is returned for them.

        Returns:
            The cursor object, containing statistical information in the result set.
        """
        ...

    def rowIdColumns(self,
                     table: str,
                     catalog: Optional[str] = None,
                     schema: Optional[str] = None,
                     nullable: bool = True) -> Cursor:
        """Return the column(s) in a database table that uniquely identify each row
        (e.g. the primary key column).  Parameter values can include wildcard characters.

        Args:
            table: Name of the database table.
            catalog: Name of the catalog (database).
            schema: Name of the table schema.
            nullable: If True, include sets of columns that are nullable.

        Returns:
            The cursor object, containing the relevant column information in the result set.
        """
        ...

    def rowVerColumns(self,
                      table: str,
                      catalog: Optional[str] = None,
                      schema: Optional[str] = None,
                      nullable: bool = True) -> Cursor:
        """Return the column(s) in a database table that are updated whenever the row
        is updated.  Parameter values can include wildcard characters.

        Args:
            table: Name of the database table.
            catalog: Name of the catalog (database).
            schema: Name of the table schema.
            nullable: If True, include sets of columns that are nullable.

        Returns:
            The cursor object, containing the relevant column information in the result set.
        """
        ...

    def primaryKeys(self,
                    table: str,
                    catalog: Optional[str] = None,
                    schema: Optional[str] = None) -> Cursor:
        """Return the column(s) in a database table that make up the primary key on
        the table.  Parameter values can include wildcard characters.

        Args:
            table: Name of the database table.
            catalog: Name of the catalog (database).
            schema: Name of the table schema.

        Returns:
            The cursor object, containing primary key information in the result set.
        """
        ...

    def foreignKeys(self,
                    table: Optional[str] = None,
                    catalog: Optional[str] = None,
                    schema: Optional[str] = None,
                    foreignTable: Optional[str] = None,
                    foreignCatalog: Optional[str] = None,
                    foreignSchema: Optional[str] = None) -> Cursor:
        """Return the foreign keys in a database table, i.e. any columns that refer to
        primary key columns on another table.  Parameter values can include wildcard characters.

        Args:
            table: Name of the database table.
            catalog: Name of the catalog (database).
            schema: Name of the table schema.
            foreignTable: Name of the foreign database table.
            foreignCatalog: Name of the foreign catalog (database).
            foreignSchema: Name of the foreign table schema.

        Returns:
            The cursor object, containing foreign key information in the result set.
        """
        ...

    def procedures(self,
                   procedure: Optional[str] = None,
                   catalog: Optional[str] = None,
                   schema: Optional[str] = None) -> Cursor:
        """Return information about stored procedures.  Parameter values can include
        wildcard characters.

        Args:
            procedure: Name of the stored procedure.
            catalog: Name of the catalog (database).
            schema: Name of the table schema.

        Returns:
            The cursor object, containing stored procedure information in the result set.
        """
        ...

    def procedureColumns(self,
                         procedure: Optional[str] = None,
                         catalog: Optional[str] = None,
                         schema: Optional[str] = None) -> Cursor:
        """Return information about the columns used as input/output parameters in
        stored procedures.  Parameter values can include wildcard characters.

        Args:
            procedure: Name of the stored procedure.
            catalog: Name of the catalog (database).
            schema: Name of the table schema.

        Returns:
            The cursor object, containing stored procedure column information in the result set.
        """
        ...

    def getTypeInfo(self, sqlType: Optional[int] = None, /) -> Cursor:
        """Return information about data types supported by the data source.

        Args:
            sqlType: The SQL data type.

        Returns:
            The cursor object, containing information about the SQL data type in the result set.
        """
        ...


class Row:
    """The class representing a single record in the result set from a query.  Objects of
    this class behave somewhat similarly to a NamedTuple.  Column values can be accessed
    by column name (i.e. using dot notation) or by row index.
    """

    @property
    def cursor_description(self) -> Tuple[Tuple[str, Any, int, int, int, int, bool]]:
        """The metadata for the columns in this Row, as retrieved from the parent Cursor object."""
        ...

    # implemented dunder methods
    def __contains__(self, key, /) -> int: ...
    def __delattr__(self, name, /) -> None: ...
    def __delitem__(self, key, /) -> None: ...
    def __eq__(self, value, /) -> bool: ...
    def __ge__(self, value, /) -> bool: ...
    def __getattribute__(self, name, /) -> Any: ...
    def __getitem__(self, key, /) -> Any: ...
    def __gt__(self, value, /) -> bool: ...
    def __iter__(self) -> Iterator[Any]: ...
    def __le__(self, value, /) -> bool: ...
    def __len__(self, /) -> int: ...
    def __lt__(self, value, /) -> bool: ...
    def __ne__(self, value, /) -> bool: ...
    def __reduce__(self) -> Any: ...
    def __repr__(self, /) -> str: ...
    def __setattr__(self, name, value, /) -> None: ...
    def __setitem__(self, key, value, /) -> None: ...


# module functions

def dataSources() -> Dict[str, str]:
    """Return all available Data Source Names (DSNs), typically from the odbcinst.ini file
    or the Windows ODBC Data Source Administrator.

    Returns:
        A dictionary of DSNs and their textual descriptions.
    """
    ...

def drivers() -> List[str]:
    """Return the names of all available ODBC drivers, typically from the odbc.ini file or the
    Windows ODBC Data Source Administrator.

    Returns:
        A list of driver names.
    """
    ...


def getDecimalSeparator() -> str:
    """Retrieve the decimal separator character used when parsing NUMERIC/DECIMAL values
    from the database, e.g. the "." in "1,234.56".

    Returns:
        The decimal separator character.
    """
    ...


def setDecimalSeparator(sep: str, /) -> None:
    """Set the decimal separator character used when parsing NUMERIC/DECIMAL values
    from the database, e.g. the "." in "1,234.56".

    Args:
        sep: The decimal separator character.
    """
    ...


def connect(connstring: Optional[str] = None,
            /, *,  # only positional parameters before, only named parameters after
            autocommit: bool = False,
            encoding: str = 'utf-16le',
            readonly: bool = False,
            timeout: int = 0,
            attrs_before: Optional[Dict[int, Union[int, bytes, bytearray, str, Sequence[str]]]] = None,
            **kwargs: Any) -> Connection:
    """Create a new ODBC connection to a database.  See the Wiki for details:
    https://github.com/mkleehammer/pyodbc/wiki/The-pyodbc-Module#connect

    Args:
        connstring: The connection string, which is passed verbatim to the driver manager.
        autocommit: If True, instructs the database to commit after each SQL statement.
        encoding: Encoding codec used when sending textual connection parameters to the database.
        readonly: To set the connection read-only.  Not all drivers and/or databases support this.
        timeout: Set the connection timeout, in seconds.  This is managed by the driver, not
            pyodbc, and not all drivers support this.
        attrs_before: Set low-level connection attributes before a connection is attempted.
        **kwargs: These key/value pairs are used to construct the connection string, or add
            to it (as "key=value;" combinations).

    Returns:
        A new Connection object.
    """
    ...
