"""
This module implements DBAPI cursor classes for MARS and non-MARS
"""
from __future__ import annotations

import collections
import csv
import typing
import warnings
from collections.abc import Iterable

import pytds
from pytds.connection import Connection, MarsConnection, NonMarsConnection
from pytds.tds_types import NVarCharType, TzInfoFactoryType

from pytds.tds_socket import _TdsSession

from pytds import tds_base
from .tds_base import logger


class Cursor(typing.Protocol, Iterable):
    """
    This class defines an interface for cursor classes.
    It is implemented by MARS and non-MARS cursor classes.
    """

    def __enter__(self) -> Cursor:
        ...

    def __exit__(self, *args) -> None:
        ...

    def get_proc_outputs(self) -> list[typing.Any]:
        ...

    def callproc(
        self,
        procname: tds_base.InternalProc | str,
        parameters: dict[str, typing.Any] | tuple[typing.Any, ...] = (),
    ) -> list[typing.Any]:
        ...

    @property
    def return_value(self) -> int | None:
        ...

    @property
    def spid(self) -> int:
        ...

    @property
    def connection(self) -> Connection | None:
        ...

    def get_proc_return_status(self) -> int | None:
        ...

    def cancel(self) -> None:
        ...

    def close(self) -> None:
        ...

    def execute(
        self,
        operation: str,
        params: list[typing.Any]
        | tuple[typing.Any, ...]
        | dict[str, typing.Any]
        | None = (),
    ) -> Cursor:
        ...

    def executemany(
        self,
        operation: str,
        params_seq: Iterable[
            list[typing.Any] | tuple[typing.Any, ...] | dict[str, typing.Any]
        ],
    ) -> None:
        ...

    def execute_scalar(
        self,
        query_string: str,
        params: list[typing.Any]
        | tuple[typing.Any, ...]
        | dict[str, typing.Any]
        | None = None,
    ) -> typing.Any:
        ...

    def nextset(self) -> bool | None:
        ...

    @property
    def rowcount(self) -> int:
        ...

    @property
    def description(self):
        ...

    def set_stream(self, column_idx: int, stream) -> None:
        ...

    @property
    def messages(
        self
    ) -> (
        list[
            tuple[
                typing.Type,
                tds_base.IntegrityError
                | tds_base.ProgrammingError
                | tds_base.OperationalError,
            ]
        ]
        | None
    ):
        ...

    @property
    def native_description(self):
        ...

    def fetchone(self) -> typing.Any:
        ...

    def fetchmany(self, size=None) -> list[typing.Any]:
        ...

    def fetchall(self) -> list[typing.Any]:
        ...

    @staticmethod
    def setinputsizes(sizes=None) -> None:
        ...

    @staticmethod
    def setoutputsize(size=None, column=0) -> None:
        ...

    def copy_to(
        self,
        file: Iterable[str] | None = None,
        table_or_view: str | None = None,
        sep: str = "\t",
        columns: Iterable[tds_base.Column | str] | None = None,
        check_constraints: bool = False,
        fire_triggers: bool = False,
        keep_nulls: bool = False,
        kb_per_batch: int | None = None,
        rows_per_batch: int | None = None,
        order: str | None = None,
        tablock: bool = False,
        schema: str | None = None,
        null_string: str | None = None,
        data: Iterable[tuple[typing.Any, ...]] | None = None,
    ):
        ...


class BaseCursor(Cursor, collections.abc.Iterator):
    """
    This class represents a base database cursor, which is used to issue queries
    and fetch results from a database connection.
    There are two actual cursor classes: one for MARS connections and one
    for non-MARS connections.
    """

    _cursor_closed_exception = tds_base.InterfaceError("Cursor is closed")

    def __init__(self, connection: Connection, session: _TdsSession):
        self.arraysize = 1
        # Null value in _session means cursor was closed
        self._session: _TdsSession | None = session
        # Keeping strong reference to connection to prevent connection from being garbage collected
        # while there are active cursors
        self._connection: Connection | None = connection

    @property
    def connection(self) -> Connection | None:
        warnings.warn(
            "connection property is deprecated on the cursor object and will be removed in future releases",
            DeprecationWarning,
        )
        return self._connection

    def __enter__(self) -> BaseCursor:
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def __iter__(self) -> BaseCursor:
        """
        Return self to make cursors compatibile with Python iteration
        protocol.
        """
        return self

    def get_proc_outputs(self) -> list[typing.Any]:
        """
        If stored procedure has result sets and OUTPUT parameters use this method
        after you processed all result sets to get values of the OUTPUT parameters.
        :return: A list of output parameter values.
        """
        if self._session is None:
            raise self._cursor_closed_exception
        return self._session.get_proc_outputs()

    def callproc(
        self,
        procname: tds_base.InternalProc | str,
        parameters: dict[str, typing.Any] | tuple[typing.Any, ...] = (),
    ) -> list[typing.Any]:
        """
        Call a stored procedure with the given name.

        :param procname: The name of the procedure to call
        :type procname: str
        :keyword parameters: The optional parameters for the procedure
        :type parameters: sequence

        Note: If stored procedure has OUTPUT parameters and result sets this
        method will not return values for OUTPUT parameters, you should
        call get_proc_outputs to get values for OUTPUT parameters.
        """
        if self._session is None:
            raise self._cursor_closed_exception
        return self._session.callproc(procname, parameters)

    @property
    def return_value(self) -> int | None:
        """Alias to :func:`get_proc_return_status`"""
        return self.get_proc_return_status()

    @property
    def spid(self) -> int:
        """MSSQL Server's session ID (SPID)

        It can be used to correlate connections between client and server logs.
        """
        if self._session is None:
            raise self._cursor_closed_exception
        return self._session._spid

    def _get_tzinfo_factory(self) -> TzInfoFactoryType | None:
        if self._session is None:
            raise self._cursor_closed_exception
        return self._session.tzinfo_factory

    def _set_tzinfo_factory(self, tzinfo_factory: TzInfoFactoryType | None) -> None:
        if self._session is None:
            raise self._cursor_closed_exception
        self._session.tzinfo_factory = tzinfo_factory

    tzinfo_factory = property(_get_tzinfo_factory, _set_tzinfo_factory)

    def get_proc_return_status(self) -> int | None:
        """Last executed stored procedure's return value

        Returns integer value returned by `RETURN` statement from last executed stored procedure.
        If no value was not returned or no stored procedure was executed return `None`.
        """
        if self._session is None:
            return None
        return self._session.get_proc_return_status()

    def cancel(self) -> None:
        """Cancel currently executing statement or stored procedure call"""
        if self._session is None:
            return
        self._session.cancel_if_pending()

    def close(self) -> None:
        """
        Closes the cursor. The cursor is unusable from this point.
        """
        logger.debug("Closing cursor")
        self._session = None
        self._connection = None

    T = typing.TypeVar("T")

    def execute(
        self,
        operation: str,
        params: list[typing.Any]
        | tuple[typing.Any, ...]
        | dict[str, typing.Any]
        | None = None,
    ) -> BaseCursor:
        """Execute an SQL query

        Optionally query can be executed with parameters.
        To make parametrized query use `%s` in the query to denote a parameter
        and pass a tuple with parameter values, e.g.:

        .. code-block::

           execute("select %s, %s", (1,2))

        This will execute query replacing first `%s` with first parameter value - 1,
        and second `%s` with second parameter value -2.

        Another option is to use named parameters with passing a dictionary, e.g.:

        .. code-block::

           execute("select %(param1)s, %(param2)s", {param1=1, param2=2})

        Both those ways of passing parameters is safe from SQL injection attacks.

        This function does not return results of the execution.
        Use :func:`fetchone` or similar to fetch results.
        """
        if self._session is None:
            raise self._cursor_closed_exception
        self._session.execute(operation, params)
        # for compatibility with pyodbc
        return self

    def executemany(
        self,
        operation: str,
        params_seq: Iterable[
            list[typing.Any] | tuple[typing.Any, ...] | dict[str, typing.Any]
        ],
    ) -> None:
        """
        Execute same SQL query multiple times for each parameter set in the `params_seq` list.
        """
        if self._session is None:
            raise self._cursor_closed_exception
        self._session.executemany(operation=operation, params_seq=params_seq)

    def execute_scalar(
        self,
        query_string: str,
        params: list[typing.Any]
        | tuple[typing.Any, ...]
        | dict[str, typing.Any]
        | None = None,
    ) -> typing.Any:
        """
        This method executes SQL query then returns first column of first row or the
        result.

        Query can be parametrized, see :func:`execute` method for details.

        This method is useful if you want just a single value, as in:

        .. code-block::

           conn.execute_scalar('SELECT COUNT(*) FROM employees')

        This method works in the same way as ``iter(conn).next()[0]``.
        Remaining rows, if any, can still be iterated after calling this
        method.
        """
        if self._session is None:
            raise self._cursor_closed_exception
        return self._session.execute_scalar(query_string, params)

    def nextset(self) -> bool | None:
        """Move to next recordset in batch statement, all rows of current recordset are
        discarded if present.

        :returns: true if successful or ``None`` when there are no more recordsets
        """
        if self._session is None:
            raise self._cursor_closed_exception
        return self._session.next_set()

    @property
    def rowcount(self) -> int:
        """Number of rows affected by previous statement

        :returns: -1 if this information was not supplied by the server
        """
        if self._session is None:
            return -1
        return self._session.rows_affected

    @property
    def description(self):
        """Cursor description, see http://legacy.python.org/dev/peps/pep-0249/#description"""
        if self._session is None:
            return None
        res = self._session.res_info
        if res:
            return res.description
        else:
            return None

    def set_stream(self, column_idx: int, stream) -> None:
        """
        This function can be used to efficiently receive values which can be very large, e.g. `TEXT`, `VARCHAR(MAX)`, `VARBINARY(MAX)`.

        When streaming is not enabled, values are loaded to memory as they are received from server and
        once entire row is loaded, it is returned.

        With this function streaming receiver can be specified via `stream` parameter which will receive chunks of the data
        as they are received. For each received chunk driver will call stream's write method.
        For example this can be used to save value of a field into a file, or to
        proces value as it is being received.

        For string fields chunks are represented as unicode strings.
        For binary fields chunks are represented as `bytes` strings.

        Example usage:

        .. code-block::

           cursor.execute("select N'very large field'")
           cursor.set_stream(0, StringIO())
           row = cursor.fetchone()
           # now row[0] contains instance of a StringIO object which was gradually
           # filled with output from server for first column.

        :param column_idx: Zero based index of a column for which to setup streaming receiver
        :type column_idx: int
        :param stream: Stream object that will be receiving chunks of data via it's `write` method.
        """
        if self._session is None:
            raise self._cursor_closed_exception
        res_info = self._session.res_info
        if not res_info:
            raise ValueError("No result set is active")
        if len(res_info.columns) <= column_idx or column_idx < 0:
            raise ValueError("Invalid value for column_idx")
        res_info.columns[column_idx].serializer.set_chunk_handler(
            pytds.tds_types._StreamChunkedHandler(stream)
        )

    @property
    def messages(
        self
    ) -> (
        list[
            tuple[
                typing.Type,
                tds_base.IntegrityError
                | tds_base.ProgrammingError
                | tds_base.OperationalError,
            ]
        ]
        | None
    ):
        """Messages generated by server, see http://legacy.python.org/dev/peps/pep-0249/#cursor-messages"""
        if self._session:
            result = []
            for msg in self._session.messages:
                ex = tds_base._create_exception_by_message(msg)
                result.append((type(ex), ex))
            return result
        else:
            return None

    @property
    def native_description(self):
        """todo document"""
        if self._session is None:
            return None
        res = self._session.res_info
        if res:
            return res.native_descr
        else:
            return None

    def fetchone(self) -> typing.Any:
        """Fetch next row.

        Returns row using currently configured factory, or ``None`` if there are no more rows
        """
        if self._session is None:
            raise self._cursor_closed_exception
        return self._session.fetchone()

    def fetchmany(self, size=None) -> list[typing.Any]:
        """Fetch next N rows

        :param size: Maximum number of rows to return, default value is cursor.arraysize
        :returns: List of rows
        """
        if self._session is None:
            raise self._cursor_closed_exception
        if size is None:
            size = self.arraysize

        rows = []
        for _ in range(size):
            row = self.fetchone()
            if not row:
                break
            rows.append(row)
        return rows

    def fetchall(self) -> list[typing.Any]:
        """Fetch all remaining rows

        Do not use this if you expect large number of rows returned by the server,
        since this method will load all rows into memory.  It is more efficient
        to load and process rows by iterating over them.
        """
        if self._session is None:
            raise self._cursor_closed_exception
        return list(row for row in self)

    def __next__(self) -> typing.Any:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    @staticmethod
    def setinputsizes(sizes=None) -> None:
        """
        This method does nothing, as permitted by DB-API specification.
        """
        pass

    @staticmethod
    def setoutputsize(size=None, column=0) -> None:
        """
        This method does nothing, as permitted by DB-API specification.
        """
        pass

    def copy_to(
        self,
        file: Iterable[str] | None = None,
        table_or_view: str | None = None,
        sep: str = "\t",
        columns: Iterable[tds_base.Column | str] | None = None,
        check_constraints: bool = False,
        fire_triggers: bool = False,
        keep_nulls: bool = False,
        kb_per_batch: int | None = None,
        rows_per_batch: int | None = None,
        order: str | None = None,
        tablock: bool = False,
        schema: str | None = None,
        null_string: str | None = None,
        data: Iterable[collections.abc.Sequence[typing.Any]] | None = None,
    ):
        """*Experimental*. Efficiently load data to database from file using ``BULK INSERT`` operation

        :param file: Source file-like object, should be in csv format. Specify
          either this or data, not both.
        :param table_or_view: Destination table or view in the database
        :type table_or_view: str

        Optional parameters:

        :keyword sep: Separator used in csv file
        :type sep: str
        :keyword columns: List of :class:`pytds.tds_base.Column` objects or column names in target
          table to insert to. SQL Server will do some conversions, so these
          may not have to match the actual table definition exactly.
          If not provided will insert into all columns assuming nvarchar(4000)
          NULL for all columns.
          If only the column name is provided, the type is assumed to be
          nvarchar(4000) NULL.
          If rows are given with file, you cannot specify non-string data
          types.
          If rows are given with data, the values must be a type supported by
          the serializer for the column in tds_types.
        :type columns: list
        :keyword check_constraints: Check table constraints for incoming data
        :type check_constraints: bool
        :keyword fire_triggers: Enable or disable triggers for table
        :type fire_triggers: bool
        :keyword keep_nulls: If enabled null values inserted as-is, instead of
          inserting default value for column
        :type keep_nulls: bool
        :keyword kb_per_batch: Kilobytes per batch can be used to optimize performance, see MSSQL
          server documentation for details
        :type kb_per_batch: int
        :keyword rows_per_batch: Rows per batch can be used to optimize performance, see MSSQL
          server documentation for details
        :type rows_per_batch: int
        :keyword order: The ordering of the data in source table. List of columns with ASC or DESC suffix.
          E.g. ``['order_id ASC', 'name DESC']``
          Can be used to optimize performance, see MSSQL server documentation for details
        :type order: list
        :keyword tablock: Enable or disable table lock for the duration of bulk load
        :keyword schema: Name of schema for table or view, if not specified default schema will be used
        :keyword null_string: String that should be interpreted as a NULL when
          reading the CSV file. Has no meaning if using data instead of file.
        :keyword data: The data to insert as an iterable of rows, which are
          iterables of values. Specify either data parameter or file parameter but not both.
        """
        if self._session is None:
            raise self._cursor_closed_exception
        # conn = self._conn()
        rows: Iterable[collections.abc.Sequence[typing.Any]]
        if data is None:
            if file is None:
                raise ValueError("No data was specified via file or data parameter")
            reader = csv.reader(file, delimiter=sep)

            if null_string is not None:

                def _convert_null_strings(csv_reader):
                    for row in csv_reader:
                        yield [r if r != null_string else None for r in row]

                reader = _convert_null_strings(reader)

            rows = reader
        else:
            rows = data

        obj_name = tds_base.tds_quote_id(table_or_view)
        if schema:
            obj_name = f"{tds_base.tds_quote_id(schema)}.{obj_name}"
        if columns:
            metadata = []
            for column in columns:
                if isinstance(column, tds_base.Column):
                    metadata.append(column)
                else:
                    metadata.append(
                        tds_base.Column(
                            name=column,
                            type=NVarCharType(size=4000),
                            flags=tds_base.Column.fNullable,
                        )
                    )
        else:
            self.execute(f"select top 1 * from {obj_name} where 1<>1")
            metadata = [
                tds_base.Column(
                    name=col[0],
                    type=NVarCharType(size=4000),
                    flags=tds_base.Column.fNullable if col[6] else 0,
                )
                for col in self.description
            ]
        col_defs = ",".join(
            f"{tds_base.tds_quote_id(col.column_name)} {col.type.get_declaration()}"
            for col in metadata
        )
        with_opts = []
        if check_constraints:
            with_opts.append("CHECK_CONSTRAINTS")
        if fire_triggers:
            with_opts.append("FIRE_TRIGGERS")
        if keep_nulls:
            with_opts.append("KEEP_NULLS")
        if kb_per_batch:
            with_opts.append("KILOBYTES_PER_BATCH = {0}".format(kb_per_batch))
        if rows_per_batch:
            with_opts.append("ROWS_PER_BATCH = {0}".format(rows_per_batch))
        if order:
            with_opts.append("ORDER({0})".format(",".join(order)))
        if tablock:
            with_opts.append("TABLOCK")
        with_part = ""
        if with_opts:
            with_part = "WITH ({0})".format(",".join(with_opts))
        operation = "INSERT BULK {0}({1}) {2}".format(obj_name, col_defs, with_part)
        self.execute(operation)
        self._session.submit_bulk(metadata, rows)
        self._session.process_simple_request()


class NonMarsCursor(BaseCursor):
    """
    This class represents a non-MARS database cursor, which is used to issue queries
    and fetch results from a database connection.

    Non-MARS connections allow only one cursor to be active at a given time.
    """

    def __init__(self, connection: NonMarsConnection, session: _TdsSession):
        super().__init__(connection=connection, session=session)


class _MarsCursor(BaseCursor):
    """
    This class represents a MARS database cursor, which is used to issue queries
    and fetch results from a database connection.

    MARS connections allow multiple cursors to be active at the same time.
    """

    def __init__(self, connection: MarsConnection, session: _TdsSession):
        super().__init__(
            connection=connection,
            session=session,
        )

    @property
    def spid(self) -> int:
        # not thread safe for connection
        return self.execute_scalar("select @@SPID")

    def close(self) -> None:
        """
        Closes the cursor. The cursor is unusable from this point.
        """
        logger.debug("Closing MARS cursor")
        if self._session is not None:
            self._session.close()
            self._session = None
        self._connection = None
