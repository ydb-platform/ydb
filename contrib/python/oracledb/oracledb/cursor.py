# -----------------------------------------------------------------------------
# Copyright (c) 2021, 2024, Oracle and/or its affiliates.
#
# This software is dual-licensed to you under the Universal Permissive License
# (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl and Apache License
# 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
# either license.
#
# If you elect to accept the software under the Apache License, Version 2.0,
# the following applies:
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# cursor.py
#
# Contains the Cursor class used for executing statements on connections and
# fetching results from queries.
# -----------------------------------------------------------------------------

from typing import Any, Union, Callable, Optional

from . import __name__ as MODULE_NAME
from . import connection as connection_module
from . import errors
from . import utils
from .fetch_info import FetchInfo
from .var import Var
from .base_impl import DbType, DB_TYPE_OBJECT
from .dbobject import DbObjectType


class BaseCursor:
    _impl = None

    def __init__(
        self,
        connection: "connection_module.Connection",
        scrollable: bool = False,
    ) -> None:
        self.connection = connection
        self._impl = connection._impl.create_cursor_impl(scrollable)

    def __del__(self):
        if self._impl is not None:
            self._impl.close(in_del=True)

    def __enter__(self):
        self._verify_open()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self._verify_open()
        self._impl.close(in_del=True)
        self._impl = None

    def __repr__(self):
        typ = self.__class__
        cls_name = f"{typ.__module__}.{typ.__qualname__}"
        return f"<{cls_name} on {self.connection!r}>"

    def _call(
        self,
        name: str,
        parameters: Union[list, tuple],
        keyword_parameters: dict,
        return_value: Any = None,
    ) -> None:
        """
        Internal method used for generating the PL/SQL block used to call
        stored procedures.
        """
        utils.verify_stored_proc_args(parameters, keyword_parameters)
        self._verify_open()
        statement, bind_values = self._call_get_execute_args(
            name, parameters, keyword_parameters, return_value
        )
        return self.execute(statement, bind_values)

    def _call_get_execute_args(
        self,
        name: str,
        parameters: Union[list, tuple],
        keyword_parameters: dict,
        return_value: str = None,
    ) -> None:
        """
        Internal method used for generating the PL/SQL block used to call
        stored procedures and functions. A tuple containing this statement and
        the bind values is returned.
        """
        bind_names = []
        bind_values = []
        statement_parts = ["begin "]
        if return_value is not None:
            statement_parts.append(":retval := ")
            bind_values.append(return_value)
        statement_parts.append(name + "(")
        if parameters:
            bind_values.extend(parameters)
            bind_names = [":%d" % (i + 1) for i in range(len(parameters))]
        if keyword_parameters:
            for arg_name, arg_value in keyword_parameters.items():
                bind_values.append(arg_value)
                bind_names.append(f"{arg_name} => :{len(bind_names) + 1}")
        statement_parts.append(",".join(bind_names))
        statement_parts.append("); end;")
        statement = "".join(statement_parts)
        return (statement, bind_values)

    def _prepare(
        self, statement: str, tag: str = None, cache_statement: bool = True
    ) -> None:
        """
        Internal method used for preparing a statement for execution.
        """
        self._impl.prepare(statement, tag, cache_statement)

    def _prepare_for_execute(
        self, statement, parameters, keyword_parameters=None
    ):
        """
        Internal method for preparing a statement for execution.
        """
        self._verify_open()
        self._impl._prepare_for_execute(
            self, statement, parameters, keyword_parameters
        )

    def _verify_fetch(self) -> None:
        """
        Verifies that fetching is possible from this cursor.
        """
        self._verify_open()
        if not self._impl.is_query(self):
            errors._raise_err(errors.ERR_NOT_A_QUERY)

    def _verify_open(self) -> None:
        """
        Verifies that the cursor is open and the associated connection is
        connected. If either condition is false an exception is raised.
        """
        if self._impl is None:
            errors._raise_err(errors.ERR_CURSOR_NOT_OPEN)
        self.connection._verify_connected()

    @property
    def arraysize(self) -> int:
        """
        Tunes the number of rows fetched and buffered by internal calls to the
        database when fetching rows from SELECT statements and REF CURSORS. The
        value can drastically affect the performance of a query since it
        directly affects the number of network round trips between Python and
        the database. For methods like fetchone() and fetchall() it does not
        change how many rows are returned to the application. For fetchmany()
        it is the default number of rows to fetch.

        Due to the performance benefits, the default value is 100 instead of
        the 1 that the DB API recommends. This value means that 100 rows are
        fetched by each internal call to the database.
        """
        self._verify_open()
        return self._impl.arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        self._verify_open()
        if not isinstance(value, int) or value <= 0:
            errors._raise_err(errors.ERR_INVALID_ARRAYSIZE)
        self._impl.arraysize = value

    def arrayvar(
        self,
        typ: Union[DbType, DbObjectType, type],
        value: Union[list, int],
        size: int = 0,
    ) -> Var:
        """
        Create an array variable associated with the cursor of the given type
        and size and return a variable object. The value is either an integer
        specifying the number of elements to allocate or it is a list and the
        number of elements allocated is drawn from the size of the list. If the
        value is a list, the variable is also set with the contents of the
        list. If the size is not specified and the type is a string or binary,
        4000 bytes is allocated. This is needed for passing arrays to PL/SQL
        (in cases where the list might be empty and the type cannot be
        determined automatically) or returning arrays from PL/SQL.

        Array variables can only be used for PL/SQL associative arrays with
        contiguous keys. For PL/SQL associative arrays with sparsely populated
        keys or for varrays and nested tables, the DbObject approach needs to
        be used instead.
        """
        self._verify_open()
        if isinstance(value, list):
            num_elements = len(value)
        elif isinstance(value, int):
            num_elements = value
        else:
            raise TypeError("expecting integer or list of values")
        var = self._impl.create_var(
            self.connection,
            typ,
            size=size,
            num_elements=num_elements,
            is_array=True,
        )
        if isinstance(value, list):
            var.setvalue(0, value)
        return var

    def bindnames(self) -> list:
        """
        Return the list of bind variable names bound to the statement. Note
        that a statement must have been prepared first.
        """
        self._verify_open()
        if self._impl.statement is None:
            errors._raise_err(errors.ERR_NO_STATEMENT_PREPARED)
        return self._impl.get_bind_names()

    @property
    def bindvars(self) -> list:
        """
        Returns the bind variables used for the last execute. The value will be
        either a list or a dictionary depending on whether binding was done by
        position or name. Care should be taken when referencing this attribute.
        In particular, elements should not be removed or replaced.
        """
        self._verify_open()
        return self._impl.get_bind_vars()

    def close(self) -> None:
        """
        Close the cursor now, rather than whenever __del__ is called. The
        cursor will be unusable from this point forward; an Error exception
        will be raised if any operation is attempted with the cursor.
        """
        self._verify_open()
        self._impl.close()
        self._impl = None

    @property
    def description(self) -> tuple:
        """
        Returns a sequence of 7-item sequences. Each of these sequences
        contains information describing one result column: (name, type,
        display_size, internal_size, precision, scale, null_ok).  This
        attribute will be None for operations that do not return rows or if the
        cursor has not had an operation invoked via the execute() method yet.
        """
        self._verify_open()
        if self._impl.is_query(self):
            return [
                FetchInfo._from_impl(i) for i in self._impl.fetch_info_impls
            ]

    @property
    def fetchvars(self) -> list:
        """
        Specifies the list of variables created for the last query that was
        executed on the cursor. Care should be taken when referencing this
        attribute. In particular, elements should not be removed or replaced.
        """
        self._verify_open()
        return self._impl.get_fetch_vars()

    def getarraydmlrowcounts(self) -> list:
        """
        Return the DML row counts after a call to executemany() with
        arraydmlrowcounts enabled. This will return a list of integers
        corresponding to the number of rows affected by the DML statement for
        each element of the array passed to executemany().
        """
        self._verify_open()
        return self._impl.get_array_dml_row_counts()

    def getbatcherrors(self) -> list:
        """
        Return the exceptions that took place after a call to executemany()
        with batcherrors enabled. This will return a list of Error objects, one
        error for each iteration that failed. The offset can be determined by
        looking at the offset attribute of the error object.
        """
        self._verify_open()
        return self._impl.get_batch_errors()

    def getimplicitresults(self) -> list:
        """
        Return a list of cursors which correspond to implicit results made
        available from a PL/SQL block or procedure without the use of OUT ref
        cursor parameters. The PL/SQL block or procedure opens the cursors and
        marks them for return to the client using the procedure
        dbms_sql.return_result. Cursors returned in this fashion should not be
        closed. They will be closed automatically by the parent cursor when it
        is closed. Closing the parent cursor will invalidate the cursors
        returned by this method.
        """
        self._verify_open()
        return self._impl.get_implicit_results(self.connection)

    @property
    def inputtypehandler(self) -> Callable:
        """
        Specifies a method called for each value that is bound to a statement
        executed on this cursor. The method signature is handler(cursor, value,
        arraysize) and the return value is expected to be a variable object or
        None in which case a default variable object will be created. If this
        attribute is None, the default behavior will take place for all values
        bound to statements.
        """
        self._verify_open()
        return self._impl.inputtypehandler

    @inputtypehandler.setter
    def inputtypehandler(self, value: Callable) -> None:
        self._verify_open()
        self._impl.inputtypehandler = value

    @property
    def lastrowid(self) -> str:
        """
        Returns the rowid of the last row modified by the cursor. If no row was
        modified by the last operation performed on the cursor, the value None
        is returned.
        """
        self._verify_open()
        return self._impl.get_lastrowid()

    @property
    def outputtypehandler(self) -> Callable:
        """
        Specifies a method called for each column that is going to be fetched
        from this cursor. The method signature is handler(cursor, name,
        defaultType, length, precision, scale) and the return value is expected
        to be a variable object or None in which case a default variable object
        will be created. If this attribute is None, the default behavior will
        take place for all columns fetched from this cursor.
        """
        self._verify_open()
        return self._impl.outputtypehandler

    @outputtypehandler.setter
    def outputtypehandler(self, value: Callable) -> None:
        self._verify_open()
        self._impl.outputtypehandler = value

    @property
    def prefetchrows(self) -> int:
        """
        Used to tune the number of rows fetched when a SELECT statement is
        executed. This value can reduce the number of round-trips to the
        database that are required to fetch rows but at the cost of additional
        memory. Setting this value to 0 can be useful when the timing of
        fetches must be explicitly controlled.
        """
        self._verify_open()
        return self._impl.prefetchrows

    @prefetchrows.setter
    def prefetchrows(self, value: int) -> None:
        self._verify_open()
        self._impl.prefetchrows = value

    def prepare(
        self, statement: str, tag: str = None, cache_statement: bool = True
    ) -> None:
        """
        This can be used before a call to execute() to define the statement
        that will be executed. When this is done, the prepare phase will not be
        performed when the call to execute() is made with None or the same
        string object as the statement. If the tag parameter is specified and
        the cache_statement parameter is True, the statement will be returned
        to the statement cache with the given tag. If the cache_statement
        parameter is False, the statement will be removed from the statement
        cache (if it was found there) or will simply not be cached. See the
        Oracle documentation for more information about the statement cache.
        """
        self._verify_open()
        self._prepare(statement, tag, cache_statement)

    @property
    def rowcount(self) -> int:
        """
        This read-only attribute specifies the number of rows that have
        currently been fetched from the cursor (for select statements), that
        have been affected by the operation (for insert, update, delete and
        merge statements), or the number of successful executions of the
        statement (for PL/SQL statements).
        """
        if self._impl is not None and self.connection._impl is not None:
            return self._impl.rowcount
        return -1

    @property
    def rowfactory(self) -> Callable:
        """
        Specifies a method to call for each row that is retrieved from the
        database.  Ordinarily a tuple is returned for each row but if this
        attribute is set, the method is called with the tuple that would
        normally be returned, and the result of the method is returned instead.
        """
        self._verify_open()
        return self._impl.rowfactory

    @rowfactory.setter
    def rowfactory(self, value: Callable) -> None:
        self._verify_open()
        self._impl.rowfactory = value

    @property
    def scrollable(self) -> bool:
        """
        Specifies whether the cursor can be scrolled or not. By default,
        cursors are not scrollable, as the server resources and response times
        are greater than for nonscrollable cursors. This attribute is checked
        and the corresponding mode set in Oracle when calling the method
        execute().
        """
        self._verify_open()
        return self._impl.scrollable

    @scrollable.setter
    def scrollable(self, value: bool) -> None:
        self._verify_open()
        self._impl.scrollable = value

    def setinputsizes(self, *args: Any, **kwargs: Any) -> Union[list, dict]:
        """
        This can be used before a call to execute(), callfunc() or callproc()
        to predefine memory areas for the operation’s parameters. Each
        parameter should be a type object corresponding to the input that will
        be used or it should be an integer specifying the maximum length of a
        string parameter. Use keyword parameters when binding by name and
        positional parameters when binding by position. The singleton None can
        be used as a parameter when using positional parameters to indicate
        that no space should be reserved for that position.
        """
        if args and kwargs:
            errors._raise_err(errors.ERR_ARGS_AND_KEYWORD_ARGS)
        elif args or kwargs:
            self._verify_open()
            return self._impl.setinputsizes(self.connection, args, kwargs)
        return []

    def setoutputsize(self, size: int, column: int = 0) -> None:
        """
        Sets a column buffer size for fetches of long columns.  However
        python-oracledb does not require it so this method does nothing.
        """
        pass

    @property
    def statement(self) -> Union[str, None]:
        """
        Returns the statement associated with the cursor, if one is present.
        """
        if self._impl is not None:
            return self._impl.statement

    def var(
        self,
        typ: Union[DbType, DbObjectType, type],
        size: int = 0,
        arraysize: int = 1,
        inconverter: Callable = None,
        outconverter: Callable = None,
        typename: str = None,
        encoding_errors: str = None,
        bypass_decode: bool = False,
        convert_nulls: bool = False,
        *,
        encodingErrors: str = None,
    ) -> "Var":
        """
        Create a variable with the specified characteristics. This method was
        designed for use with PL/SQL in/out variables where the length or type
        cannot be determined automatically from the Python object passed in or
        for use in input and output type handlers defined on cursors or
        connections.

        The typ parameter specifies the type of data that should be stored
        in the variable. This should be one of the database type constants, DB
        API constants, an object type returned from the method
        Connection.gettype() or one of the following Python types:

            Python Type         Database Type
            bool                DB_TYPE_BOOLEAN
            bytes               DB_TYPE_RAW
            datetime.date       DB_TYPE_DATE
            datetime.datetime   DB_TYPE_DATE
            datetime.timedelta  DB_TYPE_INTERVAL_DS
            decimal.Decimal     DB_TYPE_NUMBER
            float               DB_TYPE_NUMBER
            int                 DB_TYPE_NUMBER
            str                 DB_TYPE_VARCHAR

        The size parameter specifies the length of string and raw variables and
        is ignored in all other cases. If not specified for string and raw
        variables, the value 4000 is used.

        The arraysize parameter specifies the number of elements the variable
        will have. If not specified the bind array size (usually 1) is used.
        When a variable is created in an output type handler this parameter
        should be set to the cursor’s array size.

        The inconverter and outconverter parameters specify methods used for
        converting values to/from the database. More information can be found
        in the section on variable objects.

        The typename parameter specifies the name of a SQL object type and must
        be specified when using type DB_TYPE_OBJECT unless the type object
        was passed directly as the first parameter.

        The encoding_errors parameter specifies what should happen when
        decoding byte strings fetched from the database into strings. It should
        be one of the values noted in the builtin decode function.

        The bypass_decode parameter, if specified, should be passed as a
        boolean value. Passing a True value causes values of database types
        DB_TYPE_VARCHAR, DB_TYPE_CHAR, DB_TYPE_NVARCHAR, DB_TYPE_NCHAR and
        DB_TYPE_LONG to be returned as bytes instead of str, meaning that
        oracledb doesn't do any decoding.

        The convert_nulls parameter specifies whether the outconverter should
        be called when null values are fetched from the database.
        """
        self._verify_open()
        if typename is not None:
            typ = self.connection.gettype(typename)
        elif typ is DB_TYPE_OBJECT:
            errors._raise_err(errors.ERR_MISSING_TYPE_NAME_FOR_OBJECT_VAR)
        if encodingErrors is not None:
            if encoding_errors is not None:
                errors._raise_err(
                    errors.ERR_DUPLICATED_PARAMETER,
                    deprecated_name="encodingErrors",
                    new_name="encoding_errors",
                )
            encoding_errors = encodingErrors
        return self._impl.create_var(
            self.connection,
            typ,
            size,
            arraysize,
            inconverter,
            outconverter,
            encoding_errors,
            bypass_decode,
            convert_nulls=convert_nulls,
        )

    @property
    def warning(self) -> Union[errors._Error, None]:
        """
        Returns any warning that was generated during the last call to
        execute() or executemany(), or the value None if no warning was
        generated. This value will be cleared on the next call to execute() or
        executemany().
        """
        self._verify_open()
        return self._impl.warning


class Cursor(BaseCursor):
    __module__ = MODULE_NAME

    def __iter__(self):
        return self

    def __next__(self):
        self._verify_fetch()
        row = self._impl.fetch_next_row(self)
        if row is not None:
            return row
        raise StopIteration

    def _get_oci_attr(self, attr_num: int, attr_type: int) -> Any:
        """
        Returns the value of the specified OCI attribute from the internal
        handle. This is only supported in python-oracledb's thick mode and
        should only be used as directed by Oracle.
        """
        self._verify_open()
        return self._impl._get_oci_attr(attr_num, attr_type)

    def _set_oci_attr(self, attr_num: int, attr_type: int, value: Any) -> None:
        """
        Sets the value of the specified OCI attribute on the internal handle.
        This is only supported in python-oracledb's thick mode and should only
        be used as directed by Oracle.
        """
        self._verify_open()
        self._impl._set_oci_attr(attr_num, attr_type, value)

    def callfunc(
        self,
        name: str,
        return_type: Any,
        parameters: Optional[Union[list, tuple]] = None,
        keyword_parameters: Optional[dict] = None,
        *,
        keywordParameters: Optional[dict] = None,
    ) -> Any:
        """
        Call a function with the given name. The return type is specified in
        the same notation as is required by setinputsizes(). The sequence of
        parameters must contain one entry for each parameter that the function
        expects. Any keyword parameters will be included after the positional
        parameters. The result of the call is the return value of the function.
        """
        var = self.var(return_type)
        if keywordParameters is not None:
            if keyword_parameters is not None:
                errors._raise_err(
                    errors.ERR_DUPLICATED_PARAMETER,
                    deprecated_name="keywordParameters",
                    new_name="keyword_parameters",
                )
            keyword_parameters = keywordParameters
        self._call(name, parameters, keyword_parameters, var)
        return var.getvalue()

    def callproc(
        self,
        name: str,
        parameters: Optional[Union[list, tuple]] = None,
        keyword_parameters: Optional[dict] = None,
        *,
        keywordParameters: Optional[dict] = None,
    ) -> list:
        """
        Call a procedure with the given name. The sequence of parameters must
        contain one entry for each parameter that the procedure expects. The
        result of the call is a modified copy of the input sequence. Input
        parameters are left untouched; output and input/output parameters are
        replaced with possibly new values. Keyword parameters will be included
        after the positional parameters and are not returned as part of the
        output sequence.
        """
        if keywordParameters is not None:
            if keyword_parameters is not None:
                errors._raise_err(
                    errors.ERR_DUPLICATED_PARAMETER,
                    deprecated_name="keywordParameters",
                    new_name="keyword_parameters",
                )
            keyword_parameters = keywordParameters
        self._call(name, parameters, keyword_parameters)
        if parameters is None:
            return []
        return [
            v.get_value(0) for v in self._impl.bind_vars[: len(parameters)]
        ]

    def execute(
        self,
        statement: Optional[str],
        parameters: Optional[Union[list, tuple, dict]] = None,
        **keyword_parameters: Any,
    ) -> Any:
        """
        Execute a statement against the database.

        Parameters may be passed as a dictionary or sequence or as keyword
        parameters. If the parameters are a dictionary, the values will be
        bound by name and if the parameters are a sequence the values will be
        bound by position. Note that if the values are bound by position, the
        order of the variables is from left to right as they are encountered in
        the statement and SQL statements are processed differently than PL/SQL
        statements. For this reason, it is generally recommended to bind
        parameters by name instead of by position.

        Parameters passed as a dictionary are name and value pairs. The name
        maps to the bind variable name used by the statement and the value maps
        to the Python value you wish bound to that bind variable.

        A reference to the statement will be retained by the cursor. If None or
        the same string object is passed in again, the cursor will execute that
        statement again without performing a prepare or rebinding and
        redefining. This is most effective for algorithms where the same
        statement is used, but different parameters are bound to it (many
        times). Note that parameters that are not passed in during subsequent
        executions will retain the value passed in during the last execution
        that contained them.

        For maximum efficiency when reusing an statement, it is best to use the
        setinputsizes() method to specify the parameter types and sizes ahead
        of time; in particular, None is assumed to be a string of length 1 so
        any values that are later bound as numbers or dates will raise a
        TypeError exception.

        If the statement is a query, the cursor is returned as a convenience to
        the caller (so it can be used directly as an iterator over the rows in
        the cursor); otherwise, None is returned.
        """
        self._prepare_for_execute(statement, parameters, keyword_parameters)
        impl = self._impl
        impl.execute(self)
        if impl.fetch_vars is not None:
            return self

    def executemany(
        self,
        statement: Optional[str],
        parameters: Union[list, int],
        batcherrors: bool = False,
        arraydmlrowcounts: bool = False,
    ) -> None:
        """
        Prepare a statement for execution against a database and then execute
        it against all parameter mappings or sequences found in the sequence
        parameters.

        The statement is managed in the same way as the execute() method
        manages it. If the size of the buffers allocated for any of the
        parameters exceeds 2 GB and you are using the thick implementation, you
        will receive the error “DPI-1015: array size of <n> is too large”,
        where <n> varies with the size of each element being allocated in the
        buffer. If you receive this error, decrease the number of elements in
        the sequence parameters.

        If there are no parameters, or parameters have previously been bound,
        the number of iterations can be specified as an integer instead of
        needing to provide a list of empty mappings or sequences.

        When true, the batcherrors parameter enables batch error support within
        Oracle and ensures that the call succeeds even if an exception takes
        place in one or more of the sequence of parameters. The errors can then
        be retrieved using getbatcherrors().

        When true, the arraydmlrowcounts parameter enables DML row counts to be
        retrieved from Oracle after the method has completed. The row counts
        can then be retrieved using getarraydmlrowcounts().

        Both the batcherrors parameter and the arraydmlrowcounts parameter can
        only be true when executing an insert, update, delete or merge
        statement; in all other cases an error will be raised.

        For maximum efficiency, it is best to use the setinputsizes() method to
        specify the parameter types and sizes ahead of time; in particular,
        None is assumed to be a string of length 1 so any values that are later
        bound as numbers or dates will raise a TypeError exception.
        """
        self._verify_open()
        num_execs = self._impl._prepare_for_executemany(
            self, statement, parameters
        )
        self._impl.executemany(
            self, num_execs, bool(batcherrors), bool(arraydmlrowcounts)
        )

    def fetchall(self) -> list:
        """
        Fetch all (remaining) rows of a query result, returning them as a list
        of tuples. An empty list is returned if no more rows are available.
        Note that the cursor’s arraysize attribute can affect the performance
        of this operation, as internally reads from the database are done in
        batches corresponding to the arraysize.

        An exception is raised if the previous call to execute() did not
        produce any result set or no call was issued yet.
        """
        self._verify_fetch()
        result = []
        fetch_next_row = self._impl.fetch_next_row
        while True:
            row = fetch_next_row(self)
            if row is None:
                break
            result.append(row)
        return result

    def fetchmany(
        self, size: Optional[int] = None, numRows: Optional[int] = None
    ) -> list:
        """
        Fetch the next set of rows of a query result, returning a list of
        tuples. An empty list is returned if no more rows are available. Note
        that the cursor’s arraysize attribute can affect the performance of
        this operation.

        The number of rows to fetch is specified by the parameter (the second
        parameter is retained for backwards compatibility and should not be
        used). If it is not given, the cursor’s arraysize attribute determines
        the number of rows to be fetched. If the number of rows available to be
        fetched is fewer than the amount requested, fewer rows will be
        returned.

        An exception is raised if the previous call to execute() did not
        produce any result set or no call was issued yet.
        """
        self._verify_fetch()
        if size is None:
            if numRows is not None:
                size = numRows
            else:
                size = self._impl.arraysize
        elif numRows is not None:
            errors._raise_err(
                errors.ERR_DUPLICATED_PARAMETER,
                deprecated_name="numRows",
                new_name="size",
            )
        result = []
        fetch_next_row = self._impl.fetch_next_row
        while len(result) < size:
            row = fetch_next_row(self)
            if row is None:
                break
            result.append(row)
        return result

    def fetchone(self) -> Any:
        """
        Fetch the next row of a query result set, returning a single tuple or
        None when no more data is available.

        An exception is raised if the previous call to execute() did not
        produce any result set or no call was issued yet.
        """
        self._verify_fetch()
        return self._impl.fetch_next_row(self)

    def parse(self, statement: str) -> None:
        """
        This can be used to parse a statement without actually executing it
        (this step is done automatically by Oracle when a statement is
        executed).
        """
        self._verify_open()
        self._prepare(statement)
        self._impl.parse(self)

    def scroll(self, value: int = 0, mode: str = "relative") -> None:
        """
        Scroll the cursor in the result set to a new position according to the
        mode.

        If mode is “relative” (the default value), the value is taken as an
        offset to the current position in the result set. If set to “absolute”,
        value states an absolute target position. If set to “first”, the cursor
        is positioned at the first row and if set to “last”, the cursor is set
        to the last row in the result set.

        An error is raised if the mode is “relative” or “absolute” and the
        scroll operation would position the cursor outside of the result set.
        """
        self._verify_open()
        self._impl.scroll(self.connection, value, mode)


class AsyncCursor(BaseCursor):
    __module__ = MODULE_NAME

    async def __aenter__(self):
        self._verify_open()
        return self

    async def __aexit__(self, *exc_info):
        self._verify_open()
        self._impl.close(in_del=True)
        self._impl = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        self._verify_fetch()
        row = await self._impl.fetch_next_row(self)
        if row is not None:
            return row
        raise StopAsyncIteration

    async def callfunc(
        self,
        name: str,
        return_type: Any,
        parameters: Optional[Union[list, tuple]] = None,
        keyword_parameters: Optional[dict] = None,
    ) -> Any:
        """
        Call a function with the given name. The return type is specified in
        the same notation as is required by setinputsizes(). The sequence of
        parameters must contain one entry for each parameter that the function
        expects. Any keyword parameters will be included after the positional
        parameters. The result of the call is the return value of the function.
        """
        var = self.var(return_type)
        await self._call(name, parameters, keyword_parameters, var)
        return var.getvalue()

    async def callproc(
        self,
        name: str,
        parameters: Optional[Union[list, tuple]] = None,
        keyword_parameters: Optional[dict] = None,
    ) -> list:
        """
        Call a procedure with the given name. The sequence of parameters must
        contain one entry for each parameter that the procedure expects. The
        result of the call is a modified copy of the input sequence. Input
        parameters are left untouched; output and input/output parameters are
        replaced with possibly new values. Keyword parameters will be included
        after the positional parameters and are not returned as part of the
        output sequence.
        """
        await self._call(name, parameters, keyword_parameters)
        if parameters is None:
            return []
        return [
            v.get_value(0) for v in self._impl.bind_vars[: len(parameters)]
        ]

    async def execute(
        self,
        statement: Optional[str],
        parameters: Optional[Union[list, tuple, dict]] = None,
        **keyword_parameters: Any,
    ) -> None:
        """
        Execute a statement against the database.

        Parameters may be passed as a dictionary or sequence or as keyword
        parameters. If the parameters are a dictionary, the values will be
        bound by name and if the parameters are a sequence the values will be
        bound by position. Note that if the values are bound by position, the
        order of the variables is from left to right as they are encountered in
        the statement and SQL statements are processed differently than PL/SQL
        statements. For this reason, it is generally recommended to bind
        parameters by name instead of by position.

        Parameters passed as a dictionary are name and value pairs. The name
        maps to the bind variable name used by the statement and the value maps
        to the Python value you wish bound to that bind variable.

        A reference to the statement will be retained by the cursor. If None or
        the same string object is passed in again, the cursor will execute that
        statement again without performing a prepare or rebinding and
        redefining. This is most effective for algorithms where the same
        statement is used, but different parameters are bound to it (many
        times). Note that parameters that are not passed in during subsequent
        executions will retain the value passed in during the last execution
        that contained them.

        For maximum efficiency when reusing an statement, it is best to use the
        setinputsizes() method to specify the parameter types and sizes ahead
        of time; in particular, None is assumed to be a string of length 1 so
        any values that are later bound as numbers or dates will raise a
        TypeError exception.
        """
        self._prepare_for_execute(statement, parameters, keyword_parameters)
        await self._impl.execute(self)

    async def executemany(
        self,
        statement: Optional[str],
        parameters: Union[list, int],
        batcherrors: bool = False,
        arraydmlrowcounts: bool = False,
    ) -> None:
        """
        Prepare a statement for execution against a database and then execute
        it against all parameter mappings or sequences found in the sequence
        parameters.

        The statement is managed in the same way as the execute() method
        manages it. If the size of the buffers allocated for any of the
        parameters exceeds 2 GB and you are using the thick implementation, you
        will receive the error “DPI-1015: array size of <n> is too large”,
        where <n> varies with the size of each element being allocated in the
        buffer. If you receive this error, decrease the number of elements in
        the sequence parameters.

        If there are no parameters, or parameters have previously been bound,
        the number of iterations can be specified as an integer instead of
        needing to provide a list of empty mappings or sequences.

        When true, the batcherrors parameter enables batch error support within
        Oracle and ensures that the call succeeds even if an exception takes
        place in one or more of the sequence of parameters. The errors can then
        be retrieved using getbatcherrors().

        When true, the arraydmlrowcounts parameter enables DML row counts to be
        retrieved from Oracle after the method has completed. The row counts
        can then be retrieved using getarraydmlrowcounts().

        Both the batcherrors parameter and the arraydmlrowcounts parameter can
        only be true when executing an insert, update, delete or merge
        statement; in all other cases an error will be raised.

        For maximum efficiency, it is best to use the setinputsizes() method to
        specify the parameter types and sizes ahead of time; in particular,
        None is assumed to be a string of length 1 so any values that are later
        bound as numbers or dates will raise a TypeError exception.
        """
        self._verify_open()
        num_execs = self._impl._prepare_for_executemany(
            self, statement, parameters
        )
        await self._impl.executemany(
            self, num_execs, bool(batcherrors), bool(arraydmlrowcounts)
        )

    async def fetchall(self) -> list:
        """
        Fetch all (remaining) rows of a query result, returning them as a list
        of tuples. An empty list is returned if no more rows are available.
        Note that the cursor’s arraysize attribute can affect the performance
        of this operation, as internally reads from the database are done in
        batches corresponding to the arraysize.

        An exception is raised if the previous call to execute() did not
        produce any result set or no call was issued yet.
        """
        self._verify_fetch()
        result = []
        fetch_next_row = self._impl.fetch_next_row
        while True:
            row = await fetch_next_row(self)
            if row is None:
                break
            result.append(row)
        return result

    async def fetchmany(self, size: Optional[int] = None) -> list:
        """
        Fetch the next set of rows of a query result, returning a list of
        tuples. An empty list is returned if no more rows are available. Note
        that the cursor’s arraysize attribute can affect the performance of
        this operation.

        The number of rows to fetch is specified by the parameter (the second
        parameter is retained for backwards compatibility and should not be
        used). If it is not given, the cursor’s arraysize attribute determines
        the number of rows to be fetched. If the number of rows available to be
        fetched is fewer than the amount requested, fewer rows will be
        returned.

        An exception is raised if the previous call to execute() did not
        produce any result set or no call was issued yet.
        """
        self._verify_fetch()
        if size is None:
            size = self._impl.arraysize
        result = []
        fetch_next_row = self._impl.fetch_next_row
        while len(result) < size:
            row = await fetch_next_row(self)
            if row is None:
                break
            result.append(row)
        return result

    async def fetchone(self) -> Any:
        """
        Fetch the next row of a query result set, returning a single tuple or
        None when no more data is available.

        An exception is raised if the previous call to execute() did not
        produce any result set or no call was issued yet.
        """
        self._verify_fetch()
        return await self._impl.fetch_next_row(self)

    async def parse(self, statement: str) -> None:
        """
        This can be used to parse a statement without actually executing it
        (this step is done automatically by Oracle when a statement is
        executed).
        """
        self._verify_open()
        self._prepare(statement)
        await self._impl.parse(self)
