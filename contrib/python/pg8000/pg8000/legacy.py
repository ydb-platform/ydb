from datetime import date as Date, time as Time
from itertools import islice
from warnings import warn

import pg8000
from pg8000.converters import (
    BIGINT,
    BOOLEAN,
    BOOLEAN_ARRAY,
    BYTES,
    CHAR,
    CHAR_ARRAY,
    DATE,
    FLOAT,
    FLOAT_ARRAY,
    INET,
    INT2VECTOR,
    INTEGER,
    INTEGER_ARRAY,
    INTERVAL,
    JSON,
    JSONB,
    MACADDR,
    NAME,
    NAME_ARRAY,
    NULLTYPE,
    NUMERIC,
    NUMERIC_ARRAY,
    OID,
    PGInterval,
    PY_PG,
    Range,
    STRING,
    TEXT,
    TEXT_ARRAY,
    TIME,
    TIMESTAMP,
    TIMESTAMPTZ,
    UNKNOWN,
    UUID_TYPE,
    VARCHAR,
    VARCHAR_ARRAY,
    XID,
    interval_in as timedelta_in,
    make_params,
    pg_interval_in as pginterval_in,
    pg_interval_out as pginterval_out,
)
from pg8000.core import (
    Context,
    CoreConnection,
    IN_FAILED_TRANSACTION,
    IN_TRANSACTION,
    ver,
)
from pg8000.dbapi import (
    BINARY,
    Binary,
    DataError,
    DateFromTicks,
    IntegrityError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
    Warning,
    convert_paramstyle,
)
from pg8000.exceptions import DatabaseError, Error, InterfaceError

__version__ = ver

# Copyright (c) 2007-2009, Mathieu Fenniak
# Copyright (c) The Contributors
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
# * Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
# * The name of the author may not be used to endorse or promote products
# derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

__author__ = "Mathieu Fenniak"


BIGINTEGER = BIGINT
DATETIME = TIMESTAMP
NUMBER = DECIMAL = NUMERIC
DECIMAL_ARRAY = NUMERIC_ARRAY
ROWID = OID
TIMEDELTA = INTERVAL


def connect(
    user,
    host="localhost",
    database=None,
    port=5432,
    password=None,
    source_address=None,
    unix_sock=None,
    ssl_context=None,
    timeout=None,
    tcp_keepalive=True,
    application_name=None,
    replication=None,
):
    return Connection(
        user,
        host=host,
        database=database,
        port=port,
        password=password,
        source_address=source_address,
        unix_sock=unix_sock,
        ssl_context=ssl_context,
        timeout=timeout,
        tcp_keepalive=tcp_keepalive,
        application_name=application_name,
        replication=replication,
    )


apilevel = "2.0"
"""The DBAPI level supported, currently "2.0".

This property is part of the `DBAPI 2.0 specification
<http://www.python.org/dev/peps/pep-0249/>`_.
"""

threadsafety = 1
"""Integer constant stating the level of thread safety the DBAPI interface
supports. This DBAPI module supports sharing of the module only. Connections
and cursors my not be shared between threads. This gives pg8000 a threadsafety
value of 1.

This property is part of the `DBAPI 2.0 specification
<http://www.python.org/dev/peps/pep-0249/>`_.
"""

paramstyle = "format"


class Cursor:
    def __init__(self, connection, paramstyle=None):
        self._c = connection
        self.arraysize = 1
        if paramstyle is None:
            self.paramstyle = pg8000.paramstyle
        else:
            self.paramstyle = paramstyle

        self._context = None
        self._row_iter = None

        self._input_oids = ()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @property
    def connection(self):
        warn("DB-API extension cursor.connection used", stacklevel=3)
        return self._c

    @property
    def rowcount(self):
        context = self._context
        if context is None:
            return -1

        return context.row_count

    description = property(lambda self: self._getDescription())

    def _getDescription(self):
        context = self._context
        if context is None:
            return None
        row_desc = context.columns
        if row_desc is None:
            return None
        if len(row_desc) == 0:
            return None
        columns = []
        for col in row_desc:
            columns.append((col["name"], col["type_oid"], None, None, None, None, None))
        return columns

    ##
    # Executes a database operation.  Parameters may be provided as a sequence
    # or mapping and will be bound to variables in the operation.
    # <p>
    # Stability: Part of the DBAPI 2.0 specification.
    def execute(self, operation, args=(), stream=None):
        """Executes a database operation.  Parameters may be provided as a
        sequence, or as a mapping, depending upon the value of
        :data:`pg8000.paramstyle`.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

        :param operation:
            The SQL statement to execute.

        :param args:
            If :data:`paramstyle` is ``qmark``, ``numeric``, or ``format``,
            this argument should be an array of parameters to bind into the
            statement.  If :data:`paramstyle` is ``named``, the argument should
            be a dict mapping of parameters.  If the :data:`paramstyle` is
            ``pyformat``, the argument value may be either an array or a
            mapping.

        :param stream: This is a pg8000 extension for use with the PostgreSQL
            `COPY
            <http://www.postgresql.org/docs/current/static/sql-copy.html>`_
            command. For a COPY FROM the parameter must be a readable file-like
            object, and for COPY TO it must be writable.

            .. versionadded:: 1.9.11
        """
        try:
            if not self._c._in_transaction and not self._c.autocommit:
                self._c.execute_simple("begin transaction")

            if len(args) == 0 and stream is None:
                self._context = self._c.execute_simple(operation)
            else:
                statement, vals = convert_paramstyle(self.paramstyle, operation, args)
                self._context = self._c.execute_unnamed(
                    statement, vals=vals, oids=self._input_oids, stream=stream
                )

            rows = [] if self._context.rows is None else self._context.rows
            self._row_iter = iter(rows)

            self._input_oids = ()
        except AttributeError as e:
            if self._c is None:
                raise InterfaceError("Cursor closed")
            elif self._c._sock is None:
                raise InterfaceError("connection is closed")
            else:
                raise e
        except DatabaseError as e:
            msg = e.args[0]
            if isinstance(msg, dict):
                response_code = msg["C"]

                if response_code == "28000":
                    cls = InterfaceError
                elif response_code == "23505":
                    cls = IntegrityError
                else:
                    cls = ProgrammingError

                raise cls(msg)
            else:
                raise ProgrammingError(msg)

        self.input_types = []
        return self

    def executemany(self, operation, param_sets):
        """Prepare a database operation, and then execute it against all
        parameter sequences or mappings provided.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

        :param operation:
            The SQL statement to execute
        :param parameter_sets:
            A sequence of parameters to execute the statement with. The values
            in the sequence should be sequences or mappings of parameters, the
            same as the args argument of the :meth:`execute` method.
        """
        rowcounts = []
        input_oids = self._input_oids
        for parameters in param_sets:
            self._input_oids = input_oids
            self.execute(operation, parameters)
            rowcounts.append(self._context.row_count)

        if len(rowcounts) == 0:
            self._context = Context(None)
        elif -1 in rowcounts:
            self._context.row_count = -1
        else:
            self._context.row_count = sum(rowcounts)

        return self

    def fetchone(self):
        """Fetch the next row of a query result set.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

        :returns:
            A row as a sequence of field values, or ``None`` if no more rows
            are available.
        """
        try:
            return next(self)
        except StopIteration:
            return None
        except TypeError:
            raise ProgrammingError("attempting to use unexecuted cursor")
        except AttributeError:
            raise ProgrammingError("attempting to use unexecuted cursor")

    def fetchmany(self, num=None):
        """Fetches the next set of rows of a query result.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

        :param size:

            The number of rows to fetch when called.  If not provided, the
            :attr:`arraysize` attribute value is used instead.

        :returns:

            A sequence, each entry of which is a sequence of field values
            making up a row.  If no more rows are available, an empty sequence
            will be returned.
        """
        try:
            return tuple(islice(self, self.arraysize if num is None else num))
        except TypeError:
            raise ProgrammingError("attempting to use unexecuted cursor")

    def fetchall(self):
        """Fetches all remaining rows of a query result.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

        :returns:

            A sequence, each entry of which is a sequence of field values
            making up a row.
        """
        try:
            return tuple(self)
        except TypeError:
            raise ProgrammingError("attempting to use unexecuted cursor")

    def close(self):
        """Closes the cursor.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        self._c = None

    def __iter__(self):
        """A cursor object is iterable to retrieve the rows from a query.

        This is a DBAPI 2.0 extension.
        """
        return self

    def setinputsizes(self, *sizes):
        """This method is part of the `DBAPI 2.0 specification"""
        oids = []
        for size in sizes:
            if isinstance(size, int):
                oid = size
            else:
                try:
                    oid = PY_PG[size]
                except KeyError:
                    oid = UNKNOWN
            oids.append(oid)

        self._input_oids = oids

    def setoutputsize(self, size, column=None):
        """This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_, however, it is not
        implemented by pg8000.
        """
        pass

    def __next__(self):
        try:
            return next(self._row_iter)
        except AttributeError:
            if self._context is None:
                raise ProgrammingError("A query hasn't been issued.")
            else:
                raise
        except StopIteration as e:
            if self._context is None:
                raise ProgrammingError("A query hasn't been issued.")
            elif len(self._context.columns) == 0:
                raise ProgrammingError("no result set")
            else:
                raise e


class Connection(CoreConnection):
    # DBAPI Extension: supply exceptions as attributes on the connection
    Warning = property(lambda self: self._getError(Warning))
    Error = property(lambda self: self._getError(Error))
    InterfaceError = property(lambda self: self._getError(InterfaceError))
    DatabaseError = property(lambda self: self._getError(DatabaseError))
    OperationalError = property(lambda self: self._getError(OperationalError))
    IntegrityError = property(lambda self: self._getError(IntegrityError))
    InternalError = property(lambda self: self._getError(InternalError))
    ProgrammingError = property(lambda self: self._getError(ProgrammingError))
    NotSupportedError = property(lambda self: self._getError(NotSupportedError))

    def __init__(self, *args, **kwargs):
        try:
            super().__init__(*args, **kwargs)
        except DatabaseError as e:
            msg = e.args[0]
            if isinstance(msg, dict):
                response_code = msg["C"]

                if response_code == "28000":
                    cls = InterfaceError
                elif response_code == "23505":
                    cls = IntegrityError
                else:
                    cls = ProgrammingError

                raise cls(msg)
            else:
                raise ProgrammingError(msg)

        self._run_cursor = Cursor(self, paramstyle="named")
        self.autocommit = False

    def _getError(self, error):
        warn("DB-API extension connection.%s used" % error.__name__, stacklevel=3)
        return error

    def cursor(self):
        """Creates a :class:`Cursor` object bound to this
        connection.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        return Cursor(self)

    @property
    def description(self):
        return self._run_cursor._getDescription()

    @property
    def _in_transaction(self):
        return self._transaction_status in (IN_TRANSACTION, IN_FAILED_TRANSACTION)

    def commit(self):
        """Commits the current database transaction.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        self.execute_unnamed("commit")

    def rollback(self):
        """Rolls back the current database transaction.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        if not self._in_transaction:
            return
        self.execute_unnamed("rollback")

    def run(self, sql, stream=None, **params):
        self._run_cursor.execute(sql, params, stream=stream)
        if self._run_cursor._context.rows is None:
            return tuple()
        else:
            return tuple(self._run_cursor._context.rows)

    def prepare(self, operation):
        return PreparedStatement(self, operation)

    def xid(self, format_id, global_transaction_id, branch_qualifier):
        """Create a Transaction IDs (only global_transaction_id is used in pg)
        format_id and branch_qualifier are not used in postgres
        global_transaction_id may be any string identifier supported by
        postgres returns a tuple
        (format_id, global_transaction_id, branch_qualifier)"""
        return (format_id, global_transaction_id, branch_qualifier)

    def tpc_begin(self, xid):
        """Begins a TPC transaction with the given transaction ID xid.

        This method should be called outside of a transaction (i.e. nothing may
        have executed since the last .commit() or .rollback()).

        Furthermore, it is an error to call .commit() or .rollback() within the
        TPC transaction. A ProgrammingError is raised, if the application calls
        .commit() or .rollback() during an active TPC transaction.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        self._xid = xid
        if self.autocommit:
            self.execute_unnamed("begin transaction")

    def tpc_prepare(self):
        """Performs the first phase of a transaction started with .tpc_begin().
        A ProgrammingError is be raised if this method is called outside of a
        TPC transaction.

        After calling .tpc_prepare(), no statements can be executed until
        .tpc_commit() or .tpc_rollback() have been called.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        q = "PREPARE TRANSACTION '%s';" % (self._xid[1],)
        self.execute_unnamed(q)

    def tpc_commit(self, xid=None):
        """When called with no arguments, .tpc_commit() commits a TPC
        transaction previously prepared with .tpc_prepare().

        If .tpc_commit() is called prior to .tpc_prepare(), a single phase
        commit is performed. A transaction manager may choose to do this if
        only a single resource is participating in the global transaction.

        When called with a transaction ID xid, the database commits the given
        transaction. If an invalid transaction ID is provided, a
        ProgrammingError will be raised. This form should be called outside of
        a transaction, and is intended for use in recovery.

        On return, the TPC transaction is ended.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        if xid is None:
            xid = self._xid

        if xid is None:
            raise ProgrammingError("Cannot tpc_commit() without a TPC transaction!")

        try:
            previous_autocommit_mode = self.autocommit
            self.autocommit = True
            if xid in self.tpc_recover():
                self.execute_unnamed("COMMIT PREPARED '%s';" % (xid[1],))
            else:
                # a single-phase commit
                self.commit()
        finally:
            self.autocommit = previous_autocommit_mode
        self._xid = None

    def tpc_rollback(self, xid=None):
        """When called with no arguments, .tpc_rollback() rolls back a TPC
        transaction. It may be called before or after .tpc_prepare().

        When called with a transaction ID xid, it rolls back the given
        transaction. If an invalid transaction ID is provided, a
        ProgrammingError is raised. This form should be called outside of a
        transaction, and is intended for use in recovery.

        On return, the TPC transaction is ended.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        if xid is None:
            xid = self._xid

        if xid is None:
            raise ProgrammingError(
                "Cannot tpc_rollback() without a TPC prepared transaction!"
            )

        try:
            previous_autocommit_mode = self.autocommit
            self.autocommit = True
            if xid in self.tpc_recover():
                # a two-phase rollback
                self.execute_unnamed("ROLLBACK PREPARED '%s';" % (xid[1],))
            else:
                # a single-phase rollback
                self.rollback()
        finally:
            self.autocommit = previous_autocommit_mode
        self._xid = None

    def tpc_recover(self):
        """Returns a list of pending transaction IDs suitable for use with
        .tpc_commit(xid) or .tpc_rollback(xid).

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        try:
            previous_autocommit_mode = self.autocommit
            self.autocommit = True
            curs = self.cursor()
            curs.execute("select gid FROM pg_prepared_xacts")
            return [self.xid(0, row[0], "") for row in curs]
        finally:
            self.autocommit = previous_autocommit_mode


def to_statement(query):
    OUTSIDE = 0  # outside quoted string
    INSIDE_SQ = 1  # inside single-quote string '...'
    INSIDE_QI = 2  # inside quoted identifier   "..."
    INSIDE_ES = 3  # inside escaped single-quote string, E'...'
    INSIDE_PN = 4  # inside parameter name eg. :name
    INSIDE_CO = 5  # inside inline comment eg. --

    in_quote_escape = False
    placeholders = []
    output_query = []
    state = OUTSIDE
    prev_c = None
    for i, c in enumerate(query):
        if i + 1 < len(query):
            next_c = query[i + 1]
        else:
            next_c = None

        if state == OUTSIDE:
            if c == "'":
                output_query.append(c)
                if prev_c == "E":
                    state = INSIDE_ES
                else:
                    state = INSIDE_SQ
            elif c == '"':
                output_query.append(c)
                state = INSIDE_QI
            elif c == "-":
                output_query.append(c)
                if prev_c == "-":
                    state = INSIDE_CO
            elif c == ":" and next_c not in ":=" and prev_c != ":":
                state = INSIDE_PN
                placeholders.append("")
            else:
                output_query.append(c)

        elif state == INSIDE_SQ:
            if c == "'":
                if in_quote_escape:
                    in_quote_escape = False
                else:
                    if next_c == "'":
                        in_quote_escape = True
                    else:
                        state = OUTSIDE
            output_query.append(c)

        elif state == INSIDE_QI:
            if c == '"':
                state = OUTSIDE
            output_query.append(c)

        elif state == INSIDE_ES:
            if c == "'" and prev_c != "\\":
                # check for escaped single-quote
                state = OUTSIDE
            output_query.append(c)

        elif state == INSIDE_PN:
            placeholders[-1] += c
            if next_c is None or (not next_c.isalnum() and next_c != "_"):
                state = OUTSIDE
                try:
                    pidx = placeholders.index(placeholders[-1], 0, -1)
                    output_query.append("$" + str(pidx + 1))
                    del placeholders[-1]
                except ValueError:
                    output_query.append("$" + str(len(placeholders)))

        elif state == INSIDE_CO:
            output_query.append(c)
            if c == "\n":
                state = OUTSIDE

        prev_c = c

    def make_vals(args):
        return tuple(args[p] for p in placeholders)

    return "".join(output_query), make_vals


class PreparedStatement:
    def __init__(self, con, operation):
        self.con = con
        self.operation = operation
        statement, self.make_args = to_statement(operation)
        self.name_bin, self.row_desc, self.input_funcs = con.prepare_statement(
            statement, ()
        )

    def run(self, **vals):
        params = make_params(self.con.py_types, self.make_args(vals))

        try:
            if not self.con._in_transaction and not self.con.autocommit:
                self.con.execute_unnamed("begin transaction")
            self._context = self.con.execute_named(
                self.name_bin, params, self.row_desc, self.input_funcs, self.operation
            )
        except AttributeError as e:
            if self.con is None:
                raise InterfaceError("Cursor closed")
            elif self.con._sock is None:
                raise InterfaceError("connection is closed")
            else:
                raise e

        return tuple() if self._context.rows is None else tuple(self._context.rows)

    def close(self):
        self.con.close_prepared_statement(self.name_bin)
        self.con = None


__all__ = [
    "BIGINTEGER",
    "BINARY",
    "BOOLEAN",
    "BOOLEAN_ARRAY",
    "BYTES",
    "Binary",
    "CHAR",
    "CHAR_ARRAY",
    "Connection",
    "Cursor",
    "DATE",
    "DATETIME",
    "DECIMAL",
    "DECIMAL_ARRAY",
    "DataError",
    "DatabaseError",
    "Date",
    "DateFromTicks",
    "Error",
    "FLOAT",
    "FLOAT_ARRAY",
    "INET",
    "INT2VECTOR",
    "INTEGER",
    "INTEGER_ARRAY",
    "INTERVAL",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "JSON",
    "JSONB",
    "MACADDR",
    "NAME",
    "NAME_ARRAY",
    "NULLTYPE",
    "NUMBER",
    "NotSupportedError",
    "OID",
    "OperationalError",
    "PGInterval",
    "ProgrammingError",
    "ROWID",
    "Range",
    "STRING",
    "TEXT",
    "TEXT_ARRAY",
    "TIME",
    "TIMEDELTA",
    "TIMESTAMP",
    "TIMESTAMPTZ",
    "Time",
    "TimeFromTicks",
    "Timestamp",
    "TimestampFromTicks",
    "UNKNOWN",
    "UUID_TYPE",
    "VARCHAR",
    "VARCHAR_ARRAY",
    "Warning",
    "XID",
    "connect",
    "pginterval_in",
    "pginterval_out",
    "timedelta_in",
]
