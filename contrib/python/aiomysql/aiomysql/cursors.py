import re
import json
import warnings
import contextlib

from pymysql.err import (
    Warning, Error, InterfaceError, DataError,
    DatabaseError, OperationalError, IntegrityError, InternalError,
    NotSupportedError, ProgrammingError)

from .log import logger
from .connection import FIELD_TYPE

# https://github.com/PyMySQL/PyMySQL/blob/d7bb777e503d82bf2496113f07dd4ab249615efc/pymysql/cursors.py#L6-L14

#: Regular expression for :meth:`Cursor.executemany`.
#: executemany only supports simple bulk insert.
#: You can use it to load large dataset.
RE_INSERT_VALUES = re.compile(
    r"\s*((?:INSERT|REPLACE)\s.+\sVALUES?\s+)" +
    r"(\(\s*(?:%s|%\(.+\)s)\s*(?:,\s*(?:%s|%\(.+\)s)\s*)*\))" +
    r"(\s*(?:ON DUPLICATE.*)?);?\s*\Z",
    re.IGNORECASE | re.DOTALL)


class Cursor:
    """Cursor is used to interact with the database."""

    #: Max statement size which :meth:`executemany` generates.
    #:
    #: Max size of allowed statement is max_allowed_packet -
    # packet_header_size.
    #: Default value of max_allowed_packet is 1048576.
    max_stmt_length = 1024000

    def __init__(self, connection, echo=False):
        """Do not create an instance of a Cursor yourself. Call
        connections.Connection.cursor().
        """
        self._connection = connection
        self._loop = self._connection.loop
        self._description = None
        self._rownumber = 0
        self._rowcount = -1
        self._arraysize = 1
        self._executed = None
        self._result = None
        self._rows = None
        self._lastrowid = None
        self._echo = echo

    @property
    def connection(self):
        """This read-only attribute return a reference to the Connection
        object on which the cursor was created."""
        return self._connection

    @property
    def description(self):
        """This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences is a collections.namedtuple containing
        information describing one result column:

        0.  name: the name of the column returned.
        1.  type_code: the type of the column.
        2.  display_size: the actual length of the column in bytes.
        3.  internal_size: the size in bytes of the column associated to
            this column on the server.
        4.  precision: total number of significant digits in columns of
            type NUMERIC. None for other types.
        5.  scale: count of decimal digits in the fractional part in
            columns of type NUMERIC. None for other types.
        6.  null_ok: always None as not easy to retrieve from the libpq.

        This attribute will be None for operations that do not
        return rows or if the cursor has not had an operation invoked
        via the execute() method yet.
        """
        return self._description

    @property
    def rowcount(self):
        """Returns the number of rows that has been produced of affected.

        This read-only attribute specifies the number of rows that the
        last :meth:`execute` produced (for Data Query Language
        statements like SELECT) or affected (for Data Manipulation
        Language statements like UPDATE or INSERT).

        The attribute is -1 in case no .execute() has been performed
        on the cursor or the row count of the last operation if it
        can't be determined by the interface.
        """
        return self._rowcount

    @property
    def rownumber(self):
        """Row index.

        This read-only attribute provides the current 0-based index of the
        cursor in the result set or ``None`` if the index cannot be
        determined.
        """

        return self._rownumber

    @property
    def arraysize(self):
        """How many rows will be returned by fetchmany() call.

        This read/write attribute specifies the number of rows to
        fetch at a time with fetchmany(). It defaults to
        1 meaning to fetch a single row at a time.

        """
        return self._arraysize

    @arraysize.setter
    def arraysize(self, val):
        """How many rows will be returned by fetchmany() call.

        This read/write attribute specifies the number of rows to
        fetch at a time with fetchmany(). It defaults to
        1 meaning to fetch a single row at a time.

        """
        self._arraysize = val

    @property
    def lastrowid(self):
        """This read-only property returns the value generated for an
        AUTO_INCREMENT column by the previous INSERT or UPDATE statement
        or None when there is no such value available. For example,
        if you perform an INSERT into a table that contains an AUTO_INCREMENT
        column, lastrowid returns the AUTO_INCREMENT value for the new row.
        """
        return self._lastrowid

    @property
    def echo(self):
        """Return echo mode status."""
        return self._echo

    @property
    def closed(self):
        """The readonly property that returns ``True`` if connections was
        detached from current cursor
        """
        return True if not self._connection else False

    async def close(self):
        """Closing a cursor just exhausts all remaining data."""
        conn = self._connection
        if conn is None:
            return
        try:
            while (await self.nextset()):
                pass
        finally:
            self._connection = None

    def _get_db(self):
        if not self._connection:
            raise ProgrammingError("Cursor closed")
        return self._connection

    def _check_executed(self):
        if not self._executed:
            raise ProgrammingError("execute() first")

    def _conv_row(self, row):
        return row

    def setinputsizes(self, *args):
        """Does nothing, required by DB API."""

    def setoutputsizes(self, *args):
        """Does nothing, required by DB API."""

    async def nextset(self):
        """Get the next query set"""
        conn = self._get_db()
        current_result = self._result
        if current_result is None or current_result is not conn._result:
            return
        if not current_result.has_next:
            return
        self._result = None
        self._clear_result()
        await conn.next_result()
        await self._do_get_result()
        return True

    def _escape_args(self, args, conn):
        if isinstance(args, (tuple, list)):
            return tuple(conn.escape(arg) for arg in args)
        elif isinstance(args, dict):
            return {key: conn.escape(val) for (key, val) in args.items()}
        else:
            # If it's not a dictionary let's try escaping it anyways.
            # Worst case it will throw a Value error
            return conn.escape(args)

    def mogrify(self, query, args=None):
        """ Returns the exact string that is sent to the database by calling
        the execute() method. This method follows the extension to the DB
        API 2.0 followed by Psycopg.

        :param query: ``str`` sql statement
        :param args: ``tuple`` or ``list`` of arguments for sql query
        """
        conn = self._get_db()
        if args is not None:
            query = query % self._escape_args(args, conn)
        return query

    async def execute(self, query, args=None):
        """Executes the given operation

        Executes the given operation substituting any markers with
        the given parameters.

        For example, getting all rows where id is 5:
          cursor.execute("SELECT * FROM t1 WHERE id = %s", (5,))

        :param query: ``str`` sql statement
        :param args: ``tuple`` or ``list`` of arguments for sql query
        :returns: ``int``, number of rows that has been produced of affected
        """
        conn = self._get_db()

        while (await self.nextset()):
            pass

        if args is not None:
            query = query % self._escape_args(args, conn)

        await self._query(query)
        self._executed = query
        if self._echo:
            logger.info(query)
            logger.info("%r", args)
        return self._rowcount

    async def executemany(self, query, args):
        """Execute the given operation multiple times

        The executemany() method will execute the operation iterating
        over the list of parameters in seq_params.

        Example: Inserting 3 new employees and their phone number

            data = [
                ('Jane','555-001'),
                ('Joe', '555-001'),
                ('John', '555-003')
                ]
            stmt = "INSERT INTO employees (name, phone) VALUES ('%s','%s')"
            await cursor.executemany(stmt, data)

        INSERT or REPLACE statements are optimized by batching the data,
        that is using the MySQL multiple rows syntax.

        :param query: `str`, sql statement
        :param args: ``tuple`` or ``list`` of arguments for sql query
        """
        if not args:
            return

        if self._echo:
            logger.info("CALL %s", query)
            logger.info("%r", args)

        m = RE_INSERT_VALUES.match(query)
        if m:
            q_prefix = m.group(1) % ()
            q_values = m.group(2).rstrip()
            q_postfix = m.group(3) or ''
            assert q_values[0] == '(' and q_values[-1] == ')'
            return (await self._do_execute_many(
                q_prefix, q_values, q_postfix, args, self.max_stmt_length,
                self._get_db().encoding))
        else:
            rows = 0
            for arg in args:
                await self.execute(query, arg)
                rows += self._rowcount
            self._rowcount = rows
        return self._rowcount

    async def _do_execute_many(self, prefix, values, postfix, args,
                               max_stmt_length, encoding):
        conn = self._get_db()
        escape = self._escape_args
        if isinstance(prefix, str):
            prefix = prefix.encode(encoding)
        if isinstance(postfix, str):
            postfix = postfix.encode(encoding)
        sql = bytearray(prefix)
        args = iter(args)
        v = values % escape(next(args), conn)
        if isinstance(v, str):
            v = v.encode(encoding, 'surrogateescape')
        sql += v
        rows = 0
        for arg in args:
            v = values % escape(arg, conn)
            if isinstance(v, str):
                v = v.encode(encoding, 'surrogateescape')
            if len(sql) + len(v) + len(postfix) + 1 > max_stmt_length:
                r = await self.execute(sql + postfix)
                rows += r
                sql = bytearray(prefix)
            else:
                sql += b','
            sql += v
        r = await self.execute(sql + postfix)
        rows += r
        self._rowcount = rows
        return rows

    async def callproc(self, procname, args=()):
        """Execute stored procedure procname with args

        Compatibility warning: PEP-249 specifies that any modified
        parameters must be returned. This is currently impossible
        as they are only available by storing them in a server
        variable and then retrieved by a query. Since stored
        procedures return zero or more result sets, there is no
        reliable way to get at OUT or INOUT parameters via callproc.
        The server variables are named @_procname_n, where procname
        is the parameter above and n is the position of the parameter
        (from zero). Once all result sets generated by the procedure
        have been fetched, you can issue a SELECT @_procname_0, ...
        query using .execute() to get any OUT or INOUT values.

        Compatibility warning: The act of calling a stored procedure
        itself creates an empty result set. This appears after any
        result sets generated by the procedure. This is non-standard
        behavior with respect to the DB-API. Be sure to use nextset()
        to advance through all result sets; otherwise you may get
        disconnected.

        :param procname: ``str``, name of procedure to execute on server
        :param args: `sequence of parameters to use with procedure
        :returns: the original args.
        """
        conn = self._get_db()
        if self._echo:
            logger.info("CALL %s", procname)
            logger.info("%r", args)

        for index, arg in enumerate(args):
            q = "SET @_%s_%d=%s" % (procname, index, conn.escape(arg))
            await self._query(q)
            await self.nextset()

        _args = ','.join('@_%s_%d' % (procname, i) for i in range(len(args)))
        q = f"CALL {procname}({_args})"
        await self._query(q)
        self._executed = q
        return args

    def fetchone(self):
        """Fetch the next row """
        self._check_executed()
        fut = self._loop.create_future()

        if self._rows is None or self._rownumber >= len(self._rows):
            fut.set_result(None)
            return fut
        result = self._rows[self._rownumber]
        self._rownumber += 1

        fut = self._loop.create_future()
        fut.set_result(result)
        return fut

    def fetchmany(self, size=None):
        """Returns the next set of rows of a query result, returning a
        list of tuples. When no more rows are available, it returns an
        empty list.

        The number of rows returned can be specified using the size argument,
        which defaults to one

        :param size: ``int`` number of rows to return
        :returns: ``list`` of fetched rows
        """
        self._check_executed()
        fut = self._loop.create_future()
        if self._rows is None:
            fut.set_result([])
            return fut
        end = self._rownumber + (size or self._arraysize)
        result = self._rows[self._rownumber:end]
        self._rownumber = min(end, len(self._rows))

        fut.set_result(result)
        return fut

    def fetchall(self):
        """Returns all rows of a query result set

        :returns: ``list`` of fetched rows
        """
        self._check_executed()
        fut = self._loop.create_future()
        if self._rows is None:
            fut.set_result([])
            return fut

        if self._rownumber:
            result = self._rows[self._rownumber:]
        else:
            result = self._rows
        self._rownumber = len(self._rows)

        fut.set_result(result)
        return fut

    def scroll(self, value, mode='relative'):
        """Scroll the cursor in the result set to a new position according
         to mode.

        If mode is relative (default), value is taken as offset to the
        current position in the result set, if set to absolute, value
        states an absolute target position. An IndexError should be raised in
        case a scroll operation would leave the result set. In this case,
        the cursor position is left undefined (ideal would be to
        not move the cursor at all).

        :param int value: move cursor to next position according to mode.
        :param str mode: scroll mode, possible modes: `relative` and `absolute`
        """
        self._check_executed()
        if mode == 'relative':
            r = self._rownumber + value
        elif mode == 'absolute':
            r = value
        else:
            raise ProgrammingError("unknown scroll mode %s" % mode)

        if not (0 <= r < len(self._rows)):
            raise IndexError("out of range")
        self._rownumber = r

        fut = self._loop.create_future()
        fut.set_result(None)
        return fut

    async def _query(self, q):
        conn = self._get_db()
        self._last_executed = q
        self._clear_result()
        await conn.query(q)
        await self._do_get_result()

    def _clear_result(self):
        self._rownumber = 0
        self._result = None

        self._rowcount = 0
        self._description = None
        self._lastrowid = None
        self._rows = None

    async def _do_get_result(self):
        conn = self._get_db()
        self._rownumber = 0
        self._result = result = conn._result
        self._rowcount = result.affected_rows
        self._description = result.description
        self._lastrowid = result.insert_id
        self._rows = result.rows

        if result.warning_count > 0:
            await self._show_warnings(conn)

    async def _show_warnings(self, conn):
        if self._result and self._result.has_next:
            return
        ws = await conn.show_warnings()
        if ws is None:
            return
        for w in ws:
            msg = w[-1]
            warnings.warn(str(msg), Warning, 4)

    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

    def __aiter__(self):
        return self

    async def __anext__(self):
        ret = await self.fetchone()
        if ret is not None:
            return ret
        else:
            raise StopAsyncIteration  # noqa

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return


class _DeserializationCursorMixin:
    async def _do_get_result(self):
        await super()._do_get_result()
        if self._rows:
            self._rows = [self._deserialization_row(r) for r in self._rows]

    def _deserialization_row(self, row):
        if row is None:
            return None
        if isinstance(row, dict):
            dict_flag = True
        else:
            row = list(row)
            dict_flag = False
        for index, (name, field_type, *n) in enumerate(self._description):
            if field_type == FIELD_TYPE.JSON:
                point = name if dict_flag else index
                with contextlib.suppress(ValueError, TypeError):
                    row[point] = json.loads(row[point])
        if dict_flag:
            return row
        else:
            return tuple(row)

    def _conv_row(self, row):
        if row is None:
            return None
        row = super()._conv_row(row)
        return self._deserialization_row(row)


class DeserializationCursor(_DeserializationCursorMixin, Cursor):
    """A cursor automatic deserialization of json type fields"""


class _DictCursorMixin:
    # You can override this to use OrderedDict or other dict-like types.
    dict_type = dict

    async def _do_get_result(self):
        await super()._do_get_result()
        fields = []
        if self._description:
            for f in self._result.fields:
                name = f.name
                if name in fields:
                    name = f.table_name + '.' + name
                fields.append(name)
            self._fields = fields

        if fields and self._rows:
            self._rows = [self._conv_row(r) for r in self._rows]

    def _conv_row(self, row):
        if row is None:
            return None
        row = super()._conv_row(row)
        return self.dict_type(zip(self._fields, row))


class DictCursor(_DictCursorMixin, Cursor):
    """A cursor which returns results as a dictionary"""


class SSCursor(Cursor):
    """Unbuffered Cursor, mainly useful for queries that return a lot of
    data, or for connections to remote servers over a slow network.

    Instead of copying every row of data into a buffer, this will fetch
    rows as needed. The upside of this, is the client uses much less memory,
    and rows are returned much faster when traveling over a slow network,
    or if the result set is very big.

    There are limitations, though. The MySQL protocol doesn't support
    returning the total number of rows, so the only way to tell how many rows
    there are is to iterate over every row returned. Also, it currently isn't
    possible to scroll backwards, as only the current row is held in memory.
    """

    async def close(self):
        conn = self._connection
        if conn is None:
            return

        if self._result is not None and self._result is conn._result:
            await self._result._finish_unbuffered_query()

        try:
            while (await self.nextset()):
                pass
        finally:
            self._connection = None

    async def _query(self, q):
        conn = self._get_db()
        self._last_executed = q
        await conn.query(q, unbuffered=True)
        await self._do_get_result()
        return self._rowcount

    async def _read_next(self):
        """Read next row """
        row = await self._result._read_rowdata_packet_unbuffered()
        row = self._conv_row(row)
        return row

    async def fetchone(self):
        """ Fetch next row """
        self._check_executed()
        row = await self._read_next()
        if row is None:
            return
        self._rownumber += 1
        return row

    async def fetchall(self):
        """Fetch all, as per MySQLdb. Pretty useless for large queries, as
        it is buffered.
        """
        rows = []
        while True:
            row = await self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    async def fetchmany(self, size=None):
        """Returns the next set of rows of a query result, returning a
        list of tuples. When no more rows are available, it returns an
        empty list.

        The number of rows returned can be specified using the size argument,
        which defaults to one

        :param size: ``int`` number of rows to return
        :returns: ``list`` of fetched rows
        """
        self._check_executed()
        if size is None:
            size = self._arraysize

        rows = []
        for i in range(size):
            row = await self._read_next()
            if row is None:
                break
            rows.append(row)
            self._rownumber += 1
        return rows

    async def scroll(self, value, mode='relative'):
        """Scroll the cursor in the result set to a new position
        according to mode . Same as :meth:`Cursor.scroll`, but move cursor
        on server side one by one row. If you want to move 20 rows forward
        scroll will make 20 queries to move cursor. Currently only forward
        scrolling is supported.

        :param int value: move cursor to next position according to mode.
        :param str mode: scroll mode, possible modes: `relative` and `absolute`
        """

        self._check_executed()

        if mode == 'relative':
            if value < 0:
                raise NotSupportedError("Backwards scrolling not supported "
                                        "by this cursor")

            for _ in range(value):
                await self._read_next()
            self._rownumber += value
        elif mode == 'absolute':
            if value < self._rownumber:
                raise NotSupportedError(
                    "Backwards scrolling not supported by this cursor")

            end = value - self._rownumber
            for _ in range(end):
                await self._read_next()
            self._rownumber = value
        else:
            raise ProgrammingError("unknown scroll mode %s" % mode)


class SSDictCursor(_DictCursorMixin, SSCursor):
    """An unbuffered cursor, which returns results as a dictionary """
