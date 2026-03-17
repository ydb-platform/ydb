from collections import namedtuple
from itertools import islice

from ..errors import Error as DriverError
from .errors import InterfaceError, OperationalError, ProgrammingError


Column = namedtuple(
    'Column',
    'name type_code display_size internal_size precision scale null_ok'
)


class Cursor(object):

    class States(object):
        (
            NONE,
            RUNNING,
            FINISHED,
            CURSOR_CLOSED
        ) = range(4)

    _states = States()

    def __init__(self, client):
        self._client = client
        self._reset_state()

        self.arraysize = 1

        # Begin non-PEP attributes
        self._columns_with_types = None
        # End non-PEP attributes

        super(Cursor, self).__init__()

    def __repr__(self):
        is_closed = self._state == self._states.CURSOR_CLOSED
        return '<cursor object at 0x{0:x}; closed: {1:}>'.format(
            id(self), is_closed
        )

    # Iteration support.
    def __iter__(self):
        while True:
            one = self.fetchone()
            if one is None:
                return
            yield one

    # Context manager integrations.
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def description(self):
        if self._state == self._states.NONE:
            return None

        columns = self._columns or []
        types = self._types or []

        return [
            Column(name, type_code, None, None, None, None, True)
            for name, type_code in zip(columns, types)
        ]

    @property
    def rowcount(self):
        """
        :return: the number of rows that the last .execute*() produced.
        """
        return self._rowcount

    def close(self):
        """
        Close the cursor now. The cursor will be unusable from this point
        forward; an :data:`~clickhouse_driver.dbapi.Error` (or subclass)
        exception will be raised if any operation is attempted with the
        cursor.
        """
        self._client.disconnect()
        self._state = self._states.CURSOR_CLOSED

    def execute(self, operation, parameters=None):
        """
        Prepare and execute a database operation (query or command).

        :param operation: query or command to execute.
        :param parameters: sequence or mapping that will be bound to
                           variables in the operation.
        :return: None
        """
        self._check_cursor_closed()
        self._begin_query()

        try:
            execute, execute_kwargs = self._prepare()

            response = execute(
                operation, params=parameters, with_column_types=True,
                **execute_kwargs
            )

        except DriverError as orig:
            raise OperationalError(orig)

        self._process_response(response)
        self._end_query()

    def executemany(self, operation, seq_of_parameters):
        """
        Prepare a database operation (query or command) and then execute it
        against all parameter sequences found in the sequence
        `seq_of_parameters`.

        :param operation: query or command to execute.
        :param seq_of_parameters: sequences or mappings for execution.
        :return: None
        """
        self._check_cursor_closed()
        self._begin_query()

        try:
            execute, execute_kwargs = self._prepare()

            response = execute(
                operation, params=seq_of_parameters, **execute_kwargs
            )

        except DriverError as orig:
            raise OperationalError(orig)

        self._process_response(response, executemany=True)
        self._end_query()

    def fetchone(self):
        """
        Fetch the next row of a query result set, returning a single sequence,
        or None when no more data is available.

        :return: the next row of a query result set or None.
        """
        self._check_query_started()

        if self._stream_results:
            return next(self._rows, None)

        else:
            if not self._rows:
                return None

            return self._rows.pop(0)

    def fetchmany(self, size=None):
        """
        Fetch the next set of rows of a query result, returning a sequence of
        sequences (e.g. a list of tuples). An empty sequence is returned when
        no more rows are available.

        :param size: amount of rows to return.
        :return: list of fetched rows or empty list.
        """
        self._check_query_started()

        if size is None:
            size = self.arraysize

        if self._stream_results:
            if size == -1:
                return list(self._rows)
            else:
                return list(islice(self._rows, size))

        if size < 0:
            rv = self._rows
            self._rows = []
        else:
            rv = self._rows[:size]
            self._rows = self._rows[size:]

        return rv

    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples).

        :return: list of fetched rows.
        """
        self._check_query_started()

        if self._stream_results:
            return list(self._rows)

        rv = self._rows
        self._rows = []
        return rv

    def setinputsizes(self, sizes):
        # Do nothing.
        pass

    def setoutputsize(self, size, column=None):
        # Do nothing.
        pass

    # Begin non-PEP methods
    @property
    def columns_with_types(self):
        """
        :return: list of column names with corresponding types of the last
                 .execute*(). E.g. [('x', 'UInt64')].
        """
        return self._columns_with_types

    def set_stream_results(self, stream_results, max_row_buffer):
        """
        Toggles results streaming from server. Driver will consume
        block-by-block of `max_row_buffer` size and yield row-by-row from each
        block.

        :param stream_results: enable or disable results streaming.
        :param max_row_buffer: specifies the maximum number of rows to buffer
               at a time.
        :return: None
        """
        self._stream_results = stream_results
        self._max_row_buffer = max_row_buffer

    def set_settings(self, settings):
        """
        Specifies settings for cursor.

        :param settings: dictionary of query settings
        :return: None
        """
        self._settings = settings

    def set_types_check(self, types_check):
        """
        Toggles type checking for sequence of INSERT parameters.
        Disabled by default.

        :param types_check: new types check value.
        :return: None
        """
        self._types_check = types_check

    def set_external_table(self, name, structure, data):
        """
        Adds external table to cursor context.

        If the same table is specified more than once the last one is used.

        :param name: name of external table
        :param structure: list of tuples (name, type) that defines table
                          structure. Example [(x, 'Int32')].
        :param data: sequence of rows of tuples or dicts for transmission.
        :return: None
        """
        self._external_tables[name] = (structure, data)

    def set_query_id(self, query_id):
        """
        Specifies the query identifier for cursor.

        :param query_id: the query identifier.
        :return: None
        """
        self._query_id = query_id
    # End non-PEP methods

    # Private methods.
    def _prepare(self):
        external_tables = [
            {'name': name, 'structure': structure, 'data': data}
            for name, (structure, data) in self._external_tables.items()
        ] or None

        execute = self._client.execute

        if self._stream_results:
            execute = self._client.execute_iter
            self._settings = self._settings or {}
            self._settings['max_block_size'] = self._max_row_buffer

        execute_kwargs = {
            'settings': self._settings,
            'external_tables': external_tables,
            'types_check': self._types_check,
            'query_id': self._query_id
        }

        return execute, execute_kwargs

    def _process_response(self, response, executemany=False):
        if executemany:
            self._rowcount = response
            response = None

        if not response:
            self._columns = self._types = self._rows = []
            return

        if self._stream_results:
            columns_with_types = next(response)
            rows = response

        else:
            rows, columns_with_types = response

        self._columns_with_types = columns_with_types

        # Only SELECT queries have columns_with_types.
        # DDL and INSERT INTO ... SELECT queries have empty columns header.
        # We need to obtain rows count only during non-streaming SELECTs.
        if columns_with_types:
            self._columns, self._types = zip(*columns_with_types)
            if not self._stream_results:
                self._rowcount = len(rows)
        else:
            self._columns = self._types = []

        self._rows = rows

    def _reset_state(self):
        """
        Resets query state and get ready for another query.
        """
        self._state = self._states.NONE

        self._columns = None
        self._types = None
        self._rows = None
        self._rowcount = -1

        self._stream_results = False
        self._max_row_buffer = 0
        self._settings = None
        self._query_id = None
        self._external_tables = {}
        self._types_check = False

    def _begin_query(self):
        self._state = self._states.RUNNING

    def _end_query(self):
        self._state = self._states.FINISHED

    def _check_cursor_closed(self):
        if self._state == self._states.CURSOR_CLOSED:
            raise InterfaceError('cursor already closed')

    def _check_query_started(self):
        if self._state == self._states.NONE:
            raise ProgrammingError('no results to fetch')
