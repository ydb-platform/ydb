from itertools import islice

from clickhouse_driver.client import Client as DriverClient
from clickhouse_driver.errors import Error as DriverError

from ...exceptions import DatabaseException

# PEP 249 module globals
apilevel = '2.0'
# Threads may share the module and connections.
threadsafety = 2
# Python extended format codes, e.g. ...WHERE name=%(name)s
paramstyle = 'pyformat'


class Error(Exception):
    """
    Exception that is the base class of all other error exceptions.
    You can use this to catch all errors with one single except statement.
    """
    pass


def connect(*args, **kwargs):
    """
    Make new connection.
    """
    return Connection(*args, **kwargs)


class Connection(object):
    transport_cls = DriverClient

    def __init__(self, *args, **kwargs):
        url = args[0]
        self.transport = self.transport_cls.from_url(url)
        super(Connection, self).__init__()

    def close(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self):
        return Cursor(self)


class Cursor(object):
    """
    These objects represent a database cursor, which is used to manage
    the context of a fetch operation.

    Cursors are not isolated, i.e., any changes done to the database
    by a cursor are immediately visible by other cursors or connections.
    """
    class States(object):
        (
            NONE,
            RUNNING,
            FINISHED
        ) = range(3)

    _states = States()

    def __init__(self, connection):
        self._connection = connection
        self._reset_state()
        self._arraysize = 1
        super(Cursor, self).__init__()

    @property
    def rowcount(self):
        return self._rowcount

    @property
    def description(self):
        columns = self._columns or []
        types = self._types or []

        return [
            (name, type_code, None, None, None, None, True)
            for name, type_code in zip(columns, types)
        ]

    def close(self):
        self._connection.transport.disconnect()

    def make_external_tables(self, dialect, execution_options):
        external_tables = execution_options.get('external_tables')
        if external_tables is None:
            return

        tables = []
        type_compiler = dialect.type_compiler

        for table in external_tables:
            structure = []
            for c in table.columns:
                type_ = type_compiler.process(c.type, type_expression=c)
                structure.append((c.name, type_))

            tables.append({
                'name': table.name,
                'structure': structure,
                'data': table.dialect_options['clickhouse']['data']
            })

        return tables

    def _prepare(self, context=None):
        if context:
            execution_options = context.execution_options

            external_tables = self.make_external_tables(
                context.dialect, execution_options
            )
        else:
            execution_options = {}
            external_tables = None

        transport = self._connection.transport
        execute = transport.execute
        execute_iter = getattr(transport, 'execute_iter', None)

        self._stream_results = execution_options.get('stream_results', False)
        settings = execution_options.get('settings')

        if self._stream_results and execute_iter:
            execute = execute_iter
            settings = settings or {}
            settings['max_block_size'] = execution_options['max_row_buffer']

        execute_kwargs = {
            'settings': settings,
            'external_tables': external_tables,
            'types_check': execution_options.get('types_check', False)
        }

        return execute, execute_kwargs

    def execute(self, operation, parameters=None, context=None):
        self._reset_state()
        self._begin_query()

        try:
            execute, execute_kwargs = self._prepare(context)

            response = execute(
                operation, params=parameters, with_column_types=True,
                **execute_kwargs
            )

        except DriverError as orig:
            raise DatabaseException(orig)

        self._process_response(response)
        self._end_query()

    def executemany(self, operation, seq_of_parameters, context=None):
        self._reset_state()
        self._begin_query()

        try:
            execute, execute_kwargs = self._prepare(context)

            response = execute(
                operation, params=seq_of_parameters, **execute_kwargs
            )

        except DriverError as orig:
            raise DatabaseException(orig)

        self._process_response(response, executemany=True)
        self._end_query()

    def check_query_started(self):
        if self._state == self._states.NONE:
            raise RuntimeError("No query yet")

    def fetchone(self):
        self.check_query_started()

        if self._stream_results:
            return next(self._rows, None)

        else:
            if not self._rows:
                return None

            return self._rows.pop(0)

    def fetchmany(self, size=1):
        self.check_query_started()

        if self._stream_results:
            return list(islice(self._rows, size))

        rv = self._rows[:size]
        self._rows = self._rows[size:]
        return rv

    def fetchall(self):
        self.check_query_started()

        if self._stream_results:
            return list(self._rows)

        rv = self._rows
        self._rows = []
        return rv

    @property
    def arraysize(self):
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value):
        self._arraysize = value

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

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

        if columns_with_types:
            self._columns, self._types = zip(*columns_with_types)
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

    def _begin_query(self):
        self._state = self._states.RUNNING

    def _end_query(self):
        self._state = self._states.FINISHED
