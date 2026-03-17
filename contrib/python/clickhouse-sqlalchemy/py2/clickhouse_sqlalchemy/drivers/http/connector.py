from uuid import uuid4

from .escaper import Escaper
from .transport import RequestsTransport
from .utils import FORMAT_SUFFIX

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
    transport_cls = RequestsTransport

    def __init__(self, *args, **kwargs):

        stream = bool(kwargs.pop('stream', None))
        self._prefetch = not stream

        # TODO: support `stream` argument in the transport.
        self.transport = self.transport_cls(*args, **kwargs)
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

    _params_escaper = Escaper()

    _columns = None
    _types = None
    # Result data iterable:
    _response = None
    # Result data prefetch cache:
    _rows = None

    def __init__(self, connection):
        self._connection = connection
        self._reset_state()
        self._arraysize = 1
        super(Cursor, self).__init__()

    @property
    def rowcount(self):
        # TODO: initial len(self._rows)?
        return -1

    @property
    def description(self):
        columns = self._columns or []
        types = self._types or []

        return [
            (name, type_code, None, None, None, None, True)
            for name, type_code in zip(columns, types)
        ]

    def close(self):
        pass

    def execute(self, operation, parameters=None, context=None):
        raw_sql = operation

        if parameters is not None:
            raw_sql = raw_sql % self._params_escaper.escape(parameters)

        raw_sql_big = raw_sql.upper()
        if 'FORMAT' not in raw_sql_big and 'INSERT' not in raw_sql_big:
            raw_sql += ' ' + FORMAT_SUFFIX

        self._reset_state()
        self._begin_query()

        transport = self._connection.transport
        params = {'query_id': self._query_id}
        response_gen = transport.execute(raw_sql, params=params)

        self._process_response(response_gen)
        self._end_query()

    def executemany(self, operation, seq_of_parameters, context=None):
        index = operation.index('VALUES') + 7
        values_tpl = operation[index:]
        params = ', '.join(
            values_tpl % self._params_escaper.escape(params)
            for params in seq_of_parameters
        )
        self.execute(operation[:index] + params)

    def check_query_started(self):
        if self._state == self._states.NONE:
            raise RuntimeError("No query yet")

    def fetchone(self):
        self.check_query_started()

        # `self._prefetch` case:
        if self._rows is not None:
            if not self._rows:
                return None
            return self._rows.pop(0)

        return next(self._response, None)

    def fetchmany(self, size=1):
        self.check_query_started()

        # `self._prefetch` case:
        if self._rows is not None:
            rv = self._rows[:size]
            self._rows = self._rows[size:]
            return rv

        rows = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    def fetchall(self):
        self.check_query_started()

        # `self._prefetch` case:
        if self._rows is not None:
            rv = self._rows
            self._rows = []
            return rv

        rows = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

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

    # Private and non-standard methods.
    def cancel(self):
        """
        Cancels query. Not in PEP 249 standard.
        """
        if self._state == self._states.NONE or self._query_id is None:
            raise RuntimeError("No query yet")

        # Try to cancel query by sending query with the same query_id.
        transport = self._connection.transport
        params = {'query_id': self._query_id}
        transport.execute('SELECT 1', params=params)

        self._end_query()
        self._query_id = None
        self._rows = None

    @property
    def _prefetch(self):
        return self._connection._prefetch

    def _process_response(self, response):
        response = iter(response)

        self._columns = next(response, None)
        self._types = next(response, None)
        self._response = response

        if self._prefetch:
            self._rows = list(response)

    def _reset_state(self):
        """
        Resets query state and get ready for another query.
        """
        self._query_id = None
        self._state = self._states.NONE

        self._columns = None
        self._types = None
        self._rows = None
        self._response = None

    def _begin_query(self):
        self._state = self._states.RUNNING
        self._query_id = uuid4()

    def _end_query(self):
        self._state = self._states.FINISHED
