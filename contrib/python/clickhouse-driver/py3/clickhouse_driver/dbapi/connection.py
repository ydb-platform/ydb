from ..client import Client
from .. import defines
from .cursor import Cursor
from .errors import InterfaceError


class Connection(object):
    """
    Creates new Connection for accessing ClickHouse database.

    Connection is just wrapper for handling multiple cursors (clients) and
    do not initiate actual connections to the ClickHouse server.

    See parameters description in
    :data:`~clickhouse_driver.connection.Connection`.
    """
    def __init__(self, dsn=None, host=None,
                 user=defines.DEFAULT_USER, password=defines.DEFAULT_PASSWORD,
                 port=defines.DEFAULT_PORT, database=defines.DEFAULT_DATABASE,
                 **kwargs):
        self.cursors = []

        self.dsn = dsn
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.connection_kwargs = kwargs
        self.is_closed = False
        self._hosts = None
        super(Connection, self).__init__()

    def __repr__(self):
        return '<connection object at 0x{0:x}; closed: {1:}>'.format(
            id(self), self.is_closed
        )

    # Context manager integrations.
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _make_client(self):
        """
        :return: a new Client instance.
        """
        if self.dsn is not None:
            return Client.from_url(self.dsn)

        return Client(self.host, port=self.port,
                      user=self.user, password=self.password,
                      database=self.database, **self.connection_kwargs)

    def close(self):
        """
        Close the connection now. The connection will be unusable from this
        point forward; an :data:`~clickhouse_driver.dbapi.Error` (or subclass)
        exception will be raised if any operation is attempted with the
        connection. The same applies to all cursor objects trying to use the
        connection.
        """
        for cursor in self.cursors:
            cursor.close()

        self.is_closed = True

    def commit(self):
        """
        Do nothing since ClickHouse has no transactions.
        """
        pass

    def rollback(self):
        """
        Do nothing since ClickHouse has no transactions.
        """
        pass

    def cursor(self, cursor_factory=None):
        """
        :param cursor_factory: Argument can be used to create non-standard
                               cursors.
        :return: a new cursor object using the connection.
        """
        if self.is_closed:
            raise InterfaceError('connection already closed')

        client = self._make_client()
        if self._hosts is None:
            self._hosts = client.connection.hosts
        else:
            client.connection.hosts = self._hosts
        cursor_factory = cursor_factory or Cursor
        cursor = cursor_factory(client, self)
        self.cursors.append(cursor)
        return cursor
