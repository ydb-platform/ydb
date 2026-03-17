from . import err
from .cursors import Cursor
from . import converters
from ..state import sqlitelike as chdb_stateful

DEBUG = False
VERBOSE = False


class Connection(object):
    """
    Representation of a connection with chdb.
    """

    def __init__(self, path=None):
        self._closed = False
        self.encoding = "utf8"
        self._affected_rows = 0
        self._resp = None

        # Initialize sqlitelike connection
        connection_string = ":memory:" if path is None else f"file:{path}"
        self._conn = chdb_stateful.Connection(connection_string)

        # Test connection with a simple query
        cursor = self._conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()

    def close(self):
        """Send the quit message and close the socket."""
        if self._closed:
            raise err.Error("Already closed")
        self._closed = True
        self._conn.close()

    @property
    def open(self):
        """Return True if the connection is open"""
        return not self._closed

    def commit(self):
        """Commit changes to stable storage."""
        # No-op for ClickHouse
        pass

    def rollback(self):
        """Roll back the current transaction."""
        # No-op for ClickHouse
        pass

    def cursor(self, cursor=None):
        """Create a new cursor to execute queries with."""
        if self._closed:
            raise err.Error("Connection closed")
        if cursor:
            return Cursor(self)
        return Cursor(self)

    def query(self, sql, fmt="CSV"):
        """Execute a query and return the raw result."""
        if self._closed:
            raise err.InterfaceError("Connection closed")

        if isinstance(sql, str):
            sql = sql.encode(self.encoding, "surrogateescape")

        try:
            result = self._conn.query(sql.decode(), fmt)
            self._resp = result
            return result
        except Exception as error:
            raise err.InterfaceError(f"Query error: {error}")

    def escape(self, obj, mapping=None):
        """Escape whatever value you pass to it."""
        return converters.escape_item(obj, mapping)

    def escape_string(self, s):
        return converters.escape_string(s)

    def _quote_bytes(self, s):
        return converters.escape_bytes(s)

    def __enter__(self):
        """Context manager that returns a Cursor"""
        return self.cursor()

    def __exit__(self, exc, value, traceback):
        """On successful exit, commit. On exception, rollback"""
        if exc:
            self.rollback()
        else:
            self.commit()
        self.close()

    @property
    def resp(self):
        """Return the last query response"""
        return self._resp
