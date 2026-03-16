import tempfile
import shutil
import warnings

import chdb
from ..state import sqlitelike as chdb_stateful
from ..state.sqlitelike import StreamingResult

g_session = None
g_session_path = None


class Session:
    """
    Session will keep the state of query.
    If path is None, it will create a temporary directory and use it as the database path
    and the temporary directory will be removed when the session is closed.
    You can also pass in a path to create a database at that path where will keep your data.

    You can also use a connection string to pass in the path and other parameters.
    Examples:
        - ":memory:" (for in-memory database)
        - "test.db" (for relative path)
        - "file:test.db" (same as above)
        - "/path/to/test.db" (for absolute path)
        - "file:/path/to/test.db" (same as above)
        - "file:test.db?param1=value1&param2=value2" (for relative path with query params)
        - "file::memory:?verbose&log-level=test" (for in-memory database with query params)
        - "///path/to/test.db?param1=value1&param2=value2" (for absolute path)

    Connection string args handling:
        Connection string can contain query params like "file:test.db?param1=value1&param2=value2"
        "param1=value1" will be passed to ClickHouse engine as start up args.

        For more details, see `clickhouse local --help --verbose`
        Some special args handling:
        - "mode=ro" would be "--readonly=1" for clickhouse (read-only mode)

    Important:
        - There can be only one session at a time. If you want to create a new session, you need to close the existing one.
        - Creating a new session will close the existing one.
    """

    def __init__(self, path=None):
        global g_session, g_session_path
        if g_session is not None:
            warnings.warn(
                "There is already an active session. Creating a new session will close the existing one. "
                "It is recommended to close the existing session before creating a new one. "
                f"Closing the existing session {g_session_path}"
            )
            g_session.close()
            g_session_path = None
        if path is None or ":memory:" in path:
            self._cleanup = True
            self._path = tempfile.mkdtemp()
        else:
            self._cleanup = False
            self._path = path
        if chdb.g_udf_path != "":
            self._udf_path = chdb.g_udf_path
            # add udf_path to conn_str here.
            # - the `user_scripts_path` will be the value of `udf_path`
            # - the `user_defined_executable_functions_config` will be `user_scripts_path/*.xml`
            # Both of them will be added to the conn_str in the Connection class
            if "?" in self._path:
                self._conn_str = f"{self._path}&udf_path={self._udf_path}"
            else:
                self._conn_str = f"{self._path}?udf_path={self._udf_path}"
        else:
            self._udf_path = ""
            self._conn_str = f"{self._path}"
        self._conn = chdb_stateful.Connection(self._conn_str)
        g_session = self
        g_session_path = self._path

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        if self._cleanup:
            self.cleanup()
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        global g_session, g_session_path
        g_session = None
        g_session_path = None

    def cleanup(self):
        try:
            if self._conn is not None:
                self._conn.close()
                self._conn = None
            shutil.rmtree(self._path)
            global g_session, g_session_path
            g_session = None
            g_session_path = None
        except:  # noqa
            pass

    def query(self, sql, fmt="CSV", udf_path=""):
        """
        Execute a query.
        """
        if fmt == "Debug":
            warnings.warn(
                """Debug format is not supported in Session.query
Please try use parameters in connection string instead:
Eg: conn = connect(f"db_path?verbose&log-level=test")"""
            )
            fmt = "CSV"
        return self._conn.query(sql, fmt)

    # alias sql = query
    sql = query

    def send_query(self, sql, fmt="CSV") -> StreamingResult:
        """
        Execute a streaming query.
        """
        if fmt == "Debug":
            warnings.warn(
                """Debug format is not supported in Session.query
Please try use parameters in connection string instead:
Eg: conn = connect(f"db_path?verbose&log-level=test")"""
            )
            fmt = "CSV"
        return self._conn.send_query(sql, fmt)
