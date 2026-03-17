from __future__ import annotations

import contextlib
import functools
import logging
import re
from collections.abc import Callable, Generator, Iterator
from typing import Any

import psycopg2
import psycopg2.errors
import psycopg2.pool
from psycopg2.extras import Json, RealDictCursor

from procrastinate import connector, exceptions, manager

logger = logging.getLogger(__name__)


@contextlib.contextmanager
def wrap_exceptions() -> Generator[None, None, None]:
    """
    Wrap psycopg2 errors as connector exceptions.
    """
    try:
        yield
    except psycopg2.errors.UniqueViolation as exc:
        constraint_name = exc.diag.constraint_name
        queueing_lock = None
        if constraint_name == manager.QUEUEING_LOCK_CONSTRAINT:
            assert exc.diag.message_detail
            match = re.search(r"Key \((.*?)\)=\((.*?)\)", exc.diag.message_detail)
            assert match
            column, queueing_lock = match.groups()
            assert column == "queueing_lock"

        raise exceptions.UniqueViolation(
            constraint_name=constraint_name, queueing_lock=queueing_lock
        )
    except psycopg2.Error as exc:
        raise exceptions.ConnectorException from exc


def wrap_query_exceptions(func: Callable) -> Callable:
    """
    Detect "admin shutdown" errors and retry a number of times.

    This is to handle the case where the database connection (obtained from the pool)
    was actually closed by the server. In this case, pyscopg2 raises an AdminShutdown
    exception when the connection is used for issuing a query. What we do is retry when
    an AdminShutdown is raised, and until the maximum number of retries is reached.

    The number of retries is set to the pool maximum size plus one, to handle the case
    where the connections we have in the pool were all closed on the server side.
    """

    @functools.wraps(func)
    def wrapped(*args: Any, **kwargs: Any):
        final_exc = None
        try:
            max_tries = args[0]._pool.maxconn + 1
        except Exception:
            max_tries = 1
        for _ in range(max_tries):
            try:
                return func(*args, **kwargs)
            except psycopg2.errors.AdminShutdown:
                continue
        raise exceptions.ConnectorException(
            f"Could not get a valid connection after {max_tries} tries"
        ) from final_exc

    return wrapped


class Psycopg2Connector(connector.BaseConnector):
    @wrap_exceptions()
    def __init__(
        self,
        *,
        json_dumps: Callable | None = None,
        json_loads: Callable | None = None,
        **kwargs: Any,
    ):
        """
        Synchronous connector based on a ``psycopg2.pool.ThreadedConnectionPool``.

        All other arguments than ``json_dumps`` are passed to
        :py:func:`ThreadedConnectionPool` (see psycopg2 documentation__), with default
        values that may differ from those of ``psycopg2`` (see a partial list of
        parameters below).

        .. _psycopg2 doc: https://www.psycopg.org/docs/extras.html#json-adaptation
        .. __: https://www.psycopg.org/docs/pool.html
               #psycopg2.pool.ThreadedConnectionPool

        Parameters
        ----------
        json_dumps:
            The JSON dumps function to use for serializing job arguments. Defaults to
            the function used by psycopg2. See the `psycopg2 doc`_.
        json_loads:
            The JSON loads function to use for deserializing job arguments. Defaults
            to the function used by psycopg2. See the `psycopg2 doc`_. Unused if the
            pool is externally created and set into the connector through the
            ``App.open`` method.
        minconn: int
            Passed to psycopg2, default set to 1 (same as aiopg).
        maxconn: int
            Passed to psycopg2, default set to 10 (same as aiopg).
        dsn: ``Optional[str]``
            Passed to psycopg2. Default is "" instead of None, which means if no
            argument is passed, it will connect to localhost:5432 instead of a
            Unix-domain local socket file.
        cursor_factory: ``psycopg2.extensions.cursor``
            Passed to psycopg2. Default is ``psycopg2.extras.RealDictCursor``
            instead of standard cursor. There is no identified use case for changing
            this.
        """
        self.json_dumps = json_dumps
        self.json_loads = json_loads
        self._pool: psycopg2.pool.AbstractConnectionPool | None = None
        self._pool_args = self._adapt_pool_args(kwargs)
        self._pool_externally_set = False

    def get_sync_connector(self) -> Psycopg2Connector:
        return self

    @staticmethod
    def _adapt_pool_args(pool_args: dict[str, Any]) -> dict[str, Any]:
        """
        Adapt the pool args for ``psycopg2``, using sensible defaults for Procrastinate.
        """
        final_args = {
            "minconn": 1,
            "maxconn": 10,
            "dsn": "",
            "cursor_factory": RealDictCursor,
        }
        final_args.update(pool_args)
        return final_args

    def open(self, pool: psycopg2.pool.AbstractConnectionPool | None = None) -> None:
        """
        Instantiate the pool.

        pool :
            Optional pool. Procrastinate can use an existing pool. Connection parameters
            passed in the constructor will be ignored.
        """

        if pool:
            self._pool_externally_set = True
            self._pool = pool
        else:
            self._pool = self._create_pool(self._pool_args)

    @staticmethod
    @wrap_exceptions()
    def _create_pool(pool_args: dict[str, Any]) -> psycopg2.pool.AbstractConnectionPool:
        return psycopg2.pool.ThreadedConnectionPool(**pool_args)

    @wrap_exceptions()
    def close(self) -> None:
        """
        Close the pool
        """
        if self._pool and not self._pool.closed and not self._pool_externally_set:
            self._pool.closeall()

    @property
    def pool(self) -> psycopg2.pool.AbstractConnectionPool:
        if self._pool is None:  # Set by open
            raise exceptions.AppNotOpen
        return self._pool

    def _wrap_value(self, value: Any) -> Any:
        if isinstance(value, dict):
            return Json(value, dumps=self.json_dumps)
        elif isinstance(value, list):
            return [self._wrap_value(item) for item in value]
        elif isinstance(value, tuple):
            return tuple([self._wrap_value(item) for item in value])
        else:
            return value

    def _wrap_json(self, arguments: dict[str, Any]):
        return {key: self._wrap_value(value) for key, value in arguments.items()}

    @contextlib.contextmanager
    def _connection(self) -> Iterator[psycopg2.extensions.connection]:
        # in case of an admin shutdown (Postgres error code 57P01) we do not
        # rollback the connection or put the connection back to the pool as
        # this will cause a psycopg2.InterfaceError exception
        connection = self.pool.getconn()
        try:
            yield connection
        except psycopg2.errors.AdminShutdown:
            raise
        except Exception:
            connection.rollback()
            self.pool.putconn(connection)
            raise
        else:
            connection.commit()
            self.pool.putconn(connection)

    @wrap_exceptions()
    @wrap_query_exceptions
    def execute_query(self, query: str, **arguments: Any) -> None:
        with self._connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, self._wrap_json(arguments))

    @wrap_exceptions()
    @wrap_query_exceptions
    def execute_query_one(self, query: str, **arguments: Any) -> dict[str, Any]:
        with self._connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, self._wrap_json(arguments))
                # psycopg2's type say it returns a tuple, but it actually returns a
                # dict when configured with RealDictCursor
                return cursor.fetchone()  # pyright: ignore[reportReturnType]

    @wrap_exceptions()
    @wrap_query_exceptions
    def execute_query_all(self, query: str, **arguments: Any) -> dict[str, Any]:
        with self._connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, self._wrap_json(arguments))
                # psycopg2's type say it returns a tuple, but it actually returns a
                # dict when configured with RealDictCursor
                return cursor.fetchall()  # pyright: ignore[reportReturnType]
