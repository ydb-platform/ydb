from __future__ import annotations

import contextlib
import logging
import re
from collections.abc import Callable, Generator, Iterator
from typing import Any

import psycopg
import psycopg.rows
import psycopg.types.json
import psycopg_pool
from typing_extensions import LiteralString

from procrastinate import connector, exceptions, manager

logger = logging.getLogger(__name__)


@contextlib.contextmanager
def wrap_exceptions() -> Generator[None, None, None]:
    """
    Wrap psycopg errors as connector exceptions.

    This decorator is expected to be used on coroutine functions only.
    """

    try:
        yield
    except psycopg.errors.UniqueViolation as exc:
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
    except psycopg.Error as exc:
        raise exceptions.ConnectorException from exc


class SyncPsycopgConnector(connector.BaseConnector):
    def __init__(
        self,
        *,
        json_dumps: Callable | None = None,
        json_loads: Callable | None = None,
        **kwargs: Any,
    ):
        """
        Create a PostgreSQL connector using psycopg. The connector uses an
        ``psycopg_pool.ConnectionPool``, which is created internally, or
        set into the connector by calling `App.open`.

        Note that if you want to use a ``psycopg_pool.NullConnectionPool``,
        you will need to initialize it yourself and pass it to the connector
        through the ``App.open`` method.

        All other arguments than ``json_dumps`` and ``json_loads`` are passed
        to ``psycopg_pool.ConnectionPool`` (see psycopg documentation__).

        ``json_dumps`` and ``json_loads`` are used to configure new connections
        created by the pool with ``psycopg.types.json.set_json_dumps`` and
        ``psycopg.types.json.set_json_loads``.

        .. __: https://www.psycopg.org/psycopg3/docs/api/pool.html

        Parameters
        ----------
        json_dumps :
            A function to serialize JSON objects to a string. If not provided,
            JSON objects will be serialized using psycopg's default JSON
            serializer.
        json_loads :
            A function to deserialize JSON objects from a string. If not
            provided, JSON objects will be deserialized using psycopg's default
            JSON deserializer.

        """
        self._pool: psycopg_pool.ConnectionPool | None = None
        self._pool_externally_set: bool = False
        self._json_loads = json_loads
        self._json_dumps = json_dumps
        self._pool_args = kwargs

    def get_sync_connector(self) -> connector.BaseConnector:
        return self

    @property
    def pool(self) -> psycopg_pool.ConnectionPool:
        if self._pool is None:  # Set by open_async
            raise exceptions.AppNotOpen
        return self._pool

    def open(self, pool: psycopg_pool.ConnectionPool | None = None) -> None:
        """
        Instantiate the pool.

        pool :
            Optional pool. Procrastinate can use an existing pool. Connection parameters
            passed in the constructor will be ignored.
        """
        if self._pool:
            return
        if pool:
            self._pool_externally_set = True
            self._pool = pool
        else:
            self._pool = self._create_pool(self._pool_args)
            self._pool.open(wait=True)

    @staticmethod
    @wrap_exceptions()
    def _create_pool(pool_args: dict[str, Any]) -> psycopg_pool.ConnectionPool:
        pool = psycopg_pool.ConnectionPool(
            **pool_args,
            # Not specifying open=False raises a warning and will be deprecated.
            # It makes sense, as we can't really make async I/Os in a constructor.
            open=False,
            # Enables a check that will ensure the connections returned when
            # using the pool are still alive. If they have been closed by the
            # database, they will be seamlessly replaced by a new connection.
            check=psycopg_pool.ConnectionPool.check_connection,
        )
        return pool

    @wrap_exceptions()
    def close(self) -> None:
        """
        Close the pool and s all connections to be released.
        """

        if not self._pool or self._pool_externally_set:
            return

        self._pool.close()
        self._pool = None

    def _wrap_value(self, value: Any) -> Any:
        if isinstance(value, dict):
            return psycopg.types.json.Jsonb(value)
        elif isinstance(value, list):
            return [self._wrap_value(item) for item in value]
        elif isinstance(value, tuple):
            return tuple([self._wrap_value(item) for item in value])
        else:
            return value

    def _wrap_json(self, arguments: dict[str, Any]):
        return {key: self._wrap_value(value) for key, value in arguments.items()}

    @contextlib.contextmanager
    def _get_cursor(self) -> Iterator[psycopg.Cursor[psycopg.rows.DictRow]]:
        with self.pool.connection() as connection:
            with connection.cursor(row_factory=psycopg.rows.dict_row) as cursor:
                if self._json_loads:
                    psycopg.types.json.set_json_loads(
                        loads=self._json_loads, context=cursor
                    )

                if self._json_dumps:
                    psycopg.types.json.set_json_dumps(
                        dumps=self._json_dumps, context=cursor
                    )
                yield cursor

    @wrap_exceptions()
    def execute_query(self, query: LiteralString, **arguments: Any) -> None:
        with self.pool.connection() as conn:
            conn.execute(query, self._wrap_json(arguments))

    @wrap_exceptions()
    def execute_query_one(
        self, query: LiteralString, **arguments: Any
    ) -> dict[str, Any]:
        with self._get_cursor() as cursor:
            cursor.execute(query, self._wrap_json(arguments))

            result = cursor.fetchone()

            if result is None:
                raise exceptions.NoResult
            return result

    @wrap_exceptions()
    def execute_query_all(
        self, query: LiteralString, **arguments: Any
    ) -> list[dict[str, Any]]:
        with self._get_cursor() as cursor:
            cursor.execute(query, self._wrap_json(arguments))

            return cursor.fetchall()
