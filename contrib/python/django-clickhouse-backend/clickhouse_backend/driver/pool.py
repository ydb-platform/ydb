import threading
from contextlib import contextmanager
from typing import Generator, Union

from clickhouse_driver.dbapi.errors import InterfaceError

from .client import Client


class ClickhousePool:
    """A modified version of clickhouse_pool.ChPool

    Support connection from dsn and use `clickhouse_backend.driver.client.Client`.
    """

    def __init__(self, **kwargs):
        self.dsn = kwargs.pop("dsn", None)
        self.connections_min = kwargs.pop("connections_min", 10)
        self.connections_max = kwargs.pop("connections_max", 20)
        kwargs.setdefault("host", "localhost")
        self.connection_args = kwargs
        self.closed = False
        self._pool = []
        self._used = {}

        # similar to psycopg2 pools, _rused is used for mapping instances of conn
        # to their respective keys in _used
        self._rused = {}
        self._keys = 0
        self._lock = threading.Lock()

        for _ in range(self.connections_min):
            self._connect()

    def _connect(self, key: Union[str, None] = None) -> Client:
        """Create a new client and assign to a key."""
        if self.dsn is not None:
            client = Client.from_url(self.dsn)
        else:
            client = Client(**self.connection_args)
        if key is not None:
            self._used[key] = client
            self._rused[id(client)] = key
        else:
            self._pool.append(client)
        return client

    def _get_key(self):
        """Get an unused key."""
        self._keys += 1
        return self._keys

    def pull(self, key: Union[str, None] = None) -> Client:
        """Get an available client from the pool.

        Args:
            key: If known, the key of the client you would like.

        Returns:
            A clickhouse-driver client.

        """

        self._lock.acquire()
        try:
            if self.closed:
                raise InterfaceError("pool closed")

            if key is None:
                key = self._get_key()

            if key in self._used:
                return self._used[key]

            if self._pool:
                self._used[key] = client = self._pool.pop()
                self._rused[id(client)] = key
                return client

            if len(self._used) >= self.connections_max:
                raise InterfaceError("too many connections")
            return self._connect(key)
        finally:
            self._lock.release()

    def push(
        self, client: Client = None, key: Union[str, None] = None, close: bool = False
    ):
        """Return a client to the pool for reuse.

        Args:
            client: The client to return.
            key: If known, the key of the client.
            close: Close the client instead of adding back to pool.

        """

        self._lock.acquire()
        try:
            if self.closed:
                raise InterfaceError("pool closed")
            if key is None:
                key = self._rused.get(id(client))
                if key is None:
                    raise InterfaceError("trying to put unkeyed client")
            if len(self._pool) < self.connections_min and not close:
                # TODO: verify connection still valid

                # If the connection is currently executing a query, it shouldn't be reused.
                # Explicitly disconnect it instead.
                if client.connection.is_query_executing:
                    client.disconnect()
                if client.connection.connected:
                    self._pool.append(client)
            else:
                client.disconnect()

            # ensure thread doesn't put connection back once the pool is closed
            if not self.closed or key in self._used:
                del self._used[key]
                del self._rused[id(client)]
        finally:
            self._lock.release()

    def cleanup(self):
        """Close all open connections in the pool.

        This method loops through eveery client and calls disconnect.
        """

        self._lock.acquire()
        try:
            if self.closed:
                raise InterfaceError("pool closed")
            for client in self._pool + list(self._used.values()):
                try:
                    client.disconnect()
                # TODO: handle problems with disconnect
                except Exception:
                    pass
            self.closed = True
        finally:
            self._lock.release()

    @contextmanager
    def get_client(self, key: str = None) -> Generator[Client, None, None]:
        """A clean way to grab a client via a contextmanager.


        Args:
            key: If known, the key of the client to grab.

        Yields:
            Client: a clickhouse-driver client

        """
        client = self.pull(key)
        try:
            yield client
        finally:
            self.push(client=client)
