"""
Psycopg null connection pool module (async version).
"""

# Copyright (C) 2022 The Psycopg Team

from __future__ import annotations

import logging
from typing import cast

from psycopg import AsyncConnection
from psycopg.pq import TransactionStatus

from .abc import ACT, AsyncConnectFailedCB, AsyncConnectionCB, AsyncConninfoParam
from .abc import AsyncKwargsParam
from .errors import PoolTimeout, TooManyRequests
from ._compat import ConnectionTimeout
from ._acompat import AEvent
from .pool_async import AddConnection, AsyncConnectionPool
from .base_null_pool import _BaseNullConnectionPool

logger = logging.getLogger("psycopg.pool")


class AsyncNullConnectionPool(_BaseNullConnectionPool, AsyncConnectionPool[ACT]):

    def __init__(
        self,
        conninfo: AsyncConninfoParam = "",
        *,
        connection_class: type[ACT] = cast(type[ACT], AsyncConnection),
        kwargs: AsyncKwargsParam | None = None,
        min_size: int = 0,  # Note: min_size default value changed to 0.
        max_size: int | None = None,
        open: bool | None = None,
        configure: AsyncConnectionCB[ACT] | None = None,
        check: AsyncConnectionCB[ACT] | None = None,
        reset: AsyncConnectionCB[ACT] | None = None,
        name: str | None = None,
        close_returns: bool = False,
        timeout: float = 30.0,
        max_waiting: int = 0,
        max_lifetime: float = 60 * 60.0,
        max_idle: float = 10 * 60.0,
        reconnect_timeout: float = 5 * 60.0,
        reconnect_failed: AsyncConnectFailedCB | None = None,
        num_workers: int = 3,
    ):
        super().__init__(
            conninfo,
            open=open,
            connection_class=connection_class,
            check=check,
            configure=configure,
            reset=reset,
            kwargs=kwargs,
            min_size=min_size,
            max_size=max_size,
            name=name,
            close_returns=False,  # close_returns=True makes no sense
            timeout=timeout,
            max_waiting=max_waiting,
            max_lifetime=max_lifetime,
            max_idle=max_idle,
            reconnect_timeout=reconnect_timeout,
            num_workers=num_workers,
        )

    async def wait(self, timeout: float = 30.0) -> None:
        """
        Create a connection for test.

        Calling this function will verify that the connectivity with the
        database works as expected. However the connection will not be stored
        in the pool.

        Close the pool, and raise `PoolTimeout`, if not ready within *timeout*
        sec.
        """
        self._check_open_getconn()

        async with self._lock:
            assert not self._pool_full_event
            self._pool_full_event = AEvent()

        logger.info("waiting for pool %r initialization", self.name)
        self.run_task(AddConnection(self))
        if not await self._pool_full_event.wait_timeout(timeout):
            await self.close()  # stop all the tasks
            raise PoolTimeout(f"pool initialization incomplete after {timeout} sec")

        async with self._lock:
            assert self._pool_full_event
            self._pool_full_event = None

        logger.info("pool %r is ready to use", self.name)

    async def _get_ready_connection(self, timeout: float | None) -> ACT | None:
        if timeout is not None and timeout <= 0.0:
            raise PoolTimeout()

        conn: ACT | None = None
        if self.max_size == 0 or self._nconns < self.max_size:
            # Create a new connection for the client
            try:
                conn = await self._connect(timeout=timeout)
            except ConnectionTimeout as ex:
                raise PoolTimeout(str(ex)) from None
            self._nconns += 1

        elif self.max_waiting and len(self._waiting) >= self.max_waiting:
            self._stats[self._REQUESTS_ERRORS] += 1
            raise TooManyRequests(
                f"the pool {self.name!r} has already"
                + f" {len(self._waiting)} requests waiting"
            )
        return conn

    async def _maybe_close_connection(self, conn: ACT) -> bool:
        # Close the connection if no client is waiting for it, or if the pool
        # is closed. For extra refcare remove the pool reference from it.
        # Maintain the stats.
        async with self._lock:
            if not self._closed and self._waiting:
                return False

            conn._pool = None
            if conn.pgconn.transaction_status == TransactionStatus.UNKNOWN:
                self._stats[self._RETURNS_BAD] += 1
            await self._close_connection(conn)
            self._nconns -= 1
            return True

    async def resize(self, min_size: int, max_size: int | None = None) -> None:
        """Change the size of the pool during runtime.

        Only *max_size* can be changed; *min_size* must remain 0.
        """
        min_size, max_size = self._check_size(min_size, max_size)

        logger.info(
            "resizing %r to min_size=%s max_size=%s", self.name, min_size, max_size
        )
        async with self._lock:
            self._min_size = min_size
            self._max_size = max_size

    async def check(self) -> None:
        """No-op, as the pool doesn't have connections in its state."""
        pass

    async def _add_to_pool(self, conn: ACT) -> None:
        # Remove the pool reference from the connection before returning it
        # to the state, to avoid to create a reference loop.
        # Also disable the warning for open connection in conn.__del__
        conn._pool = None

        # Critical section: if there is a client waiting give it the connection
        # otherwise put it back into the pool.
        async with self._lock:
            while self._waiting:
                # If there is a client waiting (which is still waiting and
                # hasn't timed out), give it the connection and notify it.
                if await self._waiting.popleft().set(conn):
                    break
            else:
                # No client waiting for a connection: close the connection
                await self._close_connection(conn)
                # If we have been asked to wait for pool init, notify the
                # waiter if the pool is ready.
                if self._pool_full_event:
                    self._pool_full_event.set()
                else:
                    # The connection created by wait shouldn't decrease the
                    # count of the number of connection used.
                    self._nconns -= 1
