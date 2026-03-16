"""
Psycopg connection pool module (async version).
"""

# Copyright (C) 2021 The Psycopg Team

from __future__ import annotations

import logging
import warnings
from abc import ABC, abstractmethod
from time import monotonic
from types import TracebackType
from typing import Any, Generic, cast
from weakref import ref
from contextlib import asynccontextmanager
from collections import deque
from collections.abc import AsyncIterator

from psycopg import AsyncConnection
from psycopg import errors as e
from psycopg.pq import TransactionStatus

from .abc import ACT, AsyncConnectFailedCB, AsyncConnectionCB, AsyncConninfoParam
from .abc import AsyncKwargsParam
from .base import AttemptWithBackoff, BasePool
from .errors import PoolClosed, PoolTimeout, TooManyRequests
from ._compat import PSYCOPG_VERSION, AsyncPoolConnection, Self
from ._acompat import ACondition, AEvent, ALock, AQueue, AWorker, agather, asleep
from ._acompat import aspawn, current_task_name, ensure_async
from .sched_async import AsyncScheduler

if True:  # ASYNC
    import asyncio

    # The exceptions that we need to capture in order to keep the pool
    # consistent and avoid losing connections on errors in callers code.
    CLIENT_EXCEPTIONS = (Exception, asyncio.CancelledError)
else:
    CLIENT_EXCEPTIONS = Exception


logger = logging.getLogger("psycopg.pool")


class AsyncConnectionPool(Generic[ACT], BasePool):
    _pool: deque[ACT]

    def __init__(
        self,
        conninfo: AsyncConninfoParam = "",
        *,
        connection_class: type[ACT] = cast(type[ACT], AsyncConnection),
        kwargs: AsyncKwargsParam | None = None,
        min_size: int = 4,
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
        if close_returns and PSYCOPG_VERSION < (3, 3):
            if connection_class is AsyncConnection:
                connection_class = cast(type[ACT], AsyncPoolConnection)
            else:
                raise TypeError(
                    "Using 'close_returns=True' and a non-standard 'connection_class'"
                    " requires psycopg 3.3 or newer. Please check the docs at"
                    " https://www.psycopg.org/psycopg3/docs/advanced/pool.html"
                    "#pool-sqlalchemy for a workaround."
                )
        self.conninfo = conninfo
        self.kwargs = kwargs
        self.connection_class = connection_class
        self._check = check
        self._configure = configure
        self._reset = reset

        self._reconnect_failed = reconnect_failed

        # If these are asyncio objects, make sure to create them on open
        # to attach them to the right loop.
        self._lock: ALock
        self._sched: AsyncScheduler
        self._tasks: AQueue[MaintenanceTask]

        self._waiting = deque[WaitingClient[ACT]]()

        # to notify that the pool is full
        self._pool_full_event: AEvent | None = None

        self._sched_runner: AWorker | None = None
        self._workers: list[AWorker] = []

        super().__init__(
            min_size=min_size,
            max_size=max_size,
            name=name,
            close_returns=close_returns,
            timeout=timeout,
            max_waiting=max_waiting,
            max_lifetime=max_lifetime,
            max_idle=max_idle,
            reconnect_timeout=reconnect_timeout,
            num_workers=num_workers,
        )

        if True:  # ASYNC
            if open:
                self._warn_open_async()

        if open is None:
            open = self._open_implicit = True

        if open:
            self._open()

    if False:  # ASYNC

        def __del__(self) -> None:
            # If the '_closed' property is not set we probably failed in __init__.
            # Don't try anything complicated as probably it won't work.
            if getattr(self, "_closed", True):
                return

            workers = self._signal_stop_worker()
            hint = (
                "you can try to call 'close()' explicitly "
                "or to use the pool as context manager"
            )
            agather(*workers, timeout=5.0, timeout_hint=hint)

    def _check_open_getconn(self) -> None:
        super()._check_open_getconn()

        if self._open_implicit:
            self._open_implicit = False

            if True:  # ASYNC
                # If open was explicit, we already warned it in __init__
                self._warn_open_async()
            else:
                warnings.warn(
                    f"the default for the {type(self).__name__} 'open' parameter"
                    " will become 'False' in a future release. Please use"
                    " open={True|False} explicitly, or use the pool as context"
                    f" manager using: `with {type(self).__name__}(...) as pool: ...`",
                    DeprecationWarning,
                )

    if True:  # ASYNC

        def _warn_open_async(self) -> None:
            warnings.warn(
                f"opening the async pool {type(self).__name__} in the constructor"
                " is deprecated and will not be supported anymore in a future"
                " release. Please use `await pool.open()`, or use the pool as context"
                f" manager using: `async with {type(self).__name__}(...) as pool: `...",
                RuntimeWarning,
            )

    async def wait(self, timeout: float = 30.0) -> None:
        """
        Wait for the pool to be full (with `min_size` connections) after creation.

        Close the pool, and raise `PoolTimeout`, if not ready within *timeout*
        sec.

        Calling this method is not mandatory: you can try and use the pool
        immediately after its creation. The first client will be served as soon
        as a connection is ready. You can use this method if you prefer your
        program to terminate in case the environment is not configured
        properly, rather than trying to stay up the hardest it can.
        """
        self._check_open_getconn()

        async with self._lock:
            assert not self._pool_full_event
            if len(self._pool) >= self._min_size:
                return
            self._pool_full_event = AEvent()

        logger.info("waiting for pool %r initialization", self.name)
        if not await self._pool_full_event.wait_timeout(timeout):
            await self.close()  # stop all the tasks
            raise PoolTimeout(f"pool initialization incomplete after {timeout} sec")

        async with self._lock:
            assert self._pool_full_event
            self._pool_full_event = None

        logger.info("pool %r is ready to use", self.name)

    @asynccontextmanager
    async def connection(self, timeout: float | None = None) -> AsyncIterator[ACT]:
        """Context manager to obtain a connection from the pool.

        Return the connection immediately if available, otherwise wait up to
        *timeout* or `self.timeout` seconds and throw `PoolTimeout` if a
        connection is not available in time.

        Upon context exit, return the connection to the pool. Apply the normal
        :ref:`connection context behaviour <with-connection>` (commit/rollback
        the transaction in case of success/error). If the connection is no more
        in working state, replace it with a new one.
        """
        conn = await self.getconn(timeout=timeout)
        try:
            t0 = monotonic()
            async with conn:
                yield conn
        finally:
            await self.putconn(conn)
            t1 = monotonic()
            self._stats[self._USAGE_MS] += int(1000.0 * (t1 - t0))

    async def getconn(self, timeout: float | None = None) -> ACT:
        """Obtain a connection from the pool.

        You should preferably use `connection()`. Use this function only if
        it is not possible to use the connection as context manager.

        After using this function you *must* call a corresponding `putconn()`:
        failing to do so will deplete the pool. A depleted pool is a sad pool:
        you don't want a depleted pool.
        """
        if timeout is None:
            timeout = self.timeout
        deadline = monotonic() + timeout

        logger.info("connection requested from %r", self.name)
        self._stats[self._REQUESTS_NUM] += 1

        self._check_open_getconn()

        try:
            return await self._getconn_with_check_loop(deadline)

        # Re-raise the timeout exception presenting the user the global
        # timeout, not the per-attempt one.
        except PoolTimeout:
            raise PoolTimeout(
                f"couldn't get a connection after {timeout:.2f} sec"
            ) from None

    async def _getconn_with_check_loop(self, deadline: float) -> ACT:
        attempt: AttemptWithBackoff | None = None

        while True:
            conn = await self._getconn_unchecked(deadline - monotonic())
            try:
                await self._check_connection(conn)
            except CLIENT_EXCEPTIONS:
                await self._putconn(conn, from_getconn=True)
            else:
                logger.info("connection given by %r", self.name)
                return conn

            # Delay further checks to avoid a busy loop, using the same
            # backoff policy used in reconnection attempts.
            now = monotonic()
            if not attempt:
                attempt = AttemptWithBackoff(timeout=deadline - now)
            else:
                attempt.update_delay(now)

            if attempt.time_to_give_up(now):
                raise PoolTimeout()
            else:
                await asleep(attempt.delay)

    async def _getconn_unchecked(self, timeout: float) -> ACT:
        # Critical section: decide here if there's a connection ready
        # or if the client needs to wait.
        async with self._lock:
            if not (conn := (await self._get_ready_connection(timeout))):
                # No connection available: put the client in the waiting queue
                t0 = monotonic()
                pos: WaitingClient[ACT] = WaitingClient()
                self._waiting.append(pos)
                self._stats[self._REQUESTS_QUEUED] += 1

                # If there is space for the pool to grow, let's do it
                self._maybe_grow_pool()

        # If we are in the waiting queue, wait to be assigned a connection
        # (outside the critical section, so only the waiting client is locked)
        if not conn:
            try:
                conn = await pos.wait(timeout=timeout)
            except CLIENT_EXCEPTIONS:
                self._stats[self._REQUESTS_ERRORS] += 1
                raise
            finally:
                t1 = monotonic()
                self._stats[self._REQUESTS_WAIT_MS] += int(1000.0 * (t1 - t0))

        # Tell the connection it belongs to a pool to avoid closing on __exit__
        # Note that this property shouldn't be set while the connection is in
        # the pool, to avoid to create a reference loop.
        conn._pool = self
        return conn

    async def _get_ready_connection(self, timeout: float | None) -> ACT | None:
        """Return a connection, if the client deserves one."""
        if timeout is not None and timeout <= 0.0:
            raise PoolTimeout()

        conn: ACT | None = None
        if self._pool:
            # Take a connection ready out of the pool
            conn = self._pool.popleft()
            if len(self._pool) < self._nconns_min:
                self._nconns_min = len(self._pool)
        elif self.max_waiting and len(self._waiting) >= self.max_waiting:
            self._stats[self._REQUESTS_ERRORS] += 1
            raise TooManyRequests(
                f"the pool {self.name!r} has already"
                f" {len(self._waiting)} requests waiting"
            )
        return conn

    async def _check_connection(self, conn: ACT) -> None:
        if not self._check:
            return
        try:
            await self._check(conn)
        except CLIENT_EXCEPTIONS as e:
            logger.info("connection failed check: %s", e)
            raise

    def _maybe_grow_pool(self) -> None:
        # Allow only one task at time to grow the pool (or returning
        # connections might be starved).
        if self._nconns >= self._max_size or self._growing:
            return
        self._nconns += 1
        logger.info("growing pool %r to %s", self.name, self._nconns)
        self._growing = True
        self.run_task(AddConnection(self, growing=True))

    async def putconn(self, conn: ACT) -> None:
        """Return a connection to the loving hands of its pool.

        Use this function only paired with a `getconn()`. You don't need to use
        it if you use the much more comfortable `connection()` context manager.
        """
        # Quick check to discard the wrong connection
        self._check_pool_putconn(conn)

        logger.info("returning connection to %r", self.name)
        if await self._maybe_close_connection(conn):
            return

        await self._putconn(conn, from_getconn=False)

    async def drain(self) -> None:
        """
        Remove all the connections from the pool and create new ones.

        If a connection is currently out of the pool it will be closed when
        returned to the pool and replaced with a new one.

        This method is useful to force a connection re-configuration, for
        example when the adapters map changes after the pool was created.
        """
        async with self._lock:
            conns = list(self._pool)
            self._pool.clear()
            self._drained_at = monotonic()

        # Close the connection already in the pool, open new ones.
        for conn in conns:
            await self._close_connection(conn)
            self.run_task(AddConnection(self))

    async def _putconn(self, conn: ACT, from_getconn: bool) -> None:
        # Use a worker to perform eventual maintenance work in a separate task
        if self._reset:
            self.run_task(ReturnConnection(self, conn, from_getconn=from_getconn))
        else:
            await self._return_connection(conn, from_getconn=from_getconn)

    async def _maybe_close_connection(self, conn: ACT) -> bool:
        """Close a returned connection if necessary.

        Return `!True if the connection was closed.
        """
        # If the pool is closed just close the connection instead of returning
        # it to the pool. For extra refcare remove the pool reference from it.
        if not self._closed:
            return False

        await self._close_connection(conn)
        return True

    async def open(self, wait: bool = False, timeout: float = 30.0) -> None:
        """Open the pool by starting connecting and and accepting clients.

        If *wait* is `!False`, return immediately and let the background worker
        fill the pool if `min_size` > 0. Otherwise wait up to *timeout* seconds
        for the requested number of connections to be ready (see `wait()` for
        details).

        It is safe to call `!open()` again on a pool already open (because the
        method was already called, or because the pool context was entered, or
        because the pool was initialized with *open* = `!True`) but you cannot
        currently re-open a closed pool.
        """
        # Make sure the lock is created after there is an event loop
        self._ensure_lock()

        async with self._lock:
            self._open()

        if wait:
            await self.wait(timeout=timeout)

    def _open(self) -> None:
        if not self._closed:
            return

        self._check_open()

        # A lock has been most likely, but not necessarily, created in `open()`.
        self._ensure_lock()

        # Create these objects now to attach them to the right loop.
        # See #219
        self._tasks = AQueue()
        self._sched = AsyncScheduler()

        self._closed = False
        self._opened = True

        self._start_workers()
        self._start_initial_tasks()

    def _ensure_lock(self) -> None:
        """Make sure the pool lock is created.

        In async code, also make sure that the loop is running.
        """
        if True:  # ASYNC
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                raise RuntimeError(
                    f"{type(self).__name__} open with no running loop"
                ) from None

        try:
            self._lock
        except AttributeError:
            self._lock = ALock()

    def _start_workers(self) -> None:
        self._sched_runner = aspawn(self._sched.run, name=f"{self.name}-scheduler")
        assert not self._workers
        for i in range(self.num_workers):
            t = aspawn(self.worker, args=(self._tasks,), name=f"{self.name}-worker-{i}")
            self._workers.append(t)

    def _start_initial_tasks(self) -> None:
        # populate the pool with initial min_size connections in background
        for i in range(self._nconns):
            self.run_task(AddConnection(self))

        # Schedule a task to shrink the pool if connections over min_size have
        # remained unused.
        self.run_task(Schedule(self, ShrinkPool(self), self.max_idle))

    async def close(self, timeout: float = 5.0) -> None:
        """Close the pool and make it unavailable to new clients.

        All the waiting and future clients will fail to acquire a connection
        with a `PoolClosed` exception. Currently used connections will not be
        closed until returned to the pool.

        Wait *timeout* seconds for threads to terminate their job, if positive.
        If the timeout expires the pool is closed anyway, although it may raise
        some warnings on exit.
        """
        if self._closed:
            return

        async with self._lock:
            self._closed = True
            logger.debug("pool %r closed", self.name)

            # Take waiting client and pool connections out of the state
            waiting = list(self._waiting)
            self._waiting.clear()
            connections = list(self._pool)
            self._pool.clear()

            # Take the workers out of the pool. Will stop them outside the lock
            workers = await self._signal_stop_worker()

        # Now that the flag _closed is set, getconn will fail immediately,
        # putconn will just close the returned connection.

        # Wait for the worker tasks to terminate
        await agather(*workers, timeout=timeout)

        # Close the connections that were still in the pool
        for conn in connections:
            await self._close_connection(conn)

        # Signal to eventual clients in the queue that business is closed.
        for pos in waiting:
            await pos.fail(PoolClosed(f"the pool {self.name!r} is closed"))

    async def _signal_stop_worker(self) -> list[AWorker]:
        # Stop the scheduler
        await self._sched.enter(0, None)

        # Stop the worker tasks
        workers, self._workers = self._workers[:], []
        for _ in workers:
            self.run_task(StopWorker(self))

        if self._sched_runner:  # likely
            workers.append(self._sched_runner)
            self._sched_runner = None

        return workers

    async def __aenter__(self) -> Self:
        self._open_implicit = False
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def resize(self, min_size: int, max_size: int | None = None) -> None:
        """Change the size of the pool during runtime."""
        min_size, max_size = self._check_size(min_size, max_size)

        ngrow = max(0, min_size - self._min_size)

        logger.info(
            "resizing %r to min_size=%s max_size=%s", self.name, min_size, max_size
        )
        async with self._lock:
            self._min_size = min_size
            self._max_size = max_size
            self._nconns += ngrow

        for i in range(ngrow):
            self.run_task(AddConnection(self))

    async def check(self) -> None:
        """Verify the state of the connections currently in the pool.

        Test each connection: if it works return it to the pool, otherwise
        dispose of it and create a new one.
        """
        async with self._lock:
            conns = list(self._pool)
            self._pool.clear()

            # Give a chance to the pool to grow if it has no connection.
            # In case there are enough connection, or the pool is already
            # growing, this is a no-op.
            self._maybe_grow_pool()

        while conns:
            conn = conns.pop()

            # Check for expired connections
            if conn._expire_at <= monotonic():
                logger.info("discarding expired connection %s", conn)
                await self._close_connection(conn)
                self.run_task(AddConnection(self))
                continue

            # Check for broken connections
            try:
                await self.check_connection(conn)
            except CLIENT_EXCEPTIONS:
                self._stats[self._CONNECTIONS_LOST] += 1
                logger.warning("discarding broken connection: %s", conn)
                self.run_task(AddConnection(self))
            else:
                await self._add_to_pool(conn)

    @staticmethod
    async def check_connection(conn: ACT) -> None:
        """
        A simple check to verify that a connection is still working.

        Return quietly if the connection is still working, otherwise raise
        an exception.

        Used internally by `check()`, but also available for client usage,
        for instance as `!check` callback when a pool is created.
        """
        if conn.autocommit:
            await conn.execute("")
        else:
            if True:  # ASYNC
                # NOTE: with Psycopg 3.2 we could use conn.set_autocommit() in
                # the sync code too, but we want the pool to be compatible with
                # previous versions too.
                await conn.set_autocommit(True)
                try:
                    await conn.execute("")
                finally:
                    await conn.set_autocommit(False)
            else:
                conn.autocommit = True
                try:
                    conn.execute("")
                finally:
                    conn.autocommit = False

    async def reconnect_failed(self) -> None:
        """
        Called when reconnection failed for longer than `reconnect_timeout`.
        """
        if not self._reconnect_failed:
            return

        await ensure_async(self._reconnect_failed, self)

    def run_task(self, task: MaintenanceTask) -> None:
        """Run a maintenance task in a worker."""
        self._tasks.put_nowait(task)

    async def schedule_task(self, task: MaintenanceTask, delay: float) -> None:
        """Run a maintenance task in a worker in the future."""
        await self._sched.enter(delay, task.tick)

    @classmethod
    async def worker(cls, q: AQueue[MaintenanceTask]) -> None:
        """Runner to execute pending maintenance task.

        The function is designed to run as a task.

        Block on the queue *q*, run a task received. Finish running if a
        StopWorker is received.
        """
        while True:
            if isinstance((task := (await q.get())), StopWorker):
                logger.debug("terminating working task %s", current_task_name())
                return

            # Run the task. Make sure don't die in the attempt.
            try:
                await task.run()
            except CLIENT_EXCEPTIONS as ex:
                logger.warning(
                    "task run %s failed: %s: %s", task, ex.__class__.__name__, ex
                )

    async def _connect(self, timeout: float | None = None) -> ACT:
        """Return a new connection configured for the pool."""
        self._stats[self._CONNECTIONS_NUM] += 1
        conninfo = await self._resolve_conninfo()
        kwargs = await self._resolve_kwargs()
        if timeout:
            kwargs = kwargs.copy()
            kwargs["connect_timeout"] = max(round(timeout), 1)
        t0 = monotonic()
        try:
            conn = await self.connection_class.connect(conninfo, **kwargs)
        except CLIENT_EXCEPTIONS:
            self._stats[self._CONNECTIONS_ERRORS] += 1
            raise
        else:
            t1 = monotonic()
            self._stats[self._CONNECTIONS_MS] += int(1000.0 * (t1 - t0))

        conn._pool = self

        if self._configure:
            await self._configure(conn)
            if (status := conn.pgconn.transaction_status) != TransactionStatus.IDLE:
                sname = TransactionStatus(status).name
                raise e.ProgrammingError(
                    f"connection left in status {sname} by configure function"
                    f" {self._configure}: discarded"
                )

        # Set an expiry date, with some randomness to avoid mass reconnection
        self._set_connection_expiry_date(conn)
        return conn

    async def _resolve_conninfo(self) -> str:
        """Resolve conninfo (static string, sync callable, or async callable)."""
        if callable(self.conninfo):
            return await ensure_async(self.conninfo)

        return self.conninfo or ""

    async def _resolve_kwargs(self) -> dict[str, Any]:
        """Resolve kwargs (static dict, sync callable, or async callable)."""
        if not self.kwargs:
            return {}

        if callable(self.kwargs):
            return await ensure_async(self.kwargs)

        return self.kwargs

    async def _add_connection(
        self, attempt: AttemptWithBackoff | None, growing: bool = False
    ) -> None:
        """Try to connect and add the connection to the pool.

        If failed, reschedule a new attempt in the future for a few times, then
        give up, decrease the pool connections number and call
        `self.reconnect_failed()`.

        """
        now = monotonic()
        if not attempt:
            attempt = AttemptWithBackoff(timeout=self.reconnect_timeout)

        try:
            conn = await self._connect()
        except CLIENT_EXCEPTIONS as ex:
            logger.warning("error connecting in %r: %s", self.name, ex)
            if attempt.time_to_give_up(now):
                logger.warning(
                    "reconnection attempt in pool %r failed after %s sec",
                    self.name,
                    self.reconnect_timeout,
                )
                async with self._lock:
                    self._nconns -= 1
                    # If we have given up with a growing attempt, allow a new one.
                    if growing and self._growing:
                        self._growing = False
                await self.reconnect_failed()
            else:
                attempt.update_delay(now)
                await self.schedule_task(
                    AddConnection(self, attempt, growing=growing), attempt.delay
                )
            return

        logger.info("adding new connection to the pool")
        await self._add_to_pool(conn)
        if growing:
            async with self._lock:
                # Keep on growing if the pool is not full yet, or if there are
                # clients waiting and the pool can extend.
                if self._nconns < self._min_size or (
                    self._nconns < self._max_size and self._waiting
                ):
                    self._nconns += 1
                    logger.info("growing pool %r to %s", self.name, self._nconns)
                    self.run_task(AddConnection(self, growing=True))
                else:
                    self._growing = False

    async def _return_connection(self, conn: ACT, from_getconn: bool) -> None:
        """
        Return a connection to the pool after usage.
        """
        await self._reset_connection(conn)
        if from_getconn:
            if conn.pgconn.transaction_status == TransactionStatus.UNKNOWN:
                self._stats[self._CONNECTIONS_LOST] += 1
                # Connection no more in working state: create a new one.
                logger.info("not serving connection found broken")
                self.run_task(AddConnection(self))
                return

        else:
            if conn.pgconn.transaction_status == TransactionStatus.UNKNOWN:
                self._stats[self._RETURNS_BAD] += 1
                # Connection no more in working state: create a new one.
                logger.warning("discarding closed connection: %s", conn)
                self.run_task(AddConnection(self))
                return

        # Check if the connection is past its best before date
        if conn._created_at <= self._drained_at or conn._expire_at <= monotonic():
            logger.info("discarding expired connection")
            await self._close_connection(conn)
            self.run_task(AddConnection(self))
            return

        await self._add_to_pool(conn)

    async def _add_to_pool(self, conn: ACT) -> None:
        """
        Add a connection to the pool.

        The connection can be a fresh one or one already used in the pool.

        If a client is already waiting for a connection pass it on, otherwise
        put it back into the pool
        """
        # Remove the pool reference from the connection before returning it
        # to the state, to avoid to create a reference loop.
        # Also disable the warning for open connection in conn.__del__
        conn._pool = None

        # Early bailout in case the pool is closed. Don't add anything to the
        # state. There is still a remote chance that the pool will be closed
        # between here and entering the lock. Therefore we will make another
        # check later.
        if self._closed:
            await self._close_connection(conn)
            return

        # Critical section: if there is a client waiting give it the connection
        # otherwise put it back into the pool.
        async with self._lock:

            # Check if the pool was closed by the time we arrived here. It is
            # unlikely but it doesn't seem impossible, if the worker was adding
            # this connection while the main process is closing the pool.
            # Now that we are in the critical section we know for real.
            if self._closed:
                await self._close_connection(conn)
                return

            while self._waiting:
                # If there is a client waiting (which is still waiting and
                # hasn't timed out), give it the connection and notify it.

                if await self._waiting.popleft().set(conn):
                    break
            else:
                # No client waiting for a connection: put it back into the pool
                self._pool.append(conn)
                # If we have been asked to wait for pool init, notify the
                # waiter if the pool is full.
                if self._pool_full_event and len(self._pool) >= self._min_size:
                    self._pool_full_event.set()

    async def _reset_connection(self, conn: ACT) -> None:
        """
        Bring a connection to IDLE state or close it.
        """
        if (status := conn.pgconn.transaction_status) == TransactionStatus.IDLE:
            pass
        elif status == TransactionStatus.UNKNOWN:
            # Connection closed
            return

        elif status == TransactionStatus.INTRANS or status == TransactionStatus.INERROR:
            # Connection returned with an active transaction
            logger.warning("rolling back returned connection: %s", conn)
            try:
                await conn.rollback()
            except CLIENT_EXCEPTIONS as ex:
                logger.warning(
                    "rollback failed: %s: %s. Discarding connection %s",
                    ex.__class__.__name__,
                    ex,
                    conn,
                )
                await self._close_connection(conn)

        elif status == TransactionStatus.ACTIVE:
            # Connection returned during an operation. Bad... just close it.
            logger.warning("closing returned connection: %s", conn)
            await self._close_connection(conn)

        if self._reset:
            try:
                await self._reset(conn)
                if (status := conn.pgconn.transaction_status) != TransactionStatus.IDLE:
                    sname = TransactionStatus(status).name
                    raise e.ProgrammingError(
                        f"connection left in status {sname} by reset function"
                        f" {self._reset}: discarded"
                    )
            except CLIENT_EXCEPTIONS as ex:
                logger.warning("error resetting connection: %s", ex)
                await self._close_connection(conn)

    async def _close_connection(self, conn: ACT) -> None:
        conn._pool = None
        await conn.close()

    async def _shrink_pool(self) -> None:
        to_close: ACT | None = None

        async with self._lock:
            # Reset the min number of connections used
            nconns_min = self._nconns_min
            self._nconns_min = len(self._pool)

            # If the pool can shrink and connections were unused, drop one
            if self._nconns > self._min_size and nconns_min > 0 and self._pool:
                to_close = self._pool.popleft()
                self._nconns -= 1
                self._nconns_min -= 1

        if to_close:
            logger.info(
                "shrinking pool %r to %s because %s unused connections"
                " in the last %s sec",
                self.name,
                self._nconns,
                nconns_min,
                self.max_idle,
            )
            await self._close_connection(to_close)

    def _get_measures(self) -> dict[str, int]:
        rv = super()._get_measures()
        rv[self._REQUESTS_WAITING] = len(self._waiting)
        return rv


class WaitingClient(Generic[ACT]):
    """A position in a queue for a client waiting for a connection."""

    __slots__ = ("conn", "error", "_cond")

    def __init__(self) -> None:
        self.conn: ACT | None = None
        self.error: BaseException | None = None

        # The WaitingClient behaves in a way similar to an Event, but we need
        # to notify reliably the flagger that the waiter has "accepted" the
        # message and it hasn't timed out yet, otherwise the pool may give a
        # connection to a client that has already timed out getconn(), which
        # will be lost.
        self._cond = ACondition()

    async def wait(self, timeout: float) -> ACT:
        """Wait for a connection to be set and return it.

        Raise an exception if the wait times out or if fail() is called.
        """
        async with self._cond:
            if not (self.conn or self.error):
                try:
                    if not await self._cond.wait_timeout(timeout):
                        self.error = PoolTimeout(
                            f"couldn't get a connection after {timeout:.2f} sec"
                        )
                except CLIENT_EXCEPTIONS as ex:
                    self.error = ex

        if self.conn:
            return self.conn
        else:
            assert self.error
            raise self.error

    async def set(self, conn: ACT) -> bool:
        """Signal the client waiting that a connection is ready.

        Return True if the client has "accepted" the connection, False
        otherwise (typically because wait() has timed out).
        """
        async with self._cond:
            if self.conn or self.error:
                return False

            self.conn = conn
            self._cond.notify_all()
            return True

    async def fail(self, error: Exception) -> bool:
        """Signal the client that, alas, they won't have a connection today.

        Return True if the client has "accepted" the error, False otherwise
        (typically because wait() has timed out).
        """
        async with self._cond:
            if self.conn or self.error:
                return False

            self.error = error
            self._cond.notify_all()
            return True


class MaintenanceTask(ABC):
    """A task to run asynchronously to maintain the pool state."""

    def __init__(self, pool: AsyncConnectionPool[Any]):
        self.pool = ref(pool)

    def __repr__(self) -> str:
        pool = self.pool()
        name = repr(pool.name) if pool else "<pool is gone>"
        return f"<{self.__class__.__name__} {name} at 0x{id(self):x}>"

    async def run(self) -> None:
        """Run the task.

        This usually happens in a worker. Call the concrete _run()
        implementation, if the pool is still alive.
        """
        pool = self.pool()
        if not pool or pool.closed:
            # Pool is no more working. Quietly discard the operation.
            logger.debug("task run discarded: %s", self)
            return

        logger.debug("task running in %s: %s", current_task_name(), self)
        await self._run(pool)

    async def tick(self) -> None:
        """Run the scheduled task

        This function is called by the scheduler task. Use a worker to
        run the task for real in order to free the scheduler immediately.
        """
        pool = self.pool()
        if not pool or pool.closed:
            # Pool is no more working. Quietly discard the operation.
            logger.debug("task tick discarded: %s", self)
            return

        pool.run_task(self)

    @abstractmethod
    async def _run(self, pool: AsyncConnectionPool[Any]) -> None: ...


class StopWorker(MaintenanceTask):
    """Signal the maintenance worker to terminate."""

    async def _run(self, pool: AsyncConnectionPool[Any]) -> None:
        pass


class AddConnection(MaintenanceTask):
    def __init__(
        self,
        pool: AsyncConnectionPool[Any],
        attempt: AttemptWithBackoff | None = None,
        growing: bool = False,
    ):
        super().__init__(pool)
        self.attempt = attempt
        self.growing = growing

    async def _run(self, pool: AsyncConnectionPool[Any]) -> None:
        await pool._add_connection(self.attempt, growing=self.growing)


class ReturnConnection(MaintenanceTask):
    """Clean up and return a connection to the pool."""

    def __init__(self, pool: AsyncConnectionPool[Any], conn: ACT, from_getconn: bool):
        super().__init__(pool)
        self.conn = conn
        self.from_getconn = from_getconn

    async def _run(self, pool: AsyncConnectionPool[Any]) -> None:
        await pool._return_connection(self.conn, from_getconn=self.from_getconn)


class ShrinkPool(MaintenanceTask):
    """If the pool can shrink, remove one connection.

    Re-schedule periodically and also reset the minimum number of connections
    in the pool.
    """

    async def _run(self, pool: AsyncConnectionPool[Any]) -> None:
        # Reschedule the task now so that in case of any error we don't lose
        # the periodic run.
        await pool.schedule_task(self, pool.max_idle)
        await pool._shrink_pool()


class Schedule(MaintenanceTask):
    """Schedule a task in the pool scheduler.

    This task is a trampoline to allow to use a sync call (pool.run_task)
    to execute an async one (pool.schedule_task). It is pretty much no-op
    in sync code.
    """

    def __init__(
        self, pool: AsyncConnectionPool[Any], task: MaintenanceTask, delay: float
    ):
        super().__init__(pool)
        self.task = task
        self.delay = delay

    async def _run(self, pool: AsyncConnectionPool[Any]) -> None:
        await pool.schedule_task(self.task, self.delay)
