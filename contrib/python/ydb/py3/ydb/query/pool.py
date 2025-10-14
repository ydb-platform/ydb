import logging
from concurrent import futures
from typing import (
    Callable,
    Optional,
    List,
    Dict,
    Any,
    Union,
)
import time
import threading
import queue

from .base import BaseQueryTxMode, QueryExplainResultFormat
from .base import QueryClientSettings
from .session import (
    QuerySession,
)
from ..retries import (
    RetrySettings,
    retry_operation_sync,
)
from .. import issues
from .. import convert
from ..settings import BaseRequestSettings
from .._grpc.grpcwrapper import common_utils
from .._grpc.grpcwrapper import ydb_query_public_types as _ydb_query_public


logger = logging.getLogger(__name__)


class QuerySessionPool:
    """QuerySessionPool is an object to simplify operations with sessions of Query Service."""

    def __init__(
        self,
        driver: common_utils.SupportedDriverType,
        size: int = 100,
        *,
        query_client_settings: Optional[QueryClientSettings] = None,
        workers_threads_count: int = 4,
    ):
        """
        :param driver: A driver instance.
        :param size: Max size of Session Pool.
        :param query_client_settings: ydb.QueryClientSettings object to configure QueryService behavior
        :param workers_threads_count: A number of threads in executor used for *_async methods
        """

        self._driver = driver
        self._tp = futures.ThreadPoolExecutor(workers_threads_count)
        self._queue = queue.Queue()
        self._current_size = 0
        self._size = size
        self._should_stop = threading.Event()
        self._lock = threading.RLock()
        self._query_client_settings = query_client_settings

    def _create_new_session(self, timeout: Optional[float]):
        session = QuerySession(self._driver, settings=self._query_client_settings)
        session.create(settings=BaseRequestSettings().with_timeout(timeout))
        logger.debug(f"New session was created for pool. Session id: {session._state.session_id}")
        return session

    def acquire(self, timeout: Optional[float] = None) -> QuerySession:
        """Acquire a session from Session Pool.

        :param timeout: A timeout to wait in seconds.

        :return A QuerySession object.
        """

        start = time.monotonic()

        lock_acquire_timeout = timeout if timeout is not None else -1
        acquired = self._lock.acquire(timeout=lock_acquire_timeout)
        try:
            if self._should_stop.is_set():
                logger.error("An attempt to take session from closed session pool.")
                raise issues.SessionPoolClosed()

            session = None
            try:
                session = self._queue.get_nowait()
            except queue.Empty:
                pass

            finish = time.monotonic()
            timeout = timeout - (finish - start) if timeout is not None else None

            start = time.monotonic()
            if session is None and self._current_size == self._size:
                try:
                    session = self._queue.get(block=True, timeout=timeout)
                except queue.Empty:
                    raise issues.SessionPoolEmpty("Timeout on acquire session")

            if session is not None:
                if session._state.attached:
                    logger.debug(f"Acquired active session from queue: {session._state.session_id}")
                    return session
                else:
                    self._current_size -= 1
                    logger.debug(f"Acquired dead session from queue: {session._state.session_id}")

            logger.debug(f"Session pool is not large enough: {self._current_size} < {self._size}, will create new one.")
            finish = time.monotonic()
            time_left = timeout - (finish - start) if timeout is not None else None
            session = self._create_new_session(time_left)

            self._current_size += 1
            return session
        finally:
            if acquired:
                self._lock.release()

    def release(self, session: QuerySession) -> None:
        """Release a session back to Session Pool."""

        self._queue.put_nowait(session)
        logger.debug("Session returned to queue: %s", session._state.session_id)

    def checkout(self, timeout: Optional[float] = None) -> "SimpleQuerySessionCheckout":
        """Return a Session context manager, that acquires session on enter and releases session on exit.

        :param timeout: A timeout to wait in seconds.
        """

        return SimpleQuerySessionCheckout(self, timeout)

    def retry_operation_sync(self, callee: Callable, retry_settings: Optional[RetrySettings] = None, *args, **kwargs):
        """Special interface to execute a bunch of commands with session in a safe, retriable way.

        :param callee: A function, that works with session.
        :param retry_settings: RetrySettings object.

        :return: Result sets or exception in case of execution errors.
        """

        if self._should_stop.is_set():
            raise issues.SessionPoolClosed()

        retry_settings = RetrySettings() if retry_settings is None else retry_settings

        def wrapped_callee():
            with self.checkout(timeout=retry_settings.max_session_acquire_timeout) as session:
                return callee(session, *args, **kwargs)

        return retry_operation_sync(wrapped_callee, retry_settings)

    def retry_tx_async(
        self,
        callee: Callable,
        tx_mode: Optional[BaseQueryTxMode] = None,
        retry_settings: Optional[RetrySettings] = None,
        *args,
        **kwargs,
    ) -> futures.Future:
        """Asynchronously execute a transaction in a retriable way."""

        if self._should_stop.is_set():
            raise issues.SessionPoolClosed()

        return self._tp.submit(
            self.retry_tx_sync,
            callee,
            tx_mode,
            retry_settings,
            *args,
            **kwargs,
        )

    def retry_operation_async(
        self, callee: Callable, retry_settings: Optional[RetrySettings] = None, *args, **kwargs
    ) -> futures.Future:
        """Asynchronously execute a retryable operation."""

        if self._should_stop.is_set():
            raise issues.SessionPoolClosed()

        return self._tp.submit(self.retry_operation_sync, callee, retry_settings, *args, **kwargs)

    def retry_tx_sync(
        self,
        callee: Callable,
        tx_mode: Optional[BaseQueryTxMode] = None,
        retry_settings: Optional[RetrySettings] = None,
        *args,
        **kwargs,
    ):
        """Special interface to execute a bunch of commands with transaction in a safe, retriable way.

        :param callee: A function, that works with session.
        :param tx_mode: Transaction mode, which is a one from the following choices:
          1) QuerySerializableReadWrite() which is default mode;
          2) QueryOnlineReadOnly(allow_inconsistent_reads=False);
          3) QuerySnapshotReadOnly();
          4) QueryStaleReadOnly().
        :param retry_settings: RetrySettings object.

        :return: Result sets or exception in case of execution errors.
        """

        if self._should_stop.is_set():
            raise issues.SessionPoolClosed()

        tx_mode = tx_mode if tx_mode else _ydb_query_public.QuerySerializableReadWrite()
        retry_settings = RetrySettings() if retry_settings is None else retry_settings

        def wrapped_callee():
            with self.checkout(timeout=retry_settings.max_session_acquire_timeout) as session:
                with session.transaction(tx_mode=tx_mode) as tx:
                    if tx_mode.name in ["serializable_read_write", "snapshot_read_only"]:
                        tx.begin()
                    result = callee(tx, *args, **kwargs)
                    tx.commit()
                return result

        return retry_operation_sync(wrapped_callee, retry_settings)

    def execute_with_retries(
        self,
        query: str,
        parameters: Optional[dict] = None,
        retry_settings: Optional[RetrySettings] = None,
        *args,
        **kwargs,
    ) -> List[convert.ResultSet]:
        """Special interface to execute a one-shot queries in a safe, retriable way.
        Note: this method loads all data from stream before return, do not use this
        method with huge read queries.

        :param query: A query, yql or sql text.
        :param parameters: dict with parameters and YDB types;
        :param retry_settings: RetrySettings object.

        :return: Result sets or exception in case of execution errors.
        """

        if self._should_stop.is_set():
            raise issues.SessionPoolClosed()

        retry_settings = RetrySettings() if retry_settings is None else retry_settings

        def wrapped_callee():
            with self.checkout(timeout=retry_settings.max_session_acquire_timeout) as session:
                it = session.execute(query, parameters, *args, **kwargs)
                return [result_set for result_set in it]

        return retry_operation_sync(wrapped_callee, retry_settings)

    def execute_with_retries_async(
        self,
        query: str,
        parameters: Optional[dict] = None,
        retry_settings: Optional[RetrySettings] = None,
        *args,
        **kwargs,
    ) -> futures.Future:
        """Asynchronously execute a query with retries."""

        if self._should_stop.is_set():
            raise issues.SessionPoolClosed()

        return self._tp.submit(
            self.execute_with_retries,
            query,
            parameters,
            retry_settings,
            *args,
            **kwargs,
        )

    def explain_with_retries(
        self,
        query: str,
        parameters: Optional[dict] = None,
        *,
        result_format: QueryExplainResultFormat = QueryExplainResultFormat.STR,
        retry_settings: Optional[RetrySettings] = None,
    ) -> Union[str, Dict[str, Any]]:
        """
        Explain a query in retriable way. No real query execution will happen.

        :param query: A query, yql or sql text.
        :param parameters: dict with parameters and YDB types;
        :param result_format: Return format: string or dict.
        :param retry_settings: RetrySettings object.
        :return: Parsed query plan.
        """

        def callee(session: QuerySession):
            return session.explain(query, parameters, result_format=result_format)

        return self.retry_operation_sync(callee, retry_settings)

    def stop(self, timeout=None):
        acquire_timeout = timeout if timeout is not None else -1
        acquired = self._lock.acquire(timeout=acquire_timeout)
        try:
            self._should_stop.set()
            self._tp.shutdown(wait=True)
            while True:
                try:
                    session = self._queue.get_nowait()
                    session.delete()
                except queue.Empty:
                    break

            logger.debug("All session were deleted.")
        finally:
            if acquired:
                self._lock.release()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


class SimpleQuerySessionCheckout:
    def __init__(self, pool: QuerySessionPool, timeout: Optional[float]):
        self._pool = pool
        self._timeout = timeout
        self._session = None

    def __enter__(self) -> QuerySession:
        self._session = self._pool.acquire(self._timeout)
        return self._session

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._pool.release(self._session)
