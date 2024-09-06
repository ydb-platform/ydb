import asyncio
import logging
import time
import typing

import ydb

from ydb import issues, settings as settings_impl, table

from ydb.table import (
    BaseSession,
    BaseTableClient,
    _scan_query_request_factory,
    _wrap_scan_query_response,
    BaseTxContext,
)
from . import _utilities
from ydb import _apis, _session_impl

logger = logging.getLogger(__name__)


class Session(BaseSession):
    async def read_table(
        self,
        path,
        key_range=None,
        columns=(),
        ordered=False,
        row_limit=None,
        settings=None,
        use_snapshot=None,
    ):  # pylint: disable=W0236
        request = _session_impl.read_table_request_factory(
            self._state,
            path,
            key_range,
            columns,
            ordered,
            row_limit,
            use_snapshot=use_snapshot,
        )
        stream_it = await self._driver(
            request,
            _apis.TableService.Stub,
            _apis.TableService.StreamReadTable,
            settings=settings,
        )
        return _utilities.AsyncResponseIterator(stream_it, _session_impl.wrap_read_table_response)

    async def keep_alive(self, settings=None):  # pylint: disable=W0236
        return await super().keep_alive(settings)

    async def create(self, settings=None):  # pylint: disable=W0236
        res = super().create(settings)
        if asyncio.iscoroutine(res):
            res = await res
        return res

    async def delete(self, settings=None):  # pylint: disable=W0236
        return await super().delete(settings)

    async def execute_scheme(self, yql_text, settings=None):  # pylint: disable=W0236
        return await super().execute_scheme(yql_text, settings)

    async def prepare(self, query, settings=None):  # pylint: disable=W0236
        res = super().prepare(query, settings)
        if asyncio.iscoroutine(res):
            res = await res
        return res

    async def explain(self, yql_text, settings=None):  # pylint: disable=W0236
        return await super().explain(yql_text, settings)

    async def create_table(self, path, table_description, settings=None):  # pylint: disable=W0236
        return await super().create_table(path, table_description, settings)

    async def drop_table(self, path, settings=None):  # pylint: disable=W0236
        return await super().drop_table(path, settings)

    async def alter_table(
        self,
        path,
        add_columns=None,
        drop_columns=None,
        settings=None,
        alter_attributes=None,
        add_indexes=None,
        drop_indexes=None,
        set_ttl_settings=None,
        drop_ttl_settings=None,
        add_column_families=None,
        alter_column_families=None,
        alter_storage_settings=None,
        set_compaction_policy=None,
        alter_partitioning_settings=None,
        set_key_bloom_filter=None,
        set_read_replicas_settings=None,
    ):  # pylint: disable=W0236,R0913,R0914
        return await super().alter_table(
            path,
            add_columns,
            drop_columns,
            settings,
            alter_attributes,
            add_indexes,
            drop_indexes,
            set_ttl_settings,
            drop_ttl_settings,
            add_column_families,
            alter_column_families,
            alter_storage_settings,
            set_compaction_policy,
            alter_partitioning_settings,
            set_key_bloom_filter,
            set_read_replicas_settings,
        )

    def transaction(self, tx_mode=None, *, allow_split_transactions=None):
        return TxContext(
            self._driver,
            self._state,
            self,
            tx_mode,
            allow_split_transactions=allow_split_transactions,
        )

    async def describe_table(self, path, settings=None):  # pylint: disable=W0236
        return await super().describe_table(path, settings)

    async def copy_table(self, source_path, destination_path, settings=None):  # pylint: disable=W0236
        return await super().copy_table(source_path, destination_path, settings)

    async def copy_tables(self, source_destination_pairs, settings=None):  # pylint: disable=W0236
        return await super().copy_tables(source_destination_pairs, settings)

    async def rename_tables(self, rename_items, settings=None):  # pylint: disable=W0236
        return await super().rename_tables(rename_items, settings)


class TableClient(BaseTableClient):
    def session(self):
        return Session(self._driver, self._table_client_settings)

    async def bulk_upsert(self, *args, **kwargs):  # pylint: disable=W0236
        return await super().bulk_upsert(*args, **kwargs)

    async def scan_query(self, query, parameters=None, settings=None):  # pylint: disable=W0236
        request = _scan_query_request_factory(query, parameters, settings)
        response = await self._driver(
            request,
            _apis.TableService.Stub,
            _apis.TableService.StreamExecuteScanQuery,
            settings=settings,
        )
        return _utilities.AsyncResponseIterator(
            response,
            lambda resp: _wrap_scan_query_response(resp, self._table_client_settings),
        )


class TxContext(BaseTxContext):
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._tx_state.tx_id is not None:
            # It's strictly recommended to close transactions directly
            # by using commit_tx=True flag while executing statement or by
            # .commit() or .rollback() methods, but here we trying to do best
            # effort to avoid useless open transactions
            logger.warning("Potentially leaked tx: %s", self._tx_state.tx_id)
            try:
                await self.rollback()
            except issues.Error:
                logger.warning("Failed to rollback leaked tx: %s", self._tx_state.tx_id)

            self._tx_state.tx_id = None

    async def execute(self, query, parameters=None, commit_tx=False, settings=None):  # pylint: disable=W0236

        return await super().execute(query, parameters, commit_tx, settings)

    async def commit(self, settings=None):  # pylint: disable=W0236
        res = super().commit(settings)
        if asyncio.iscoroutine(res):
            res = await res
        return res

    async def rollback(self, settings=None):  # pylint: disable=W0236
        res = super().rollback(settings)
        if asyncio.iscoroutine(res):
            res = await res
        return res

    async def begin(self, settings=None):  # pylint: disable=W0236
        res = super().begin(settings)
        if asyncio.iscoroutine(res):
            res = await res
        return res


async def retry_operation(callee, retry_settings=None, *args, **kwargs):  # pylint: disable=W1113
    """
    The retry operation helper can be used to retry a coroutine that raises YDB specific
    exceptions.

    :param callee: A coroutine to retry.
    :param retry_settings: An instance of ydb.RetrySettings that describes how the coroutine
    should be retried. If None, default instance of retry settings will be used.
    :param args: A tuple with positional arguments to be passed into the coroutine.
    :param kwargs: A dictionary with keyword arguments to be passed into the coroutine.

    Returns awaitable result of coroutine. If retries are not succussful exception is raised.
    """

    opt_generator = ydb.retry_operation_impl(callee, retry_settings, *args, **kwargs)
    for next_opt in opt_generator:
        if isinstance(next_opt, ydb.YdbRetryOperationSleepOpt):
            await asyncio.sleep(next_opt.timeout)
        else:
            try:
                return await next_opt.result
            except BaseException as e:  # pylint: disable=W0703
                next_opt.set_exception(e)


class SessionCheckout:
    __slots__ = ("_acquired", "_pool", "_blocking", "_timeout", "_retry_timeout")

    def __init__(self, pool, timeout, retry_timeout):
        """
        A context manager that checkouts a session from the specified pool and
        returns it on manager exit.
        :param pool: A SessionPool instance
        :param blocking: A flag that specifies that session acquire method should blocks
        :param timeout: A timeout in seconds for session acquire
        """
        self._pool: SessionPool = pool
        self._acquired = None
        self._timeout = timeout
        self._retry_timeout = retry_timeout

    async def __aenter__(self):
        self._acquired = await self._pool.acquire(self._timeout, self._retry_timeout)
        return self._acquired

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._acquired is not None:
            await self._pool.release(self._acquired)


class SessionPool:
    def __init__(self, driver: "ydb.aio.Driver", size: int, min_pool_size: int = 0):
        self._driver_await_timeout = 3
        self._should_stop = asyncio.Event()
        self._waiters = 0
        self._driver = driver
        self._active_queue = asyncio.PriorityQueue()
        self._active_count = 0
        self._size = size
        self._req_settings = settings_impl.BaseRequestSettings().with_timeout(3)
        self._logger = logger.getChild(self.__class__.__name__)
        self._min_pool_size = min_pool_size
        self._keep_alive_threshold = 4 * 60
        self._terminating = False
        self._init_session_timeout = 20

        self._keep_alive_task = asyncio.ensure_future(self._keep_alive_loop())

        self._min_pool_tasks = []

        for _ in range(self._min_pool_size):
            self._min_pool_tasks.append(asyncio.ensure_future(self._init_and_put(self._init_session_timeout)))

    async def retry_operation(
        self, callee: typing.Callable, *args, retry_settings: table.RetrySettings = None, **kwargs
    ):

        if retry_settings is None:
            retry_settings = table.RetrySettings()

        async def wrapper_callee():
            async with self.checkout(timeout=retry_settings.get_session_client_timeout) as session:
                return await callee(session, *args, **kwargs)

        return await retry_operation(wrapper_callee, retry_settings)

    def _create(self) -> Session:
        self._active_count += 1
        session = self._driver.table_client.session()
        self._logger.debug("Created session %s", session)
        return session

    async def _init_session_logic(self, session: ydb.ISession) -> typing.Optional[ydb.ISession]:
        try:
            await self._driver.wait(self._driver_await_timeout)
            session = await session.create(self._req_settings)
            return session
        except issues.Error as e:
            self._logger.error("Failed to create session. Reason: %s", str(e))
        except Exception as e:  # pylint: disable=W0703
            self._logger.exception("Failed to create session. Reason: %s", str(e))
        except BaseException as e:  # pylint: disable=W0703
            self._logger.exception("Failed to create session. Reason (base exception): %s", str(e))
            raise

        return None

    async def _init_session(self, session: ydb.ISession, retry_num: int = None) -> typing.Optional[ydb.ISession]:
        """
        :param retry_num: Number of retries. If None - retries until success.
        :return:
        """
        i = 0
        while retry_num is None or i < retry_num:
            curr_sess = await self._init_session_logic(session)
            if curr_sess:
                return curr_sess
            i += 1
        return None

    async def _prepare_session(self, timeout, retry_num) -> ydb.ISession:
        session = self._create()
        try:
            new_sess = await asyncio.wait_for(self._init_session(session, retry_num=retry_num), timeout=timeout)
            if not new_sess:
                self._destroy(session)
            return new_sess
        except BaseException as e:
            self._destroy(session)
            raise e

    async def _get_session_from_queue(self, timeout: float):
        task_wait = asyncio.ensure_future(asyncio.wait_for(self._active_queue.get(), timeout=timeout))
        task_should_stop = asyncio.ensure_future(self._should_stop.wait())
        done, _ = await asyncio.wait((task_wait, task_should_stop), return_when=asyncio.FIRST_COMPLETED)
        if task_should_stop in done:
            task_wait.cancel()
            return self._create()
        _, session = task_wait.result()
        return session

    async def acquire(self, timeout: float = None, retry_timeout: float = None, retry_num: int = None) -> Session:

        if self._should_stop.is_set():
            self._logger.error("Take session from closed session pool")
            raise ValueError("Take session from closed session pool.")

        if retry_timeout is None:
            retry_timeout = timeout

        try:
            _, session = self._active_queue.get_nowait()
            self._logger.debug("Acquired active session from queue: %s", session.session_id)
            return session
        except asyncio.QueueEmpty:
            pass

        if self._active_count < self._size:
            self._logger.debug(
                "Session pool is not large enough (active_count < size: %d < %d). " "will create a new session.",
                self._active_count,
                self._size,
            )
            try:
                session = await self._prepare_session(timeout=retry_timeout, retry_num=retry_num)
            except asyncio.TimeoutError:
                raise issues.SessionPoolEmpty("Timeout when creating session") from None

            if session is not None:
                self._logger.debug("Acquired new created session: %s", session.session_id)
                return session

        try:
            self._waiters += 1
            session = await self._get_session_from_queue(timeout)
            return session
        except asyncio.TimeoutError:
            raise issues.SessionPoolEmpty("Timeout when wait") from None
        finally:
            self._waiters -= 1

    def _is_min_pool_size_satisfied(self, delta=0):
        if self._terminating:
            return True
        return self._active_count + delta >= self._min_pool_size

    async def _init_and_put(self, timeout=10):
        sess = await self._prepare_session(timeout=timeout, retry_num=None)
        await self.release(session=sess)

    def _destroy(self, session: ydb.ISession, wait_for_del: bool = False):
        self._logger.debug("Requested session destroy: %s.", session)
        self._active_count -= 1
        self._logger.debug(
            "Session %s is no longer active. Current active count %d.",
            session,
            self._active_count,
        )

        if self._waiters > 0 or not self._is_min_pool_size_satisfied():
            asyncio.ensure_future(self._init_and_put(self._init_session_timeout))

        if session.initialized():
            coro = session.delete(self._req_settings)
            if wait_for_del:
                self._logger.debug("Sent delete on session %s", session)
                return coro
            else:
                asyncio.ensure_future(coro)
        return None

    async def release(self, session: Session):
        self._release_nowait(session)

    def _release_nowait(self, session: Session):
        self._logger.debug("Put on session %s", session.session_id)
        if session.closing():
            self._destroy(session)
            return False

        if session.pending_query():
            self._destroy(session)
            return False
        if not session.initialized() or self._should_stop.is_set():
            self._destroy(session)
            return False

        # self._active_queue has no size limit, it means that put_nowait will be successfully always
        self._active_queue.put_nowait((time.time() + 10 * 60, session))
        self._logger.debug("Session returned to queue: %s", session.session_id)

    async def _pick_for_keepalive(self):
        try:
            priority, session = self._active_queue.get_nowait()
        except asyncio.QueueEmpty:
            return None

        till_expire = priority - time.time()
        if till_expire < self._keep_alive_threshold:
            return session
        await self._active_queue.put((priority, session))
        return None

    async def _send_keep_alive(self, session: ydb.ISession):
        if session is None:
            return False
        if self._should_stop.is_set():
            self._destroy(session)
            return False
        await session.keep_alive(self._req_settings)
        try:
            await self.release(session)
        except BaseException:  # pylint: disable=W0703
            self._destroy(session)

    async def _keep_alive_loop(self):
        while True:
            try:
                await asyncio.wait_for(self._should_stop.wait(), timeout=self._keep_alive_threshold // 4)
                break
            except asyncio.TimeoutError:
                while True:
                    session = await self._pick_for_keepalive()
                    if not session:
                        break
                    asyncio.ensure_future(self._send_keep_alive(session))

    async def stop(self, timeout=None):
        self._logger.debug("Requested session pool stop.")
        self._should_stop.set()
        self._terminating = True

        for task in self._min_pool_tasks:
            task.cancel()

        self._logger.debug("Destroying sessions in active queue")

        tasks = []

        while True:
            try:
                _, session = self._active_queue.get_nowait()
                tasks.append(self._destroy(session, wait_for_del=True))

            except asyncio.QueueEmpty:
                break

        await asyncio.gather(*tasks)

        self._logger.debug("Destroyed active sessions")

        await asyncio.wait_for(self._keep_alive_task, timeout=timeout)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()

    async def wait_until_min_size(self):
        await asyncio.gather(*self._min_pool_tasks)

    def checkout(self, timeout: float = None, retry_timeout: float = None):
        return SessionCheckout(self, timeout, retry_timeout=retry_timeout)
