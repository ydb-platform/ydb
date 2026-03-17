from __future__ import annotations

import posixpath
from collections.abc import Sequence
from enum import Enum
from typing import NamedTuple

import ydb
from ydb import QuerySessionPool as SessionPool
from ydb import QueryTxContext as TxContext
from ydb.aio import QuerySessionPool as AsyncSessionPool
from ydb.aio import QueryTxContext as AsyncTxContext
from ydb.retries import retry_operation_async
from ydb.retries import retry_operation_sync

from .cursors import AsyncCursor
from .cursors import Cursor
from .errors import InterfaceError
from .errors import InternalError
from .errors import NotSupportedError
from .utils import handle_ydb_errors
from .utils import maybe_get_current_trace_id
from .utils import prepare_credentials


class IsolationLevel(str, Enum):
    SERIALIZABLE = "SERIALIZABLE"
    ONLINE_READONLY = "ONLINE READONLY"
    ONLINE_READONLY_INCONSISTENT = "ONLINE READONLY INCONSISTENT"
    STALE_READONLY = "STALE READONLY"
    SNAPSHOT_READONLY = "SNAPSHOT READONLY"
    SNAPSHOT_READWRITE = "SNAPSHOT READWRITE"
    AUTOCOMMIT = "AUTOCOMMIT"


class _IsolationSettings(NamedTuple):
    ydb_mode: ydb.BaseQueryTxMode
    interactive: bool


_ydb_isolation_settings_map = {
    IsolationLevel.AUTOCOMMIT: _IsolationSettings(
        ydb.QuerySerializableReadWrite(), interactive=False
    ),
    IsolationLevel.SERIALIZABLE: _IsolationSettings(
        ydb.QuerySerializableReadWrite(), interactive=True
    ),
    IsolationLevel.ONLINE_READONLY: _IsolationSettings(
        ydb.QueryOnlineReadOnly(), interactive=False
    ),
    IsolationLevel.ONLINE_READONLY_INCONSISTENT: _IsolationSettings(
        ydb.QueryOnlineReadOnly().with_allow_inconsistent_reads(),
        interactive=False,
    ),
    IsolationLevel.STALE_READONLY: _IsolationSettings(
        ydb.QueryStaleReadOnly(), interactive=False
    ),
    IsolationLevel.SNAPSHOT_READONLY: _IsolationSettings(
        ydb.QuerySnapshotReadOnly(), interactive=True
    ),
    IsolationLevel.SNAPSHOT_READWRITE: _IsolationSettings(
        ydb.QuerySnapshotReadWrite(), interactive=True
    ),
}


class BaseConnection:
    _driver_cls = ydb.Driver
    _pool_cls = ydb.QuerySessionPool

    def __init__(
        self,
        host: str = "",
        port: str = "",
        database: str = "",
        ydb_table_path_prefix: str = "",
        protocol: str | None = None,
        credentials: ydb.Credentials | dict | str | None = None,
        ydb_session_pool: SessionPool | AsyncSessionPool | None = None,
        root_certificates_path: str | None = None,
        root_certificates: str | None = None,
        driver_config_kwargs: dict | None = None,
        **kwargs: dict,
    ) -> None:
        protocol = protocol if protocol else "grpc"
        self.endpoint = f"{protocol}://{host}:{port}"
        self.credentials = prepare_credentials(credentials)
        self.database = database
        self.table_path_prefix = ydb_table_path_prefix

        self.connection_kwargs: dict = kwargs

        driver_config_kwargs = driver_config_kwargs or {}

        self._shared_session_pool: bool = False

        self._tx_context: TxContext | AsyncTxContext | None = None
        self._tx_mode: ydb.BaseQueryTxMode = ydb.QuerySerializableReadWrite()
        self.interactive_transaction: bool = False

        if ydb_session_pool is not None:
            self._shared_session_pool = True
            self._session_pool = ydb_session_pool
            settings = self._get_client_settings()
            self._session_pool._query_client_settings = settings
            self._driver = self._session_pool._driver
        else:
            if root_certificates is None:
                root_certificates = ydb.load_ydb_root_certificate(
                    root_certificates_path
                )

            driver_config = ydb.DriverConfig(
                endpoint=self.endpoint,
                database=self.database,
                credentials=self.credentials,
                query_client_settings=self._get_client_settings(),
                root_certificates=root_certificates,
                **driver_config_kwargs,
            )
            self._driver = self._driver_cls(driver_config)
            self._session_pool = self._pool_cls(self._driver, size=5)

        self._session: ydb.QuerySession | ydb.aio.QuerySession | None = None
        self.request_settings: ydb.BaseRequestSettings = (
            ydb.BaseRequestSettings()
        )
        self.retry_settings: ydb.RetrySettings = ydb.RetrySettings()

    def set_isolation_level(self, isolation_level: IsolationLevel) -> None:
        if self._tx_context and self._tx_context.tx_id:
            raise InternalError(
                "Failed to set transaction mode: transaction is already began"
            )

        ydb_isolation_settings = _ydb_isolation_settings_map[isolation_level]

        self._tx_mode = ydb_isolation_settings.ydb_mode
        self.interactive_transaction = ydb_isolation_settings.interactive

    def get_isolation_level(self) -> str:
        if self._tx_mode.name == ydb.QuerySerializableReadWrite().name:
            if self.interactive_transaction:
                return IsolationLevel.SERIALIZABLE
            return IsolationLevel.AUTOCOMMIT
        if self._tx_mode.name == ydb.QueryOnlineReadOnly().name:
            if self._tx_mode.allow_inconsistent_reads:
                return IsolationLevel.ONLINE_READONLY_INCONSISTENT
            return IsolationLevel.ONLINE_READONLY
        if self._tx_mode.name == ydb.QueryStaleReadOnly().name:
            return IsolationLevel.STALE_READONLY
        if self._tx_mode.name == ydb.QuerySnapshotReadOnly().name:
            return IsolationLevel.SNAPSHOT_READONLY
        msg = f"{self._tx_mode.name} is not supported"
        raise NotSupportedError(msg)

    def set_ydb_request_settings(self, value: ydb.BaseRequestSettings) -> None:
        self.request_settings = value

    def get_ydb_request_settings(self) -> ydb.BaseRequestSettings:
        return self.request_settings

    def set_ydb_retry_settings(self, value: ydb.RetrySettings) -> None:
        self.retry_settings = value

    def get_ydb_retry_settings(self) -> ydb.RetrySettings:
        return self.retry_settings

    def _get_request_settings(self) -> ydb.BaseRequestSettings:
        settings = self.request_settings.make_copy()

        if self.request_settings.trace_id is None:
            settings = settings.with_trace_id(maybe_get_current_trace_id())

        return settings

    def _get_client_settings(self) -> ydb.QueryClientSettings:
        return (
            ydb.QueryClientSettings()
            .with_native_date_in_result_sets(True)
            .with_native_datetime_in_result_sets(True)
            .with_native_timestamp_in_result_sets(True)
            .with_native_interval_in_result_sets(True)
            .with_native_json_in_result_sets(False)
        )


class Connection(BaseConnection):
    _driver_cls = ydb.Driver
    _pool_cls = ydb.QuerySessionPool
    _cursor_cls = Cursor

    def __init__(
        self,
        host: str = "",
        port: str = "",
        database: str = "",
        ydb_table_path_prefix: str = "",
        protocol: str | None = None,
        credentials: ydb.Credentials | None = None,
        ydb_session_pool: SessionPool | AsyncSessionPool | None = None,
        root_certificates_path: str | None = None,
        root_certificates: str | None = None,
        driver_config_kwargs: dict | None = None,
        **kwargs: dict,
    ) -> None:
        super().__init__(
            host=host,
            port=port,
            database=database,
            ydb_table_path_prefix=ydb_table_path_prefix,
            protocol=protocol,
            credentials=credentials,
            ydb_session_pool=ydb_session_pool,
            root_certificates_path=root_certificates_path,
            root_certificates=root_certificates,
            driver_config_kwargs=driver_config_kwargs,
            **kwargs,
        )
        self._current_cursor: Cursor | None = None

    def cursor(self) -> Cursor:
        return self._cursor_cls(
            connection=self,
            session_pool=self._session_pool,
            tx_mode=self._tx_mode,
            tx_context=self._tx_context,
            table_path_prefix=self.table_path_prefix,
            request_settings=self.request_settings,
            retry_settings=self.retry_settings,
        )

    def wait_ready(self, timeout: int = 10) -> None:
        try:
            self._driver.wait(timeout, fail_fast=True)
        except ydb.Error as e:
            raise InterfaceError(e.message, original_error=e) from e
        except Exception as e:
            self._driver.stop()
            msg = (
                "Failed to connect to YDB, details "
                f"{self._driver.discovery_debug_details()}"
            )
            raise InterfaceError(msg) from e

    @handle_ydb_errors
    def begin(self) -> None:
        self._tx_context = None
        if self.interactive_transaction:
            self._session = self._session_pool.acquire()
            self._tx_context = self._session.transaction(self._tx_mode)

    @handle_ydb_errors
    def commit(self) -> None:
        if self._tx_context:
            settings = self._get_request_settings()
            self._tx_context.commit(settings=settings)
            self._tx_context = None
        if self._session:
            self._session_pool.release(self._session)
            self._session = None

    @handle_ydb_errors
    def rollback(self) -> None:
        if self._tx_context:
            settings = self._get_request_settings()
            self._tx_context.rollback(settings=settings)
            self._tx_context = None
        if self._session:
            self._session_pool.release(self._session)
            self._session = None

    @handle_ydb_errors
    def close(self) -> None:
        self.rollback()

        if self._session:
            self._session_pool.release(self._session)

        if not self._shared_session_pool:
            self._session_pool.stop()
            self._driver.stop()

    @handle_ydb_errors
    def describe(self, table_path: str) -> ydb.TableSchemeEntry:
        settings = self._get_request_settings()

        abs_table_path = posixpath.join(
            self.database, self.table_path_prefix, table_path
        )
        return self._driver.table_client.describe_table(
            abs_table_path,
            settings=settings,
        )

    @handle_ydb_errors
    def check_exists(self, table_path: str) -> bool:
        abs_table_path = posixpath.join(
            self.database, self.table_path_prefix, table_path
        )
        return self._check_path_exists(abs_table_path)

    @handle_ydb_errors
    def get_table_names(self) -> list[str]:
        abs_dir_path = posixpath.join(self.database, self.table_path_prefix)
        names = self._get_entity_names(
            abs_dir_path,
            [ydb.SchemeEntryType.TABLE, ydb.SchemeEntryType.COLUMN_TABLE],
        )
        return [posixpath.relpath(path, abs_dir_path) for path in names]

    @handle_ydb_errors
    def get_view_names(self) -> list[str]:
        abs_dir_path = posixpath.join(self.database, self.table_path_prefix)
        names = self._get_entity_names(
            abs_dir_path, [ydb.SchemeEntryType.VIEW]
        )
        return [posixpath.relpath(path, abs_dir_path) for path in names]

    def _check_path_exists(self, table_path: str) -> bool:
        try:
            settings = self._get_request_settings()

            def callee() -> None:
                self._driver.scheme_client.describe_path(
                    table_path, settings=settings
                )

            retry_operation_sync(callee)
        except ydb.SchemeError:
            return False
        else:
            return True

    def _get_entity_names(
        self, abs_dir_path: str, etypes: list[ydb.SchemeEntryType]
    ) -> list[str]:
        settings = self._get_request_settings()

        def callee() -> ydb.Directory:
            return self._driver.scheme_client.list_directory(
                abs_dir_path,
                settings=settings,
            )

        directory = retry_operation_sync(callee)
        result = []
        for child in directory.children:
            child_abs_path = posixpath.join(abs_dir_path, child.name)
            if child.type in etypes:
                result.append(child_abs_path)
            elif child.is_directory() and not child.name.startswith("."):
                result.extend(self._get_entity_names(child_abs_path, etypes))
        return result

    @handle_ydb_errors
    def bulk_upsert(
        self,
        table_name: str,
        rows: Sequence,
        column_types: ydb.BulkUpsertColumns,
    ) -> None:
        settings = self._get_request_settings()
        abs_table_path = posixpath.join(
            self.database, self.table_path_prefix, table_name
        )

        self._driver.table_client.bulk_upsert(
            abs_table_path,
            rows=rows,
            column_types=column_types,
            settings=settings,
        )

    def _invalidate_session(self) -> None:
        if self._tx_context:
            self._tx_context = None
        if self._session:
            self._session_pool.release(self._session)
            self._session = None


class AsyncConnection(BaseConnection):
    _driver_cls = ydb.aio.Driver
    _pool_cls = ydb.aio.QuerySessionPool
    _cursor_cls = AsyncCursor

    def __init__(
        self,
        host: str = "",
        port: str = "",
        database: str = "",
        ydb_table_path_prefix: str = "",
        protocol: str | None = None,
        credentials: ydb.Credentials | None = None,
        ydb_session_pool: SessionPool | AsyncSessionPool | None = None,
        root_certificates_path: str | None = None,
        root_certificates: str | None = None,
        driver_config_kwargs: dict | None = None,
        **kwargs: dict,
    ) -> None:
        super().__init__(
            host=host,
            port=port,
            database=database,
            ydb_table_path_prefix=ydb_table_path_prefix,
            protocol=protocol,
            credentials=credentials,
            ydb_session_pool=ydb_session_pool,
            root_certificates_path=root_certificates_path,
            root_certificates=root_certificates,
            driver_config_kwargs=driver_config_kwargs,
            **kwargs,
        )
        self._current_cursor: AsyncCursor | None = None

    def cursor(self) -> AsyncCursor:
        return self._cursor_cls(
            connection=self,
            session_pool=self._session_pool,
            tx_mode=self._tx_mode,
            tx_context=self._tx_context,
            table_path_prefix=self.table_path_prefix,
            request_settings=self.request_settings,
            retry_settings=self.retry_settings,
        )

    async def wait_ready(self, timeout: int = 10) -> None:
        try:
            await self._driver.wait(timeout, fail_fast=True)
        except ydb.Error as e:
            raise InterfaceError(e.message, original_error=e) from e
        except Exception as e:
            await self._driver.stop()
            msg = (
                "Failed to connect to YDB, details "
                f"{self._driver.discovery_debug_details()}"
            )
            raise InterfaceError(msg) from e

    @handle_ydb_errors
    async def begin(self) -> None:
        self._tx_context = None
        if self.interactive_transaction:
            self._session = await self._session_pool.acquire()
            self._tx_context = self._session.transaction(self._tx_mode)

    @handle_ydb_errors
    async def commit(self) -> None:
        if self._tx_context:
            settings = self._get_request_settings()
            await self._tx_context.commit(settings=settings)
            self._tx_context = None
        if self._session:
            await self._session_pool.release(self._session)
            self._session = None

    @handle_ydb_errors
    async def rollback(self) -> None:
        if self._tx_context:
            settings = self._get_request_settings()
            await self._tx_context.rollback(settings=settings)
            self._tx_context = None
        if self._session:
            await self._session_pool.release(self._session)
            self._session = None

    @handle_ydb_errors
    async def close(self) -> None:
        await self.rollback()

        if self._session:
            await self._session_pool.release(self._session)

        if not self._shared_session_pool:
            await self._session_pool.stop()
            await self._driver.stop()

    @handle_ydb_errors
    async def describe(self, table_path: str) -> ydb.TableSchemeEntry:
        settings = self._get_request_settings()

        abs_table_path = posixpath.join(
            self.database, self.table_path_prefix, table_path
        )
        return await self._driver.table_client.describe_table(
            abs_table_path,
            settings=settings,
        )

    @handle_ydb_errors
    async def check_exists(self, table_path: str) -> bool:
        abs_table_path = posixpath.join(
            self.database, self.table_path_prefix, table_path
        )
        return await self._check_path_exists(abs_table_path)

    @handle_ydb_errors
    async def get_table_names(self) -> list[str]:
        abs_dir_path = posixpath.join(self.database, self.table_path_prefix)
        names = await self._get_entity_names(
            abs_dir_path,
            [ydb.SchemeEntryType.TABLE, ydb.SchemeEntryType.COLUMN_TABLE],
        )
        return [posixpath.relpath(path, abs_dir_path) for path in names]

    @handle_ydb_errors
    async def get_view_names(self) -> list[str]:
        abs_dir_path = posixpath.join(self.database, self.table_path_prefix)
        names = await self._get_entity_names(
            abs_dir_path,
            [ydb.SchemeEntryType.VIEW],
        )
        return [posixpath.relpath(path, abs_dir_path) for path in names]

    async def _check_path_exists(self, table_path: str) -> bool:
        try:
            settings = self._get_request_settings()

            async def callee() -> None:
                await self._driver.scheme_client.describe_path(
                    table_path,
                    settings=settings,
                )

            await retry_operation_async(callee)
        except ydb.SchemeError:
            return False
        else:
            return True

    async def _get_entity_names(
        self, abs_dir_path: str, etypes: list[ydb.SchemeEntryType]
    ) -> list[str]:
        settings = self._get_request_settings()

        async def callee() -> ydb.Directory:
            return await self._driver.scheme_client.list_directory(
                abs_dir_path,
                settings=settings,
            )

        directory = await retry_operation_async(callee)
        result = []
        for child in directory.children:
            child_abs_path = posixpath.join(abs_dir_path, child.name)
            if child.type in etypes:
                result.append(child_abs_path)
            elif child.is_directory() and not child.name.startswith("."):
                result.extend(
                    await self._get_entity_names(child_abs_path, etypes)
                )
        return result

    @handle_ydb_errors
    async def bulk_upsert(
        self,
        table_name: str,
        rows: Sequence,
        column_types: ydb.BulkUpsertColumns,
    ) -> None:
        settings = self._get_request_settings()
        abs_table_path = posixpath.join(
            self.database, self.table_path_prefix, table_name
        )

        await self._driver.table_client.bulk_upsert(
            abs_table_path,
            rows=rows,
            column_types=column_types,
            settings=settings,
        )

    async def _invalidate_session(self) -> None:
        if self._tx_context:
            self._tx_context = None
        if self._session:
            await self._session_pool.release(self._session)
            self._session = None


def connect(*args: tuple, **kwargs: dict) -> Connection:
    conn = Connection(*args, **kwargs)  # type: ignore
    conn.wait_ready()
    return conn


async def async_connect(*args: tuple, **kwargs: dict) -> AsyncConnection:
    conn = AsyncConnection(*args, **kwargs)  # type: ignore
    await conn.wait_ready()
    return conn
