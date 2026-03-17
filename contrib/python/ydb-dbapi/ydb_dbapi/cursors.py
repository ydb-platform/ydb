from __future__ import annotations

import functools
import itertools
from collections.abc import AsyncIterator
from collections.abc import Generator
from collections.abc import Iterator
from collections.abc import Sequence
from inspect import iscoroutinefunction
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Union

import ydb
from typing_extensions import Self

from .errors import DatabaseError
from .errors import InterfaceError
from .errors import ProgrammingError
from .utils import CursorStatus
from .utils import handle_ydb_errors
from .utils import maybe_get_current_trace_id

if TYPE_CHECKING:
    from .connections import AsyncConnection
    from .connections import Connection

    ParametersType = dict[
        str,
        Union[
            Any,
            tuple[Any, Union[ydb.PrimitiveType, ydb.AbstractTypeBuilder]],
            ydb.TypedValue,
        ],
    ]


def _get_column_type(type_obj: Any) -> str:
    return str(ydb.convert.type_to_native(type_obj))


def invalidate_cursor_on_ydb_error(func: Callable) -> Callable:
    if iscoroutinefunction(func):

        @functools.wraps(func)
        async def awrapper(
            self: AsyncCursor, *args: tuple, **kwargs: dict
        ) -> Any:
            try:
                return await func(self, *args, **kwargs)
            except ydb.Error:
                self._state = CursorStatus.finished
                await self._connection._invalidate_session()
                raise

        return awrapper

    @functools.wraps(func)
    def wrapper(self: Cursor, *args: tuple, **kwargs: dict) -> Any:
        try:
            return func(self, *args, **kwargs)
        except ydb.Error:
            self._state = CursorStatus.finished
            self._connection._invalidate_session()
            raise

    return wrapper


class BufferedCursor:
    def __init__(self) -> None:
        self.arraysize: int = 1
        self._rows: Iterator | None = None
        self._rows_count: int = -1
        self._description: list[tuple] | None = None
        self._state: CursorStatus = CursorStatus.ready

        self._table_path_prefix: str = ""

    @property
    def description(self) -> list[tuple] | None:
        return self._description

    @property
    def rowcount(self) -> int:
        return self._rows_count

    def setinputsizes(self) -> None:
        pass

    def setoutputsize(self) -> None:
        pass

    def _rows_iterable(
        self, result_set: ydb.convert.ResultSet
    ) -> Generator[tuple]:
        try:
            for row in result_set.rows:
                # returns tuple to be compatible with SqlAlchemy and because
                #  of this PEP to return a sequence:
                # https://www.python.org/dev/peps/pep-0249/#fetchmany
                yield row[::]
        except ydb.Error as e:
            raise DatabaseError(e.message, original_error=e) from e

    def _update_result_set(
        self,
        result_set: ydb.convert.ResultSet,
        replace_current: bool = True,
    ) -> None:
        self._update_description(result_set)

        new_rows_iter = self._rows_iterable(result_set)
        new_rows_count = len(result_set.rows) or -1

        if self._rows is None or replace_current:
            self._rows = new_rows_iter
            self._rows_count = new_rows_count
        else:
            self._rows = itertools.chain(self._rows, new_rows_iter)
            if new_rows_count != -1:
                if self._rows_count != -1:
                    self._rows_count += new_rows_count
                else:
                    self._rows_count = new_rows_count

    def _update_description(self, result_set: ydb.convert.ResultSet) -> None:
        if not result_set.columns:
            # We should not rely on 'empty' result sets,
            # because they can appear at any moment
            return

        self._description = [
            (
                col.name,
                _get_column_type(col.type),
                None,
                None,
                None,
                None,
                None,
            )
            for col in result_set.columns
        ]

    def _fill_buffer(self, result_set_list: list) -> None:
        for result_set in result_set_list:
            self._update_result_set(result_set, replace_current=False)

    def _raise_if_running(self) -> None:
        if self._state == CursorStatus.running:
            raise ProgrammingError(
                "Some records have not been fetched. "
                "Fetch the remaining records before executing the next query."
            )

    def _raise_if_closed(self) -> None:
        if self.is_closed:
            raise InterfaceError(
                "Could not perform operation: Cursor is closed."
            )

    @property
    def is_closed(self) -> bool:
        return self._state == CursorStatus.closed

    def _begin_query(self) -> None:
        self._state = CursorStatus.running

    def _finish_query(self) -> None:
        self._state = CursorStatus.finished

    def _fetchone_from_buffer(self) -> tuple | None:
        self._raise_if_closed()
        return next(self._rows or iter([]), None)

    def _fetchmany_from_buffer(self, size: int | None = None) -> list:
        self._raise_if_closed()
        return list(
            itertools.islice(self._rows or iter([]), size or self.arraysize)
        )

    def _fetchall_from_buffer(self) -> list:
        self._raise_if_closed()
        return list(self._rows or iter([]))

    def _append_table_path_prefix(self, query: str) -> str:
        if self._table_path_prefix:
            prgm = f'PRAGMA TablePathPrefix = "{self._table_path_prefix}";\n'
            return prgm + query
        return query


class Cursor(BufferedCursor):
    def __init__(
        self,
        connection: Connection,
        session_pool: ydb.QuerySessionPool,
        tx_mode: ydb.BaseQueryTxMode,
        request_settings: ydb.BaseRequestSettings,
        retry_settings: ydb.RetrySettings,
        tx_context: ydb.QueryTxContext | None = None,
        table_path_prefix: str = "",
    ) -> None:
        super().__init__()
        self._connection = connection
        self._session_pool = session_pool
        self._tx_mode = tx_mode
        self._request_settings = request_settings
        self._retry_settings = retry_settings
        self._tx_context = tx_context
        self._table_path_prefix = table_path_prefix
        self._stream: Iterator | None = None

    def fetchone(self) -> tuple | None:
        return self._fetchone_from_buffer()

    def fetchmany(self, size: int | None = None) -> list:
        size = size or self.arraysize
        return self._fetchmany_from_buffer(size)

    def fetchall(self) -> list:
        return self._fetchall_from_buffer()

    def _get_request_settings(self) -> ydb.BaseRequestSettings:
        settings = self._request_settings.make_copy()

        if self._request_settings.trace_id is None:
            settings = settings.with_trace_id(maybe_get_current_trace_id())

        return settings

    def _materialize(
        self, stream: Iterator[ydb.convert.ResultSet]
    ) -> list[ydb.convert.ResultSet]:
        return list(stream)

    @handle_ydb_errors
    @invalidate_cursor_on_ydb_error
    def _execute_generic_query(
        self, query: str, parameters: ParametersType | None = None
    ) -> list[ydb.convert.ResultSet]:
        settings = self._get_request_settings()

        def callee(
            session: ydb.QuerySession,
        ) -> list[ydb.convert.ResultSet]:
            return self._materialize(
                session.execute(
                    query=query,
                    parameters=parameters,
                    settings=settings,
                )
            )

        return self._session_pool.retry_operation_sync(
            callee,
            retry_settings=self._retry_settings,
        )

    @handle_ydb_errors
    @invalidate_cursor_on_ydb_error
    def _execute_session_query(
        self,
        query: str,
        parameters: ParametersType | None = None,
    ) -> list[ydb.convert.ResultSet]:
        settings = self._get_request_settings()

        def callee(
            session: ydb.QuerySession,
        ) -> list[ydb.convert.ResultSet]:
            return self._materialize(
                session.transaction(self._tx_mode).execute(
                    query=query,
                    parameters=parameters,
                    commit_tx=True,
                    settings=settings,
                )
            )

        return self._session_pool.retry_operation_sync(
            callee,
            retry_settings=self._retry_settings,
        )

    @handle_ydb_errors
    @invalidate_cursor_on_ydb_error
    def _execute_transactional_query(
        self,
        tx_context: ydb.QueryTxContext,
        query: str,
        parameters: ParametersType | None = None,
    ) -> list[ydb.convert.ResultSet]:
        settings = self._get_request_settings()
        return self._materialize(
            tx_context.execute(
                query=query,
                parameters=parameters,
                commit_tx=False,
                settings=settings,
            )
        )

    def execute_scheme(
        self,
        query: str,
        parameters: ParametersType | None = None,
    ) -> None:
        self._raise_if_closed()

        query = self._append_table_path_prefix(query)
        self._begin_query()

        result_list = self._execute_generic_query(
            query=query, parameters=parameters
        )
        self._fill_buffer(result_list)
        self._finish_query()

    def execute(
        self,
        query: str,
        parameters: ParametersType | None = None,
    ) -> None:
        self._raise_if_closed()
        self._raise_if_running()

        query = self._append_table_path_prefix(query)
        self._begin_query()

        if self._tx_context is not None:
            result_list = self._execute_transactional_query(
                tx_context=self._tx_context, query=query, parameters=parameters
            )
        else:
            result_list = self._execute_session_query(
                query=query, parameters=parameters
            )

        self._fill_buffer(result_list)
        self._finish_query()

    def executemany(
        self, query: str, seq_of_parameters: Sequence[ParametersType]
    ) -> None:
        for parameters in seq_of_parameters:
            self.execute(query, parameters)

    def nextset(self) -> bool:
        return False

    def close(self) -> None:
        if self._state == CursorStatus.closed:
            return

        self._state = CursorStatus.closed

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object,
    ) -> None:
        self.close()


class AsyncCursor(BufferedCursor):
    def __init__(
        self,
        connection: AsyncConnection,
        session_pool: ydb.aio.QuerySessionPool,
        tx_mode: ydb.BaseQueryTxMode,
        request_settings: ydb.BaseRequestSettings,
        retry_settings: ydb.RetrySettings,
        tx_context: ydb.aio.QueryTxContext | None = None,
        table_path_prefix: str = "",
    ) -> None:
        super().__init__()
        self._connection = connection
        self._session_pool = session_pool
        self._tx_mode = tx_mode
        self._request_settings = request_settings
        self._retry_settings = retry_settings
        self._tx_context = tx_context
        self._table_path_prefix = table_path_prefix
        self._stream: AsyncIterator | None = None

    def fetchone(self) -> tuple | None:
        return self._fetchone_from_buffer()

    def fetchmany(self, size: int | None = None) -> list:
        size = size or self.arraysize
        return self._fetchmany_from_buffer(size)

    def fetchall(self) -> list:
        return self._fetchall_from_buffer()

    def _get_request_settings(self) -> ydb.BaseRequestSettings:
        settings = self._request_settings.make_copy()

        if self._request_settings.trace_id is None:
            settings = settings.with_trace_id(maybe_get_current_trace_id())

        return settings

    async def _materialize(
        self, stream: AsyncIterator[ydb.convert.ResultSet]
    ) -> list[ydb.convert.ResultSet]:
        return [result_set async for result_set in stream]

    @handle_ydb_errors
    @invalidate_cursor_on_ydb_error
    async def _execute_generic_query(
        self, query: str, parameters: ParametersType | None = None
    ) -> list[ydb.convert.ResultSet]:
        settings = self._get_request_settings()

        async def callee(
            session: ydb.aio.QuerySession,
        ) -> list[ydb.convert.ResultSet]:
            return await self._materialize(
                await session.execute(
                    query=query,
                    parameters=parameters,
                    settings=settings,
                )
            )

        return await self._session_pool.retry_operation_async(
            callee,
            retry_settings=self._retry_settings,
        )

    @handle_ydb_errors
    @invalidate_cursor_on_ydb_error
    async def _execute_session_query(
        self,
        query: str,
        parameters: ParametersType | None = None,
    ) -> list[ydb.convert.ResultSet]:
        settings = self._get_request_settings()

        async def callee(
            session: ydb.aio.QuerySession,
        ) -> list[ydb.convert.ResultSet]:
            return await self._materialize(
                await session.transaction(self._tx_mode).execute(
                    query=query,
                    parameters=parameters,
                    commit_tx=True,
                    settings=settings,
                )
            )

        return await self._session_pool.retry_operation_async(
            callee,
            retry_settings=self._retry_settings,
        )

    @handle_ydb_errors
    @invalidate_cursor_on_ydb_error
    async def _execute_transactional_query(
        self,
        tx_context: ydb.aio.QueryTxContext,
        query: str,
        parameters: ParametersType | None = None,
    ) -> list[ydb.convert.ResultSet]:
        settings = self._get_request_settings()
        return await self._materialize(
            await tx_context.execute(
                query=query,
                parameters=parameters,
                commit_tx=False,
                settings=settings,
            )
        )

    async def execute_scheme(
        self,
        query: str,
        parameters: ParametersType | None = None,
    ) -> None:
        self._raise_if_closed()

        query = self._append_table_path_prefix(query)
        self._begin_query()

        result_list = await self._execute_generic_query(
            query=query, parameters=parameters
        )
        self._fill_buffer(result_list)
        self._finish_query()

    async def execute(
        self,
        query: str,
        parameters: ParametersType | None = None,
    ) -> None:
        self._raise_if_closed()
        self._raise_if_running()

        query = self._append_table_path_prefix(query)

        self._begin_query()

        if self._tx_context is not None:
            result_list = await self._execute_transactional_query(
                tx_context=self._tx_context, query=query, parameters=parameters
            )
        else:
            result_list = await self._execute_session_query(
                query=query, parameters=parameters
            )

        self._fill_buffer(result_list)
        self._finish_query()

    async def executemany(
        self, query: str, seq_of_parameters: Sequence[ParametersType]
    ) -> None:
        for parameters in seq_of_parameters:
            await self.execute(query, parameters)

    async def nextset(self) -> bool:
        return False

    def close(self) -> None:
        if self._state == CursorStatus.closed:
            return

        self._state = CursorStatus.closed

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object,
    ) -> None:
        self.close()
