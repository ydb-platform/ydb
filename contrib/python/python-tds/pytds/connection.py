"""
This module implements DBAPI connection classes for both MARS and non-MARS variants
"""
from __future__ import annotations

import typing
import warnings
import weakref
from . import tds_base
from .tds_socket import _TdsSocket
from . import row_strategies
from .tds_base import logger
from . import connection_pool

if typing.TYPE_CHECKING:
    from .cursor import Cursor, NonMarsCursor, _MarsCursor


class Connection(typing.Protocol):
    """
    This class defines interface for connection object according to DBAPI specification.
    This interface is implemented by MARS and non-MARS connection classes.
    """

    @property
    def autocommit(self) -> bool:
        ...

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        ...

    @property
    def isolation_level(self) -> int:
        ...

    @isolation_level.setter
    def isolation_level(self, level: int) -> None:
        ...

    def __enter__(self) -> BaseConnection:
        ...

    def __exit__(self, *args) -> None:
        ...

    def commit(self) -> None:
        ...

    def rollback(self) -> None:
        ...

    def close(self) -> None:
        ...

    @property
    def mars_enabled(self) -> bool:
        ...

    def cursor(self) -> Cursor:
        ...


class BaseConnection(Connection):
    """
    Base connection class.  It implements most of the common logic for
    MARS and non-MARS connection classes.
    """

    _connection_closed_exception = tds_base.InterfaceError("Connection closed")

    def __init__(
        self,
        pooling: bool,
        key: connection_pool.PoolKeyType,
        tds_socket: _TdsSocket,
    ) -> None:
        # _tds_socket is set to None when connection is closed
        self._tds_socket: _TdsSocket | None = tds_socket
        self._key = key
        self._pooling = pooling
        # references to all cursors opened from connection
        # those references used to close cursors when connection is closed
        self._cursors: weakref.WeakSet[Cursor] = weakref.WeakSet()

    @property
    def as_dict(self) -> bool:
        """
        Instructs all cursors this connection creates to return results
        as a dictionary rather than a tuple.
        """
        if not self._tds_socket:
            raise self._connection_closed_exception
        return (
            self._tds_socket.main_session.row_strategy
            == row_strategies.dict_row_strategy
        )

    @as_dict.setter
    def as_dict(self, value: bool) -> None:
        warnings.warn(
            "setting as_dict property on the active connection, instead create connection with needed row_strategy",
            DeprecationWarning,
        )
        if not self._tds_socket:
            raise self._connection_closed_exception
        if value:
            self._tds_socket.main_session.row_strategy = (
                row_strategies.dict_row_strategy
            )
        else:
            self._tds_socket.main_session.row_strategy = (
                row_strategies.tuple_row_strategy
            )

    @property
    def autocommit_state(self) -> bool:
        """
        An alias for `autocommit`, provided for compatibility with pymssql
        """
        if not self._tds_socket:
            raise self._connection_closed_exception
        return self._tds_socket.main_session.autocommit

    def set_autocommit(self, value: bool) -> None:
        """An alias for `autocommit`, provided for compatibility with ADO dbapi"""
        if not self._tds_socket:
            raise self._connection_closed_exception
        self._tds_socket.main_session.autocommit = value

    @property
    def autocommit(self) -> bool:
        """
        The current state of autocommit on the connection.
        """
        if not self._tds_socket:
            raise self._connection_closed_exception
        return self._tds_socket.main_session.autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        if not self._tds_socket:
            raise self._connection_closed_exception
        self._tds_socket.main_session.autocommit = value

    @property
    def isolation_level(self) -> int:
        """Isolation level for transactions,
        for possible values see :ref:`isolation-level-constants`

        .. seealso:: `SET TRANSACTION ISOLATION LEVEL`__ in MSSQL documentation

            .. __: http://msdn.microsoft.com/en-us/library/ms173763.aspx
        """
        if not self._tds_socket:
            raise self._connection_closed_exception
        return self._tds_socket.main_session.isolation_level

    @isolation_level.setter
    def isolation_level(self, level: int) -> None:
        if not self._tds_socket:
            raise self._connection_closed_exception
        self._tds_socket.main_session.isolation_level = level

    @property
    def tds_version(self) -> int:
        """
        Version of the TDS protocol that is being used by this connection
        """
        if not self._tds_socket:
            raise self._connection_closed_exception
        return self._tds_socket.tds_version

    @property
    def product_version(self):
        """
        Version of the MSSQL server
        """
        if not self._tds_socket:
            raise self._connection_closed_exception
        return self._tds_socket.product_version

    def __enter__(self) -> BaseConnection:
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def commit(self) -> None:
        """
        Commit transaction which is currently in progress.
        """
        if not self._tds_socket:
            raise self._connection_closed_exception
        # Setting cont to True to start new transaction
        # after current transaction is rolled back
        self._tds_socket.main_session.commit(cont=True)

    def rollback(self) -> None:
        """
        Roll back transaction which is currently in progress.
        """
        if self._tds_socket:
            # Setting cont to True to start new transaction
            # after current transaction is rolled back
            self._tds_socket.main_session.rollback(cont=True)

    def close(self) -> None:
        """Close connection to an MS SQL Server.

        This function tries to close the connection and free all memory used.
        It can be called more than once in a row. No exception is raised in
        this case.
        """
        if self._tds_socket:
            logger.debug("Closing connection")
            if self._pooling:
                connection_pool.connection_pool.add(
                    self._key, (self._tds_socket, self._tds_socket.main_session)
                )
            else:
                self._tds_socket.close()
            logger.debug("Closing all cursors which were opened by connection")
            for cursor in self._cursors:
                cursor.close()
            self._tds_socket = None


class MarsConnection(BaseConnection):
    """
    MARS connection class, this object is created by calling :func:`connect`
    with use_mars parameter set to False.
    """

    def __init__(
        self,
        pooling: bool,
        key: connection_pool.PoolKeyType,
        tds_socket: _TdsSocket,
    ):
        super().__init__(pooling=pooling, key=key, tds_socket=tds_socket)

    @property
    def mars_enabled(self) -> bool:
        return True

    def cursor(self) -> _MarsCursor:
        """
        Return cursor object that can be used to make queries and fetch
        results from the database.
        """
        from .cursor import _MarsCursor

        if not self._tds_socket:
            raise self._connection_closed_exception
        cursor = _MarsCursor(
            connection=self,
            session=self._tds_socket.create_session(),
        )
        self._cursors.add(cursor)
        return cursor

    def close(self):
        if self._tds_socket:
            self._tds_socket.close_all_mars_sessions()
        super().close()


class NonMarsConnection(BaseConnection):
    """
    Non-MARS connection class, this object should be created by calling :func:`connect`
    with use_mars parameter set to False.
    """

    def __init__(
        self,
        pooling: bool,
        key: connection_pool.PoolKeyType,
        tds_socket: _TdsSocket,
    ):
        super().__init__(pooling=pooling, key=key, tds_socket=tds_socket)
        self._active_cursor: NonMarsCursor | None = None

    @property
    def mars_enabled(self) -> bool:
        return False

    def cursor(self) -> NonMarsCursor:
        """
        Return cursor object that can be used to make queries and fetch
        results from the database.
        """
        from .cursor import NonMarsCursor

        if not self._tds_socket:
            raise self._connection_closed_exception
        # Only one cursor can be active at any given time
        if self._active_cursor:
            self._active_cursor.cancel()
            self._active_cursor.close()
        cursor = NonMarsCursor(
            connection=self,
            session=self._tds_socket.main_session,
        )
        self._active_cursor = cursor
        self._cursors.add(cursor)
        return cursor
