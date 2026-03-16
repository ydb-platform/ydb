"""
psycopg server-side cursor base objects.
"""

# Copyright (C) 2020 The Psycopg Team

from __future__ import annotations

from typing import Any
from warnings import warn

from . import errors as e
from . import pq, sql
from .abc import ConnectionType, Params, PQGen, Query
from .rows import Row
from ._compat import Interpolation, Template
from .generators import execute
from ._cursor_base import BaseCursor

DEFAULT_ITERSIZE = 100

TEXT = pq.Format.TEXT
BINARY = pq.Format.BINARY

COMMAND_OK = pq.ExecStatus.COMMAND_OK
TUPLES_OK = pq.ExecStatus.TUPLES_OK

IDLE = pq.TransactionStatus.IDLE
INTRANS = pq.TransactionStatus.INTRANS


class ServerCursorMixin(BaseCursor[ConnectionType, Row]):
    """Mixin to add ServerCursor behaviour and implementation a BaseCursor."""

    __slots__ = """_name _scrollable _withhold _described itersize _format
        _iter_rows _page_pos
    """.split()

    def __init__(self, name: str, scrollable: bool | None, withhold: bool):
        self._name = name
        self._scrollable = scrollable
        self._withhold = withhold
        self._described = False
        self.itersize: int = DEFAULT_ITERSIZE
        self._format = TEXT

        # Hold the state during iteration: a fetched page and position within it
        self._iter_rows: list[Row] | None = None
        self._page_pos = 0

    def __del__(self, __warn: Any = warn) -> None:
        if self.closed:
            return

        __warn(
            f"{object.__repr__(self)} was deleted while still open."
            " Please use 'with' or '.close()' to close the cursor properly",
            ResourceWarning,
        )

    def __repr__(self) -> str:
        # Insert the name as the second word
        parts = super().__repr__().split(None, 1)
        parts.insert(1, f"{self._name!r}")
        return " ".join(parts)

    @property
    def name(self) -> str:
        """The name of the cursor."""
        return self._name

    @property
    def scrollable(self) -> bool | None:
        """
        Whether the cursor is scrollable or not.

        If `!None` leave the choice to the server. Use `!True` if you want to
        use `scroll()` on the cursor.
        """
        return self._scrollable

    @property
    def withhold(self) -> bool:
        """
        If the cursor can be used after the creating transaction has committed.
        """
        return self._withhold

    @property
    def rownumber(self) -> int | None:
        """Index of the next row to fetch in the current result.

        `!None` if there is no result to fetch.
        """
        res = self.pgresult
        # command_status is empty if the result comes from
        # describe_portal, which means that we have just executed the DECLARE,
        # so we can assume we are at the first row.
        tuples = res and (res.status == TUPLES_OK or res.command_status == b"")
        return self._pos if tuples else None

    def _declare_gen(
        self, query: Query, params: Params | None = None, binary: bool | None = None
    ) -> PQGen[None]:
        """Generator implementing `ServerCursor.execute()`."""

        query = self._make_declare_statement(query)

        # If the cursor is being reused, the previous one must be closed.
        if self._described:
            yield from self._close_gen()
            self._described = False

        self._iter_rows = None
        yield from self._start_query(query)
        pgq = self._convert_query(query, params)
        self._execute_send(pgq, force_extended=True)
        results = yield from execute(self._conn.pgconn)
        if results[-1].status != COMMAND_OK:
            self._raise_for_result(results[-1])

        # Set the format, which will be used by describe and fetch operations
        if binary is None:
            self._format = self.format
        else:
            self._format = BINARY if binary else TEXT

        # The above result only returned COMMAND_OK. Get the cursor shape
        yield from self._describe_gen()

    def _describe_gen(self) -> PQGen[None]:
        self._pgconn.send_describe_portal(self._name.encode(self._encoding))
        results = yield from execute(self._pgconn)
        self._check_results(results)
        self._results = results
        self._select_current_result(0, format=self._format)
        self._described = True

    def _close_gen(self) -> PQGen[None]:
        ts = self._conn.pgconn.transaction_status

        # if the connection is not in a sane state, don't even try
        if ts != IDLE and ts != INTRANS:
            return

        # If we are IDLE, a WITHOUT HOLD cursor will surely have gone already.
        if not self._withhold and ts == IDLE:
            return

        # if we didn't declare the cursor ourselves we still have to close it
        # but we must make sure it exists.
        if not self._described:
            query = sql.SQL(
                "SELECT 1 FROM pg_catalog.pg_cursors WHERE name = {}"
            ).format(sql.Literal(self._name))
            res = yield from self._conn._exec_command(query)
            # pipeline mode otherwise, unsupported here.
            assert res is not None
            if res.ntuples == 0:
                return

        query = sql.SQL("CLOSE {}").format(sql.Identifier(self._name))
        yield from self._conn._exec_command(query)

    def _fetch_gen(self, num: int | None) -> PQGen[list[Row]]:
        if self.closed:
            raise e.InterfaceError("the cursor is closed")
        # If we are stealing the cursor, make sure we know its shape
        if not self._described:
            yield from self._start_query()
            yield from self._describe_gen()

        query = sql.SQL("FETCH FORWARD {} FROM {}").format(
            sql.SQL("ALL") if num is None else sql.Literal(num),
            sql.Identifier(self._name),
        )
        res = yield from self._conn._exec_command(query, result_format=self._format)
        # pipeline mode otherwise, unsupported here.
        assert res is not None

        self.pgresult = res
        self._tx.set_pgresult(res, set_loaders=False)
        return self._tx.load_rows(0, res.ntuples, self._make_row)

    def _scroll_gen(self, value: int, mode: str) -> PQGen[None]:
        if mode not in ("relative", "absolute"):
            raise ValueError(f"bad mode: {mode}. It should be 'relative' or 'absolute'")
        query = sql.SQL("MOVE{} {} FROM {}").format(
            sql.SQL(" ABSOLUTE" if mode == "absolute" else ""),
            sql.Literal(value),
            sql.Identifier(self._name),
        )
        yield from self._conn._exec_command(query)

    def _make_declare_statement(self, query: Query) -> Query:
        parts = [sql.SQL("DECLARE"), sql.Identifier(self._name)]
        if self._scrollable is not None:
            parts.append(sql.SQL("SCROLL" if self._scrollable else "NO SCROLL"))
        parts.append(sql.SQL("CURSOR"))
        if self._withhold:
            parts.append(sql.SQL("WITH HOLD"))
        parts.append(sql.SQL("FOR "))
        declare = sql.SQL(" ").join(parts)

        if isinstance(query, Template):
            # t"{declare:q}{query:q}" but compatible with Python < 3.14
            return Template(
                Interpolation(declare, "declare", None, "q"),
                Interpolation(query, "query", None, "q"),
            )

        if isinstance(query, bytes):
            query = query.decode(self._encoding)
        if not isinstance(query, sql.Composable):
            query = sql.SQL(query)

        return declare + query
