"""Asynchronous SQLite persistent queue API.

This module provides asynchronous SQLite-based queue interfaces.
"""
import asyncio
import logging
import sqlite3
import time as _time
from typing import Any, Optional
import aiosqlite

from persistqueue.exceptions import Empty
import persistqueue.serializers.pickle

log = logging.getLogger(__name__)

_EMPTY = object()


class AsyncSQLiteQueue:
    """Asynchronous SQLite3 FIFO queue."""
    _TABLE_NAME = 'queue'
    _KEY_COLUMN = '_id'
    _SQL_CREATE = (
        'CREATE TABLE IF NOT EXISTS {table_name} ('
        '{key_column} INTEGER PRIMARY KEY AUTOINCREMENT, '
        'data BLOB, timestamp FLOAT)'
    )
    _SQL_INSERT = 'INSERT INTO {table_name} (data, timestamp) VALUES (?, ?)'
    _SQL_SELECT = (
        'SELECT {key_column}, data, timestamp FROM {table_name} '
        'ORDER BY {key_column} ASC LIMIT 1'
    )
    _SQL_SELECT_WHERE = (
        'SELECT {key_column}, data, timestamp FROM {table_name} WHERE '
        '{column} {op} ? ORDER BY {key_column} ASC LIMIT 1'
    )
    _SQL_UPDATE = 'UPDATE {table_name} SET data = ? WHERE {key_column} = ?'
    _SQL_DELETE = 'DELETE FROM {table_name} WHERE {key_column} {op} ?'

    def __init__(self, path: str, name: str = 'default',
                 serializer: Any = persistqueue.serializers.pickle,
                 auto_commit: bool = True) -> None:
        """Initialize async SQLite queue.

        Args:
            path: Database file path
            name: Queue name
            serializer: Serializer
            auto_commit: Whether to auto-commit transactions
        """
        self.path = path
        self.name = name
        self._serializer = serializer
        self.auto_commit = auto_commit
        self.total = 0
        self.cursor = 0
        self._lock = asyncio.Lock()
        self._put_event = asyncio.Event()
        self._action_lock = asyncio.Lock()
        self._conn = None

    async def _init(self) -> None:
        """Asynchronously initialize database connection and table."""
        if self._conn is None:
            self._conn = await aiosqlite.connect(self.path)
            await self._conn.execute(
                self._SQL_CREATE.format(
                    table_name=self._table_name,
                    key_column=self._key_column
                )
            )
            await self._conn.commit()

            if not self.auto_commit:
                head = await self._select()
                if head:
                    self.cursor = head[0] - 1
                else:
                    self.cursor = 0
            self.total = await self._count()

    @property
    def _table_name(self) -> str:
        return self._TABLE_NAME

    @property
    def _key_column(self) -> str:
        return self._KEY_COLUMN

    async def put(self, item: Any, block: bool = True) -> int:
        """Put item into queue.

        Args:
            item: Item to put into queue
            block: Whether to block (
                   ignored for compatibility with Python queue API)

        Returns:
            ID of inserted record
        """
        async with self._action_lock:
            await self._init()
            obj = self._serializer.dumps(item)
            _id = await self._insert_into(obj, _time.time())
            self.total += 1
            self._put_event.set()
            return _id

    async def put_nowait(self, item: Any) -> int:
        """Non-blocking put."""
        return await self.put(item, block=False)

    async def _insert_into(self, *record: Any) -> int:
        """Insert record into database."""
        cursor = await self._conn.execute(
            self._SQL_INSERT.format(
                table_name=self._table_name
            ), record
        )
        if self.auto_commit:
            await self._conn.commit()
        return cursor.lastrowid

    async def get(self, block: bool = True, timeout: Optional[float] = None,
                  id: Optional[int] = None, raw: bool = False) -> Any:
        """Get item from queue.

        Args:
            block: Whether to block and wait
            timeout: Timeout duration
            id: Specific record ID
            raw: Whether to return raw data

        Returns:
            Item from queue
        """
        if isinstance(id, dict) and "pqid" in id:
            rowid = id.get("pqid")
        elif isinstance(id, int):
            rowid = id
        else:
            rowid = None

        if not block:
            serialized = await self._pop(raw=raw, rowid=rowid)
            if serialized is _EMPTY:
                raise Empty
        elif timeout is None:
            # Block until put event
            serialized = await self._pop(raw=raw, rowid=rowid)
            while serialized is _EMPTY:
                # If we're looking for a specific ID and it doesn't exist,
                # don't wait
                if rowid is not None:
                    raise Empty
                self._put_event.clear()
                await self._put_event.wait()
                serialized = await self._pop(raw=raw, rowid=rowid)
        elif timeout < 0:
            raise ValueError("'timeout' must be a non-negative number")
        else:
            # Block until timeout
            endtime = _time.time() + timeout
            serialized = await self._pop(raw=raw, rowid=rowid)
            while serialized is _EMPTY:
                # If we're looking for a specific ID and it doesn't exist,
                # don't wait
                if rowid is not None:
                    raise Empty
                self._put_event.clear()
                remaining = endtime - _time.time()
                if remaining <= 0.0:
                    raise Empty
                try:
                    await asyncio.wait_for(self._put_event.wait(), remaining)
                except asyncio.TimeoutError:
                    raise Empty
                serialized = await self._pop(raw=raw, rowid=rowid)
        return serialized

    async def get_nowait(self,
                         id: Optional[int] = None,
                         raw: bool = False) -> Any:
        """Non-blocking get."""
        return await self.get(block=False, id=id, raw=raw)

    async def _pop(self, rowid: Optional[int] = None,
                   raw: bool = False) -> Optional[Any]:
        """Internal pop method."""
        async with self._action_lock:
            await self._init()
            if self.auto_commit:
                row = await self._select(rowid=rowid)
                if row and row[0] is not None:
                    await self._delete(row[0])
                    self.total -= 1
                    item = self._serializer.loads(row[1])
                    if raw:
                        return {
                            'pqid': row[0],
                            'data': item,
                            'timestamp': row[2],
                        }
                    else:
                        return item
                else:
                    return _EMPTY
            else:
                row = await self._select(
                    rowid=rowid
                )
                if row and row[0] is not None:
                    self.cursor = row[0]
                    self.total -= 1
                    # Delete the record in non-auto-commit mode
                    await self._delete(row[0])
                    item = self._serializer.loads(row[1])
                    if raw:
                        return {
                            'pqid': row[0],
                            'data': item,
                            'timestamp': row[2],
                        }
                    else:
                        return item
                else:
                    return _EMPTY

    async def _select(self, *args, **kwargs) -> Optional[tuple]:
        """Select record."""
        await self._init()
        rowid = kwargs.get('rowid')
        op = kwargs.get('op', '=')
        column = kwargs.get('column', self._KEY_COLUMN)

        if rowid is not None:
            sql = self._SQL_SELECT_WHERE.format(
                table_name=self._table_name,
                key_column=self._key_column,
                column=column,
                op=op
            )
            cursor = await self._conn.execute(sql, (rowid,))
        else:
            sql = self._SQL_SELECT.format(
                table_name=self._table_name,
                key_column=self._key_column
            )
            cursor = await self._conn.execute(sql)

        row = await cursor.fetchone()
        await cursor.close()
        return row

    async def _delete(self, key: Any, op: str = '=') -> None:
        """Delete record."""
        sql = self._SQL_DELETE.format(
            table_name=self._table_name,
            key_column=self._key_column,
            op=op
        )
        await self._conn.execute(sql, (key,))
        if self.auto_commit:
            await self._conn.commit()

    async def _count(self) -> int:
        """Count total records."""
        await self._init()
        cursor = await self._conn.execute(
            f'SELECT COUNT(*) FROM {self._table_name}'
        )
        count = await cursor.fetchone()
        await cursor.close()
        return count[0] if count else 0

    async def update(self, item: Any, id: Optional[int] = None) -> int:
        """Update item in queue."""
        if isinstance(item, dict) and "pqid" in item:
            _id = item.get("pqid")
            item = item.get("data")
        if id is not None:
            _id = id
        if _id is None:
            raise ValueError("Provide id or raw item")

        await self._init()
        obj = self._serializer.dumps(item)
        await self._update(_id, obj)
        return _id

    async def _update(self, key: Any, *args: Any) -> None:
        """Update record."""
        sql = self._SQL_UPDATE.format(
            table_name=self._table_name,
            key_column=self._key_column
        )
        args = list(args) + [key]
        await self._conn.execute(sql, args)
        if self.auto_commit:
            await self._conn.commit()

    async def task_done(self) -> None:
        """Mark task as done."""
        if not self.auto_commit:
            await self._conn.commit()

    async def qsize(self) -> int:
        """Return queue size."""
        return await self._count()

    async def empty(self) -> bool:
        """Check if queue is empty."""
        return await self.qsize() == 0

    async def full(self) -> bool:
        """Check if queue is full (SQL queues always return False)."""
        return False

    async def close(self) -> None:
        """Close database connection."""
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def __aenter__(self):
        """Async context manager entry."""
        await self._init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()


class AsyncFIFOSQLiteQueue(AsyncSQLiteQueue):
    """Asynchronous SQLite3 FIFO queue (alias)."""
    pass


class AsyncFILOSQLiteQueue(AsyncSQLiteQueue):
    """Asynchronous SQLite3 FILO queue."""
    _TABLE_NAME = 'filo_queue'
    _SQL_SELECT = (
        'SELECT {key_column}, data FROM {table_name} '
        'ORDER BY {key_column} DESC LIMIT 1'
    )


class AsyncUniqueQ(AsyncSQLiteQueue):
    """Asynchronous unique queue."""
    _TABLE_NAME = 'unique_queue'
    _SQL_CREATE = (
        'CREATE TABLE IF NOT EXISTS {table_name} ('
        '{key_column} INTEGER PRIMARY KEY AUTOINCREMENT, '
        'data BLOB, timestamp FLOAT, UNIQUE (data))'
    )

    async def put(self, item: Any) -> Any:
        """Put item into unique queue."""
        async with self._action_lock:
            await self._init()
            obj = self._serializer.dumps(item, sort_keys=True)
            _id = None
            try:
                _id = await self._insert_into(obj, _time.time())
            except sqlite3.IntegrityError:
                pass
            else:
                self.total += 1
                self._put_event.set()
            return _id
