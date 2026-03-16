"""A thread-safe sqlite3 based persistent queue in Python."""
import logging
import sqlite3
import time as _time
import threading
from typing import Any
from persistqueue import sqlbase

sqlite3.enable_callback_tracebacks(True)
log = logging.getLogger(__name__)


class SQLiteQueue(sqlbase.SQLiteBase):
    """SQLite3 based FIFO queue."""
    _TABLE_NAME = 'queue'
    _KEY_COLUMN = '_id'  # the name of the key column, used in DB CRUD
    # SQL to create a table
    _SQL_CREATE = (
        'CREATE TABLE IF NOT EXISTS {table_name} ('
        '{key_column} INTEGER PRIMARY KEY AUTOINCREMENT, '
        'data BLOB, timestamp FLOAT)'
    )
    # SQL to insert a record
    _SQL_INSERT = 'INSERT INTO {table_name} (data, timestamp) VALUES (?, ?)'
    # SQL to select a record
    _SQL_SELECT_ID = (
        'SELECT {key_column}, data, timestamp FROM {table_name} WHERE'
        ' {key_column} = {rowid}'
    )
    _SQL_SELECT = (
        'SELECT {key_column}, data, timestamp FROM {table_name} '
        'ORDER BY {key_column} ASC LIMIT 1'
    )
    _SQL_SELECT_WHERE = (
        'SELECT {key_column}, data, timestamp FROM {table_name} WHERE'
        ' {column} {op} ? ORDER BY {key_column} ASC LIMIT 1 '
    )
    _SQL_UPDATE = 'UPDATE {table_name} SET data = ? WHERE {key_column} = ?'
    _SQL_DELETE = 'DELETE FROM {table_name} WHERE {key_column} {op} ?'

    def put(self, item: Any, block: bool = True) -> int:
        # block kwarg is noop and only here to align with python's queue
        obj = self._serializer.dumps(item)
        _id = self._insert_into(obj, _time.time())
        self.total += 1
        self.put_event.set()
        return _id

    def put_nowait(self, item: Any) -> int:
        return self.put(item, block=False)

    def _init(self) -> None:
        super(SQLiteQueue, self)._init()
        self.action_lock = threading.Lock()
        if not self.auto_commit:
            head = self._select()
            if head:
                self.cursor = head[0] - 1
            else:
                self.cursor = 0
        self.total = self._count()


FIFOSQLiteQueue = SQLiteQueue


class FILOSQLiteQueue(SQLiteQueue):
    """SQLite3 based FILO queue."""
    _TABLE_NAME = 'filo_queue'
    # SQL to select a record
    _SQL_SELECT = (
        'SELECT {key_column}, data FROM {table_name} '
        'ORDER BY {key_column} DESC LIMIT 1'
    )


class UniqueQ(SQLiteQueue):
    _TABLE_NAME = 'unique_queue'
    _SQL_CREATE = (
        'CREATE TABLE IF NOT EXISTS {table_name} ('
        '{key_column} INTEGER PRIMARY KEY AUTOINCREMENT, '
        'data BLOB, timestamp FLOAT, UNIQUE (data))'
    )

    def put(self, item: Any) -> Any:
        obj = self._serializer.dumps(item, sort_keys=True)
        _id = None
        try:
            _id = self._insert_into(obj, _time.time())
        except sqlite3.IntegrityError:
            pass
        else:
            self.total += 1
            self.put_event.set()
        return _id
