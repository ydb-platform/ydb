import logging
import sqlite3
import time as _time
import threading
from typing import Any
from persistqueue import sqlbase

sqlite3.enable_callback_tracebacks(True)
log = logging.getLogger(__name__)


class PriorityQueue(sqlbase.SQLiteBase):
    """SQLite3 based Priority queue."""
    _TABLE_NAME = 'priority_queue'
    _KEY_COLUMN = '_id'  # the name of the key column, used in DB CRUD
    # SQL to create a table
    _SQL_CREATE = (
        'CREATE TABLE IF NOT EXISTS {table_name} ('
        '{key_column} INTEGER PRIMARY KEY AUTOINCREMENT, '
        'data BLOB, timestamp FLOAT, priority INTEGER)'
    )
    # SQL to insert a record
    _SQL_INSERT = (
        'INSERT INTO {table_name} (data, timestamp, priority) '
        'VALUES (?, ?, ?)'
    )
    # SQL to select a record by id
    _SQL_SELECT_ID = (
        'SELECT {key_column}, data, timestamp, priority FROM {table_name} '
        'WHERE {key_column} = {rowid}'
    )
    # SQL to select the highest priority record
    _SQL_SELECT = (
        'SELECT {key_column}, data, timestamp, priority FROM {table_name} '
        'ORDER BY priority ASC, timestamp ASC LIMIT 1'
    )
    # SQL to select with condition
    _SQL_SELECT_WHERE = (
        'SELECT {key_column}, data, timestamp, priority FROM {table_name} '
        'WHERE {column} {op} ? ORDER BY priority ASC, timestamp ASC LIMIT 1 '
    )
    _SQL_UPDATE = 'UPDATE {table_name} SET data = ? WHERE {key_column} = ?'
    _SQL_DELETE = 'DELETE FROM {table_name} WHERE {key_column} {op} ?'

    def put(self, item: Any, priority: int = 0, block: bool = True) -> int:
        # block kwarg is noop and only here to align with python's queue
        obj = self._serializer.dumps(item)
        _id = self._insert_into(obj, _time.time(), priority)
        self.total += 1
        self.put_event.set()
        return _id

    def put_nowait(self, item: Any, priority: int = 0) -> int:
        return self.put(item, priority=priority, block=False)

    def _init(self) -> None:
        super(PriorityQueue, self)._init()
        self.action_lock = threading.Lock()
        if not self.auto_commit:
            head = self._select()
            if head:
                self.cursor = head[0] - 1
            else:
                self.cursor = 0
        self.total = self._count()
