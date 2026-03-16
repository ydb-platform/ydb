import logging
import sqlite3
import time as _time
import threading
import warnings
from typing import Any, Dict, Optional, Tuple

from . import sqlbase
from .exceptions import Empty

sqlite3.enable_callback_tracebacks(True)
log = logging.getLogger(__name__)

# 10 seconds interval for `wait` of event
TICK_FOR_WAIT = 10


class AckStatus:
    inited = '0'
    ready = '1'
    unack = '2'
    acked = '5'
    ack_failed = '9'


class SQLiteAckQueue(sqlbase.SQLiteBase):
    """SQLite3 based FIFO queue with ack support."""
    _TABLE_NAME = 'ack_queue'
    _KEY_COLUMN = '_id'  # the name of the key column, used in DB CRUD
    _MAX_ACKED_LENGTH = 1000  # deprecated
    # SQL to create a table
    _SQL_CREATE = (
        'CREATE TABLE IF NOT EXISTS {table_name} ('
        '{key_column} INTEGER PRIMARY KEY AUTOINCREMENT, '
        'data BLOB, timestamp FLOAT, status INTEGER)'
    )
    # SQL to insert a record
    _SQL_INSERT = (
        'INSERT INTO {table_name} (data, timestamp, status)'
        ' VALUES (?, ?, %s)' % AckStatus.inited
    )
    # SQL to select a record
    _SQL_SELECT_ID = (
        'SELECT {key_column}, data, timestamp, status FROM {table_name} WHERE'
        ' {key_column} = {rowid}'
    )
    _SQL_SELECT = (
        'SELECT {key_column}, data, timestamp, status FROM {table_name} '
        'WHERE {key_column} > {rowid} AND status < %s '
        'ORDER BY {key_column} ASC LIMIT 1' % AckStatus.unack
    )
    _SQL_MARK_ACK_UPDATE = (
        'UPDATE {table_name} SET status = ? WHERE {key_column} = ?'
    )
    _SQL_SELECT_WHERE = (
        'SELECT {key_column}, data, timestamp FROM {table_name}'
        ' WHERE {key_column} > {rowid} AND status < %s AND'
        ' {column} {op} ? ORDER BY {key_column} ASC'
        ' LIMIT 1 ' % AckStatus.unack
    )
    _SQL_UPDATE = 'UPDATE {table_name} SET data = ? WHERE {key_column} = ?'

    def __init__(self, path: str, auto_resume: bool = True, **kwargs):
        super(SQLiteAckQueue, self).__init__(path, **kwargs)
        if not self.auto_commit:
            warnings.warn("disable auto commit is not supported in ack queue")
            self.auto_commit = True
        self._unack_cache = {}
        if auto_resume:
            self.resume_unack_tasks()

    def resume_unack_tasks(self) -> None:
        unack_count = self.unack_count()
        if unack_count:
            log.info("resume %d unack tasks", unack_count)
        sql = 'UPDATE {} set status = ? WHERE status = ?'.format(
            self._table_name)
        with self.tran_lock:
            with self._putter as tran:
                tran.execute(sql, (AckStatus.ready, AckStatus.unack,))
            self.total = self._count()

    def put(self, item: Any) -> Optional[int]:
        obj = self._serializer.dumps(item)
        _id = self._insert_into(obj, _time.time())
        self.total += 1
        self.put_event.set()
        return _id

    def _init(self) -> None:
        super(SQLiteAckQueue, self)._init()
        self.action_lock = threading.Lock()
        self.total = self._count()

    def _count(self) -> int:
        sql = 'SELECT COUNT({}) FROM {} WHERE status < ?'.format(
            self._key_column, self._table_name
        )
        row = self._getter.execute(sql, (AckStatus.unack,)).fetchone()
        return row[0] if row else 0

    def _ack_count_via_status(self, status: str) -> int:
        sql = 'SELECT COUNT({}) FROM {} WHERE status = ?'.format(
            self._key_column, self._table_name
        )
        row = self._getter.execute(sql, (status,)).fetchone()
        return row[0] if row else 0

    def unack_count(self) -> int:
        return self._ack_count_via_status(AckStatus.unack)

    def acked_count(self) -> int:
        return self._ack_count_via_status(AckStatus.acked)

    def ready_count(self) -> int:
        return self._ack_count_via_status(AckStatus.ready)

    def ack_failed_count(self) -> int:
        return self._ack_count_via_status(AckStatus.ack_failed)

    @sqlbase.with_conditional_transaction
    def _mark_ack_status(self, key: int, status: str) -> None:
        return self._sql_mark_ack_status, (status, key,)

    @sqlbase.with_conditional_transaction
    def clear_acked_data(
        self, max_delete: int = 1000, keep_latest: int = 1000,
        clear_ack_failed: bool = False
    ) -> None:
        acked_clear_all = ''
        acked_to_delete = ''
        acked_to_keep = ''
        if self._MAX_ACKED_LENGTH != 1000 and not max_delete:
            # Added for backward compatibility for
            # those that set the _MAX_ACKED_LENGTH
            print(
                "_MAX_ACKED_LENGTH has been deprecated.  "
                "Use clear_acked_data(keep_latest=1000, max_delete=1000)"
            )
            keep_latest = self._MAX_ACKED_LENGTH
        if clear_ack_failed:
            acked_clear_all = 'OR status = %s' % AckStatus.ack_failed
        if max_delete and max_delete > 0:
            acked_to_delete = 'LIMIT %d' % max_delete
        if keep_latest and keep_latest > 0:
            acked_to_keep = 'OFFSET %d' % keep_latest
        sql = """DELETE FROM {table_name}
            WHERE {key_column} IN (
                SELECT _id FROM {table_name}
                WHERE status = ? {clear_ack_failed}
                ORDER BY {key_column}
                DESC {acked_to_delete} {acked_to_keep}
            )""".format(
            table_name=self._table_name,
            key_column=self._key_column,
            acked_to_delete=acked_to_delete,
            acked_to_keep=acked_to_keep,
            clear_ack_failed=acked_clear_all,
        )
        return sql, AckStatus.acked

    @property
    def _sql_mark_ack_status(self) -> str:
        return self._SQL_MARK_ACK_UPDATE.format(
            table_name=self._table_name, key_column=self._key_column
        )

    def _pop(self, rowid: Optional[int] = None, next_in_order: bool = False,
             raw: bool = False) -> Optional[Dict[str, Any]]:
        with self.action_lock:
            row = self._select(next_in_order=next_in_order, rowid=rowid)
            if row and row[0] is not None:
                self._mark_ack_status(row[0], AckStatus.unack)
                serialized_data = row[1]
                item = self._serializer.loads(serialized_data)
                self._unack_cache[row[0]] = item
                self.total -= 1
                if raw:
                    return {'pqid': row[0], 'data': item, 'timestamp': row[2]}
                else:
                    return item
            return None

    def _find_item_id(self, item: Any, search: bool = True) -> Optional[int]:
        if item is None:
            return None
        elif isinstance(item, dict) and "pqid" in item:
            return item.get("pqid")
        elif search:
            for key, value in self._unack_cache.items():
                if value is item:
                    return key
        elif isinstance(item, int) or (
            isinstance(item, str) and item.isnumeric()
        ):
            return int(item)
        log.warning("Item id not Interger and can't find item in unack cache.")
        return None

    def _check_id(self, item: Any, id: Optional[int]) -> Tuple[Any, bool]:
        if id is not None and item is not None:
            raise ValueError("Specify an id or an item, not both.")
        elif id is None and item is None:
            raise ValueError("Specify an id or an item.")
        elif id is not None:
            search = False
            item = id
        else:
            search = True
        return item, search

    def ack(self, item: Any = None,
            id: Optional[int] = None) -> Optional[int]:

        item, search = self._check_id(item, id)
        with self.action_lock:
            _id = self._find_item_id(item, search)
            if _id is None:
                return None
            self._mark_ack_status(_id, AckStatus.acked)
            if _id in self._unack_cache:
                self._unack_cache.pop(_id)
        return _id

    def ack_failed(self, item: Any = None,
                   id: Optional[int] = None) -> Optional[int]:
        item, search = self._check_id(item, id)
        with self.action_lock:
            _id = self._find_item_id(item, search)
            if _id is None:
                return None
            self._mark_ack_status(_id, AckStatus.ack_failed)
            if _id in self._unack_cache:
                self._unack_cache.pop(_id)
        return _id

    def nack(self, item: Any = None,
             id: Optional[int] = None) -> Optional[int]:
        item, search = self._check_id(item, id)
        with self.action_lock:
            _id = self._find_item_id(item, search)
            if _id is None:
                return None
            self._mark_ack_status(_id, AckStatus.ready)
            if _id in self._unack_cache:
                self._unack_cache.pop(_id)
            self.total += 1
        return _id

    def update(self, item: Any, id: Optional[int] = None) -> Optional[int]:
        _id = None
        if isinstance(item, dict) and "pqid" in item:
            _id = item.get("pqid")
            item = item.get("data")
        if id is not None:
            _id = id
        if _id is None:
            raise ValueError("Provide an id or raw item")
        obj = self._serializer.dumps(item)
        self._update(_id, obj)
        return _id

    def get(
            self, block: bool = True, timeout: Optional[float] = None,
            id: Optional[int] = None, next_in_order: bool = False,
            raw: bool = False) -> Any:
        rowid = self._find_item_id(id, search=False)
        if rowid is None and next_in_order:
            raise ValueError(
                "'next_in_order' requires the preceding 'id' be specified."
            )
        if next_in_order and not isinstance(next_in_order, bool):
            raise ValueError("'next_in_order' must be a boolean (True/False)")
        if not block:
            serialized = self._pop(
                next_in_order=next_in_order, raw=raw, rowid=rowid
            )
            if serialized is None:
                raise Empty
        elif timeout is None:
            # block until a put event.
            serialized = self._pop(
                next_in_order=next_in_order, raw=raw, rowid=rowid
            )
            while serialized is None:
                self.put_event.clear()
                self.put_event.wait(TICK_FOR_WAIT)
                serialized = self._pop(
                    next_in_order=next_in_order, raw=raw, rowid=rowid
                )
        elif timeout < 0:
            raise ValueError("'timeout' must be a non-negative number")
        else:
            # block until the timeout reached
            endtime = _time.time() + timeout
            serialized = self._pop(
                next_in_order=next_in_order, raw=raw, rowid=rowid
            )
            while serialized is None:
                self.put_event.clear()
                remaining = endtime - _time.time()
                if remaining <= 0.0:
                    raise Empty
                self.put_event.wait(
                    TICK_FOR_WAIT if TICK_FOR_WAIT < remaining else remaining
                )
                serialized = self._pop(
                    next_in_order=next_in_order, raw=raw, rowid=rowid
                )
        return serialized

    def task_done(self) -> None:
        """Persist the current state if auto_commit=False."""
        if not self.auto_commit:
            self._task_done()

    def queue(self) -> Any:
        rows = self._sql_queue()
        datarows = []
        for row in rows:
            item = {
                'id': row[0],
                'data': self._serializer.loads(row[1]),
                'timestamp': row[2],
                'status': row[3],
            }
            datarows.append(item)
        return datarows

    @property
    def size(self) -> int:
        return self.total

    def qsize(self) -> int:
        return max(0, self.size)

    def active_size(self) -> int:
        return max(0, self.size + len(self._unack_cache))

    def empty(self) -> bool:
        return self.size == 0

    def full(self) -> bool:
        return False

    def __len__(self) -> int:
        return self.size


FIFOSQLiteAckQueue = SQLiteAckQueue


class FILOSQLiteAckQueue(SQLiteAckQueue):
    """SQLite3 based FILO queue with ack support."""
    _TABLE_NAME = 'ack_filo_queue'
    # SQL to select a record
    _SQL_SELECT = (
        'SELECT {key_column}, data, timestamp, status FROM {table_name} '
        'WHERE {key_column} < {rowid} and status < %s '
        'ORDER BY {key_column} DESC LIMIT 1' % AckStatus.unack
    )


class UniqueAckQ(SQLiteAckQueue):
    _TABLE_NAME = 'ack_unique_queue'
    _SQL_CREATE = (
        'CREATE TABLE IF NOT EXISTS {table_name} ('
        '{key_column} INTEGER PRIMARY KEY AUTOINCREMENT, '
        'data BLOB, timestamp FLOAT, status INTEGER, UNIQUE (data))'
    )

    def put(self, item: Any) -> Optional[int]:
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
