import logging
import os
import time as _time
import sqlite3
import threading
from typing import Any, Callable, Tuple, Optional

from persistqueue.exceptions import Empty
import persistqueue.serializers.pickle

sqlite3.enable_callback_tracebacks(True)
log = logging.getLogger(__name__)

# 10 seconds interval for `wait` of event
TICK_FOR_WAIT = 10


def with_conditional_transaction(func: Callable) -> Callable:
    def _execute(obj: 'SQLBase', *args: Any, **kwargs: Any) -> Any:
        # for MySQL, connection pool should be used since db connection is
        # basically not thread-safe
        _putter = obj._putter
        if str(type(obj)).find("MySQLQueue") > 0:
            # use fresh connection from pool not the shared one
            _putter = obj.get_pooled_conn()
        with obj.tran_lock:
            with _putter as tran:
                # For sqlite3, commit() is called automatically afterwards
                # but for other db API, this is not TRUE!
                stat, param = func(obj, *args, **kwargs)
                s = str(type(tran))
                if s.find("Cursor") > 0:
                    cur = tran
                    cur.execute(stat, param)
                else:
                    cur = tran.cursor()
                    cur.execute(stat, param)
                    cur.close()
                    tran.commit()
                return cur.lastrowid

    return _execute


def commit_ignore_error(conn: sqlite3.Connection) -> None:
    """Ignore the error of no transaction is active.

    The transaction may be already committed by user's task_done call.
    It's safe to ignore all errors of this kind.
    """
    try:
        conn.commit()
    except sqlite3.OperationalError as ex:
        if 'no transaction is active' in str(ex):
            log.debug(
                'Not able to commit the transaction, '
                'may already be committed.'
            )
        else:
            raise


class SQLBase(object):
    """SQL base class."""

    """SQL base class."""
    _TABLE_NAME = 'base'  # DB table name
    _KEY_COLUMN = ''  # the name of the key column, used in DB CRUD
    _SQL_CREATE = ''  # SQL to create a table
    _SQL_UPDATE = ''  # SQL to update a record
    _SQL_INSERT = ''  # SQL to insert a record
    _SQL_SELECT = ''  # SQL to select a record
    _SQL_SELECT_ID = ''  # SQL to select a record with criteria
    _SQL_SELECT_WHERE = ''  # SQL to select a record with criteria
    _SQL_DELETE = ''  # SQL to delete a record

    def __init__(self) -> None:
        self._serializer = persistqueue.serializers.pickle
        self.auto_commit = True  # Transaction commit behavior
        # SQL transaction lock
        self.tran_lock = threading.Lock()
        # Event signaling new data
        self.put_event = threading.Event()
        # Lock for atomic actions
        self.action_lock = threading.Lock()
        self.total = 0  # Total tasks
        self.cursor = 0  # Cursor for task processing
        # Connection for getting tasks
        self._getter = None
        # Connection for putting tasks
        self._putter = None

    @with_conditional_transaction
    def _insert_into(self, *record: Any) -> Tuple[str, Tuple[Any, ...]]:
        return self._sql_insert, record

    @with_conditional_transaction
    def _update(self, key: Any, *args: Any) -> Tuple[str, Tuple[Any, ...]]:
        args = list(args) + [key]
        return self._sql_update, args

    @with_conditional_transaction
    def _delete(self, key: Any, op: str = '=') -> Tuple[str, Tuple[Any, ...]]:

        sql = self._SQL_DELETE.format(
            table_name=self._table_name, key_column=self._key_column, op=op)
        return sql, (key,)

    def _pop(self, rowid: Optional[int] = None, raw: bool = False
             ) -> Optional[Any]:
        with self.action_lock:
            if self.auto_commit:
                row = self._select(rowid=rowid)
                # Perhaps a sqlite3 bug, sometimes (None, None) is returned
                # by select, below can avoid these invalid records.
                if row and row[0] is not None:
                    self._delete(row[0])
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
                row = self._select(
                    self.cursor, op=">", column=self._KEY_COLUMN, rowid=rowid
                )
                if row and row[0] is not None:
                    self.cursor = row[0]
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
            return None

    def update(self, item: Any, id: Optional[int] = None) -> int:
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

    def get(self, block: bool = True, timeout: Optional[float] = None,
            id: Optional[int] = None, raw: bool = False
            ) -> Any:
        if isinstance(id, dict) and "pqid" in id:
            rowid = id.get("pqid")
        elif isinstance(id, int):
            rowid = id
        else:
            rowid = None
        if not block:
            serialized = self._pop(raw=raw, rowid=rowid)
            if serialized is None:
                raise Empty
        elif timeout is None:
            # block until a put event.
            serialized = self._pop(raw=raw, rowid=rowid)
            while serialized is None:
                self.put_event.clear()
                self.put_event.wait(TICK_FOR_WAIT)
                serialized = self._pop(raw=raw, rowid=rowid)
        elif timeout < 0:
            raise ValueError("'timeout' must be a non-negative number")
        else:
            # block until the timeout reached
            endtime = _time.time() + timeout
            serialized = self._pop(raw=raw, rowid=rowid)
            while serialized is None:
                self.put_event.clear()
                remaining = endtime - _time.time()
                if remaining <= 0.0:
                    raise Empty
                self.put_event.wait(
                    TICK_FOR_WAIT if TICK_FOR_WAIT < remaining else remaining
                )
                serialized = self._pop(raw=raw, rowid=rowid)
        return serialized

    def get_nowait(self, id: Optional[int] = None, raw: bool = False) -> Any:
        return self.get(block=False, id=id, raw=raw)

    def task_done(self) -> None:
        """Persist the current state if auto_commit=False."""
        if not self.auto_commit:
            self._delete(self.cursor, op='<=')
            self._task_done()

    def queue(self) -> Any:
        rows = self._sql_queue().fetchall()
        datarows = []
        for row in rows:
            item = {
                'id': row[0],
                'data': self._serializer.loads(row[1]),
                'timestamp': row[2],
            }
            datarows.append(item)
        return datarows

    @with_conditional_transaction
    def shrink_disk_usage(self) -> Tuple[str, Tuple[()]]:
        sql = """VACUUM"""
        return sql, ()

    @property
    def size(self) -> int:
        return self.total

    def qsize(self) -> int:
        return max(0, self.size)

    def empty(self) -> bool:
        return self.size == 0

    def full(self) -> bool:
        return False

    def __len__(self) -> int:
        return self.size

    def _select(self, *args, **kwargs) -> Any:
        start_key = self._start_key()
        op = kwargs.get('op', None)
        column = kwargs.get('column', None)
        next_in_order = kwargs.get('next_in_order', False)
        rowid = kwargs.get('rowid') if kwargs.get('rowid', None) else start_key
        if not next_in_order and rowid != start_key:
            # Get the record by the id
            result = self._getter.execute(
                self._sql_select_id(rowid), args
            ).fetchone()
        elif op and column:
            # Get the next record with criteria
            rowid = rowid if next_in_order else start_key
            result = self._getter.execute(
                self._sql_select_where(rowid, op, column), args
            ).fetchone()
        else:
            # Get the next record
            rowid = rowid if next_in_order else start_key
            result = self._getter.execute(
                self._sql_select(rowid), args
            ).fetchone()
        if (
                next_in_order
                and rowid != start_key
                and (not result or len(result) == 0)
        ):
            # sqlackqueue: if we're at the end, start over
            kwargs['rowid'] = start_key
            result = self._select(*args, **kwargs)
        return result

    def _count(self) -> int:
        sql = 'SELECT COUNT({}) FROM {}'.format(
            self._key_column, self._table_name
        )
        row = self._getter.execute(sql).fetchone()
        return row[0] if row else 0

    def _start_key(self) -> int:
        if self._TABLE_NAME == 'ack_filo_queue':
            return 9223372036854775807  # maxsize
        else:
            return 0

    def _task_done(self) -> None:
        """Only required if auto-commit is set as False."""
        commit_ignore_error(self._putter)

    def _sql_queue(self) -> Any:
        sql = 'SELECT * FROM {}'.format(self._table_name)
        return self._getter.execute(sql)

    @property
    def _table_name(self) -> str:
        return '`{}_{}`'.format(self._TABLE_NAME, self.name)

    @property
    def _key_column(self) -> str:
        return self._KEY_COLUMN

    @property
    def _sql_create(self) -> str:
        return self._SQL_CREATE.format(
            table_name=self._table_name, key_column=self._key_column
        )

    @property
    def _sql_insert(self) -> str:
        return self._SQL_INSERT.format(
            table_name=self._table_name, key_column=self._key_column
        )

    @property
    def _sql_update(self) -> str:
        return self._SQL_UPDATE.format(
            table_name=self._table_name, key_column=self._key_column
        )

    def _sql_select_id(self, rowid) -> str:
        return self._SQL_SELECT_ID.format(
            table_name=self._table_name,
            key_column=self._key_column,
            rowid=rowid,
        )

    def _sql_select(self, rowid) -> str:
        return self._SQL_SELECT.format(
            table_name=self._table_name,
            key_column=self._key_column,
            rowid=rowid,
        )

    def _sql_select_where(self, rowid, op, column) -> str:
        return self._SQL_SELECT_WHERE.format(
            table_name=self._table_name,
            key_column=self._key_column,
            rowid=rowid,
            op=op,
            column=column,
        )

    def __del__(self) -> None:
        """Handles sqlite connection when queue was deleted"""
        if self._getter:
            self._getter.close()
        if self._putter:
            self._putter.close()


class SQLiteBase(SQLBase):
    """SQLite3 base class."""
    _TABLE_NAME = 'base'  # DB table name
    _KEY_COLUMN = ''  # the name of the key column, used in DB CRUD
    _SQL_CREATE = ''  # SQL to create a table
    _SQL_UPDATE = ''  # SQL to update a record
    _SQL_INSERT = ''  # SQL to insert a record
    _SQL_SELECT = ''  # SQL to select a record
    _SQL_SELECT_ID = ''  # SQL to select a record with criteria
    _SQL_SELECT_WHERE = ''  # SQL to select a record with criteria
    _SQL_DELETE = ''  # SQL to delete a record
    _MEMORY = ':memory:'  # flag indicating store DB in memory

    def __init__(self, path: str, name: str = 'default',
                 multithreading: bool = False, timeout: float = 10.0,
                 auto_commit: bool = True,
                 serializer: Any = persistqueue.serializers.pickle,
                 db_file_name: Optional[str] = None) -> None:
        """Initiate a queue in sqlite3 or memory.

        :param path: path for storing DB file.
        :param name: the suffix for the table name,
                     table name would be ${_TABLE_NAME}_${name}
        :param multithreading: if set to True, two db connections will be,
                               one for **put** and one for **get**.
        :param timeout: timeout in second waiting for the database lock.
        :param auto_commit: Set to True, if commit is required on every
                            INSERT/UPDATE action, otherwise False, whereas
                            a **task_done** is required to persist changes
                            after **put**.
        :param serializer: The serializer parameter controls how enqueued data
                           is serialized. It must have methods dump(value, fp)
                           and load(fp). The dump method must serialize the
                           value and write it to fp, and may be called for
                           multiple values with the same fp. The load method
                           must deserialize and return one value from fp,
                           and may be called multiple times with the same fp
                           to read multiple values.
        :param db_file_name: set the db file name of the queue data, otherwise
                             default to `data.db`
        """
        super(SQLiteBase, self).__init__()
        self.memory_sql = False
        self.path = path
        self.name = name
        self.timeout = timeout
        self.multithreading = multithreading
        self.auto_commit = auto_commit
        self._serializer = serializer
        self.db_file_name = "data.db"
        if db_file_name:
            self.db_file_name = db_file_name
        self._init()

    def _init(self) -> None:
        """Initialize the tables in DB."""
        if self.path == self._MEMORY:
            self.memory_sql = True
            log.debug("Initializing Sqlite3 Queue in memory.")
        elif not os.path.exists(self.path):
            os.makedirs(self.path)
            log.debug(
                'Initializing Sqlite3 Queue with path {}'.format(self.path)
            )
        self._conn = self._new_db_connection(
            self.path, self.multithreading, self.timeout
        )
        self._getter = self._conn
        self._putter = self._conn

        self._conn.execute(self._sql_create)
        self._conn.commit()
        # Setup another session only for disk-based queue.
        if self.multithreading:
            if not self.memory_sql:
                self._putter = self._new_db_connection(
                    self.path, self.multithreading, self.timeout
                )
        self._conn.text_factory = str
        self._putter.text_factory = str

        # SQLite3 transaction lock
        self.tran_lock = threading.Lock()
        self.put_event = threading.Event()

    def _new_db_connection(self, path, multithreading, timeout
                           ) -> sqlite3.Connection:
        conn = None
        if path == self._MEMORY:
            conn = sqlite3.connect(path, check_same_thread=not multithreading)
        else:
            conn = sqlite3.connect(
                '{}/{}'.format(path, self.db_file_name),
                timeout=timeout,
                check_same_thread=not multithreading,
            )
        conn.execute('PRAGMA journal_mode=WAL;')
        return conn

    def close(self) -> None:
        """Closes sqlite connections"""
        if self._getter is not None:
            self._getter.close()
        if self._putter is not None:
            self._putter.close()

    def __del__(self) -> None:
        """Handles sqlite connection when queue was deleted"""
        self.close()
