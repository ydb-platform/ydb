from dbutils.pooled_db import PooledDB
import threading
import time as _time
import persistqueue
from .sqlbase import SQLBase
from typing import Any, Optional


class MySQLQueue(SQLBase):
    """Mysql(or future standard dbms) based FIFO queue."""
    _TABLE_NAME = 'queue'
    _KEY_COLUMN = '_id'  # the name of the key column, used in DB CRUD
    # SQL to create a table
    _SQL_CREATE = (
        'CREATE TABLE IF NOT EXISTS {table_name} ('
        '{key_column} INTEGER PRIMARY KEY AUTO_INCREMENT, '
        'data BLOB, timestamp FLOAT)')
    # SQL to insert a record
    _SQL_INSERT = 'INSERT INTO {table_name} (data, timestamp) VALUES (%s, %s)'
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
        ' {column} {op} %s ORDER BY {key_column} ASC LIMIT 1 '
    )
    _SQL_UPDATE = 'UPDATE {table_name} SET data = %s WHERE {key_column} = %s'
    _SQL_DELETE = 'DELETE FROM {table_name} WHERE {key_column} {op} %s'

    def __init__(
        self,
        host: str,
        user: str,
        passwd: str,
        db_name: str,
        name: Optional[str] = None,
        port: int = 3306,
        charset: str = 'utf8mb4',
        auto_commit: bool = True,
        serializer: Any = persistqueue.serializers.pickle,
    ) -> None:
        super(MySQLQueue, self).__init__()
        self.name = name if name else "sql"
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db_name = db_name
        self.port = port
        self.charset = charset
        self._serializer = serializer
        self.auto_commit = auto_commit
        self.tran_lock = threading.Lock()
        self.put_event = threading.Event()
        self.action_lock = threading.Lock()
        self._connection_pool = None
        self._getter = None
        self._putter = None
        self._new_db_connection()
        self._init()

    def _new_db_connection(self) -> None:
        try:
            import pymysql
        except ImportError:
            print("Please install mysql library via 'pip install PyMySQL'")
            raise
        db_pool = PooledDB(pymysql, 2, 10, 5, 10, True,
                           host=self.host, port=self.port, user=self.user,
                           passwd=self.passwd, database=self.db_name,
                           charset=self.charset)
        self._connection_pool = db_pool
        conn = db_pool.connection()
        cursor = conn.cursor()
        cursor.execute("SELECT VERSION()")
        _ = cursor.fetchone()
        cursor.execute(self._sql_create)
        conn.commit()
        cursor.execute("use %s" % self.db_name)
        self._putter = MySQLConn(queue=self)
        self._getter = self._putter

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
        self.action_lock = threading.Lock()
        if not self.auto_commit:
            head = self._select()
            if head:
                self.cursor = head[0] - 1
            else:
                self.cursor = 0
        self.total = self._count()

    def get_pooled_conn(self) -> Any:
        return self._connection_pool.connection()


class MySQLConn:
    """MySqlConn defines a common structure for
    both mysql and sqlite3 connections.
    used to mitigate the interface differences between drivers/db
    """

    def __init__(self,
                 queue: Optional[MySQLQueue] = None,
                 conn: Optional[Any] = None) -> None:
        self._queue = queue
        if queue is not None:
            self._conn = queue.get_pooled_conn()
        else:
            self._conn = conn
        self._cursor = None
        self.closed = False

    def __enter__(self) -> Any:
        self._cursor = self._conn.cursor()
        return self._conn

    def __exit__(self,
                 exc_type: Optional[type],
                 exc_val: Optional[BaseException],
                 exc_tb: Optional[Any]) -> None:
        # do not commit() but to close() , keep same behavior
        # with dbutils
        self._cursor.close()

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        if self._queue is not None:
            conn = self._queue.get_pooled_conn()
        else:
            conn = self._conn
        cursor = conn.cursor()
        cursor.execute(*args, **kwargs)
        return cursor

    def close(self) -> None:
        if not self.closed:
            self._conn.close()
        self.closed = True

    def commit(self) -> None:
        if not self.closed:
            self._conn.commit()
