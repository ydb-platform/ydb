import logging
import sqlite3
from persistqueue import sqlbase
from typing import Any, Iterator

log = logging.getLogger(__name__)


class PDict(sqlbase.SQLiteBase, dict):
    _TABLE_NAME = 'dict'
    _KEY_COLUMN = 'key'
    _SQL_CREATE = ('CREATE TABLE IF NOT EXISTS {table_name} ('
                   '{key_column} TEXT PRIMARY KEY, data BLOB)')
    _SQL_INSERT = 'INSERT INTO {table_name} (key, data) VALUES (?, ?)'
    _SQL_SELECT = ('SELECT {key_column}, data FROM {table_name} '
                   'WHERE {key_column} = ?')
    _SQL_UPDATE = 'UPDATE {table_name} SET data = ? WHERE {key_column} = ?'
    _SQL_DELETE = 'DELETE FROM {table_name} WHERE {key_column} {op} ?'

    def __init__(self,
                 path: str,
                 name: str,
                 multithreading: bool = False) -> None:
        # PDict is always auto_commit=True
        super().__init__(path, name=name,
                         multithreading=multithreading,
                         auto_commit=True)

    def __iter__(self) -> Iterator:
        raise NotImplementedError('Not supported.')

    def keys(self) -> Iterator:
        raise NotImplementedError('Not supported.')

    def iterkeys(self) -> Iterator:
        raise NotImplementedError('Not supported.')

    def values(self) -> Iterator:
        raise NotImplementedError('Not supported.')

    def itervalues(self) -> Iterator:
        raise NotImplementedError('Not supported.')

    def iteritems(self) -> Iterator:
        raise NotImplementedError('Not supported.')

    def items(self) -> Iterator:
        raise NotImplementedError('Not supported.')

    def __contains__(self, item: Any) -> bool:
        row = self._select(item)
        return row is not None

    def __setitem__(self, key: Any, value: Any) -> None:
        obj = self._serializer.dumps(value)
        try:
            self._insert_into(key, obj)
        except sqlite3.IntegrityError:
            self._update(key, obj)

    def __getitem__(self, item: Any) -> Any:
        row = self._select(item)
        if row:
            return self._serializer.loads(row[1])
        else:
            raise KeyError('Key: {} not exists.'.format(item))

    def get(self, key: Any, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def __delitem__(self, key: Any) -> None:
        self._delete(key)

    def __len__(self) -> int:
        return self._count()
