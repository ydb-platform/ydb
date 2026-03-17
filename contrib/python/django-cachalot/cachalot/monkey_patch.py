import re
import types
from collections.abc import Iterable
from functools import wraps
from time import time

from django.core.exceptions import EmptyResultSet
from django.db.backends.utils import CursorWrapper
from django.db.models.signals import post_migrate
from django.db.models.sql.compiler import (
    SQLCompiler, SQLInsertCompiler, SQLUpdateCompiler, SQLDeleteCompiler,
)
from django.db.transaction import Atomic, get_connection

from .api import invalidate, LOCAL_STORAGE
from .cache import cachalot_caches
from .settings import cachalot_settings, ITERABLES
from .utils import (
    _get_table_cache_keys, _get_tables_from_sql,
    UncachableQuery, is_cachable, filter_cachable,
)


WRITE_COMPILERS = (SQLInsertCompiler, SQLUpdateCompiler, SQLDeleteCompiler)

SQL_DATA_CHANGE_RE = re.compile(
    '|'.join([
        fr'(\W|\A){re.escape(keyword)}(\W|\Z)'
        for keyword in ['update', 'insert', 'delete', 'alter', 'create', 'drop']
    ]),
    flags=re.IGNORECASE,
)

def _unset_raw_connection(original):
    def inner(compiler, *args, **kwargs):
        compiler.connection.raw = False
        try:
            return original(compiler, *args, **kwargs)
        finally:
            compiler.connection.raw = True
    return inner


def _get_result_or_execute_query(execute_query_func, cache,
                                 cache_key, table_cache_keys):
    try:
        data = cache.get_many(table_cache_keys + [cache_key])
    except (KeyError, ModuleNotFoundError):
        data = None

    new_table_cache_keys = set(table_cache_keys)
    if data:
        new_table_cache_keys.difference_update(data)

        if not new_table_cache_keys:
            try:
                timestamp, result = data.pop(cache_key)
                if timestamp >= max(data.values()):
                    return result
            except (KeyError, TypeError, ValueError):
                # In case `cache_key` is not in `data` or contains bad data,
                # we simply run the query and cache again the results.
                pass

    result = execute_query_func()

    if result.__class__ == types.GeneratorType and not cachalot_settings.CACHALOT_CACHE_ITERATORS:
        return result

    if result.__class__ not in ITERABLES and isinstance(result, Iterable):
        result = list(result)

    now = time()
    to_be_set = {k: now for k in new_table_cache_keys}
    to_be_set[cache_key] = (now, result)
    cache.set_many(to_be_set, cachalot_settings.CACHALOT_TIMEOUT)

    return result


def _patch_compiler(original):
    @wraps(original)
    @_unset_raw_connection
    def inner(compiler, *args, **kwargs):
        execute_query_func = lambda: original(compiler, *args, **kwargs)
        # Checks if utils/cachalot_disabled
        if not getattr(LOCAL_STORAGE, "cachalot_enabled", True):
            return execute_query_func()

        db_alias = compiler.using
        if db_alias not in cachalot_settings.CACHALOT_DATABASES \
                or isinstance(compiler, WRITE_COMPILERS):
            return execute_query_func()

        try:
            cache_key = cachalot_settings.CACHALOT_QUERY_KEYGEN(compiler)
            table_cache_keys = _get_table_cache_keys(compiler)
        except (EmptyResultSet, UncachableQuery):
            return execute_query_func()

        return _get_result_or_execute_query(
            execute_query_func,
            cachalot_caches.get_cache(db_alias=db_alias),
            cache_key, table_cache_keys)

    return inner


def _patch_write_compiler(original):
    @wraps(original)
    @_unset_raw_connection
    def inner(write_compiler, *args, **kwargs):
        db_alias = write_compiler.using
        table = write_compiler.query.get_meta().db_table
        if is_cachable(table):
            invalidate(table, db_alias=db_alias,
                       cache_alias=cachalot_settings.CACHALOT_CACHE)
        return original(write_compiler, *args, **kwargs)

    return inner


def _patch_orm():
    if cachalot_settings.CACHALOT_ENABLED:
        SQLCompiler.execute_sql = _patch_compiler(SQLCompiler.execute_sql)
    for compiler in WRITE_COMPILERS:
        compiler.execute_sql = _patch_write_compiler(compiler.execute_sql)


def _unpatch_orm():
    if hasattr(SQLCompiler.execute_sql, '__wrapped__'):
        SQLCompiler.execute_sql = SQLCompiler.execute_sql.__wrapped__
    for compiler in WRITE_COMPILERS:
        compiler.execute_sql = compiler.execute_sql.__wrapped__


def _patch_cursor():
    def _patch_cursor_execute(original):
        @wraps(original)
        def inner(cursor, sql, *args, **kwargs):
            try:
                return original(cursor, sql, *args, **kwargs)
            finally:
                connection = cursor.db
                if getattr(connection, 'raw', True):
                    if isinstance(sql, bytes):
                        sql = sql.decode('utf-8')
                    sql = sql.lower()
                    if SQL_DATA_CHANGE_RE.search(sql):
                        tables = filter_cachable(
                            _get_tables_from_sql(connection, sql))
                        if tables:
                            invalidate(
                                *tables, db_alias=connection.alias,
                                cache_alias=cachalot_settings.CACHALOT_CACHE)

        return inner

    if cachalot_settings.CACHALOT_INVALIDATE_RAW:
        CursorWrapper.execute = _patch_cursor_execute(CursorWrapper.execute)
        CursorWrapper.executemany = _patch_cursor_execute(CursorWrapper.executemany)


def _unpatch_cursor():
    if hasattr(CursorWrapper.execute, '__wrapped__'):
        CursorWrapper.execute = CursorWrapper.execute.__wrapped__
        CursorWrapper.executemany = CursorWrapper.executemany.__wrapped__


def _patch_atomic():
    def patch_enter(original):
        @wraps(original)
        def inner(self):
            cachalot_caches.enter_atomic(self.using)
            original(self)

        return inner

    def patch_exit(original):
        @wraps(original)
        def inner(self, exc_type, exc_value, traceback):
            needs_rollback = get_connection(self.using).needs_rollback
            try:
                original(self, exc_type, exc_value, traceback)
            finally:
                cachalot_caches.exit_atomic(
                    self.using, exc_type is None and not needs_rollback)

        return inner

    Atomic.__enter__ = patch_enter(Atomic.__enter__)
    Atomic.__exit__ = patch_exit(Atomic.__exit__)


def _unpatch_atomic():
    Atomic.__enter__ = Atomic.__enter__.__wrapped__
    Atomic.__exit__ = Atomic.__exit__.__wrapped__


def _invalidate_on_migration(sender, **kwargs):
    invalidate(*sender.get_models(), db_alias=kwargs['using'],
               cache_alias=cachalot_settings.CACHALOT_CACHE)


def patch():
    post_migrate.connect(_invalidate_on_migration)

    _patch_cursor()
    _patch_atomic()
    _patch_orm()


def unpatch():
    post_migrate.disconnect(_invalidate_on_migration)

    _unpatch_cursor()
    _unpatch_atomic()
    _unpatch_orm()
