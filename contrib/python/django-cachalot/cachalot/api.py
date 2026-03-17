from contextlib import contextmanager
from typing import Any, Optional, Tuple, Union

from django.apps import apps
from django.conf import settings
from django.db import connections

from .cache import cachalot_caches
from .settings import cachalot_settings
from .signals import post_invalidation
from .transaction import AtomicCache
from .utils import _invalidate_tables


try:
    from asgiref.local import Local
    LOCAL_STORAGE = Local()
except ImportError:
    import threading
    LOCAL_STORAGE = threading.local()


__all__ = ('invalidate', 'get_last_invalidation', 'cachalot_disabled')


def _cache_db_tables_iterator(tables, cache_alias, db_alias):
    no_tables = not tables
    cache_aliases = settings.CACHES if cache_alias is None else (cache_alias,)
    db_aliases = settings.DATABASES if db_alias is None else (db_alias,)
    for db_alias in db_aliases:
        if no_tables:
            tables = connections[db_alias].introspection.table_names()
        if tables:
            for cache_alias in cache_aliases:
                yield cache_alias, db_alias, tables


def _get_tables(tables_or_models):
    for table_or_model in tables_or_models:
        if isinstance(table_or_model, str) and '.' in table_or_model:
            try:
                table_or_model = apps.get_model(table_or_model)
            except LookupError:
                pass
        yield (table_or_model if isinstance(table_or_model, str)
               else table_or_model._meta.db_table)


def invalidate(
    *tables_or_models: Tuple[Union[str, Any], ...],
    cache_alias: Optional[str] = None,
    db_alias: Optional[str] = None,
) -> None:
    """
    Clears what was cached by django-cachalot implying one or more SQL tables
    or models from ``tables_or_models``.
    If ``tables_or_models`` is not specified, all tables found in the database
    (including those outside Django) are invalidated.

    If ``cache_alias`` is specified, it only clears the SQL queries stored
    on this cache, otherwise queries from all caches are cleared.

    If ``db_alias`` is specified, it only clears the SQL queries executed
    on this database, otherwise queries from all databases are cleared.

    :arg tables_or_models: SQL tables names, models or models lookups
                           (or a combination)
    :type tables_or_models: tuple of strings or models
    :arg cache_alias: Alias from the Django ``CACHES`` setting
    :arg db_alias: Alias from the Django ``DATABASES`` setting
    :returns: Nothing
    """
    send_signal = False
    invalidated = set()
    for cache_alias, db_alias, tables in _cache_db_tables_iterator(
            list(_get_tables(tables_or_models)), cache_alias, db_alias):
        cache = cachalot_caches.get_cache(cache_alias, db_alias)
        if not isinstance(cache, AtomicCache):
            send_signal = True
        _invalidate_tables(cache, db_alias, tables)
        invalidated.update(tables)

    if send_signal:
        for table in invalidated:
            post_invalidation.send(table, db_alias=db_alias)


def get_last_invalidation(
    *tables_or_models: Tuple[Union[str, Any], ...],
    cache_alias: Optional[str] = None,
    db_alias: Optional[str] = None,
) -> float:
    """
    Returns the timestamp of the most recent invalidation of the given
    ``tables_or_models``.  If ``tables_or_models`` is not specified,
    all tables found in the database (including those outside Django) are used.

    If ``cache_alias`` is specified, it only fetches invalidations
    in this cache, otherwise invalidations in all caches are fetched.

    If ``db_alias`` is specified, it only fetches invalidations
    for this database, otherwise invalidations for all databases are fetched.

    :arg tables_or_models: SQL tables names, models or models lookups
                           (or a combination)
    :type tables_or_models: tuple of strings or models
    :arg cache_alias: Alias from the Django ``CACHES`` setting
    :arg db_alias: Alias from the Django ``DATABASES`` setting
    :returns: The timestamp of the most recent invalidation
    """
    last_invalidation = 0.0
    for cache_alias, db_alias, tables in _cache_db_tables_iterator(
            list(_get_tables(tables_or_models)), cache_alias, db_alias):
        get_table_cache_key = cachalot_settings.CACHALOT_TABLE_KEYGEN
        table_cache_keys = [get_table_cache_key(db_alias, t) for t in tables]
        invalidations = cachalot_caches.get_cache(
            cache_alias, db_alias).get_many(table_cache_keys).values()
        if invalidations:
            current_last_invalidation = max(invalidations)
            if current_last_invalidation > last_invalidation:
                last_invalidation = current_last_invalidation
    return last_invalidation


@contextmanager
def cachalot_disabled(all_queries: bool = False):
    """
    Context manager for temporarily disabling cachalot.
    If you evaluate the same queryset a second time,
    like normally for Django querysets, this will access
    the variable that saved it in-memory. For example:

    .. code-block:: python

        with cachalot_disabled():
            qs = Test.objects.filter(blah=blah)
            # Does a single query to the db
            list(qs)  # Evaluates queryset
            # Because the qs was evaluated, it's
            # saved in memory:
            list(qs)  # this does 0 queries.
            # This does 1 query to the db
            list(Test.objects.filter(blah=blah))

    If you evaluate the queryset outside the context manager, any duplicate
    query will use the cached result unless an object creation happens in between
    the original and duplicate query.

    :arg all_queries: Any query, including already evaluated queries, are re-evaluated.
    """
    was_enabled = getattr(LOCAL_STORAGE, "cachalot_enabled", cachalot_settings.CACHALOT_ENABLED)
    LOCAL_STORAGE.cachalot_enabled = False
    LOCAL_STORAGE.disable_on_all = all_queries
    yield
    LOCAL_STORAGE.cachalot_enabled = was_enabled
