import logging

from django.contrib.staticfiles.storage import staticfiles_storage
from django.core.cache import caches
from django.db import connections
from django.db.migrations.executor import MigrationExecutor

from . import HealthcheckFailure

# don't bother with typing on Python <3.5
try:
    from typing import Optional
except ImportError:
    pass


log = logging.getLogger(__name__)


def check_database(query="SELECT 1", database="default"):
    # type: (str, str) -> None
    """
    Run a SQL query against the specified database.

    :param str query: SQL to execute
    :param str database: Database alias to execute against
    :return None:
    """
    try:
        with connections[database].cursor() as cursor:
            cursor.execute(query)
    except Exception:
        log.exception("%s database connection failed", database)
        raise HealthcheckFailure("database error")


def check_staticfile(filename):
    # type: (str) -> None
    """
    Verify a static file is reachable

    :param str filename: static file to verify
    :return None:
    """
    if not staticfiles_storage.exists(filename):
        log.error("Can't find %s in static files.", filename)
        raise HealthcheckFailure("static files error")


def check_cache(key="django-alive", cache="default"):
    # type: (str, str) -> None
    """
    Fetch a cache key against the specified cache.

    :param str key: Cache key to fetch (does not need to exist)
    :param str cache: Cache alias to execute against
    :return None:
    """
    try:
        caches[cache].get(key)
    except Exception:
        log.exception("%s cache connection failed", cache)
        raise HealthcheckFailure("cache error")


def check_migrations(alias=None):
    # type: (Optional[str]) -> None
    """
    Verify all defined migrations have been applied

    :param str alias: An optional database alias (default: check all defined databases)
    """
    for db_conn in connections.all():
        if alias and db_conn.alias != alias:
            continue
        executor = MigrationExecutor(db_conn)
        plan = executor.migration_plan(executor.loader.graph.leaf_nodes())
        if plan:
            log.error("Migrations pending on '%s' database", db_conn.alias)
            raise HealthcheckFailure("database migrations pending")
