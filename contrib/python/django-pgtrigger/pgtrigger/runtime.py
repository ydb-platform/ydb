"""
Functions for runtime-configuration of triggers, such as ignoring
them or dynamically setting the search path.
"""

from __future__ import annotations

import contextlib
import threading
from collections.abc import Generator
from typing import TYPE_CHECKING, List, Union

from django.db import connections

from pgtrigger import registry, utils

if utils.psycopg_maj_version == 2:
    import psycopg2.extensions
    import psycopg2.sql as psycopg_sql
elif utils.psycopg_maj_version == 3:
    import psycopg.pq
    import psycopg.sql as psycopg_sql
else:
    raise AssertionError

if TYPE_CHECKING:
    from django.db.backends.utils import CursorWrapper
    from typing_extensions import TypeAlias

    from pgtrigger import Timing

_Query: "TypeAlias" = "str | bytes | psycopg_sql.SQL | psycopg_sql.Composed"

# All triggers currently being ignored
_ignore = threading.local()

# All schemas in the search path
_schema = threading.local()


def _query_to_str(query: _Query, cursor: CursorWrapper) -> str:
    if isinstance(query, str):
        return query
    elif isinstance(query, bytes):
        return query.decode()
    elif isinstance(query, (psycopg_sql.SQL, psycopg_sql.Composed)):
        return query.as_string(cursor.connection)
    else:  # pragma: no cover
        raise TypeError(f"Unsupported query type: {type(query)}")


def _is_concurrent_statement(sql: _Query, cursor: CursorWrapper) -> bool:
    """
    True if the sql statement is concurrent and cannot be ran in a transaction
    """
    sql = _query_to_str(sql, cursor)
    sql = sql.strip().lower() if sql else ""
    return sql.startswith("create") and "concurrently" in sql


def _is_transaction_errored(cursor):
    """
    True if the current transaction is in an errored state
    """
    if utils.psycopg_maj_version == 2:
        return (
            cursor.connection.get_transaction_status()
            == psycopg2.extensions.TRANSACTION_STATUS_INERROR
        )
    elif utils.psycopg_maj_version == 3:
        return cursor.connection.info.transaction_status == psycopg.pq.TransactionStatus.INERROR
    else:
        raise AssertionError


def _can_inject_variable(cursor, sql):
    """True if we can inject a SQL variable into a statement.

    A named cursor automatically prepends
    "NO SCROLL CURSOR WITHOUT HOLD FOR" to the query, which
    causes invalid SQL to be generated. There is no way
    to override this behavior in psycopg, so ignoring triggers
    cannot happen for named cursors. Django only names cursors
    for iterators and other statements that read the database,
    so it seems to be safe to ignore named cursors.

    Concurrent index creation is also incompatible with local variable
    setting. Ignore these cases for now.
    """
    return (
        not getattr(cursor, "name", None)
        and not _is_concurrent_statement(sql, cursor)
        and not _is_transaction_errored(cursor)
    )


def _execute_wrapper(execute_result):
    if utils.psycopg_maj_version == 3:
        while execute_result is not None and execute_result.nextset():
            pass
    return execute_result


def _inject_pgtrigger_ignore(execute, sql, params, many, context):
    """
    A connection execution wrapper that sets a pgtrigger.ignore
    variable in the executed SQL. This lets other triggers know when
    they should ignore execution
    """
    if _can_inject_variable(context["cursor"], sql):
        serialized_ignore = "{" + ",".join(_ignore.value) + "}"
        sql = _query_to_str(sql, context["cursor"])
        sql = f"SELECT set_config('pgtrigger.ignore', %s, true); {sql}"
        params = [serialized_ignore, *(params or ())]

    return _execute_wrapper(execute(sql, params, many, context))


@contextlib.contextmanager
def _set_ignore_session_state(database=None):
    """Starts a session where triggers can be ignored"""
    connection = utils.connection(database)
    if _inject_pgtrigger_ignore not in connection.execute_wrappers:
        with connection.execute_wrapper(_inject_pgtrigger_ignore):
            try:
                yield
            finally:
                if connection.in_atomic_block:
                    # We've finished ignoring triggers and are in a transaction,
                    # so flush the local variable.
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT set_config('pgtrigger.ignore', NULL, false);")
    else:
        yield


@contextlib.contextmanager
def _ignore_session(databases=None):
    """Starts a session where triggers can be ignored"""
    with contextlib.ExitStack() as stack:
        for database in utils.postgres_databases(databases):
            stack.enter_context(_set_ignore_session_state(database=database))

        yield


@contextlib.contextmanager
def _set_ignore_state(model, trigger):
    """
    Manage state to ignore a single URI
    """
    if not hasattr(_ignore, "value"):
        _ignore.value = set()

    pgid = trigger.get_pgid(model)
    if pgid not in _ignore.value:
        # In order to preserve backwards compatibiliy with older installations
        # of the _pgtrigger_ignore func, we must set a full URI (old version)
        # and trigger ID (new version).
        # This will be removed in version 5
        uri = f"{model._meta.db_table}:{pgid}"

        try:
            _ignore.value.add(uri)
            _ignore.value.add(pgid)
            yield
        finally:
            _ignore.value.remove(uri)
            _ignore.value.remove(pgid)
    else:  # The trigger is already being ignored
        yield


@contextlib.contextmanager
def ignore(*uris: str, databases: Union[List[str], None] = None) -> Generator[None]:
    """
    Dynamically ignore registered triggers matching URIs from executing in
    an individual thread.
    If no URIs are provided, ignore all pgtriggers from executing in an
    individual thread.

    Args:
        *uris: Trigger URIs to ignore. If none are provided, all
            triggers will be ignored.
        databases: The databases to use. If none, all postgres databases
            will be used.

    Example:
        Ingore triggers in a context manager:

            with pgtrigger.ignore("my_app.Model:trigger_name"):
                # Do stuff while ignoring trigger

    Example:
        Ignore multiple triggers as a decorator:

            @pgtrigger.ignore("my_app.Model:trigger_name", "my_app.Model:other_trigger")
            def my_func():
                # Do stuff while ignoring trigger
    """
    with contextlib.ExitStack() as stack:
        stack.enter_context(_ignore_session(databases=databases))

        for model, trigger in registry.registered(*uris):
            stack.enter_context(_set_ignore_state(model, trigger))

        yield


ignore.session = _ignore_session


def is_ignored(uri: str) -> bool:
    """
    Check if a trigger URI is currently being ignored.

    Args:
        uri: The trigger URI to check.

    Example:
        Check if a trigger is being ignored:

            with pgtrigger.ignore("my_app.Model:trigger_name"):
                pgtrigger.is_ignored("my_app.Model:trigger_name")  # Returns True

            pgtrigger.is_ignored("my_app.Model:trigger_name")  # Returns False
    """
    model, trigger = registry.registered(uri)[0]
    pg_trigger_id = trigger.get_pgid(model)
    ignored_pg_trigger_ids = getattr(_ignore, "value", set())
    return pg_trigger_id in ignored_pg_trigger_ids


def _inject_schema(execute, sql, params, many, context):
    """
    A connection execution wrapper that sets the schema
    variable in the executed SQL.
    """
    if _can_inject_variable(context["cursor"], sql) and _schema.value:
        path = ", ".join(val if not val.startswith("$") else f'"{val}"' for val in _schema.value)
        sql = f"SELECT set_config('search_path', %s, true); {sql}"
        params = [path, *(params or ())]

    return _execute_wrapper(execute(sql, params, many, context))


@contextlib.contextmanager
def _set_schema_session_state(database=None):
    connection = utils.connection(database)

    if _inject_schema not in connection.execute_wrappers:
        if connection.in_atomic_block:
            # If this is the first time we are setting the search path,
            # register the pre_execute_hook and store a reference to the original
            # search path. Note that we must use this approach because we cannot
            # simply reset the search_path at the end. A user may have previously
            # set it
            with connection.cursor() as cursor:
                cursor.execute("SELECT current_setting('search_path')")
                initial_search_path = cursor.fetchall()[0][0]

        with connection.execute_wrapper(_inject_schema):
            try:
                yield
            finally:
                if connection.in_atomic_block:
                    # We've finished modifying the search path and are in a transaction,
                    # so flush the local variable
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "SELECT set_config('search_path', %s, false)", [initial_search_path]
                        )
    else:
        yield


@contextlib.contextmanager
def _schema_session(databases=None):
    """Starts a session where the search path can be modified"""
    with contextlib.ExitStack() as stack:
        for database in utils.postgres_databases(databases):
            stack.enter_context(_set_schema_session_state(database=database))

        yield


@contextlib.contextmanager
def _set_schema_state(*schemas):
    if not hasattr(_schema, "value"):
        # Use a list instead of a set because ordering is important to the search path
        _schema.value = []

    schemas = [s for s in schemas if s not in _schema.value]
    try:
        _schema.value.extend(schemas)
        yield
    finally:
        for s in schemas:
            _schema.value.remove(s)


@contextlib.contextmanager
def schema(*schemas: str, databases: Union[List[str], None] = None) -> Generator[None]:
    """
    Sets the search path to the provided schemas.

    If nested, appends the schemas to the search path if not already in it.

    Args:
        *schemas: Schemas that should be appended to the search path.
            Schemas already in the search path from nested calls will not be
            appended.
        databases: The databases to set the search path. If none, all postgres
            databases will be used.
    """
    with contextlib.ExitStack() as stack:
        stack.enter_context(_schema_session(databases=databases))
        stack.enter_context(_set_schema_state(*schemas))

        yield


schema.session = _schema_session


def constraints(timing: "Timing", *uris: str, databases: Union[List[str], None] = None) -> None:
    """
    Set deferrable constraint timing for the given triggers, which
    will persist until overridden or until end of transaction.
    Must be in a transaction to run this.

    Args:
        timing: The timing value that overrides the default trigger timing.
        *uris: Trigger URIs over which to set constraint timing.
            If none are provided, all trigger constraint timing will
            be set. All triggers must be deferrable.
        databases: The databases on which to set constraints. If none, all
            postgres databases will be used.

    Raises:
        RuntimeError: If the database of any triggers is not in a transaction.
        ValueError: If any triggers are not deferrable.
    """

    for model, trigger in registry.registered(*uris):
        if not trigger.timing:
            raise ValueError(
                f"Trigger {trigger.name} on model {model._meta.label_lower} is not deferrable."
            )

    for database in utils.postgres_databases(databases):
        if not connections[database].in_atomic_block:
            raise RuntimeError(f'Database "{database}" is not in a transaction.')

        names = ", ".join(trigger.get_pgid(model) for model, trigger in registry.registered(*uris))

        with connections[database].cursor() as cursor:
            cursor.execute(f"SET CONSTRAINTS {names} {timing}")
