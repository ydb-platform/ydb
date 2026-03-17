"""
The primary functional API for pgtrigger
"""

import logging
from typing import List, Tuple, Union

from django.db import DEFAULT_DB_ALIAS, connections

from pgtrigger import features, registry, utils

# The core pgtrigger logger
LOGGER = logging.getLogger("pgtrigger")


def install(*uris: str, database: Union[str, None] = None) -> None:
    """
    Install triggers.

    Args:
        *uris: URIs of triggers to install. If none are provided,
            all triggers are installed and orphaned triggers are pruned.
        database: The database. Defaults to the "default" database.
    """
    for model, trigger in registry.registered(*uris):
        LOGGER.info(
            "pgtrigger: Installing %s trigger for %s table on %s database.",
            trigger,
            model._meta.db_table,
            database or DEFAULT_DB_ALIAS,
        )
        trigger.install(model, database=database)

    if not uris and features.prune_on_install():  # pragma: no branch
        prune(database=database)


def prunable(database: Union[str, None] = None) -> List[Tuple[str, str, bool, str]]:
    """Return triggers that are candidates for pruning

    Args:
        database: The database. Defaults to the "default" database.

    Returns:
        A list of tuples consisting of the table, trigger ID, enablement, and database
    """
    if not utils.is_postgres(database):
        return []

    registered = {
        (utils.quote(model._meta.db_table), trigger.get_pgid(model))
        for model, trigger in registry.registered()
    }

    with utils.connection(database).cursor() as cursor:
        parent_trigger_clause = "tgparentid = 0 AND" if utils.pg_maj_version(cursor) >= 13 else ""

        # Only select triggers that are in the current search path. We accomplish
        # this by parsing the tgrelid and only selecting triggers that don't have
        # a schema name in their path
        cursor.execute(
            f"""
            SELECT tgrelid::regclass, tgname, tgenabled
                FROM pg_trigger
                WHERE tgname LIKE 'pgtrigger_%%' AND
                      {parent_trigger_clause}
                      array_length(parse_ident(tgrelid::regclass::varchar), 1) = 1
            """
        )
        triggers = set(cursor.fetchall())

    return [
        (trigger[0], trigger[1], trigger[2] == "O", database or DEFAULT_DB_ALIAS)
        for trigger in triggers
        if (utils.quote(trigger[0]), trigger[1]) not in registered
    ]


def prune(database: Union[str, None] = None) -> None:
    """
    Remove any pgtrigger triggers in the database that are not used by models.
    I.e. if a model or trigger definition is deleted from a model, ensure
    it is removed from the database

    Args:
        database: The database. Defaults to the "default" database.
    """
    for trigger in prunable(database=database):
        LOGGER.info(
            "pgtrigger: Pruning trigger %s for table %s on %s database.",
            trigger[1],
            trigger[0],
            trigger[3],
        )

        connection = connections[trigger[3]]
        uninstall_sql = utils.render_uninstall(trigger[0], trigger[1])
        with connection.cursor() as cursor:
            cursor.execute(uninstall_sql)


def enable(*uris: str, database: Union[str, None] = None) -> None:
    """
    Enables registered triggers.

    Args:
        *uris: URIs of triggers to enable. If none are provided,
            all triggers are enabled.
        database: The database. Defaults to the "default" database.
    """
    for model, trigger in registry.registered(*uris):
        LOGGER.info(
            "pgtrigger: Enabling %s trigger for %s table on %s database.",
            trigger,
            model._meta.db_table,
            database or DEFAULT_DB_ALIAS,
        )
        trigger.enable(model, database=database)


def uninstall(*uris: str, database: Union[str, None] = None) -> None:
    """
    Uninstalls triggers.

    Args:
        *uris: URIs of triggers to uninstall. If none are provided,
            all triggers are uninstalled and orphaned triggers are pruned.
        database: The database. Defaults to the "default" database.
    """
    for model, trigger in registry.registered(*uris):
        LOGGER.info(
            "pgtrigger: Uninstalling %s trigger for %s table on %s database.",
            trigger,
            model._meta.db_table,
            database or DEFAULT_DB_ALIAS,
        )
        trigger.uninstall(model, database=database)

    if not uris and features.prune_on_install():
        prune(database=database)


def disable(*uris: str, database: Union[str, None] = None) -> None:
    """
    Disables triggers.

    Args:
        *uris: URIs of triggers to disable. If none are provided,
            all triggers are disabled.
        database: The database. Defaults to the "default" database.
    """
    for model, trigger in registry.registered(*uris):
        LOGGER.info(
            "pgtrigger: Disabling %s trigger for %s table on %s database.",
            trigger,
            model._meta.db_table,
            database or DEFAULT_DB_ALIAS,
        )
        trigger.disable(model, database=database)
