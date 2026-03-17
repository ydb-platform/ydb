from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import DEFAULT_DB_ALIAS, connections
from django.utils.version import get_version_tuple


def _psycopg_version():
    try:
        import psycopg as Database
    except ImportError:
        import psycopg2 as Database
    except Exception as exc:  # pragma: no cover
        raise ImproperlyConfigured("Error loading psycopg2 or psycopg module") from exc

    version_tuple = get_version_tuple(Database.__version__.split(" ", 1)[0])

    if version_tuple[0] not in (2, 3):  # pragma: no cover
        raise ImproperlyConfigured(f"Pysocpg version {version_tuple[0]} not supported")

    return version_tuple


psycopg_version = _psycopg_version()
psycopg_maj_version = psycopg_version[0]


class AttrDict(dict):
    """A dictionary where keys can be accessed as attributes"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__ = self


def connection(database=None):
    """
    Obtains the connection used for a trigger / model pair. The database
    for the connection is selected based on the write DB in the database
    router config.
    """
    return connections[database or DEFAULT_DB_ALIAS]


def pg_maj_version(cursor):
    """Return the major version of Postgres that's running"""
    version = getattr(cursor.connection, "server_version", cursor.connection.info.server_version)
    return int(str(version)[:-4])


def is_postgres(database):
    return connection(database).vendor == "postgresql"


def postgres_databases(databases=None):
    """Return postgres databases from the provided list of databases.

    If no databases are provided, return all postgres databases
    """
    databases = databases or list(settings.DATABASES)
    assert isinstance(databases, list)
    return [database for database in databases if is_postgres(database)]


def exec_sql(sql, database=None, fetchall=False):
    if is_postgres(database):  # pragma: no branch
        with connection(database).cursor() as cursor:
            cursor.execute(sql)

            if fetchall:
                return cursor.fetchall()


def quote(label, char='"'):
    """Conditionally wraps a label in quotes"""
    if label.startswith(char) or label.endswith(char):
        return label
    else:
        return f"{char}{label}{char}"


def render_uninstall(table, trigger_pgid):
    """Renders uninstallation SQL"""
    return f"DROP TRIGGER IF EXISTS {trigger_pgid} ON {quote(table)};"
