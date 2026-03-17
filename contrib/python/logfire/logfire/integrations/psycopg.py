from typing import TypedDict


class CommenterOptions(TypedDict, total=False):
    """The `commenter_options` parameter for `instrument_psycopg`."""

    db_driver: bool
    """Include the database driver name in the comment e.g. 'psycopg2'."""

    dbapi_threadsafety: bool
    """Include the DB-API threadsafety value in the comment."""

    dbapi_level: bool
    """Include the DB-API level in the comment."""

    libpq_version: bool
    """Include the libpq version in the comment."""

    driver_paramstyle: bool
    """Include the driver paramstyle in the comment e.g. 'driver_paramstyle=pyformat'"""

    opentelemetry_values: bool
    """Enabling this flag will add traceparent values to the comment."""
