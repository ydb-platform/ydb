from typing import TypedDict


class CommenterOptions(TypedDict, total=False):
    """The `commenter_options` parameter for `instrument_sqlalchemy`."""

    db_driver: bool
    """Include the database driver name in the comment e.g. 'psycopg2'."""
    db_framework: bool
    """Enabling this flag will add the database framework name and version to the comment e.g. 'sqlalchemy:1.4.0'."""
    opentelemetry_values: bool
    """Enabling this flag will add traceparent values to the comment."""
