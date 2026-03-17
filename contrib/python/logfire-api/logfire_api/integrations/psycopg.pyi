from typing import TypedDict

class CommenterOptions(TypedDict, total=False):
    """The `commenter_options` parameter for `instrument_psycopg`."""
    db_driver: bool
    dbapi_threadsafety: bool
    dbapi_level: bool
    libpq_version: bool
    driver_paramstyle: bool
    opentelemetry_values: bool
