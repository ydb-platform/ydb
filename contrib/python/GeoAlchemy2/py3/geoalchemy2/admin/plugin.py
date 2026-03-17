from collections import defaultdict
from functools import partial

from sqlalchemy import event
from sqlalchemy.engine import CreateEnginePlugin

from geoalchemy2.admin import select_dialect


class GeoEngine(CreateEnginePlugin):
    """A plugin to create `sqlalchemy.Engine` objects with specific events to handle spatial data.

    This plugin should only be used with the :func:`sqlalchemy.create_engine` function. This plugin
    will automatically attach the relevant event listeners to the new engine depending on its
    dialect. For some specific dialects it's even possible to pass arguments to customize the
    functions called by the listeners. These arguments have the following form:
    `geoalchemy2_<event>_<dialect>_<argument_name>`.
    For example, creating a new SQLite DB and initialize the SpatiaLite extension with only the
    WGS84 SRID can be performed this way:

    .. code-block:: python

        db_url = "sqlite:////tmp/test_db.sqlite?geoalchemy2_connect_sqlite_init_mode=WGS84"
        engine = sqlalchemy.create_engine(db_url, plugins=["geoalchemy2"])

    The names of the parameters can be found in the event listener of each dialect. Note that all
    dialects don't have listeners for all events.
    """

    def __init__(self, url, kwargs):
        super().__init__(url, kwargs)

        # Consume the parameters from the URL query
        self.params = defaultdict(lambda: defaultdict(dict))

        transaction = url.query.get("geoalchemy2_connect_sqlite_transaction", None)
        if transaction is not None:
            self.params["connect"]["sqlite"]["transaction"] = self.str_to_bool(transaction)

        init_mode = url.query.get("geoalchemy2_connect_sqlite_init_mode", None)
        if init_mode is not None:
            self.params["connect"]["sqlite"]["init_mode"] = init_mode

        journal_mode = url.query.get("geoalchemy2_connect_sqlite_journal_mode", None)
        if journal_mode is not None:
            self.params["connect"]["sqlite"]["journal_mode"] = journal_mode

        before_cursor_execute_convert_mysql = url.query.get(
            "geoalchemy2_before_cursor_execute_mysql_convert", None
        )
        if before_cursor_execute_convert_mysql is not None:
            self.params["before_cursor_execute"]["mysql"]["convert"] = self.str_to_bool(
                before_cursor_execute_convert_mysql
            )

        before_cursor_execute_convert_mariadb = url.query.get(
            "geoalchemy2_before_cursor_execute_mariadb_convert", None
        )
        if before_cursor_execute_convert_mariadb is not None:
            self.params["before_cursor_execute"]["mariadb"]["convert"] = self.str_to_bool(
                before_cursor_execute_convert_mariadb
            )

    @staticmethod
    def str_to_bool(argument):
        """Cast argument to bool."""
        lowered = str(argument).lower()
        if lowered in ("yes", "y", "true", "t", "1", "enable", "on"):
            return True
        elif lowered in ("no", "n", "false", "f", "0", "disable", "off"):
            return False
        raise ValueError(argument)

    def update_url(self, url):
        """Update the URL to one that no longer includes specific parameters."""
        return url.difference_update_query(
            [
                "geoalchemy2_connect_sqlite_transaction",
                "geoalchemy2_connect_sqlite_init_mode",
                "geoalchemy2_connect_sqlite_journal_mode",
                "geoalchemy2_before_cursor_execute_mysql_convert",
                "geoalchemy2_before_cursor_execute_mariadb_convert",
            ],
        )

    def engine_created(self, engine):
        """Attach event listeners after the new Engine object is created."""
        dialect_module = select_dialect(engine.dialect.name)

        if hasattr(dialect_module, "connect"):
            params = dict(self.params["connect"].get(engine.dialect.name, {}))
            func = partial(dialect_module.connect, **params)
            event.listen(engine, "connect", func)

        if hasattr(dialect_module, "before_cursor_execute"):
            params = dict(self.params["before_cursor_execute"].get(engine.dialect.name, {}))
            func = partial(dialect_module.before_cursor_execute, **params)
            event.listen(engine, "before_cursor_execute", func, retval=True)
