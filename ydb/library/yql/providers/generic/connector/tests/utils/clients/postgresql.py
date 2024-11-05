from contextlib import contextmanager
import time
from datetime import datetime
from typing import Tuple

import pg8000.dbapi

from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings
from ydb.library.yql.providers.generic.connector.tests.utils.log import make_logger

LOGGER = make_logger(__name__)


class Client:
    # database name -> pool
    settings: Settings.PostgreSQL

    def __init__(self, settings: Settings.PostgreSQL):
        self.settings = settings
        self.pools = dict()
        LOGGER.debug("initializing client")

    @contextmanager
    def get_cursor(self, dbname: str):
        conn, cursor = self._make_cursor(dbname=dbname)
        yield conn, cursor
        cursor.close()
        conn.close()

    def _make_cursor(self, dbname: str) -> Tuple[pg8000.dbapi.Connection, pg8000.dbapi.Cursor]:
        LOGGER.debug(f"making cursor for database {dbname}")
        start = datetime.now()
        attempt = 0

        while (datetime.now() - start).total_seconds() < 10:
            attempt += 1
            try:
                LOGGER.debug(
                    f"trying to connect PostgreSQL: {self.settings.host_external}:{self.settings.port_external}"
                )
                conn = pg8000.dbapi.Connection(
                    user=self.settings.username,
                    password=self.settings.password,
                    host=self.settings.host_external,
                    port=self.settings.port_external,
                    database=dbname,
                    timeout=10,
                )
                conn.autocommit = True

                cur = conn.cursor()
                return conn, cur
            except Exception as e:
                LOGGER.error(f"connection attempt #{attempt} failed: {e} {e.args}")
                time.sleep(1)
                continue

        ss = self.settings
        params = f'{ss.username} {ss.password} {ss.host_external} {ss.port_external} {dbname}'
        raise Exception(f"Failed to connect PostgreSQL in {attempt} attempt(s) with params: {params}")
