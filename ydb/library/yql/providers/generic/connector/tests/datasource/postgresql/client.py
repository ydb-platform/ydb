from contextlib import contextmanager
import time
from datetime import datetime
from typing import Tuple
import sys

import pg8000.dbapi

from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings


class Client:
    # database name -> pool
    settings: Settings.PostgreSQL

    def __init__(self, settings: Settings.PostgreSQL):
        self.settings = settings
        self.pools = dict()

    @contextmanager
    def get_cursor(self, dbname: str):
        conn, cursor = self._make_cursor(dbname=dbname)
        yield conn, cursor
        cursor.close()
        conn.close()

    def _make_cursor(self, dbname: str) -> Tuple[pg8000.dbapi.Connection, pg8000.dbapi.Cursor]:
        start = datetime.now()
        attempt = 0

        while (datetime.now() - start).total_seconds() < 10:
            attempt += 1
            try:
                sys.stdout.write(
                    f"Trying to connect PostgreSQL: {self.settings.host_external}:{self.settings.port_external}\n"
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
                sys.stderr.write(f"attempt #{attempt} failed: {e} {e.args}\n")
                time.sleep(3)
                continue

        ss = self.settings
        params = f'{ss.username} {ss.password} {ss.host_external} {ss.port_external} {dbname}'
        raise Exception(f"Failed to connect PostgreSQL in {attempt} attempt(s) with params: {params}")
