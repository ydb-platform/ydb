# Copyright 2015 Oliver Cope
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime

from yoyo.backends.base import DatabaseBackend


class SQLiteBackend(DatabaseBackend):
    driver_module = "sqlite3"
    list_tables_sql = "SELECT name FROM sqlite_master WHERE type = 'table'"

    def connect(self, dburi):
        # Ensure that multiple connections share the same data
        # https://sqlite.org/sharedcache.html
        conn = self.driver.connect(
            f"file:{dburi.database}?cache=shared",
            uri=True,
            detect_types=self.driver.PARSE_DECLTYPES,
        )
        conn.isolation_level = None
        return conn

    def __enter__(self):
        self._saved_converters = self.driver.converters.copy()
        self._saved_adapters = self.driver.adapters.copy()
        self.driver.register_adapter(datetime.datetime, adapt_datetime_iso8601)
        self.driver.register_converter("timestamp", convert_iso8601)
        return super().__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        self.driver.converters = self._saved_converters
        self.driver.adapters = self._saved_adapters
        return super().__exit__(exc_type, exc_value, traceback)


def adapt_datetime_iso8601(val):
    """
    Adapt datetime.datetime to timezone-naive ISO 8601 date.
    """
    return val.isoformat()


def convert_iso8601(val):
    """
    Convert ISO 8601 date string to datetime.datetime object.
    """
    return datetime.datetime.fromisoformat(val.decode())
