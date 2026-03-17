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

from yoyo.backends.base import DatabaseBackend


class OracleBackend(DatabaseBackend):
    driver_module = "cx_Oracle"
    list_tables_sql = (
        "SELECT table_name FROM all_tables "
        "WHERE owner=user and :database=:database"
    )

    def begin(self):
        """Oracle is always in a transaction, and has no "BEGIN" statement."""
        self._in_transaction = True

    def connect(self, dburi):
        kwargs = dburi.args
        if dburi.username is not None:
            kwargs["user"] = dburi.username
        if dburi.password is not None:
            kwargs["password"] = dburi.password
        # Oracle combines the hostname, port and database into a single DSN.
        # The DSN can also be a "net service name"
        kwargs["dsn"] = ""
        if dburi.hostname is not None:
            kwargs["dsn"] = dburi.hostname
        if dburi.port is not None:
            kwargs["dsn"] += ":{0}".format(dburi.port)
        if dburi.database is not None:
            if kwargs["dsn"]:
                kwargs["dsn"] += "/{0}".format(dburi.database)
            else:
                kwargs["dsn"] = dburi.database

        return self.driver.connect(**kwargs)
