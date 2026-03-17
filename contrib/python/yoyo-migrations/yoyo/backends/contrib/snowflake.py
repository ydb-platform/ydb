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


class SnowflakeBackend(DatabaseBackend):
    driver_module = "snowflake.connector"

    def connect(self, dburi):
        database, schema = dburi.database.split("/")
        return self.driver.connect(
            user=dburi.username,
            password=dburi.password,
            account=dburi.hostname,
            database=database,
            schema=schema,
            **dburi.args,
        )

    def savepoint(self, id):
        pass

    def savepoint_release(self, id):
        pass

    def savepoint_rollback(self, id):
        pass
