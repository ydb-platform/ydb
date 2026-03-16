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


class ODBCBackend(DatabaseBackend):
    driver_module = "pyodbc"

    def connect(self, dburi):
        args = [
            ("UID", dburi.username),
            ("PWD", dburi.password),
            ("ServerName", dburi.hostname),
            ("Port", dburi.port),
            ("Database", dburi.database),
        ]
        args.extend(dburi.args.items())
        s = ";".join("{}={}".format(k, v) for k, v in args if v is not None)
        return self.driver.connect(s)
