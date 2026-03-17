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

import time
from datetime import datetime
from datetime import timezone

from yoyo import exceptions
from yoyo.backends.core.postgresql import PostgresqlBackend


class RedshiftBackend(PostgresqlBackend):
    def list_tables(self, **kwargs):
        current_schema = self.execute("SELECT current_schema()").fetchone()[0]
        return super(PostgresqlBackend, self).list_tables(
            schema=current_schema, **kwargs
        )

    # Redshift does not support ROLLBACK TO SAVEPOINT
    def savepoint(self, id):
        pass

    def savepoint_release(self, id):
        pass

    def savepoint_rollback(self, id):
        self.rollback()

    # Redshift does not enforce primary and unique keys
    def _insert_lock_row(self, pid, timeout, poll_interval=0.5):
        poll_interval = min(poll_interval, timeout)
        started = time.time()
        while True:
            with self.transaction():
                # prevents isolation violation errors
                self.execute("LOCK {}".format(self.lock_table_quoted))
                cursor = self.execute(
                    "SELECT pid FROM {}".format(self.lock_table_quoted)
                )
                row = cursor.fetchone()
                if not row:
                    self.execute(
                        "INSERT INTO {} (locked, ctime, pid) "
                        "VALUES (1, :when, :pid)".format(self.lock_table_quoted),
                        {
                            "when": datetime.now(timezone.utc).replace(tzinfo=None),
                            "pid": pid,
                        },
                    )
                    return
                elif timeout and time.time() > started + timeout:
                    raise exceptions.LockTimeout(
                        "Process {} has locked this database "
                        "(run yoyo break-lock to remove this lock)".format(row[0])
                    )
                else:
                    time.sleep(poll_interval)
