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

from collections import abc
from datetime import datetime
from datetime import timezone
from contextlib import contextmanager
from importlib import import_module
from itertools import count
from logging import getLogger
from importlib_metadata import entry_points

import getpass
import os
import pickle
import socket
import time
import typing as t
import uuid

from yoyo import exceptions
from yoyo import internalmigrations
from yoyo import utils
from yoyo.migrations import topological_sort

logger = getLogger("yoyo.migrations")


class TransactionManager:
    """
    Returned by the :meth:`~yoyo.backends.DatabaseBackend.transaction`
    context manager.

    If rollback is called, the transaction is flagged to be rolled back
    when the context manager block closes
    """

    def __init__(self, backend, rollback_on_exit=False):
        self.backend = backend
        self.rollback_on_exit = rollback_on_exit

    def __enter__(self):
        self._do_begin()
        return self

    def __exit__(self, exc_type, value, traceback):
        if exc_type:
            self._do_rollback()
            return None

        if self.rollback_on_exit:
            self._do_rollback()
        else:
            self._do_commit()

    def _do_begin(self):
        """
        Instruct the backend to begin a transaction
        """
        self.backend.begin()

    def _do_commit(self):
        """
        Instruct the backend to commit the transaction
        """
        self.backend.commit()

    def _do_rollback(self):
        """
        Instruct the backend to roll back the transaction
        """
        self.backend.rollback()


class SavepointTransactionManager(TransactionManager):
    id = None
    id_generator = count(1)

    def _do_begin(self):
        assert self.id is None
        self.id = "sp_{}".format(next(self.id_generator))
        self.backend.savepoint(self.id)

    def _do_commit(self):
        """
        This does nothing.

        Trying to the release savepoint here could cause an database error in
        databases where DDL queries cause the transaction to be committed
        and all savepoints released.
        """

    def _do_rollback(self):
        self.backend.savepoint_rollback(self.id)


class DatabaseBackend:
    driver_module = ""

    log_table = "_yoyo_log"
    lock_table = "yoyo_lock"
    list_tables_sql = "SELECT table_name FROM information_schema.tables"
    version_table = "_yoyo_version"
    migration_table = "_yoyo_migrations"
    is_applied_sql = """
        SELECT COUNT(1) FROM {0.migration_table_quoted}
        WHERE {quoted.id}=:id}"""
    mark_migration_sql = (
        "INSERT INTO {0.migration_table_quoted} "
        "({quoted.migration_hash}, {quoted.migration_id}, {quoted.applied_at_utc}) "
        "VALUES (:migration_hash, :migration_id, :when)"
    )

    unmark_migration_sql = (
        "DELETE FROM {0.migration_table_quoted} WHERE "
        "{quoted.migration_hash} = :migration_hash"
    )

    applied_migrations_sql = (
        "SELECT {quoted.migration_hash} FROM "
        "{0.migration_table_quoted} "
        "ORDER by {quoted.applied_at_utc}"
    )
    create_test_table_sql = (
        "CREATE TABLE {table_name_quoted} ({quoted.id} INT PRIMARY KEY)"
    )
    log_migration_sql = (
        "INSERT INTO {0.log_table_quoted} "
        "({quoted.id}, {quoted.migration_hash}, {quoted.migration_id}, "
        "{quoted.operation}, {quoted.username}, {quoted.hostname}, "
        "{quoted.created_at_utc}) "
        "VALUES (:id, :migration_hash, :migration_id, "
        ":operation, :username, :hostname, :created_at_utc)"
    )
    create_lock_table_sql = (
        "CREATE TABLE {0.lock_table_quoted} ("
        "{quoted.locked} INT DEFAULT 1, "
        "{quoted.ctime} TIMESTAMP,"
        "{quoted.pid} INT NOT NULL,"
        "PRIMARY KEY ({quoted.locked}))"
    )

    _driver = None
    _is_locked = False
    _in_transaction = False
    _internal_schema_updated = False
    _transactional_ddl_cache: dict[bytes, bool] = {}

    def __init__(self, dburi, migration_table):
        self.uri = dburi
        self.DatabaseError = self.driver.DatabaseError
        self._connection = self.connect(dburi)
        self.init_connection(self._connection)
        self.migration_table = migration_table
        self.has_transactional_ddl = self._transactional_ddl_cache.get(
            pickle.dumps(self.uri), True
        )

    def init_database(self):
        self.create_lock_table()
        self.has_transactional_ddl = self._check_transactional_ddl()
        self._transactional_ddl_cache[
            pickle.dumps(self.uri)
        ] = self.has_transactional_ddl

    def _load_driver_module(self):
        """
        Load the dbapi driver module and register the base exception class
        """
        driver = get_dbapi_module(self.driver_module)
        exceptions.register(driver.DatabaseError)
        return driver

    def format_sql(self, s, **kwargs):
        """
        Take a string in the format used by the various ``..._sql`` class
        variables and format it.
        """
        quote_identifier = self.quote_identifier

        class Quoter:
            def __getattr__(self, s):
                return quote_identifier(s)

        return s.format(self, quoted=Quoter(), **kwargs)

    @property
    def driver(self):
        if self._driver:
            return self._driver
        self._driver = self._load_driver_module()
        return self._driver

    @property
    def connection(self):
        return self._connection

    def init_connection(self, connection):
        """
        Called when creating a connection or after a rollback. May do any
        db specific tasks required to make the connection ready for use.
        """

    def copy(self):
        """
        Return a copy of the backend with a independent db
        connection.
        """
        return self.__class__(self.uri, self.migration_table)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.connection.close()

    def __getattr__(self, attrname):
        if attrname.endswith("_quoted"):
            unquoted = getattr(self, attrname.rsplit("_quoted")[0])
            return self.quote_identifier(unquoted)
        raise AttributeError(attrname)

    def connect(self, dburi):
        raise NotImplementedError()

    def quote_identifier(self, s):
        assert "\x00" not in s
        quoted = s.replace('"', '""')
        return f'"{quoted}"'

    def _check_transactional_ddl(self):
        """
        Return True if the database supports committing/rolling back
        DDL statements within a transaction
        """
        table_name = "yoyo_tmp_{}".format(utils.get_random_string(10))
        table_name_quoted = self.quote_identifier(table_name)
        sql = self.format_sql(
            self.create_test_table_sql, table_name_quoted=table_name_quoted
        )
        try:
            with self.transaction(rollback_on_exit=True):
                self.execute(sql)
        except self.DatabaseError:
            return False

        try:
            with self.transaction():
                self.execute(f"DROP TABLE {table_name_quoted}")
        except self.DatabaseError:
            return True
        return False

    def list_tables(self, **kwargs) -> list[str]:
        """
        Return a list of tables present in the backend.
        This is used by the test suite to clean up tables
        generated during testing
        """
        cursor = self.execute(
            self.list_tables_sql,
            dict({"database": self.uri.database}, **kwargs),
        )
        return [row[0] for row in cursor.fetchall()]

    def transaction(self, rollback_on_exit=False):
        if not self._in_transaction:
            return TransactionManager(self, rollback_on_exit=rollback_on_exit)

        else:
            return SavepointTransactionManager(self, rollback_on_exit=rollback_on_exit)

    def cursor(self):
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()
        self._in_transaction = False

    def rollback(self):
        self.connection.rollback()
        self.init_connection(self.connection)
        self._in_transaction = False

    def begin(self):
        """
        Begin a new transaction
        """
        assert not self._in_transaction
        self._in_transaction = True
        self.execute("BEGIN")

    def savepoint(self, id):
        """
        Create a new savepoint with the given id
        """
        self.execute(f"SAVEPOINT {self.quote_identifier(id)}")

    def savepoint_release(self, id):
        """
        Release (commit) the savepoint with the given id
        """
        self.execute(f"RELEASE SAVEPOINT {self.quote_identifier(id)}")

    def savepoint_rollback(self, id):
        """
        Rollback the savepoint with the given id
        """
        self.execute(f"ROLLBACK TO SAVEPOINT {self.quote_identifier(id)}")

    @contextmanager
    def disable_transactions(self):
        """
        Disable the connection's transaction support, for example by
        setting the isolation mode to 'autocommit'
        """
        self.rollback()
        yield

    @contextmanager
    def lock(self, timeout=10):
        """
        Create a lock to prevent concurrent migrations.

        :param timeout: duration in seconds before raising a LockTimeout error.
        """
        if self._is_locked:
            yield
            return

        pid = os.getpid()
        self._insert_lock_row(pid, timeout)
        try:
            self._is_locked = True
            yield
            self._is_locked = False
        finally:
            self._delete_lock_row(pid)

    def _insert_lock_row(self, pid, timeout, poll_interval=0.5):
        poll_interval = min(poll_interval, timeout)
        started = time.time()
        qi = self.quote_identifier
        while True:
            try:
                with self.transaction():
                    self.execute(
                        f"""
                        INSERT INTO {self.lock_table_quoted}
                        ({qi('locked')}, {qi('ctime')}, {qi('pid')})
                        VALUES (1, :when, :pid)
                        """,
                        {
                            "when": datetime.now(timezone.utc).replace(tzinfo=None),
                            "pid": pid,
                        },
                    )
            except self.DatabaseError:
                if timeout and time.time() > started + timeout:
                    cursor = self.execute(
                        f"SELECT {qi('pid')} FROM {self.lock_table_quoted}"
                    )
                    row = cursor.fetchone()
                    if row:
                        raise exceptions.LockTimeout(
                            f"Process {row[0]} has locked this database "
                            "(run yoyo break-lock to remove this lock)"
                        )
                    else:
                        raise exceptions.LockTimeout(
                            "Database locked (run yoyo break-lock to remove this lock)"
                        )
                time.sleep(poll_interval)
            else:
                return

    def _delete_lock_row(self, pid):
        with self.transaction():
            qi = self.quote_identifier
            self.execute(
                f"DELETE FROM {self.lock_table_quoted} WHERE {qi('pid')}=:pid",
                {"pid": pid},
            )

    def break_lock(self):
        with self.transaction():
            self.execute(f"DELETE FROM {self.lock_table_quoted}")

    def execute(self, sql, params: t.Union[abc.Mapping[str, t.Any], None] = None):
        """
        Create a new cursor, execute a single statement and return the cursor
        object.

        :param sql: A single SQL statement, optionally with named parameters
                    (eg 'SELECT * FROM foo WHERE :bar IS NULL')
        :param params: A dictionary of parameters
        """
        cursor = self.cursor()
        sql, queryparams = utils.change_param_style(self.driver.paramstyle, sql, params)
        cursor.execute(sql, queryparams)
        return cursor

    def create_lock_table(self):
        """
        Create the lock table if it does not already exist.
        """
        try:
            with self.transaction():
                self.execute(self.format_sql(self.create_lock_table_sql))
        except self.DatabaseError:
            pass

    def ensure_internal_schema_updated(self):
        """
        Check and upgrade yoyo's internal schema.
        """
        if self._internal_schema_updated:
            return
        if internalmigrations.needs_upgrading(self):
            assert not self._in_transaction
            with self.lock():
                internalmigrations.upgrade(self)
                self.connection.commit()
                self._internal_schema_updated = True

    def is_applied(self, migration):
        return migration.hash in self.get_applied_migration_hashes()

    def get_applied_migration_hashes(self):
        """
        Return the list of migration hashes in the order in which they
        were applied
        """
        self.ensure_internal_schema_updated()
        sql = self.format_sql(self.applied_migrations_sql)
        return [row[0] for row in self.execute(sql).fetchall()]

    def to_apply(self, migrations):
        """
        Return the subset of migrations not already applied.
        """
        applied = self.get_applied_migration_hashes()
        ms = (m for m in migrations if m.hash not in applied)
        return migrations.__class__(topological_sort(ms), migrations.post_apply)

    def to_rollback(self, migrations):
        """
        Return the subset of migrations already applied and which may be
        rolled back.

        The order of migrations will be reversed.
        """
        applied = self.get_applied_migration_hashes()
        ms = (m for m in migrations if m.hash in applied)
        return migrations.__class__(
            reversed(list(topological_sort(ms))), migrations.post_apply
        )

    def apply_migrations(self, migrations, force=False):
        if migrations:
            self.apply_migrations_only(migrations, force=force)
            self.run_post_apply(migrations, force=force)

    def apply_migrations_only(self, migrations, force=False):
        """
        Apply the list of migrations, but do not run any post-apply hooks
        present.
        """
        if not migrations:
            return
        for m in migrations:
            try:
                self.apply_one(m, force=force)
            except exceptions.BadMigration:
                continue

    def run_post_apply(self, migrations, force=False):
        """
        Run any post-apply migrations present in ``migrations``
        """
        for m in migrations.post_apply:
            self.apply_one(m, mark=False, force=force)

    def rollback_migrations(self, migrations, force=False):
        self.ensure_internal_schema_updated()
        if not migrations:
            return
        for m in migrations:
            try:
                self.rollback_one(m, force)
            except exceptions.BadMigration:
                continue

    def mark_migrations(self, migrations):
        self.ensure_internal_schema_updated()
        with self.transaction():
            for m in migrations:
                try:
                    self.mark_one(m)
                except exceptions.BadMigration:
                    continue

    def unmark_migrations(self, migrations):
        self.ensure_internal_schema_updated()
        with self.transaction():
            for m in migrations:
                try:
                    self.unmark_one(m)
                except exceptions.BadMigration:
                    continue

    def apply_one(self, migration, force=False, mark=True):
        """
        Apply a single migration
        """
        logger.info("Applying %s", migration.id)
        self.ensure_internal_schema_updated()
        with self.copy() as migration_backend:
            migration.process_steps(migration_backend, "apply", force=force)
        self.log_migration(migration, "apply")
        if mark:
            with self.transaction():
                self.mark_one(migration, log=False)

    def rollback_one(self, migration, force=False):
        """
        Rollback a single migration
        """
        logger.info("Rolling back %s", migration.id)
        self.ensure_internal_schema_updated()
        with self.copy() as migration_backend:
            migration.process_steps(migration_backend, "rollback", force=force)
        self.log_migration(migration, "rollback")
        with self.transaction():
            self.unmark_one(migration, log=False)

    def unmark_one(self, migration, log=True):
        self.ensure_internal_schema_updated()
        sql = self.format_sql(self.unmark_migration_sql)
        self.execute(sql, {"migration_hash": migration.hash})
        if log:
            self.log_migration(migration, "unmark")

    def mark_one(self, migration, log=True):
        self.ensure_internal_schema_updated()
        logger.info("Marking %s applied", migration.id)
        sql = self.format_sql(self.mark_migration_sql)
        self.execute(
            sql,
            {
                "migration_hash": migration.hash,
                "migration_id": migration.id,
                "when": datetime.now(timezone.utc).replace(tzinfo=None),
            },
        )
        if log:
            self.log_migration(migration, "mark")

    def log_migration(self, migration, operation):
        sql = self.format_sql(self.log_migration_sql)
        self.execute(sql, self.get_log_data(migration, operation))

    def get_log_data(self, migration=None, operation="apply"):
        """
        Return a dict of data for insertion into the ``_yoyo_log`` table
        """
        assert operation in {"apply", "rollback", "mark", "unmark"}
        return {
            "id": str(uuid.uuid1()),
            "migration_id": migration.id if migration else None,
            "migration_hash": migration.hash if migration else None,
            "username": getpass.getuser(),
            "hostname": socket.getfqdn(),
            "created_at_utc": datetime.now(timezone.utc).replace(tzinfo=None),
            "operation": operation,
        }


def get_backend_class(name):
    backend_eps = entry_points(group="yoyo.backends")
    return backend_eps[name].load()


def get_dbapi_module(name):
    """
    Import and return the named DB-API driver module
    """
    return import_module(name)
