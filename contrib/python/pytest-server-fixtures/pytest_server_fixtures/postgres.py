# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import os
import logging
import subprocess

import errno
import pytest
from six import text_type

from pytest_server_fixtures import CONFIG
from pytest_fixture_config import requires_config

from .base import TestServer

log = logging.getLogger(__name__)


@pytest.fixture(scope='session')
@requires_config(CONFIG, ['pg_config_executable'])
def postgres_server_sess(request):
    """A session-scoped Postgres Database fixture"""
    return _postgres_server(request)


def _postgres_server(request):
    server = PostgresServer()
    server.start()
    request.addfinalizer(server.teardown)
    return server


class PostgresServer(TestServer):
    """
    Exposes a server.connect() method returning a raw psycopg2 connection.
    Also exposes a server.connection_config property returning a dict with connection parameters
    """
    random_port = True

    def __init__(self, database_name="integration", skip_on_missing_postgres=False, **kwargs):
        self.database_name = database_name
        # TODO make skip configurable with a pytest flag
        self._fail = pytest.skip if skip_on_missing_postgres else pytest.exit
        super(PostgresServer, self).__init__(workspace=None, delete=True, preserve_sys_path=False, **kwargs)

    def kill(self, retries=5):
        if hasattr(self, 'pid'):
            try:
                os.kill(self.pid, self.kill_signal)
            except OSError as e:
                if e.errno == errno.ESRCH:  # "No such process"
                    pass
                else:
                    raise

    def pre_setup(self):
        """
        Find postgres server binary
        Set up connection parameters
        """
        (self.workspace / 'db').mkdir()  # pylint: disable=no-value-for-parameter

        try:
            self.pg_bin = subprocess.check_output([CONFIG.pg_config_executable, "--bindir"]).decode('utf-8').rstrip()
        except OSError as e:
            msg = "Failed to get pg_config --bindir: " + text_type(e)
            print(msg)
            self._fail(msg)
        initdb_path = self.pg_bin + '/initdb'
        if not os.path.exists(initdb_path):
            msg = "Unable to find pg binary specified by pg_config: {} is not a file".format(initdb_path)
            print(msg)
            self._fail(msg)
        try:
            subprocess.check_call([initdb_path, str(self.workspace / 'db')])
        except OSError as e:
            msg = "Failed to launch postgres: " + text_type(e)
            print(msg)
            self._fail(msg)

    @property
    def connection_config(self):
        return {
            u'host': u'localhost',
            u'user': os.environ[u'USER'],
            u'port': self.port,
            u'database': self.database_name
        }

    @property
    def run_cmd(self):
        cmd = [
            self.pg_bin + '/postgres',
            '-F',
            '-k', str(self.workspace / 'db'),
            '-D', str(self.workspace / 'db'),
            '-p', str(self.port),
            '-c', "log_min_messages=FATAL"
        ]  # yapf: disable
        return cmd

    def check_server_up(self):
        from psycopg2 import OperationalError
        conn = None
        try:
            print("Connecting to Postgres at localhost:{}".format(self.port))
            conn = self.connect('postgres')
            conn.set_session(autocommit=True)
            with conn.cursor() as cursor:
                cursor.execute("CREATE DATABASE " + self.database_name)
            self.connection = self.connect(self.database_name)
            with open(self.workspace / 'db' / 'postmaster.pid', 'r') as f:
                self.pid = int(f.readline().rstrip())
            return True
        except OperationalError as e:
            print("Could not connect to test postgres: {}".format(e))
        finally:
            if conn:
                conn.close()
        return False

    def connect(self, database=None):
        import psycopg2
        cfg = self.connection_config
        if database is not None:
            cfg[u'database'] = database
        return psycopg2.connect(**cfg)
