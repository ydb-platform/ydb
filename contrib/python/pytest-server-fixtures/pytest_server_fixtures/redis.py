'''
Created on 25 Apr 2012

@author: eeaston

'''
from __future__ import absolute_import
import socket

import pytest

from pytest_server_fixtures import CONFIG
from pytest_fixture_config import requires_config

from .base2 import TestServerV2


def _redis_server(request):
    """ Does the redis server work, this is used within different scoped
        fixtures.
    """
    test_server = RedisTestServer()
    request.addfinalizer(lambda p=test_server: p.teardown())
    test_server.start()
    return test_server


@pytest.fixture(scope='function')
@requires_config(CONFIG, ['redis_executable'])
def redis_server(request):
    """ Function-scoped Redis server in a local thread.

        Attributes
        ----------
        api: (``redis.Redis``)   Redis client API connected to this server
        .. also inherits all attributes from the `workspace` fixture
    """
    return _redis_server(request)


@pytest.fixture(scope='session')
@requires_config(CONFIG, ['redis_executable'])
def redis_server_sess(request):
    """ Same as redis_server fixture, scoped for test session
    """
    return _redis_server(request)


class RedisTestServer(TestServerV2):
    """This will look for 'redis_executable' in configuration and use as the
    redis-server to run.
    """

    def __init__(self, db=0, delete=True, **kwargs):
        global redis
        import redis

        super(RedisTestServer, self).__init__(delete=delete, **kwargs)
        self.db = db
        self._api = None
        self._port = self._get_port(6379)

    @property
    def api(self):
        if not self.hostname:
            raise "Redis not ready"
        if not self._api:
            self._api = redis.Redis(host=self.hostname, port=self.port, db=self.db)
        return self._api

    @property
    def cmd(self):
        return "redis-server"

    @property
    def cmd_local(self):
        return CONFIG.redis_executable

    def get_args(self, **kwargs):
        cmd = [
            "--bind", self._listen_hostname,
            "--port", str(self.port),
            "--timeout", "0",
            "--loglevel", "notice",
            "--databases", "1",
            "--maxmemory", "2gb",
            "--maxmemory-policy", "noeviction",
            "--appendonly", "no",
            "--slowlog-log-slower-than", "-1",
            "--slowlog-max-len", "1024",
        ]

        return cmd

    @property
    def image(self):
        return CONFIG.redis_image

    @property
    def port(self):
        return self._port

    def check_server_up(self):
        """ Ping the server
        """
        print("pinging Redis at %s:%s db %s" % (
            self.hostname, self.port, self.db
        ))

        if not self.hostname:
            return False

        try:
            return self.api.ping()
        except redis.ConnectionError as e:
            print("server not up yet (%s)" % e)
            return False
