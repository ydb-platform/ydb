import logging

import pytest
from pytest_server_fixtures import CONFIG
from pytest_fixture_config import yield_requires_config

from .base2 import TestServerV2

log = logging.getLogger(__name__)


def _mongo_server():
    """ This does the actual work - there are several versions of this used
        with different scopes.
    """
    test_server = MongoTestServer()
    try:
        test_server.start()
        yield test_server
    finally:
        test_server.teardown()


@pytest.yield_fixture(scope='function')
@yield_requires_config(CONFIG, ['mongo_bin'])
def mongo_server():
    """ Function-scoped MongoDB server started in a local thread.
        This also provides a temp workspace.
        We tear down, and cleanup mongos at the end of the test.

        For completeness, we tidy up any outstanding mongo temp directories
        at the start and end of each test session

        Attributes
        ----------
        api (`pymongo.MongoClient`)  : PyMongo Client API connected to this server
        .. also inherits all attributes from the `workspace` fixture
    """
    for server in _mongo_server():
        yield server


@pytest.yield_fixture(scope='session')
@yield_requires_config(CONFIG, ['mongo_bin'])
def mongo_server_sess():
    """ Same as mongo_server fixture, scoped as session instead.
    """
    for server in _mongo_server():
        yield server


@pytest.yield_fixture(scope='class')
@yield_requires_config(CONFIG, ['mongo_bin'])
def mongo_server_cls(request):
    """ Same as mongo_server fixture, scoped for test classes.
    """
    for server in _mongo_server():
        request.cls.mongo_server = server
        yield server


@pytest.yield_fixture(scope='module')
@yield_requires_config(CONFIG, ['mongo_bin'])
def mongo_server_module():
    """ Same as mongo_server fixture, scoped for test modules.
    """
    for server in _mongo_server():
        yield server


class MongoTestServer(TestServerV2):

    def __init__(self, delete=True, **kwargs):
        super(MongoTestServer, self).__init__(delete=delete, **kwargs)
        self._port = self._get_port(27017)
        self.api = None

    @property
    def cmd(self):
        return 'mongod'

    @property
    def cmd_local(self):
        return CONFIG.mongo_bin

    def get_args(self, **kwargs):
        cmd = [
            '--bind_ip=%s' % self._listen_hostname,
            '--port=%s' % self.port,
            '--nounixsocket',
            '--syncdelay=0',
            '--nojournal',
            '--quiet',
        ]

        if 'workspace' in kwargs:
            cmd.append('--dbpath=%s' % str(kwargs['workspace']))

        return cmd

    @property
    def image(self):
        return CONFIG.mongo_image

    @property
    def port(self):
        return self._port

    def check_server_up(self):
        """Test connection to the server."""
        import pymongo
        from pymongo.errors import AutoReconnect, ConnectionFailure

        # Hostname must exist before continuing
        # Some server class (e.g. Docker) will only allocate an IP after the
        # container has started.
        if not self.hostname:
            return False

        log.info("Connecting to Mongo at %s:%s" % (self.hostname, self.port))
        try:
            with pymongo.MongoClient(self.hostname, self.port, serverselectiontimeoutms=200) as initial_api:
                initial_api.list_database_names()

            # Configure the client with default timeouts in case the server goes slow
            self.api = pymongo.MongoClient(self.hostname, self.port)
            return True
        except (AutoReconnect, ConnectionFailure) as e:
            pass
        return False

    def teardown(self):
        if self.api:
            self.api.close()
            self.api = None
        super(MongoTestServer, self).teardown()
