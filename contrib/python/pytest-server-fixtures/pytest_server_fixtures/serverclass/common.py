"""
Common utils for all serverclasses
"""
import os
import threading

from pytest_server_fixtures import CONFIG
from pytest_server_fixtures.util import get_random_id

SERVER_ID_LEN = 8

def merge_dicts(x, y):
    """Given two dicts, merge them into a new dict as a shallow copy."""
    z = x.copy()
    z.update(y)
    return z


def is_debug():
    return 'DEBUG' in os.environ and os.environ['DEBUG'] == '1'


class ServerFixtureNotRunningException(Exception):
    """Thrown when a kubernetes pod is not in running state."""
    pass


class ServerFixtureNotTerminatedException(Exception):
    """Thrown when a kubernetes pod is still running."""
    pass


class ServerClass(threading.Thread):
    """Example interface for ServerClass."""

    def __init__(self,
                 cmd,
                 get_args,
                 env):
        """
        Initialise the server class.
        Server fixture will be started here.
        """
        super(ServerClass, self).__init__()

        # set serverclass thread to a daemon thread
        self.daemon = True

        self._id = get_random_id(SERVER_ID_LEN)
        self._cmd = cmd
        self._get_args = get_args
        self._env = env or {}

    def run(self):
        """In a new thread, wait for the server to return."""
        raise NotImplementedError("Concrete class should implement this")

    def launch(self):
        """Start the server."""
        raise NotImplementedError("Concrete class should implement this")

    def teardown(self):
        """Kill the server."""
        raise NotImplementedError("Concrete class should implement this")

    @property
    def is_running(self):
        """Tell if the server is running."""
        raise NotImplementedError("Concrete class should implement this")

    @property
    def hostname(self):
        """Get server's hostname."""
        raise NotImplementedError("Concrete class should implement this")

    @property
    def name(self):
        return "server-fixtures-%s-%s" % (CONFIG.session_id, self._id)

