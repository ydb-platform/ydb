from typing import Dict

from .. import uwsgi


class _Request:
    """Current request information."""

    __slots__ = []

    @property
    def env(self) -> Dict[str, str]:
        """Request environment dictionary."""
        return uwsgi.env

    @property
    def id(self) -> int:
        """Returns current request number (handled by worker on core)."""
        return uwsgi.request_id()

    @property
    def total_count(self) -> int:
        """Returns the total number of requests managed so far by the pool of uWSGI workers."""
        return uwsgi.total_requests()

    @property
    def fd(self) -> int:
        """Returns current request file descriptor."""
        return uwsgi.connection_fd()

    @property
    def content_length(self) -> int:
        """Returns current post content length."""
        return uwsgi.cl()

    def log(self):
        """Instructs uWSGI to log current request data."""
        uwsgi.log_this_request()

    def add_var(self, name: str, value: str) -> bool:
        """Registers custom request variable.

        Can be used for better integration with the internal routing subsystem.

        :param name:

        :param value:

        :raises ValueError: If buffer size is not enough.

        """
        return uwsgi.add_var(name, value)
