"""
This module defines an :py:class:`aiohttp.ClientSession` adapter that
returns :py:class:`twisted.internet.defer.Deferred` responses.
"""

# Third party imports
try:
    from twisted.internet import threads
except ImportError:  # pragma: no cover
    threads = None

# Local imports
from uplink.clients import interfaces, io, register


class TwistedClient(interfaces.HttpClientAdapter):
    """
    Client that returns [`twisted.internet.defer.Deferred`][twisted.internet.defer.Deferred]
    responses.

    Note:
        This client is an optional feature and requires the [`twisted`][twisted]
        package. For example, here's how to install this extra using pip:

        ```bash
        $ pip install uplink[twisted]
        ```

    Args:
        session ([`requests.Session`][requests.Session], optional): The session
            that should handle sending requests. If this argument is
            omitted or set to `None`, a new session will be
            created.
    """

    def __init__(self, session=None):
        if threads is None:
            raise NotImplementedError("twisted is not installed.")
        self._proxy = register.get_client(session)

    @property
    def exceptions(self):
        return self._proxy.exceptions

    @staticmethod
    def io():
        return io.TwistedStrategy()

    def apply_callback(self, callback, response):
        return threads.deferToThread(self._proxy.apply_callback, callback, response)

    def send(self, request):
        return threads.deferToThread(self._proxy.send, request)
