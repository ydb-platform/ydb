# Local imports
from uplink.clients import exceptions, io


class HttpClientAdapter(io.Client):
    """An adapter of an HTTP client library."""

    __exceptions = exceptions.Exceptions()

    def io(self):
        """Returns the execution strategy for this client."""
        raise NotImplementedError

    @property
    def exceptions(self):
        """
        uplink.clients.exceptions.Exceptions: An enum of standard HTTP
        client errors that have been mapped to client specific
        exceptions.
        """
        return self.__exceptions

    def send(self, request):
        raise NotImplementedError

    def apply_callback(self, callback, response):
        raise NotImplementedError
