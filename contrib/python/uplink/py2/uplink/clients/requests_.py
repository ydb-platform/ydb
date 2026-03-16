# Standard library imports

# Third party imports
import requests

# Local imports
from uplink.clients import exceptions, io, interfaces, register


class RequestsClient(interfaces.HttpClientAdapter):
    """
    A :py:mod:`requests` client that returns
    :py:class:`requests.Response` responses.

    Args:
        session (:py:class:`requests.Session`, optional): The session
            that should handle sending requests. If this argument is
            omitted or set to :py:obj:`None`, a new session will be
            created.
    """

    exceptions = exceptions.Exceptions()

    def __init__(self, session=None, **kwargs):
        self.__auto_created_session = False
        if session is None:
            session = self._create_session(**kwargs)
            self.__auto_created_session = True
        self.__session = session

    def __del__(self):
        if self.__auto_created_session:
            self.__session.close()

    @staticmethod
    @register.handler
    def with_session(session, *args, **kwargs):
        if isinstance(session, requests.Session):
            return RequestsClient(session, *args, **kwargs)

    @staticmethod
    def _create_session(**kwargs):
        session = requests.Session()
        for key in kwargs:
            setattr(session, key, kwargs[key])
        return session

    def send(self, request):
        method, url, extras = request
        return self.__session.request(method=method, url=url, **extras)

    def apply_callback(self, callback, response):
        return callback(response)

    @staticmethod
    def io():
        return io.BlockingStrategy()


# === Register client exceptions === #
RequestsClient.exceptions.BaseClientException = requests.RequestException
RequestsClient.exceptions.ConnectionError = requests.ConnectionError
RequestsClient.exceptions.ConnectionTimeout = requests.ConnectTimeout
RequestsClient.exceptions.ServerTimeout = requests.ReadTimeout
RequestsClient.exceptions.SSLError = requests.exceptions.SSLError
RequestsClient.exceptions.InvalidURL = requests.exceptions.InvalidURL
