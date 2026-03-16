"""
This module defines an :py:class:`aiohttp.ClientSession` adapter
that returns awaitable responses.
"""
# Standard library imports
import asyncio
import collections
import inspect
import threading
from concurrent import futures

# Third-party imports
try:
    import aiohttp
except ImportError:  # pragma: no cover
    aiohttp = None

# Local imports
from uplink.clients import exceptions, io, interfaces, register


def threaded_callback(callback):
    async def new_callback(response):
        if isinstance(response, aiohttp.ClientResponse):
            await response.text()
            response = ThreadedResponse(response)
        response = callback(response)
        if isinstance(response, ThreadedResponse):
            return response.unwrap()
        else:
            return response

    return new_callback


class AiohttpClient(interfaces.HttpClientAdapter):
    """
    An :py:mod:`aiohttp` client that creates awaitable responses.

    Note:
        This client is an optional feature and requires the :py:mod:`aiohttp`
        package. For example, here's how to install this extra using pip::

            $ pip install uplink[aiohttp]

    Args:
        session (:py:class:`aiohttp.ClientSession`, optional):
            The session that should handle sending requests. If this
            argument is omitted or set to :py:obj:`None`, a new session
            will be created.
    """

    exceptions = exceptions.Exceptions()

    # TODO: Update docstrings to include aiohttp constructor parameters.

    __ARG_SPEC = collections.namedtuple("__ARG_SPEC", "args kwargs")

    def __init__(self, session=None, **kwargs):
        if aiohttp is None:
            raise NotImplementedError("aiohttp is not installed.")
        self._auto_created_session = False
        if session is None:
            session = self._create_session(**kwargs)
        self._session = session
        self._sync_callback_adapter = threaded_callback

    def __del__(self):
        # TODO: Consider replacing this with a close method
        if self._auto_created_session:
            # aiohttp v3.0 has made ClientSession.close a coroutine,
            # so we check whether it is one here and register it
            # to run appropriately at exit
            if asyncio.iscoroutinefunction(self._session.close):
                asyncio.get_event_loop().run_until_complete(
                    self._session.close()
                )
            else:
                self._session.close()

    async def session(self):
        """Returns the underlying `aiohttp.ClientSession`."""
        if isinstance(self._session, self.__ARG_SPEC):
            args, kwargs = self._session
            self._session = aiohttp.ClientSession(*args, **kwargs)
            self._auto_created_session = True
        return self._session

    def wrap_callback(self, callback):
        func = inspect.unwrap(callback)
        if not asyncio.iscoroutinefunction(func):
            callback = self._sync_callback_adapter(callback)
        return callback

    @staticmethod
    @register.handler
    def with_session(session, *args, **kwargs):
        """
        Builds a client instance if the first argument is a
        :py:class:`aiohttp.ClientSession`. Otherwise, return :py:obj:`None`.
        """
        if isinstance(session, aiohttp.ClientSession):
            return AiohttpClient(session, *args, **kwargs)

    @classmethod
    def _create_session(cls, *args, **kwargs):
        return cls.__ARG_SPEC(args, kwargs)

    @classmethod
    def create(cls, *args, **kwargs):
        """
        Builds a client instance with
        :py:class:`aiohttp.ClientSession` arguments.

        Instead of directly initializing this class with a
        :py:class:`aiohttp.ClientSession`, use this method to have the
        client lazily construct a session when sending the first
        request. Hence, this method guarantees that the creation of the
        underlying session happens inside of a coroutine.

        Args:
            *args: positional arguments that
                :py:class:`aiohttp.ClientSession` takes.
            **kwargs: keyword arguments that
                :py:class:`aiohttp.ClientSession` takes.
        """
        session_build_args = cls._create_session(*args, **kwargs)
        return AiohttpClient(session=session_build_args)

    async def send(self, request):
        method, url, extras = request
        session = await self.session()
        response = await session.request(method, url, **extras)

        # Make `aiohttp` response "quack" like a `requests` response
        response.status_code = response.status

        return response

    def apply_callback(self, callback, response):
        return self.wrap_callback(callback)(response)

    @staticmethod
    def io():
        return io.AsyncioStrategy()


class ThreadedCoroutine(object):
    def __init__(self, coroutine):
        self.__coroutine = coroutine

    def __call__(self, *args, **kwargs):
        with AsyncioExecutor() as executor:
            future = executor.submit(self.__coroutine, *args, **kwargs)
            result = future.result()
        return result


class ThreadedResponse(object):
    def __init__(self, response):
        self.__response = response

    def __getattr__(self, item):
        value = getattr(self.__response, item)
        if asyncio.iscoroutinefunction(value):
            return ThreadedCoroutine(value)
        return value

    def unwrap(self):
        return self.__response


class AsyncioExecutor(futures.Executor):
    """
    Executor that runs asyncio coroutines in a shadow thread.

    Credit to Vincent Michel, who wrote the original implementation:
    https://gist.github.com/vxgmichel/d16e66d1107a369877f6ef7e646ac2e5
    """

    def __init__(self):
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._target)
        self._thread.start()

    def _target(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def submit(self, fn, *args, **kwargs):
        coro = fn(*args, **kwargs)
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def shutdown(self, wait=True):
        self._loop.call_soon_threadsafe(self._loop.stop)
        if wait:  # pragma: no cover
            self._thread.join()


# === Register client exceptions === #
if aiohttp is not None:  # pragma: no cover
    AiohttpClient.exceptions.BaseClientException = aiohttp.ClientError
    AiohttpClient.exceptions.ConnectionError = aiohttp.ClientConnectionError
    AiohttpClient.exceptions.ConnectionTimeout = aiohttp.ClientConnectorError
    AiohttpClient.exceptions.ServerTimeout = aiohttp.ServerTimeoutError
    AiohttpClient.exceptions.SSLError = aiohttp.ClientSSLError
    AiohttpClient.exceptions.InvalidURL = aiohttp.InvalidURL
