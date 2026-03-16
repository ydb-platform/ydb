import collections.abc
import typing

import pytest

from testsuite import types
from testsuite.mockserver import server
from testsuite.utils import callinfo, http

TestpointHandler = typing.Callable[
    [types.JsonAnyOptional],
    types.MaybeAsyncResult[types.JsonAnyOptional],
]
TestpointDecorator = typing.Callable[
    [TestpointHandler],
    callinfo.AsyncCallQueue,
]


class TestpointFixture(collections.abc.MutableMapping):
    """Testpoint control object."""

    def __init__(self, *, checker_factory) -> None:
        self._handlers: dict[str, callinfo.AsyncCallQueue] = {}
        self._checker_factory = checker_factory

    def __getitem__(self, name: str) -> callinfo.AsyncCallQueue:
        return self._handlers[name]

    def __setitem__(self, key: str, value: callinfo.AsyncCallQueue):
        self._handlers[key] = value

    def __delitem__(self, key):
        if isinstance(key, callinfo.AsyncCallQueue):
            names = [
                name for name, value in self._handlers.items() if value == key
            ]
            if not names:
                raise KeyError(f'{key!r}')
            for name in names:
                del self._handlers[name]
        else:
            del self._handlers[key]

    def __len__(self):
        return len(self._handlers)

    def __iter__(self):
        return iter(self._handlers)

    def __call__(self, name: str) -> TestpointDecorator:
        """Returns decorator for registering testpoint called ``name``.

        After decoration function is wrapped with `AsyncCallQueue`_.
        """

        checker = self._checker_factory(name)

        def decorator(func) -> callinfo.AsyncCallQueue:
            wrapped = callinfo.acallqueue(func, checker=checker)
            self[name] = wrapped
            return wrapped

        return decorator


@pytest.fixture(scope='session')
def testpoint_checker_factory():
    """Testpoint checker factory fixture.

    Can be used to control whether or not testpoint is valid.
    Feel free to override, e.g.:

    .. code-block::

       @pytest.fixture
       def testpoint_checker_factory(testpoint_enabled)
           def create_checker(name):
               def checker(opname):
                   if testpoint_enabled(name):
                       return
                   pytest.fail(
                       f'{opname}() called on disabled testpoint {name}'
                   )
           return create_checker
    """

    def create_checker(name):
        return None

    return create_checker


@pytest.fixture
async def testpoint(
    mockserver: server.MockserverFixture,
    testpoint_checker_factory,
) -> TestpointFixture:
    """Testpoint fixture returns testpoint session instance that works
    as decorator that registers testpoint handler. Original function is
    wrapped with :ref:`AsyncCallQueue`

    :param name: testpoint name
    :returns: decorator

    .. code-block::

       def test_foo(testpoint):
           @testpoint('foo'):
           def testpoint_handler(data):
               pass

           ...
           # testpoint_handler is AsyncCallQueue instance, e.g.:
           assert testpoint_handler.has_calls
           assert testpoint_handler.next_call == {...}
           aseert testpoint_handler.wait_call() == {...}
    """

    session = TestpointFixture(checker_factory=testpoint_checker_factory)

    @mockserver.json_handler('/testpoint')
    async def _handler(request: http.Request):
        body = request.json
        handler = session.get(body['name'])
        if handler is None:
            return {'data': None, 'handled': False}
        data = await handler(body['data'])
        return {'data': data, 'handled': True}

    return session
