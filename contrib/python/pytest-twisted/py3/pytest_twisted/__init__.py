import functools
import inspect
import itertools
import signal
import sys
import threading
import warnings

import decorator
import greenlet
import pytest

from twisted.internet import defer, error
from twisted.internet.threads import blockingCallFromThread
from twisted.python import failure

if sys.version_info[0] == 3:
    from pytest_twisted.three import (
        _async_pytest_fixture_setup,
        _async_pytest_pyfunc_call,
    )
elif sys.version_info[0] == 2:
    from pytest_twisted.two import _async_pytest_pyfunc_call


class WrongReactorAlreadyInstalledError(Exception):
    pass


class UnrecognizedCoroutineMarkError(Exception):
    @classmethod
    def from_mark(cls, mark):
        return cls(
            'Coroutine wrapper mark not recognized: {}'.format(repr(mark)),
        )


class AsyncGeneratorFixtureDidNotStopError(Exception):
    @classmethod
    def from_generator(cls, generator):
        return cls(
            'async fixture did not stop: {}'.format(generator),
        )


class AsyncFixtureUnsupportedScopeError(Exception):
    @classmethod
    def from_scope(cls, scope):
        return cls(
            'Unsupported scope {0!r} used for async fixture'.format(scope)
        )


class _config:
    external_reactor = False


class _instances:
    gr_twisted = None
    reactor = None


def _deprecate(deprecated, recommended):
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            warnings.warn(
                '{deprecated} has been deprecated, use {recommended}'.format(
                    deprecated=deprecated,
                    recommended=recommended,
                ),
                DeprecationWarning,
                stacklevel=2,
            )
            return f(*args, **kwargs)

        return wrapper

    return decorator


def blockon(d):
    if _config.external_reactor:
        return block_from_thread(d)

    return blockon_default(d)


def blockon_default(d):
    current = greenlet.getcurrent()
    assert (
        current is not _instances.gr_twisted
    ), "blockon cannot be called from the twisted greenlet"
    result = []

    def cb(r):
        result.append(r)
        if greenlet.getcurrent() is not current:
            current.switch(result)

    d.addCallbacks(cb, cb)
    if not result:
        _result = _instances.gr_twisted.switch()
        assert _result is result, "illegal switch in blockon"

    if isinstance(result[0], failure.Failure):
        result[0].raiseException()

    return result[0]


def block_from_thread(d):
    return blockingCallFromThread(_instances.reactor, lambda x: x, d)


def decorator_apply(dec, func):
    """
    Decorate a function by preserving the signature even if dec
    is not a signature-preserving decorator.

    https://github.com/micheles/decorator/blob/55a68b5ef1951614c5c37a6d201b1f3b804dbce6/docs/documentation.md#dealing-with-third-party-decorators
    """
    return decorator.FunctionMaker.create(
        func, 'return decfunc(%(signature)s)',
        dict(decfunc=dec(func)), __wrapped__=func)


class DecoratorArgumentsError(Exception):
    pass


def repr_args_kwargs(*args, **kwargs):
    arguments = ', '.join(itertools.chain(
        (repr(x) for x in args),
        ('{}={}'.format(k, repr(v)) for k, v in kwargs.items())
    ))

    return '({})'.format(arguments)


def _positional_not_allowed_exception(*args, **kwargs):
    arguments = repr_args_kwargs(*args, **kwargs)

    return DecoratorArgumentsError(
        'Positional decorator arguments not allowed: {}'.format(arguments),
    )


def _optional_arguments():
    def decorator_decorator(d):
        # TODO: this should get the signature of d minus the f or something
        def decorator_wrapper(*args, **decorator_arguments):
            """this is decorator_wrapper"""
            if len(args) > 1:
                raise _positional_not_allowed_exception()

            if len(args) == 1:
                maybe_f = args[0]

                if len(decorator_arguments) > 0 or not callable(maybe_f):
                    raise _positional_not_allowed_exception()

                f = maybe_f
                return d(f)

            # TODO: this should get the signature of d minus the kwargs
            def decorator_closure_on_arguments(f):
                return d(f, **decorator_arguments)

            return decorator_closure_on_arguments

        return decorator_wrapper

    return decorator_decorator


@_optional_arguments()
def inlineCallbacks(f):
    """
    Mark as inline callbacks test for pytest-twisted processing and apply
    @inlineCallbacks.

    Unlike @ensureDeferred, @inlineCallbacks can be applied here because it
    does not call nor schedule the test function.  Further, @inlineCallbacks
    must be applied here otherwise pytest identifies the test as a 'yield test'
    for which they dropped support in 4.0 and now they skip.
    """
    decorated = decorator_apply(defer.inlineCallbacks, f)
    _set_mark(o=decorated, mark='inline_callbacks_test')

    return decorated


@_optional_arguments()
def ensureDeferred(f):
    """
    Mark as async test for pytest-twisted processing.

    Unlike @inlineCallbacks, @ensureDeferred must not be applied here since it
    would call and schedule the test function.
    """
    _set_mark(o=f, mark='async_test')

    return f


def init_twisted_greenlet():
    if _instances.reactor is None or _instances.gr_twisted:
        return

    if not _instances.reactor.running:
        if not isinstance(threading.current_thread(), threading._MainThread):
            warnings.warn(
                (
                    'Will not attempt to block Twisted signal configuration'
                    ' since we are not running in the main thread.  See'
                    ' https://github.com/pytest-dev/pytest-twisted/issues/153.'
                ),
                RuntimeWarning,
            )
        elif signal.getsignal(signal.SIGINT) == signal.default_int_handler:
            signal.signal(
                signal.SIGINT,
                functools.partial(signal.default_int_handler),
            )
        _instances.gr_twisted = greenlet.greenlet(_instances.reactor.run)
        # give me better tracebacks:
        failure.Failure.cleanFailure = lambda self: None
    else:
        _config.external_reactor = True


def stop_twisted_greenlet():
    if _instances.gr_twisted:
        try:
            _instances.reactor.stop()
        except error.ReactorNotRunning:
            # Sometimes the reactor is stopped before we get here.  For
            # example, this can happen in response to a SIGINT in some cases.
            pass
        _instances.gr_twisted.switch()


def _get_mark(o, default=None):
    """Get the pytest-twisted test or fixture mark."""
    return getattr(o, _mark_attribute_name, default)


def _set_mark(o, mark):
    """Set the pytest-twisted test or fixture mark."""
    setattr(o, _mark_attribute_name, mark)


def _marked_async_fixture(mark):
    @functools.wraps(pytest.fixture)
    @_optional_arguments()
    def fixture(f, *args, **kwargs):
        try:
            scope = args[0]
        except IndexError:
            scope = kwargs.get('scope', 'function')

        if scope not in ['function', 'module']:
            # TODO: handle...
            #       - class
            #       - package
            #       - session
            #       - dynamic
            #
            #       https://docs.pytest.org/en/latest/reference.html#pytest-fixture-api
            #       then remove this and update docs, or maybe keep it around
            #       in case new options come in without support?
            #
            #       https://github.com/pytest-dev/pytest-twisted/issues/56
            raise AsyncFixtureUnsupportedScopeError.from_scope(scope=scope)

        _set_mark(f, mark)
        result = pytest.fixture(*args, **kwargs)(f)

        return result

    return fixture


_mark_attribute_name = '_pytest_twisted_mark'
async_fixture = _marked_async_fixture('async_fixture')
async_yield_fixture = _marked_async_fixture('async_yield_fixture')


def pytest_fixture_setup(fixturedef, request):
    """Interface pytest to async for async and async yield fixtures."""
    # TODO: what about _adding_ inlineCallbacks fixture support?
    maybe_mark = _get_mark(fixturedef.func)
    if maybe_mark is None:
        return None

    mark = maybe_mark

    _run_inline_callbacks(
        _async_pytest_fixture_setup,
        fixturedef,
        request,
        mark,
    )

    return not None


def _create_async_yield_fixture_finalizer(coroutine):
    def finalizer():
        _run_inline_callbacks(
            _tear_it_down,
            defer.ensureDeferred(coroutine.__anext__()),
        )

    return finalizer


@defer.inlineCallbacks
def _tear_it_down(deferred):
    """Tear down a specific async yield fixture."""
    try:
        yield deferred
    except StopAsyncIteration:
        return

    # TODO: six.raise_from()
    raise AsyncGeneratorFixtureDidNotStopError.from_generator(
        generator=deferred,
    )


def _run_inline_callbacks(f, *args):
    """Interface into Twisted greenlet to run and wait for a deferred."""
    if _instances.gr_twisted is not None:
        if _instances.gr_twisted.dead:
            raise RuntimeError("twisted reactor has stopped")

        def in_reactor(d, f, *args):
            return defer.maybeDeferred(f, *args).chainDeferred(d)

        d = defer.Deferred()
        _instances.reactor.callLater(0.0, in_reactor, d, f, *args)
        blockon_default(d)
    else:
        if not _instances.reactor.running:
            raise RuntimeError("twisted reactor is not running")
        blockingCallFromThread(_instances.reactor, f, *args)


@pytest.mark.tryfirst
def pytest_pyfunc_call(pyfuncitem):
    """Interface to async test call handler."""
    # TODO: only handle 'our' tests?  what is the point of handling others?
    #       well, because our interface allowed people to return deferreds
    #       from arbitrary tests so we kinda have to keep this up for now
    maybe_hypothesis = getattr(pyfuncitem.obj, "hypothesis", None)
    if maybe_hypothesis is None:
        _run_inline_callbacks(
            _async_pytest_pyfunc_call,
            pyfuncitem,
            pyfuncitem.obj,
            {}
        )
        result = not None
    else:
        hypothesis = maybe_hypothesis
        f = hypothesis.inner_test

        def inner_test(**kwargs):
            return _run_inline_callbacks(
                _async_pytest_pyfunc_call,
                pyfuncitem,
                f,
                kwargs,
            )

        pyfuncitem.obj.hypothesis.inner_test = inner_test
        result = None

    return result


@pytest.fixture(scope="session", autouse=True)
def twisted_greenlet():
    """Provide the twisted greenlet in fixture form."""
    return _instances.gr_twisted


def init_default_reactor():
    """Install the default Twisted reactor."""
    import twisted.internet.default

    module = inspect.getmodule(twisted.internet.default.install)

    module_name = module.__name__.split(".")[-1]
    reactor_type_name, = (x for x in dir(module) if x.lower() == module_name)
    reactor_type = getattr(module, reactor_type_name)

    _install_reactor(
        reactor_installer=twisted.internet.default.install,
        reactor_type=reactor_type,
    )


def init_qt5_reactor():
    """Install the qt5reactor...  reactor."""
    import qt5reactor

    _install_reactor(
        reactor_installer=qt5reactor.install, reactor_type=qt5reactor.QtReactor
    )


def init_asyncio_reactor():
    """Install the Twisted reactor for asyncio."""
    from twisted.internet import asyncioreactor

    _install_reactor(
        reactor_installer=asyncioreactor.install,
        reactor_type=asyncioreactor.AsyncioSelectorReactor,
    )


reactor_installers = {
    "default": init_default_reactor,
    "qt5reactor": init_qt5_reactor,
    "asyncio": init_asyncio_reactor,
}


def _install_reactor(reactor_installer, reactor_type):
    """Install the specified reactor and create the greenlet."""
    try:
        reactor_installer()
    except error.ReactorAlreadyInstalledError:
        import twisted.internet.reactor

        if not isinstance(twisted.internet.reactor, reactor_type):
            raise WrongReactorAlreadyInstalledError(
                "expected {} but found {}".format(
                    reactor_type, type(twisted.internet.reactor)
                )
            )

    import twisted.internet.reactor

    _instances.reactor = twisted.internet.reactor
    init_twisted_greenlet()


def pytest_addoption(parser):
    """Add options into the pytest CLI."""
    group = parser.getgroup("twisted")
    group.addoption(
        "--reactor",
        default="default",
        choices=tuple(reactor_installers.keys()),
    )


def pytest_configure(config):
    """Identify and install chosen reactor."""
    pytest.inlineCallbacks = _deprecate(
        deprecated='pytest.inlineCallbacks',
        recommended='pytest_twisted.inlineCallbacks',
    )(inlineCallbacks)
    pytest.blockon = _deprecate(
        deprecated='pytest.blockon',
        recommended='pytest_twisted.blockon',
    )(blockon)

    reactor_installers[config.getoption("reactor")]()


def pytest_unconfigure(config):
    """Stop the reactor greenlet."""
    stop_twisted_greenlet()


def _use_asyncio_selector_if_required(config):
    """Set asyncio selector event loop policy if needed."""
    # https://twistedmatrix.com/trac/ticket/9766
    # https://github.com/pytest-dev/pytest-twisted/issues/80

    is_asyncio = config.getoption("reactor", "default") == "asyncio"

    if is_asyncio and sys.platform == 'win32' and sys.version_info >= (3, 8):
        import asyncio

        selector_policy = asyncio.WindowsSelectorEventLoopPolicy()
        asyncio.set_event_loop_policy(selector_policy)
