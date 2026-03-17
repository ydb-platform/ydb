__all__ = ['coroutine',
           'iscoroutinefunction', 'iscoroutine',
           'From', 'Return']

import functools
import inspect
import opcode
import os
import sys
import textwrap
import traceback
import types

from . import compat
from . import events
from . import futures
from .log import logger


# Opcode of "yield from" instruction
_YIELD_FROM = opcode.opmap.get('YIELD_FROM', None)

# If you set _DEBUG to true, @coroutine will wrap the resulting
# generator objects in a CoroWrapper instance (defined below).  That
# instance will log a message when the generator is never iterated
# over, which may happen when you forget to use "yield from" with a
# coroutine call.  Note that the value of the _DEBUG flag is taken
# when the decorator is used, so to be of any use it must be set
# before you define your coroutines.  A downside of using this feature
# is that tracebacks show entries for the CoroWrapper.__next__ method
# when _DEBUG is true.
_DEBUG = bool(os.environ.get('TROLLIUSDEBUG'))


try:
    _types_coroutine = types.coroutine
except AttributeError:
    _types_coroutine = None

try:
    _inspect_iscoroutinefunction = inspect.iscoroutinefunction
except AttributeError:
    _inspect_iscoroutinefunction = lambda func: False

try:
    from collections.abc import Coroutine as _CoroutineABC, \
                                Awaitable as _AwaitableABC
except ImportError:
    _CoroutineABC = _AwaitableABC = None


if _YIELD_FROM is not None:
    # Check for CPython issue #21209
    exec('''if 1:
        def has_yield_from_bug():
            class MyGen:
                def __init__(self):
                    self.send_args = None
                def __iter__(self):
                    return self
                def __next__(self):
                    return 42
                def send(self, *what):
                    self.send_args = what
                    return None
            def yield_from_gen(gen):
                yield from gen
            value = (1, 2, 3)
            gen = MyGen()
            coro = yield_from_gen(gen)
            next(coro)
            coro.send(value)
            return gen.send_args != (value,)
''')
    _YIELD_FROM_BUG = has_yield_from_bug()
    del has_yield_from_bug
else:
    _YIELD_FROM_BUG = False


if compat.PY35:
    return_base_class = Exception
else:
    return_base_class = StopIteration

class ReturnException(return_base_class):
    def __init__(self, *args):
        return_base_class.__init__(self)
        if not args:
            self.value = None
        elif len(args) == 1:
            self.value = args[0]
        else:
            self.value = args
        self.raised = False
        if _DEBUG:
            frame = sys._getframe(1)
            self._source_traceback = traceback.extract_stack(frame)
            # explicitly clear the reference to avoid reference cycles
            frame = None
        else:
            self._source_traceback = None

    def __del__(self):
        if self.raised:
            return

        fmt = 'Return(%r) used without raise'
        if self._source_traceback:
            fmt += '\nReturn created at (most recent call last):\n'
            tb = ''.join(traceback.format_list(self._source_traceback))
            fmt += tb.rstrip()
        logger.error(fmt, self.value)


if compat.PY33 and not compat.PY35:
    # Don't use the Return class on Python 3.3 and 3.4 to support asyncio
    # coroutines (to avoid the warning emited in Return destructor).
    #
    # The problem is that ReturnException inherits from StopIteration.
    # "yield from trollius_coroutine". Task._step() does not receive the Return
    # exception, because "yield from" handles it internally. So it's not
    # possible to set the raised attribute to True to avoid the warning in
    # Return destructor.
    def Return(*args):
        if not args:
            value = None
        elif len(args) == 1:
            value = args[0]
        else:
            value = args
        return StopIteration(value)
else:
    Return = ReturnException


def debug_wrapper(gen):
    # This function is called from 'sys.set_coroutine_wrapper'.
    # We only wrap here coroutines defined via 'async def' syntax.
    # Generator-based coroutines are wrapped in @coroutine
    # decorator.
    return CoroWrapper(gen, None)


def _coroutine_at_yield_from(coro):
    """Test if the last instruction of a coroutine is "yield from".

    Return False if the coroutine completed.
    """
    frame = coro.gi_frame
    if frame is None:
        return False
    code = coro.gi_code
    assert frame.f_lasti >= 0
    offset = frame.f_lasti + 1
    instr = code.co_code[offset]
    return (instr == _YIELD_FROM)


class CoroWrapper:
    # Wrapper for coroutine object in _DEBUG mode.

    def __init__(self, gen, func=None):
        assert inspect.isgenerator(gen) or inspect.iscoroutine(gen), gen
        self.gen = gen
        self.func = func # Used to unwrap @coroutine decorator
        self._source_traceback = traceback.extract_stack(sys._getframe(1))
        self.__name__ = getattr(gen, '__name__', None)
        self.__qualname__ = getattr(gen, '__qualname__', None)

    def __repr__(self):
        coro_repr = _format_coroutine(self)
        if self._source_traceback:
            frame = self._source_traceback[-1]
            coro_repr += ', created at %s:%s' % (frame[0], frame[1])
        return '<%s %s>' % (self.__class__.__name__, coro_repr)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.gen)
    next = __next__

    if _YIELD_FROM_BUG:
        # For for CPython issue #21209: using "yield from" and a custom
        # generator, generator.send(tuple) unpacks the tuple instead of passing
        # the tuple unchanged. Check if the caller is a generator using "yield
        # from" to decide if the parameter should be unpacked or not.
        def send(self, *value):
            frame = sys._getframe()
            caller = frame.f_back
            assert caller.f_lasti >= 0
            if caller.f_code.co_code[caller.f_lasti] != _YIELD_FROM:
                value = value[0]
            return self.gen.send(value)
    else:
        def send(self, value):
            return self.gen.send(value)

    def throw(self, exc_type, exc_value=None, exc_tb=None):
        return self.gen.throw(exc_type, exc_value, exc_tb)

    def close(self):
        return self.gen.close()

    @property
    def gi_frame(self):
        return self.gen.gi_frame

    @property
    def gi_running(self):
        return self.gen.gi_running

    @property
    def gi_code(self):
        return self.gen.gi_code

    if compat.PY35:

        __await__ = __iter__ # make compatible with 'await' expression

        @property
        def gi_yieldfrom(self):
            return self.gen.gi_yieldfrom

        @property
        def cr_await(self):
            return self.gen.cr_await

        @property
        def cr_running(self):
            return self.gen.cr_running

        @property
        def cr_code(self):
            return self.gen.cr_code

        @property
        def cr_frame(self):
            return self.gen.cr_frame

    def __del__(self):
        # Be careful accessing self.gen.frame -- self.gen might not exist.
        gen = getattr(self, 'gen', None)
        frame = getattr(gen, 'gi_frame', None)
        if frame is None:
            frame = getattr(gen, 'cr_frame', None)
        if frame is not None and frame.f_lasti == -1:
            msg = '%r was never yielded from' % self
            tb = getattr(self, '_source_traceback', ())
            if tb:
                tb = ''.join(traceback.format_list(tb))
                msg += ('\nCoroutine object created at '
                        '(most recent call last):\n')
                msg += tb.rstrip()
            logger.error(msg)

if not compat.PY34:
    # Backport functools.update_wrapper() from Python 3.4:
    # - Python 2.7 fails if assigned attributes don't exist
    # - Python 2.7 and 3.1 don't set the __wrapped__ attribute
    # - Python 3.2 and 3.3 set __wrapped__ before updating __dict__
    def _update_wrapper(wrapper,
                       wrapped,
                       assigned = functools.WRAPPER_ASSIGNMENTS,
                       updated = functools.WRAPPER_UPDATES):
        """Update a wrapper function to look like the wrapped function

           wrapper is the function to be updated
           wrapped is the original function
           assigned is a tuple naming the attributes assigned directly
           from the wrapped function to the wrapper function (defaults to
           functools.WRAPPER_ASSIGNMENTS)
           updated is a tuple naming the attributes of the wrapper that
           are updated with the corresponding attribute from the wrapped
           function (defaults to functools.WRAPPER_UPDATES)
        """
        for attr in assigned:
            try:
                value = getattr(wrapped, attr)
            except AttributeError:
                pass
            else:
                setattr(wrapper, attr, value)
        for attr in updated:
            getattr(wrapper, attr).update(getattr(wrapped, attr, {}))
        # Issue #17482: set __wrapped__ last so we don't inadvertently copy it
        # from the wrapped function when updating __dict__
        wrapper.__wrapped__ = wrapped
        # Return the wrapper so this can be used as a decorator via partial()
        return wrapper

    def _wraps(wrapped,
              assigned = functools.WRAPPER_ASSIGNMENTS,
              updated = functools.WRAPPER_UPDATES):
        """Decorator factory to apply update_wrapper() to a wrapper function

           Returns a decorator that invokes update_wrapper() with the decorated
           function as the wrapper argument and the arguments to wraps() as the
           remaining arguments. Default arguments are as for update_wrapper().
           This is a convenience function to simplify applying partial() to
           update_wrapper().
        """
        return functools.partial(_update_wrapper, wrapped=wrapped,
                                 assigned=assigned, updated=updated)
else:
    _wraps = functools.wraps

_PEP479 = (sys.version_info >= (3, 5))
if _PEP479:
    # Need exec() because yield+return raises a SyntaxError on Python 2
    exec(textwrap.dedent('''
        def pep479_wrapper(func, coro_func):
            @_wraps(func)
            def pep479_wrapped(*args, **kw):
                coro = coro_func(*args, **kw)
                value = None
                error = None
                while True:
                    try:
                        if error is not None:
                            value = coro.throw(error)
                        elif value is not None:
                            value = coro.send(value)
                        else:
                            value = next(coro)
                    except RuntimeError:
                        # FIXME: special case for
                        # FIXME: "isinstance(exc.__context__, StopIteration)"?
                        raise
                    except StopIteration as exc:
                        return exc.value
                    except Return as exc:
                        exc.raised = True
                        return exc.value
                    except BaseException as exc:
                        raise

                    try:
                        value = yield value
                        error = None
                    except BaseException as exc:
                        value = None
                        error = exc

            return pep479_wrapped
        '''))


def coroutine(func):
    """Decorator to mark coroutines.

    If the coroutine is not yielded from before it is destroyed,
    an error message is logged.
    """
    if _inspect_iscoroutinefunction(func):
        # In Python 3.5 that's all we need to do for coroutines
        # defiend with "async def".
        # Wrapping in CoroWrapper will happen via
        # 'sys.set_coroutine_wrapper' function.
        return func

    if inspect.isgeneratorfunction(func):
        coro = func
    else:
        @_wraps(func)
        def coro(*args, **kw):
            res = func(*args, **kw)
            if (isinstance(res, futures._FUTURE_CLASSES)
                or inspect.isgenerator(res)):
                res = yield From(res)
            elif _AwaitableABC is not None:
                # If 'func' returns an Awaitable (new in 3.5) we
                # want to run it.
                try:
                    await_meth = res.__await__
                except AttributeError:
                    pass
                else:
                    if isinstance(res, _AwaitableABC):
                        res = yield From(await_meth())
            raise Return(res)

    if _PEP479:
        # FIXME: use @_wraps
        coro = pep479_wrapper(func, coro)
        coro = _wraps(func)(coro)

    if not _DEBUG:
        if _types_coroutine is None:
            wrapper = coro
        else:
            wrapper = _types_coroutine(coro)
    else:
        @_wraps(func)
        def wrapper(*args, **kwds):
            w = CoroWrapper(coro(*args, **kwds), func=func)
            if w._source_traceback:
                del w._source_traceback[-1]
            # Python < 3.5 does not implement __qualname__
            # on generator objects, so we set it manually.
            # We use getattr as some callables (such as
            # functools.partial may lack __qualname__).
            w.__name__ = getattr(func, '__name__', None)
            w.__qualname__ = getattr(func, '__qualname__', None)
            return w

    wrapper._is_coroutine = True  # For iscoroutinefunction().
    return wrapper


def iscoroutinefunction(func):
    """Return True if func is a decorated coroutine function."""
    return (getattr(func, '_is_coroutine', False) or
            _inspect_iscoroutinefunction(func))


_COROUTINE_TYPES = (types.GeneratorType, CoroWrapper)
if _CoroutineABC is not None:
    _COROUTINE_TYPES += (_CoroutineABC,)
if events.asyncio is not None:
    # Accept also asyncio CoroWrapper for interoperability
    if hasattr(events.asyncio, 'coroutines'):
        _COROUTINE_TYPES += (events.asyncio.coroutines.CoroWrapper,)
    else:
        # old asyncio/Python versions
        _COROUTINE_TYPES += (events.asyncio.tasks.CoroWrapper,)

def iscoroutine(obj):
    """Return True if obj is a coroutine object."""
    return isinstance(obj, _COROUTINE_TYPES)


def _format_coroutine(coro):
    assert iscoroutine(coro)

    coro_name = None
    if isinstance(coro, CoroWrapper):
        func = coro.func
        coro_name = coro.__qualname__
        if coro_name is not None:
            coro_name = '{0}()'.format(coro_name)
    else:
        func = coro

    if coro_name is None:
        coro_name = events._format_callback(func, ())

    try:
        coro_code = coro.gi_code
    except AttributeError:
        coro_code = coro.cr_code

    try:
        coro_frame = coro.gi_frame
    except AttributeError:
        coro_frame = coro.cr_frame

    filename = coro_code.co_filename
    if (isinstance(coro, CoroWrapper)
    and not inspect.isgeneratorfunction(coro.func)
    and coro.func is not None):
        filename, lineno = events._get_function_source(coro.func)
        if coro_frame is None:
            coro_repr = ('%s done, defined at %s:%s'
                         % (coro_name, filename, lineno))
        else:
            coro_repr = ('%s running, defined at %s:%s'
                         % (coro_name, filename, lineno))
    elif coro_frame is not None:
        lineno = coro_frame.f_lineno
        coro_repr = ('%s running at %s:%s'
                     % (coro_name, filename, lineno))
    else:
        lineno = coro_code.co_firstlineno
        coro_repr = ('%s done, defined at %s:%s'
                     % (coro_name, filename, lineno))

    return coro_repr


class FromWrapper(object):
    __slots__ = ('obj',)

    def __init__(self, obj):
        if isinstance(obj, FromWrapper):
            obj = obj.obj
            assert not isinstance(obj, FromWrapper)
        self.obj = obj

def From(obj):
    if not _DEBUG:
        return obj
    else:
        return FromWrapper(obj)
