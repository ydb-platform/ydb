from contextlib import ContextDecorator, contextmanager  # reexport these for backwards compat
import functools
from inspect import unwrap  # reexport this for backwards compat
import inspect

from . colls import omit

__all__ = ['decorator', 'wraps', 'unwrap', 'ContextDecorator', 'contextmanager']


def decorator(deco):
    """
    Transforms a flat wrapper into decorator::

        @decorator
        def func(call, methods, content_type=DEFAULT):  # These are decorator params
            # Access call arg by name
            if call.request.method not in methods:
                # ...
            # Decorated functions and all the arguments are accesible as:
            print(call._func, call_args, call._kwargs)
            # Finally make a call:
            return call()
    """
    if has_single_arg(deco):
        return make_decorator(deco)
    elif has_1pos_and_kwonly(deco):
        # Any arguments after first become decorator arguments
        # And a decorator with arguments is essentially a decorator fab
        # TODO: use pos-only arg once in Python 3.8+ only
        def decorator_fab(_func=None, **dkwargs):
            if _func is not None:
                return make_decorator(deco, (), dkwargs)(_func)
            return make_decorator(deco, (), dkwargs)
    else:
        def decorator_fab(*dargs, **dkwargs):
            return make_decorator(deco, dargs, dkwargs)

    return wraps(deco)(decorator_fab)


def make_decorator(deco, dargs=(), dkwargs={}):
    @wraps(deco)
    def _decorator(func):
        def wrapper(*args, **kwargs):
            call = Call(func, args, kwargs)
            return deco(call, *dargs, **dkwargs)
        return wraps(func)(wrapper)

    # NOTE: should I update name to show args?
    # Save these for introspection
    _decorator._func, _decorator._args, _decorator._kwargs = deco, dargs, dkwargs
    return _decorator


class Call(object):
    """
    A call object to pass as first argument to decorator.

    Call object is just a proxy for decorated function
    with call arguments saved in its attributes.
    """
    def __init__(self, func, args, kwargs):
        self._func, self._args, self._kwargs = func, args, kwargs

    def __call__(self, *a, **kw):
        if not a and not kw:
            return self._func(*self._args, **self._kwargs)
        else:
            return self._func(*(self._args + a), **dict(self._kwargs, **kw))

    def __getattr__(self, name):
        try:
            res = self.__dict__[name] = arggetter(self._func)(name, self._args, self._kwargs)
            return res
        except TypeError as e:
            raise AttributeError(*e.args)

    def __str__(self):
        func = getattr(self._func, '__qualname__', str(self._func))
        args = ", ".join(list(map(str, self._args)) + ["%s=%s" % t for t in self._kwargs.items()])
        return "%s(%s)" % (func, args)

    def __repr__(self):
        return "<Call %s>" % self


def has_single_arg(func):
    sig = inspect.signature(func)
    if len(sig.parameters) != 1:
        return False
    arg = next(iter(sig.parameters.values()))
    return arg.kind not in (arg.VAR_POSITIONAL, arg.VAR_KEYWORD)

def has_1pos_and_kwonly(func):
    from collections import Counter
    from inspect import Parameter as P

    sig = inspect.signature(func)
    kinds = Counter(p.kind for p in sig.parameters.values())
    return kinds[P.POSITIONAL_ONLY] + kinds[P.POSITIONAL_OR_KEYWORD] == 1 \
        and kinds[P.VAR_POSITIONAL] == 0

def get_argnames(func):
    func = getattr(func, '__original__', None) or unwrap(func)
    return func.__code__.co_varnames[:func.__code__.co_argcount]

def arggetter(func, _cache={}):
    if func in _cache:
        return _cache[func]

    original = getattr(func, '__original__', None) or unwrap(func)
    code = original.__code__

    # Instrospect pos and kw names
    posnames = code.co_varnames[:code.co_argcount]
    n = code.co_argcount
    kwonlynames = code.co_varnames[n:n + code.co_kwonlyargcount]
    n += code.co_kwonlyargcount
    # TODO: remove this check once we drop Python 3.7
    if hasattr(code, 'co_posonlyargcount'):
        kwnames = posnames[code.co_posonlyargcount:] + kwonlynames
    else:
        kwnames = posnames + kwonlynames

    varposname = varkwname = None
    if code.co_flags & inspect.CO_VARARGS:
        varposname = code.co_varnames[n]
        n += 1
    if code.co_flags & inspect.CO_VARKEYWORDS:
        varkwname = code.co_varnames[n]

    allnames = set(code.co_varnames)
    indexes = {name: i for i, name in enumerate(posnames)}
    defaults = {}
    if original.__defaults__:
        defaults.update(zip(posnames[-len(original.__defaults__):], original.__defaults__))
    if original.__kwdefaults__:
        defaults.update(original.__kwdefaults__)

    def get_arg(name, args, kwargs):
        if name not in allnames:
            raise TypeError("%s() doesn't have argument named %s" % (func.__name__, name))

        index = indexes.get(name)
        if index is not None and index < len(args):
            return args[index]
        elif name in kwargs and name in kwnames:
            return kwargs[name]
        elif name == varposname:
            return args[len(posnames):]
        elif name == varkwname:
            return omit(kwargs, kwnames)
        elif name in defaults:
            return defaults[name]
        else:
            raise TypeError("%s() missing required argument: '%s'" % (func.__name__, name))

    _cache[func] = get_arg
    return get_arg


### Add __original__ to update_wrapper and @wraps

def update_wrapper(wrapper,
                   wrapped,
                   assigned = functools.WRAPPER_ASSIGNMENTS,
                   updated = functools.WRAPPER_UPDATES):
    functools.update_wrapper(wrapper, wrapped, assigned, updated)

    # Set an original ref for faster and more convenient access
    wrapper.__original__ = getattr(wrapped, '__original__', None) or unwrap(wrapped)

    return wrapper

update_wrapper.__doc__ = functools.update_wrapper.__doc__


def wraps(wrapped,
          assigned = functools.WRAPPER_ASSIGNMENTS,
          updated = functools.WRAPPER_UPDATES):
    return functools.partial(update_wrapper, wrapped=wrapped, assigned=assigned, updated=updated)

wraps.__doc__ = functools.wraps.__doc__
