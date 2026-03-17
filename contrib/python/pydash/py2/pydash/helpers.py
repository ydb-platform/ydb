# -*- coding: utf-8 -*-
"""Generic utility methods not part of main API."""

from __future__ import absolute_import

from functools import wraps
import inspect
from operator import attrgetter, itemgetter
import warnings

import pydash as pyd

from ._compat import PY2, Iterable, Mapping, Sequence, getfullargspec, iteritems, string_types


class _NoValue(object):
    """
    Represents an unset value.

    Used to differentiate between an explicit ``None`` and an unset value.
    """

    pass


#: Singleton object that differentiates between an explicit ``None`` value and
#: an unset value.
NoValue = _NoValue()


def callit(iteratee, *args, **kwargs):
    """Inspect argspec of `iteratee` function and only pass the supported arguments when calling
    it."""
    maxargs = len(args)
    argcount = kwargs["argcount"] if "argcount" in kwargs else getargcount(iteratee, maxargs)
    argstop = min([maxargs, argcount])

    return iteratee(*args[:argstop])


def getargcount(iteratee, maxargs):
    """Return argument count of iteratee function."""
    if hasattr(iteratee, "_argcount"):
        # Optimization feature where argcount of iteratee is known and properly
        # set by initator.
        return iteratee._argcount

    if isinstance(iteratee, type) or pyd.is_builtin(iteratee):
        # Only pass single argument to type iteratees or builtins.
        argcount = 1
    else:
        argcount = 1

        try:
            argcount = _getargcount(iteratee, maxargs)
        except TypeError:  # pragma: no cover
            # PY2: Python2.7 throws a TypeError on classes that have __call__() defined but Python3
            # doesn't. So if we fail with TypeError here, try iteratee as iteratee.__call__.
            if PY2 and hasattr(iteratee, "__call__"):  # noqa: B004
                try:
                    argcount = _getargcount(iteratee.__call__, maxargs)
                except TypeError:
                    pass

    return argcount


def _getargcount(iteratee, maxargs):
    argcount = None

    try:
        # PY2: inspect.signature was added in Python 3.
        # Try to use inspect.signature when possible since it works better for our purpose of
        # getting the iteratee argcount since it takes into account the "self" argument in callable
        # classes.
        sig = inspect.signature(iteratee)
    except (TypeError, ValueError, AttributeError):
        pass
    else:  # pragma: no cover
        if not any(
            param.kind == inspect.Parameter.VAR_POSITIONAL for param in sig.parameters.values()
        ):
            argcount = len(sig.parameters)

    if argcount is None:
        argspec = getfullargspec(iteratee)
        if argspec and not argspec.varargs:  # pragma: no cover
            # Use inspected arg count.
            argcount = len(argspec.args)

    if argcount is None:
        # Assume all args are handleable.
        argcount = maxargs

    return argcount


def iteriteratee(obj, iteratee=None, reverse=False):
    """Return iterative iteratee based on collection type."""
    if iteratee is None:
        cbk = pyd.identity
        argcount = 1
    else:
        cbk = pyd.iteratee(iteratee)
        argcount = getargcount(cbk, maxargs=3)

    items = iterator(obj)

    if reverse:
        items = reversed(tuple(items))

    for key, item in items:
        yield (callit(cbk, item, key, obj, argcount=argcount), item, key, obj)


def iterator(obj):
    """Return iterative based on object type."""
    if isinstance(obj, dict):
        return iteritems(obj)
    elif hasattr(obj, "iteritems"):
        return obj.iteritems()  # noqa: B301
    elif hasattr(obj, "items"):
        return iter(obj.items())
    elif isinstance(obj, Iterable):
        return enumerate(obj)
    else:
        return iteritems(getattr(obj, "__dict__", {}))


def base_get(obj, key, default=NoValue):
    """
    Safely get an item by `key` from a sequence or mapping object when `default` provided.

    Args:
        obj (list|dict): Sequence or mapping to retrieve item from.
        key (mixed): Key or index identifying which item to retrieve.

    Keyword Args:
        use_default (bool, optional): Whether to use `default` value when `key` doesn't exist in
            `obj`.
        default (mixed, optional): Default value to return if `key` not found in `obj`.

    Returns:
        mixed: `obj[key]`, `obj.key`, or `default`.

    Raises:
        KeyError: If `obj` is missing key, index, or attribute and no default value provided.
    """
    if isinstance(obj, dict):
        value = _base_get_dict(obj, key, default=default)
    elif not isinstance(obj, (Mapping, Sequence)) or (
        isinstance(obj, tuple) and hasattr(obj, "_fields")
    ):
        # Don't use getattr for dict/list objects since we don't want class methods/attributes
        # returned for them but do allow getattr for namedtuple.
        value = _base_get_object(obj, key, default=default)
    else:
        value = _base_get_item(obj, key, default=default)

    if value is NoValue:
        # Raise if there's no default provided.
        raise KeyError('Object "{0}" does not have key "{1}"'.format(repr(obj), key))

    return value


def _base_get_dict(obj, key, default=NoValue):
    value = obj.get(key, NoValue)
    if value is NoValue:
        value = default
        if not isinstance(key, int):
            # Try integer key fallback.
            try:
                value = obj.get(int(key), default)
            except Exception:
                pass
    return value


def _base_get_item(obj, key, default=NoValue):
    try:
        return obj[key]
    except Exception:
        pass

    if not isinstance(key, int):
        try:
            return obj[int(key)]
        except Exception:
            pass

    return default


def _base_get_object(obj, key, default=NoValue):
    value = _base_get_item(obj, key, default=NoValue)
    if value is NoValue:
        value = default
        try:
            value = getattr(obj, key)
        except Exception:
            pass
    return value


def base_set(obj, key, value, allow_override=True):
    """
    Set an object's `key` to `value`. If `obj` is a ``list`` and the `key` is the next available
    index position, append to list; otherwise, pad the list of ``None`` and then append to the list.

    Args:
        obj (list|dict): Object to assign value to.
        key (mixed): Key or index to assign to.
        value (mixed): Value to assign.
    """
    if isinstance(obj, dict):
        if allow_override or key not in obj:
            obj[key] = value
    elif isinstance(obj, list):
        key = int(key)

        if key < len(obj):
            if allow_override:
                obj[key] = value
        else:
            if key > len(obj):
                # Pad list object with None values up to the index key so we can append the value
                # into the key index.
                obj[:] = (obj + [None] * key)[:key]
            obj.append(value)
    elif (allow_override or not hasattr(obj, key)) and obj is not None:
        setattr(obj, key, value)

    return obj


def parse_iteratee(iteratee_keyword, *args, **kwargs):
    """Try to find iteratee function passed in either as a keyword argument or as the last
    positional argument in `args`."""
    iteratee = kwargs.get(iteratee_keyword)
    last_arg = args[-1]

    if iteratee is None and (
        callable(last_arg)
        or isinstance(last_arg, string_types)
        or isinstance(last_arg, dict)
        or last_arg is None
    ):
        iteratee = last_arg
        args = args[:-1]

    return (iteratee, args)


class iterator_with_default(object):
    """A wrapper around an iterator object that provides a default."""

    def __init__(self, collection, default):
        self.iter = iter(collection)
        self.default = default

    def __iter__(self):
        return self

    def next_default(self):
        ret = self.default
        self.default = NoValue
        return ret

    def __next__(self):
        ret = next(self.iter, self.next_default())
        if ret is NoValue:
            raise StopIteration
        return ret

    next = __next__


def deprecated(func):  # pragma: no cover
    """
    This is a decorator which can be used to mark functions as deprecated.

    It will result in a warning being emitted when the function is used.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        warnings.warn(
            "Call to deprecated function {0}.".format(func.__name__),
            category=DeprecationWarning,
            stacklevel=3,
        )
        return func(*args, **kwargs)

    return wrapper
