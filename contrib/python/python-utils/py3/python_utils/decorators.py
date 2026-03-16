"""
This module provides various utility decorators for Python functions
and methods.

The decorators include:

1. `set_attributes`: Sets attributes on functions and classes.
2. `listify`: Converts any generator to a list or other collection.
3. `sample`: Limits calls to a function based on a sample rate.
4. `wraps_classmethod`: Wraps classmethods with type info from a
   regular method.

Each decorator is designed to enhance the functionality of Python
functions and methods in a simple and reusable manner.
"""

import contextlib
import functools
import logging
import random

from . import types

_T = types.TypeVar('_T')
_P = types.ParamSpec('_P')
_S = types.TypeVar('_S', covariant=True)


def set_attributes(**kwargs: types.Any) -> types.Callable[..., types.Any]:
    """Decorator to set attributes on functions and classes.

    A common usage for this pattern is the Django Admin where
    functions can get an optional short_description. To illustrate:

    Example from the Django admin using this decorator:
    https://docs.djangoproject.com/en/3.0/ref/contrib/admin/#django.contrib.admin.ModelAdmin.list_display

    Our simplified version:

    >>> @set_attributes(short_description='Name')
    ... def upper_case_name(self, obj):
    ...     return ('%s %s' % (obj.first_name, obj.last_name)).upper()

    The standard Django version:

    >>> def upper_case_name(obj):
    ...     return ('%s %s' % (obj.first_name, obj.last_name)).upper()

    >>> upper_case_name.short_description = 'Name'

    """

    def _set_attributes(
        function: types.Callable[_P, _T],
    ) -> types.Callable[_P, _T]:
        for key, value in kwargs.items():
            setattr(function, key, value)
        return function

    return _set_attributes


def listify(
    collection: types.Callable[
        [types.Iterable[_T]], types.Collection[_T]
    ] = list,
    allow_empty: bool = True,
) -> types.Callable[
    [types.Callable[..., types.Optional[types.Iterable[_T]]]],
    types.Callable[..., types.Collection[_T]],
]:
    """
    Convert any generator to a list or other type of collection.

    >>> @listify()
    ... def generator():
    ...     yield 1
    ...     yield 2
    ...     yield 3

    >>> generator()
    [1, 2, 3]

    >>> @listify()
    ... def empty_generator():
    ...     pass

    >>> empty_generator()
    []

    >>> @listify(allow_empty=False)
    ... def empty_generator_not_allowed():
    ...     pass

    >>> empty_generator_not_allowed()  # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    TypeError: ... `allow_empty` is `False`

    >>> @listify(collection=set)
    ... def set_generator():
    ...     yield 1
    ...     yield 1
    ...     yield 2

    >>> set_generator()
    {1, 2}

    >>> @listify(collection=dict)
    ... def dict_generator():
    ...     yield 'a', 1
    ...     yield 'b', 2

    >>> dict_generator()
    {'a': 1, 'b': 2}
    """

    def _listify(
        function: types.Callable[..., types.Optional[types.Iterable[_T]]],
    ) -> types.Callable[..., types.Collection[_T]]:
        def __listify(
            *args: types.Any, **kwargs: types.Any
        ) -> types.Collection[_T]:
            result: types.Optional[types.Iterable[_T]] = function(
                *args, **kwargs
            )
            if result is None:
                if allow_empty:
                    return collection(iter(()))
                else:
                    raise TypeError(
                        f'{function} returned `None` and `allow_empty` '
                        'is `False`'
                    )
            else:
                return collection(result)

        return __listify

    return _listify


def sample(
    sample_rate: float,
) -> types.Callable[
    [types.Callable[_P, _T]],
    types.Callable[_P, types.Optional[_T]],
]:
    """
    Limit calls to a function based on given sample rate.
    Number of calls to the function will be roughly equal to
    sample_rate percentage.

    Usage:

    >>> @sample(0.5)
    ... def demo_function(*args, **kwargs):
    ...     return 1

    Calls to *demo_function* will be limited to 50% approximatly.
    """

    def _sample(
        function: types.Callable[_P, _T],
    ) -> types.Callable[_P, types.Optional[_T]]:
        @functools.wraps(function)
        def __sample(
            *args: _P.args, **kwargs: _P.kwargs
        ) -> types.Optional[_T]:
            if random.random() < sample_rate:
                return function(*args, **kwargs)
            else:
                logging.debug(
                    'Skipped execution of %r(%r, %r) due to sampling',
                    function,
                    args,
                    kwargs,
                )
                return None

        return __sample

    return _sample


def wraps_classmethod(
    wrapped: types.Callable[types.Concatenate[_S, _P], _T],
) -> types.Callable[
    [
        types.Callable[types.Concatenate[types.Any, _P], _T],
    ],
    types.Callable[types.Concatenate[_S, _P], _T],
]:
    """
    Like `functools.wraps`, but for wrapping classmethods with the type info
    from a regular method.
    """

    def _wraps_classmethod(
        wrapper: types.Callable[types.Concatenate[types.Any, _P], _T],
    ) -> types.Callable[types.Concatenate[_S, _P], _T]:
        # For some reason `functools.update_wrapper` fails on some test
        # runs but not while running actual code
        with contextlib.suppress(AttributeError):
            wrapper = functools.update_wrapper(
                wrapper,
                wrapped,
                assigned=tuple(
                    a
                    for a in functools.WRAPPER_ASSIGNMENTS
                    if a != '__annotations__'
                ),
            )
        if annotations := getattr(wrapped, '__annotations__', {}):
            annotations.pop('self', None)
            wrapper.__annotations__ = annotations

        return wrapper

    return _wraps_classmethod
