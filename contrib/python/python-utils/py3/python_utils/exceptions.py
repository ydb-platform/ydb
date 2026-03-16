"""
This module provides utility functions for raising and reraising exceptions.

Functions:
    raise_exception(exception_class, *args, **kwargs):
        Returns a function that raises an exception of the given type with
        the given arguments.

    reraise(*args, **kwargs):
        Reraises the current exception.
"""

from . import types


def raise_exception(
    exception_class: types.Type[Exception],
    *args: types.Any,
    **kwargs: types.Any,
) -> types.Callable[..., None]:
    """
    Returns a function that raises an exception of the given type with the
    given arguments.

    >>> raise_exception(ValueError, 'spam')('eggs')
    Traceback (most recent call last):
        ...
    ValueError: spam
    """

    def raise_(*args_: types.Any, **kwargs_: types.Any) -> types.Any:
        raise exception_class(*args, **kwargs)

    return raise_


def reraise(*args: types.Any, **kwargs: types.Any) -> types.Any:
    """
    Reraises the current exception.

    This function seems useless, but it can be useful when you need to pass
    a callable to another function that raises an exception.
    """
    raise
