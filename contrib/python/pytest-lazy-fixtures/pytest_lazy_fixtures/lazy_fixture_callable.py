from __future__ import annotations

from inspect import isfunction
from typing import TYPE_CHECKING, Callable

from .lazy_fixture import LazyFixtureWrapper

if TYPE_CHECKING:
    import pytest


class LazyFixtureCallableWrapper(LazyFixtureWrapper):
    _func: Callable | None
    args: tuple
    kwargs: dict

    def __init__(self, callable_or_name: Callable | str, *args, **kwargs):
        if callable(callable_or_name):
            self._func = callable_or_name
            self.name = (
                callable_or_name.__name__ if isfunction(callable_or_name) else callable_or_name.__class__.__name__
            )
        else:
            self.name = callable_or_name
            self._func = None
        self.args = args
        self.kwargs = kwargs

    def get_func(self, request: pytest.FixtureRequest) -> Callable:
        func = self._func
        if func is None:
            func = self.load_fixture(request)
            if not callable(func):
                msg = "Passed fixture is not callable"
                raise TypeError(msg)
        return func


def lfc(name: Callable | str, *args, **kwargs) -> LazyFixtureCallableWrapper:
    """lfc is a lazy fixture callable."""
    return LazyFixtureCallableWrapper(name, *args, **kwargs)
