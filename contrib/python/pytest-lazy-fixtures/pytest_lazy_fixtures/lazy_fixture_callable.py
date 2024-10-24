from typing import Callable, Optional, Union

import pytest

from .lazy_fixture import LazyFixtureWrapper


class LazyFixtureCallableWrapper(LazyFixtureWrapper):
    _func: Optional[Callable]
    args: tuple
    kwargs: dict

    def __init__(self, func_or_name: Union[Callable, str], *args, **kwargs):
        if callable(func_or_name):
            self._func = func_or_name
            self.name = func_or_name.__name__
        else:
            self.name = func_or_name
            self._func = None
        self.args = args
        self.kwargs = kwargs

    def get_func(self, request: pytest.FixtureRequest) -> Callable:
        func = self._func
        if func is None:
            func = self.load_fixture(request)
            assert callable(func)
        return func


def lfc(name: Union[Callable, str], *args, **kwargs) -> LazyFixtureCallableWrapper:
    """lfc is a lazy fixture callable."""
    return LazyFixtureCallableWrapper(name, *args, **kwargs)
