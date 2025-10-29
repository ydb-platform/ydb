from __future__ import annotations

import inspect
from inspect import isfunction
from typing import TYPE_CHECKING, Callable

from .lazy_fixture import LazyFixtureWrapper

if TYPE_CHECKING:
    import pytest


def _fill_unbound_params(
    func: Callable[..., object], args: tuple[object, ...], kwargs: dict[str, object]
) -> dict[str, object]:
    """Analyze a callable's signature and bind lazy fixtures to unbound parameters.

    This function takes a callable and its provided arguments, examines the callable's
    signature, and returns a dictionary that maps any unbound parameter names to their
    corresponding lazy fixtures.

    Only parameters that are not already bound through the provided args/kwargs will
    get mapped to lazy fixtures. Any parameters that have existing values provided
    maintain those values.
    """

    try:
        sig = inspect.signature(func)
    except (ValueError, TypeError):
        # Cowardly refuse to figure out the missing params
        return {}

    bound = sig.bind_partial(*args, **kwargs)

    # Apply the defaults arguments, as we don't want to override them.
    bound.apply_defaults()

    unbound = [name for name in sig.parameters if name not in bound.arguments]

    return {unbound_param: LazyFixtureWrapper(unbound_param) for unbound_param in unbound}


class LazyFixtureCallableWrapper(LazyFixtureWrapper):
    _func: Callable | None
    args: tuple
    kwargs: dict

    def __init__(self, callable_or_name: Callable | str, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

        if callable(callable_or_name):
            self._func = callable_or_name
            self.name = (
                callable_or_name.__name__ if isfunction(callable_or_name) else callable_or_name.__class__.__name__
            )
            # If we have a direct callable, analyze its signature and pre-fill
            # any unbound parameters with lf(param_name).
            self.kwargs.update(_fill_unbound_params(self._func, self.args, self.kwargs))
        else:
            self.name = callable_or_name
            self._func = None

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
