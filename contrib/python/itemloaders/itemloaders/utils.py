"""
Copy/paste from scrapy source at the moment, to ensure tests are working.
Refactoring to come later
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Generator, Iterable
from functools import partial
from typing import Any


def arg_to_iter(arg: Any) -> Iterable[Any]:
    """Return an iterable based on *arg*.

    If *arg* is a list, a tuple or a generator, it will be returned as is.

    If *arg* is ``None``, an empty list will be returned.

    If *arg* is anything else, a list will be returned with *arg* as its only
    item, i.e. ``[arg]``.
    """
    if arg is None:
        return []
    if isinstance(arg, (list, tuple, Generator)):
        return arg
    return [arg]


def get_func_args(func: Callable[..., Any], stripself: bool = False) -> list[str]:
    """Return the argument name list of a callable object"""
    if not callable(func):
        raise TypeError(f"func must be callable, got {type(func).__name__!r}")

    args: list[str] = []
    try:
        sig = inspect.signature(func)
    except ValueError:
        return args

    if isinstance(func, partial):
        partial_args = func.args
        partial_kw = func.keywords

        for name, param in sig.parameters.items():
            if param.name in partial_args:
                continue
            if partial_kw and param.name in partial_kw:
                continue
            args.append(name)
    else:
        args = list(sig.parameters)

    if stripself and args and args[0] == "self":
        args = args[1:]
    return args
