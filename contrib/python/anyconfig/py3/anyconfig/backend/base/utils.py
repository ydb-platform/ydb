#
# Copyright (C) 2012 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Provides utility functions in anyconfig.backend.base."""
from __future__ import annotations

import functools
import pathlib
import typing

if typing.TYPE_CHECKING:
    import collections.abc


def not_implemented(
    *_args: typing.Any, **_options: typing.Any,
) -> None:
    """Raise NotImplementedError."""
    raise NotImplementedError


def ensure_outdir_exists(
    filepath: str | pathlib.Path,
) -> None:
    """Make dir to dump 'filepath' if that dir does not exist.

    :param filepath: path of file to dump
    """
    pathlib.Path(filepath).parent.mkdir(parents=True, exist_ok=True)


def to_method(
    func: collections.abc.Callable[..., typing.Any],
) -> collections.abc.Callable[..., typing.Any]:
    """Lift :func:`func` to a method.

    It will be called with the first argument 'self' ignored.

    :param func: Any callable object
    """
    @functools.wraps(func)
    def wrapper(
        *args: typing.Any, **kwargs: typing.Any,
    ) -> collections.abc.Callable[..., typing.Any]:
        """Original function decorated."""
        return func(*args[1:], **kwargs)

    return wrapper
