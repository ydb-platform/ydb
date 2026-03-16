#
# Copyright (C) 2023 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
# pylint: disable=missing-docstring
r"""Load data from .py.

.. warning::

   - Both load_literal_data_from_string and load_literal_data_from_path only
     parse and never execute the given string contains the code so that these
     do not have vulnerabilities because of aribitary code execution (ACE)
     exploits.  But it should be possible to DoS attack.

   - load_data_from_py has vulnerabilities because it execute the code. You
     must avoid to load .py data from unknown sources with this.
"""
from __future__ import annotations

import ast
import importlib
import importlib.util
import importlib.abc
import typing
import warnings

if typing.TYPE_CHECKING:
    import pathlib


DATA_VAR_NAME: str = "DATA"


def load_literal_data_from_string(content: str) -> typing.Any:
    """Load test data expressed by literal data string ``content``."""
    return ast.literal_eval(content)


def load_literal_data_from_path(path: pathlib.Path) -> typing.Any:
    """Load test data expressed by literal data from .py files.

    .. note:: It should be safer than the above function.
    """
    return load_literal_data_from_string(path.read_text().strip())


def load_data_from_py(
    path: pathlib.Path, *,
    data_name: str | None = None,
    fallback: bool = False,
) -> typing.Any:
    """Load test data from .py files by evaluating it.

    .. note:: It's not safe and has vulnerabilities for ACE attacks. .
    """
    if data_name is None:
        data_name = DATA_VAR_NAME

    spec = importlib.util.spec_from_file_location("testmod", str(path))
    if spec and isinstance(spec.loader, importlib.abc.Loader):
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        try:
            return getattr(mod, data_name)
        except (TypeError, ValueError, AttributeError):
            warnings.warn(
                f"No valid data '{data_name}' was found in {mod!r}.",
                stacklevel=2,
            )

    if fallback:
        return None

    msg = f"Faied to load data from: {path!r}"
    raise ValueError(msg)


def load_from_path(
    path: pathlib.Path, *,
    allow_exec: bool = False,
    data_name: str | None = None,
    fallback: bool = False,
) -> typing.Any:
    """Load data from given path `path`.

    It will choose the appropriate function by the keyword, `data_name`, in the
    content of the file.

    :param allow_exec: The code will be executed if True
    """
    if allow_exec and (data_name or DATA_VAR_NAME) in path.read_text():
        return load_data_from_py(
            path, data_name=data_name, fallback=fallback,
        )

    return load_literal_data_from_path(path)
