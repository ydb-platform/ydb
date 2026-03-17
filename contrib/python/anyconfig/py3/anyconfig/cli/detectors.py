#
# Copyright (C) 2011 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Detect file type and parser from inputs and/or output."""
from __future__ import annotations

import collections
import os
import pathlib
import typing
import warnings

from .. import api
from . import constants, utils

if typing.TYPE_CHECKING:
    import argparse

    try:
        from typing import TypeGuard
    except ImportError:
        from typing_extensions import TypeGuard


def are_same_file_types(paths: list[str]) -> bool:
    """Test if all of the types for given file paths ``paths`` are same."""
    if not paths:
        return False

    if len(paths) == 1:
        return True

    exts = [pathlib.Path(p).suffix for p in paths]
    return all(x and exts[0] == x for x in exts[1:])


def find_by_the_type(io_type: str) -> str | None:
    """Check the type given by users."""
    default = None

    if not io_type:
        return default

    try:
        return api.find(None, io_type).type()  # type: ignore[attr-defined]

    except api.UnknownProcessorTypeError:
        # Just ignore it should be wrong type.
        warnings.warn(
            "Ignored the given type because it looks wrong or "
            "is not supported by installed parser backends: "
            f"{io_type}", stacklevel=2,
        )

    return default


def find_by_the_paths(
    paths: list[str], *, ignore_errors: bool = True,
) -> str | None:
    """Try to detect file (parser) type from given file paths ``paths``."""
    default = None
    msg = (
        "*** You have to specify file type[s] with "
        "-I/--itype or -O/--otype options explicitly. ***"
    )
    paths_s = ", ".join(paths)

    if not are_same_file_types(paths):
        if ignore_errors:
            return default

        utils.exit_with_output(
            "Failed to detect a file type because given file paths "
            "may contain files with multiple types: "
            f"{paths_s}{os.linesep}{msg}",
            1,
        )

    if constants.STD_IN_OR_OUT not in paths:
        try:
            return api.find(paths[0]).type()  # type: ignore[attr-defined]

        except api.UnknownFileTypeError:
            if not ignore_errors:
                utils.exit_with_output(
                    "Failed to detect the file type because it is/those are "
                    f"unknown file type[s]: {paths_s}{os.linesep}{msg}",
                    1,
                )

    return default


def try_detecting_input_type(
    args: argparse.Namespace, *, ignore_errors: bool = True,
) -> str | None:
    """Try to resolve a file type and parser of inputs."""
    # First, try the type given by users.
    if args.itype:
        itype = find_by_the_type(args.itype)
        if itype:
            return itype

    # Next, try to detect from the filename given by users.
    if args.inputs:
        return find_by_the_paths(args.inputs, ignore_errors=ignore_errors)

    return None


def try_detecting_output_type(
    args: argparse.Namespace,
) -> str | None:
    """Try to resolve a file type and parser of outputs (``args.output``)."""
    # First, try the type given by users.
    if args.otype:
        otype = find_by_the_type(args.otype)
        if otype:
            return otype

    # Next, try to detect from the filename given by users.
    if args.output:
        otype = find_by_the_paths([args.output])
        if otype:
            return otype

    # Lastly, try to detect the input type and use it as an output type also.
    itype = try_detecting_input_type(args)
    if not itype:
        utils.exit_with_output(
            "Failed to find or detect the file type: "
            f"itype={args.itype}, otype={args.otype}, "
            f"output={args.output}, inputs={', '.join(args.inputs)}",
            1,
        )

    return itype


def is_dict_like(obj: typing.Any) -> TypeGuard[dict]:
    """Return True if `obj` is a dict."""
    return isinstance(obj, (dict, collections.abc.Mapping))
