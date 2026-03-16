#
# Copyright (C) 2012 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Utility funtions for anyconfig.ionfo."""
from __future__ import annotations

import itertools
import locale
import pathlib
import typing
import warnings

from .constants import GLOB_MARKER, PATH_SEP

if typing.TYPE_CHECKING:
    import collections.abc


def get_encoding() -> str:
    """Get the (prefered) encoding or 'utf-8'."""
    return (locale.getpreferredencoding() or "UTF-8").lower()


def get_path_and_ext(path: pathlib.Path) -> tuple[pathlib.Path, str]:
    """Normaliez path objects and retunr it with file extension."""
    try:
        abs_path = path.expanduser().resolve()
    except (RuntimeError, OSError) as exc:
        warnings.warn(f"Failed to resolve {path!s}, exc={exc!r}", stacklevel=2)
        abs_path = path

    file_ext = path.suffix

    return (
        abs_path,
        file_ext[1:] if file_ext.startswith(".") else "",
    )


def expand_from_path(
    path: pathlib.Path, marker: str = GLOB_MARKER,
) -> collections.abc.Iterator[pathlib.Path]:
    """Expand ``path`` contains '*' in its path str."""
    if not path.is_absolute():
        path = path.resolve()

    idx_part = list(
        enumerate(
            itertools.takewhile(lambda p: marker not in p, path.parts),
        ),
    )[-1]

    if not idx_part:
        msg = f"It should not happen: {path!r}"
        raise ValueError(msg)

    idx = idx_part[0] + 1
    if len(path.parts) > idx:
        base = pathlib.Path(path.parts[0]).joinpath(*path.parts[:idx])
        pattern = PATH_SEP.join(path.parts[idx:])
        yield from sorted(base.glob(pattern))

    else:  # No marker was found.
        yield path
