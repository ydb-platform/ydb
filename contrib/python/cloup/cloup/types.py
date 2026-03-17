"""
Parameter types and "shortcuts" for creating commonly used types.
"""
import pathlib
from typing import Type, Any

import click


def path(
    *,
    path_type: Type[Any] = pathlib.Path,
    exists: bool = False,
    file_okay: bool = True,
    dir_okay: bool = True,
    readable: bool = True,
    writable: bool = False,
    executable: bool = False,
    resolve_path: bool = False,
    allow_dash: bool = False,
) -> click.Path:
    """Shortcut for :class:`click.Path` with ``path_type=pathlib.Path``."""
    return click.Path(**locals())


def dir_path(
    *,
    path_type: Type[Any] = pathlib.Path,
    exists: bool = False,
    readable: bool = True,
    writable: bool = False,
    executable: bool = False,
    resolve_path: bool = False,
    allow_dash: bool = False,
) -> click.Path:
    """Shortcut for :class:`click.Path` with
    ``file_okay=False, path_type=pathlib.Path``."""
    return click.Path(**locals(), file_okay=False)


def file_path(
    *,
    path_type: Type[Any] = pathlib.Path,
    exists: bool = False,
    readable: bool = True,
    writable: bool = False,
    executable: bool = False,
    resolve_path: bool = False,
    allow_dash: bool = False,
) -> click.Path:
    """Shortcut for :class:`click.Path` with
    ``dir_okay=False, path_type=pathlib.Path``."""
    return click.Path(**locals(), dir_okay=False)
