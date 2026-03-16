# status.py - Type annotations for status related objects
#
# Copyright Matt Harbison <mharbison72@gmail.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import annotations

import abc

from typing import (
    Iterator,
    Protocol,
)


class Status(Protocol):
    """Struct with a list of files per status.

    The 'deleted', 'unknown' and 'ignored' properties are only
    relevant to the working copy.
    """

    modified: list[bytes]
    """The list of files with modifications."""

    added: list[bytes]
    """The list of files that started being tracked."""

    removed: list[bytes]
    """The list of files that stopped being tracked."""

    deleted: list[bytes]
    """The list of files in the working directory that are deleted from the
    file system (but not in the removed state)."""

    unknown: list[bytes]
    """The list of files in the working directory that are not tracked."""

    ignored: list[bytes]
    """The list of files in the working directory that are ignored."""

    clean: list[bytes]
    """The list of files that are not in any other state."""

    @abc.abstractmethod
    def __iter__(self) -> Iterator[list[bytes]]:
        """Iterates over each of the categories of file lists."""

    @abc.abstractmethod
    def __repr__(self) -> str:
        """Creates a string representation of the file lists."""
