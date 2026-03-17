# mercurial/interfaces/_basetypes.py - internal base type aliases for interfaces
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.
#
# This module contains trivial type aliases that other interfaces might need,
# in a location designed to avoid import cycles.  This is for internal usage
# by the modules in `mercurial.interfaces`, instead of importing the `types`
# module.
#
# For using type aliases outside `mercurial.interfaces`, look at the
# `mercurial.interfaces.types` module.

from __future__ import annotations

from typing import Any

UserMsgT = bytes
"""Text (maybe) displayed to the user."""

HgPathT = bytes
"""A path usable with Mercurial's vfs."""

FsPathT = bytes
"""A path on disk (after vfs encoding)."""

# I doubt we will be able to any anything more precise cheaply, however this is
# useful to richer annotations.
NodeIdT = bytes
"""a nodeid identifier"""

# TODO: create a Protocol class,
RepoT = Any

# TODO: create a Protocol class,
RevlogT = Any

# I doubt we will be able to any anything more precise cheaply, however this is
# useful to richer annotations.
RevnumT = int
"""a revision number"""

# See revset.matchany()
RevsetAliasesT = dict[bytes, bytes]
"""A mapping of a revset alias name to its (string) definition."""

# TODO: create a Protocol class,
UiT = Any

# TODO: make a protocol class for this
VfsT = Any

VfsKeyT = bytes
"""Vfs identifier, typically used in a VfsMap."""

CallbackCategoryT = bytes
"""Key identifying a callback category."""
