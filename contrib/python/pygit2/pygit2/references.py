# Copyright 2010-2025 The pygit2 contributors
#
# This file is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2,
# as published by the Free Software Foundation.
#
# In addition to the permissions in the GNU General Public License,
# the authors give you unlimited permission to link the compiled
# version of this file into combinations with other programs,
# and to distribute those combinations without any restriction
# coming from the use of this file.  (The General Public License
# restrictions do apply in other respects; for example, they cover
# modification of the file, and distribution when not linked into
# a combined executable.)
#
# This file is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; see the file COPYING.  If not, write to
# the Free Software Foundation, 51 Franklin Street, Fifth Floor,
# Boston, MA 02110-1301, USA.

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

from pygit2 import Oid

from .enums import ReferenceFilter

# Need BaseRepository for type hints, but don't let it cause a circular dependency
if TYPE_CHECKING:
    from ._pygit2 import Reference
    from .repository import BaseRepository


class References:
    def __init__(self, repository: BaseRepository) -> None:
        self._repository = repository

    def __getitem__(self, name: str) -> 'Reference':
        return self._repository.lookup_reference(name)

    def get(self, key: str) -> 'Reference' | None:
        try:
            return self[key]
        except KeyError:
            return None

    def __iter__(self) -> Iterator[str]:
        iter = self._repository.references_iterator_init()
        while True:
            ref = self._repository.references_iterator_next(iter)
            if ref:
                yield ref.name
            else:
                return

    def iterator(
        self, references_return_type: ReferenceFilter = ReferenceFilter.ALL
    ) -> Iterator['Reference']:
        """Creates a new iterator and fetches references for a given repository.

        Can also filter and pass all refs or only branches or only tags.

        Parameters:

        references_return_type: ReferenceFilter
            Optional specifier to filter references. By default, all references are
            returned.

            The following values are accepted:
            - ReferenceFilter.ALL, fetches all refs, this is the default
            - ReferenceFilter.BRANCHES, fetches only branches
            - ReferenceFilter.TAGS, fetches only tags

        TODO: Add support for filtering by reference types notes and remotes.
        """

        # Enforce ReferenceFilter type - raises ValueError if we're given an invalid value
        references_return_type = ReferenceFilter(references_return_type)

        iter = self._repository.references_iterator_init()
        while True:
            ref = self._repository.references_iterator_next(
                iter, references_return_type
            )
            if ref:
                yield ref
            else:
                return

    def create(self, name: str, target: Oid | str, force: bool = False) -> 'Reference':
        return self._repository.create_reference(name, target, force)

    def delete(self, name: str) -> None:
        self[name].delete()

    def __contains__(self, name: str) -> bool:
        return self.get(name) is not None

    @property
    def objects(self) -> list['Reference']:
        return self._repository.listall_reference_objects()

    def compress(self) -> None:
        return self._repository.compress_references()
