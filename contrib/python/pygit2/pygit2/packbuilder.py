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

from os import PathLike
from typing import TYPE_CHECKING

# Import from pygit2
from .errors import check_error
from .ffi import C, ffi
from .utils import to_bytes

if TYPE_CHECKING:
    from pygit2 import Oid, Repository
    from pygit2.repository import BaseRepository


class PackBuilder:
    def __init__(self, repo: 'Repository | BaseRepository') -> None:
        cpackbuilder = ffi.new('git_packbuilder **')
        err = C.git_packbuilder_new(cpackbuilder, repo._repo)
        check_error(err)

        self._repo = repo
        self._packbuilder = cpackbuilder[0]
        self._cpackbuilder = cpackbuilder

    @property
    def _pointer(self) -> bytes:
        return bytes(ffi.buffer(self._packbuilder)[:])

    def __del__(self) -> None:
        C.git_packbuilder_free(self._packbuilder)

    def __len__(self) -> int:
        return C.git_packbuilder_object_count(self._packbuilder)

    @staticmethod
    def __convert_object_to_oid(oid: 'Oid') -> 'ffi.GitOidC':
        git_oid = ffi.new('git_oid *')
        ffi.buffer(git_oid)[:] = oid.raw[:]
        return git_oid

    def add(self, oid: 'Oid') -> None:
        git_oid = self.__convert_object_to_oid(oid)
        err = C.git_packbuilder_insert(self._packbuilder, git_oid, ffi.NULL)
        check_error(err)

    def add_recur(self, oid: 'Oid') -> None:
        git_oid = self.__convert_object_to_oid(oid)
        err = C.git_packbuilder_insert_recur(self._packbuilder, git_oid, ffi.NULL)
        check_error(err)

    def set_threads(self, n_threads: int) -> int:
        return C.git_packbuilder_set_threads(self._packbuilder, n_threads)

    def write(self, path: str | bytes | PathLike[str] | None = None) -> None:
        path_bytes = ffi.NULL if path is None else to_bytes(path)
        err = C.git_packbuilder_write(
            self._packbuilder, path_bytes, 0, ffi.NULL, ffi.NULL
        )
        check_error(err)

    @property
    def written_objects_count(self) -> int:
        return C.git_packbuilder_written(self._packbuilder)
