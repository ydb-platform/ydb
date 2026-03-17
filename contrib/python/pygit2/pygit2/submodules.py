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

from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from ._pygit2 import Oid
from .callbacks import RemoteCallbacks, git_fetch_options
from .enums import SubmoduleIgnore, SubmoduleStatus
from .errors import check_error
from .ffi import C, ffi
from .utils import maybe_string, to_bytes

# Need BaseRepository for type hints, but don't let it cause a circular dependency
if TYPE_CHECKING:
    from pygit2 import Repository
    from pygit2._libgit2.ffi import GitSubmoduleC

    from .repository import BaseRepository


class Submodule:
    _repo: BaseRepository
    _subm: 'GitSubmoduleC'

    @classmethod
    def _from_c(cls, repo: BaseRepository, cptr: 'GitSubmoduleC') -> 'Submodule':
        subm = cls.__new__(cls)

        subm._repo = repo
        subm._subm = cptr

        return subm

    def __del__(self) -> None:
        C.git_submodule_free(self._subm)

    def open(self) -> Repository:
        """Open the repository for a submodule."""
        crepo = ffi.new('git_repository **')
        err = C.git_submodule_open(crepo, self._subm)
        check_error(err)

        return self._repo._from_c(crepo[0], True)  # type: ignore[attr-defined]

    def init(self, overwrite: bool = False) -> None:
        """
        Just like "git submodule init", this copies information about the submodule
        into ".git/config".

        Parameters:

        overwrite
            By default, existing submodule entries will not be overwritten,
            but setting this to True forces them to be updated.
        """
        err = C.git_submodule_init(self._subm, int(overwrite))
        check_error(err)

    def update(
        self,
        init: bool = False,
        callbacks: Optional[RemoteCallbacks] = None,
        depth: int = 0,
    ) -> None:
        """
        Update a submodule. This will clone a missing submodule and checkout
        the subrepository to the commit specified in the index of the
        containing repository. If the submodule repository doesn't contain the
        target commit (e.g. because fetchRecurseSubmodules isn't set), then the
        submodule is fetched using the fetch options supplied in options.

        Parameters:

        init
            If the submodule is not initialized, setting this flag to True will
            initialize the submodule before updating. Otherwise, this will raise
            an error if attempting to update an uninitialized repository.

        callbacks
            Optional RemoteCallbacks to clone or fetch the submodule.

        depth
            Number of commits to fetch.
            The default is 0 (full commit history).
        """

        opts = ffi.new('git_submodule_update_options *')
        C.git_submodule_update_options_init(
            opts, C.GIT_SUBMODULE_UPDATE_OPTIONS_VERSION
        )
        opts.fetch_opts.depth = depth

        with git_fetch_options(callbacks, opts=opts.fetch_opts) as payload:
            err = C.git_submodule_update(self._subm, int(init), opts)
            payload.check_error(err)

    def reload(self, force: bool = False) -> None:
        """
        Reread submodule info from config, index, and HEAD.

        Call this to reread cached submodule information for this submodule if
        you have reason to believe that it has changed.

        Parameters:

        force
            Force reload even if the data doesn't seem out of date
        """
        err = C.git_submodule_reload(self._subm, int(force))
        check_error(err)

    @property
    def name(self):
        """Name of the submodule."""
        name = C.git_submodule_name(self._subm)
        return ffi.string(name).decode('utf-8')

    @property
    def path(self):
        """Path of the submodule."""
        path = C.git_submodule_path(self._subm)
        return ffi.string(path).decode('utf-8')

    @property
    def url(self) -> str | None:
        """URL of the submodule."""
        url = C.git_submodule_url(self._subm)
        return maybe_string(url)

    @url.setter
    def url(self, url: str) -> None:
        crepo = self._repo._repo
        cname = ffi.new('char[]', to_bytes(self.name))
        curl = ffi.new('char[]', to_bytes(url))
        err = C.git_submodule_set_url(crepo, cname, curl)
        check_error(err)

    @property
    def branch(self):
        """Branch that is to be tracked by the submodule."""
        branch = C.git_submodule_branch(self._subm)
        return ffi.string(branch).decode('utf-8')

    @property
    def head_id(self) -> Oid | None:
        """
        The submodule's HEAD commit id (as recorded in the superproject's
        current HEAD tree).
        Returns None if the superproject's HEAD doesn't contain the submodule.
        """

        head = C.git_submodule_head_id(self._subm)
        if head == ffi.NULL:
            return None
        return Oid(raw=bytes(ffi.buffer(head.id)[:]))


class SubmoduleCollection:
    """Collection of submodules in a repository."""

    def __init__(self, repository: BaseRepository):
        self._repository = repository

    def __getitem__(self, name: str | Path) -> Submodule:
        """
        Look up submodule information by name or path.
        Raises KeyError if there is no such submodule.
        """
        csub = ffi.new('git_submodule **')
        cpath = ffi.new('char[]', to_bytes(name))

        err = C.git_submodule_lookup(csub, self._repository._repo, cpath)
        check_error(err)
        return Submodule._from_c(self._repository, csub[0])

    def __contains__(self, name: str) -> bool:
        return self.get(name) is not None

    def __iter__(self) -> Iterator[Submodule]:
        for s in self._repository.listall_submodules():
            yield self[s]

    def get(self, name: str) -> Submodule | None:
        """
        Look up submodule information by name or path.
        Unlike __getitem__, this returns None if the submodule is not found.
        """
        try:
            return self[name]
        except KeyError:
            return None

    def add(
        self,
        url: str,
        path: str,
        link: bool = True,
        callbacks: Optional[RemoteCallbacks] = None,
        depth: int = 0,
    ) -> Submodule:
        """
        Add a submodule to the index.
        The submodule is automatically cloned.

        Returns: the submodule that was added.

        Parameters:

        url
            The URL of the submodule.

        path
            The path within the parent repository to add the submodule

        link
            Should workdir contain a gitlink to the repo in `.git/modules` vs. repo directly in workdir.

        callbacks
            Optional RemoteCallbacks to clone the submodule.

        depth
            Number of commits to fetch.
            The default is 0 (full commit history).
        """
        csub = ffi.new('git_submodule **')
        curl = ffi.new('char[]', to_bytes(url))
        cpath = ffi.new('char[]', to_bytes(path))
        gitlink = 1 if link else 0

        err = C.git_submodule_add_setup(
            csub, self._repository._repo, curl, cpath, gitlink
        )
        check_error(err)

        submodule_instance = Submodule._from_c(self._repository, csub[0])

        # Prepare options
        opts = ffi.new('git_submodule_update_options *')
        C.git_submodule_update_options_init(
            opts, C.GIT_SUBMODULE_UPDATE_OPTIONS_VERSION
        )
        opts.fetch_opts.depth = depth

        with git_fetch_options(callbacks, opts=opts.fetch_opts) as payload:
            crepo = ffi.new('git_repository **')
            err = C.git_submodule_clone(crepo, submodule_instance._subm, opts)
            payload.check_error(err)

        # Clean up submodule repository
        from .repository import Repository

        Repository._from_c(crepo[0], True)

        err = C.git_submodule_add_finalize(submodule_instance._subm)
        check_error(err)
        return submodule_instance

    def init(
        self, submodules: Optional[Iterable[str]] = None, overwrite: bool = False
    ) -> None:
        """
        Initialize submodules in the repository. Just like "git submodule init",
        this copies information about the submodules into ".git/config".

        Parameters:

        submodules
            Optional list of submodule paths or names to initialize.
            Default argument initializes all submodules.

        overwrite
            Flag indicating if initialization should overwrite submodule entries.
        """
        if submodules is None:
            submodules = self._repository.listall_submodules()

        instances = [self[s] for s in submodules]

        for submodule in instances:
            submodule.init(overwrite)

    def update(
        self,
        submodules: Optional[Iterable[str]] = None,
        init: bool = False,
        callbacks: Optional[RemoteCallbacks] = None,
        depth: int = 0,
    ) -> None:
        """
        Update submodules. This will clone a missing submodule and checkout
        the subrepository to the commit specified in the index of the
        containing repository. If the submodule repository doesn't contain the
        target commit (e.g. because fetchRecurseSubmodules isn't set), then the
        submodule is fetched using the fetch options supplied in options.

        Parameters:

        submodules
            Optional list of submodule paths or names. If you omit this parameter
            or pass None, all submodules will be updated.

        init
            If the submodule is not initialized, setting this flag to True will
            initialize the submodule before updating. Otherwise, this will raise
            an error if attempting to update an uninitialized repository.

        callbacks
            Optional RemoteCallbacks to clone or fetch the submodule.

        depth
            Number of commits to fetch.
            The default is 0 (full commit history).
        """
        if submodules is None:
            submodules = self._repository.listall_submodules()

        instances = [self[s] for s in submodules]

        for submodule in instances:
            submodule.update(init, callbacks, depth)

    def status(
        self, name: str, ignore: SubmoduleIgnore = SubmoduleIgnore.UNSPECIFIED
    ) -> SubmoduleStatus:
        """
        Get the status of a submodule.

        Returns: A combination of SubmoduleStatus flags.

        Parameters:

        name
            Submodule name or path.

        ignore
            A SubmoduleIgnore value indicating how deeply to examine the working directory.
        """
        cstatus = ffi.new('unsigned int *')
        err = C.git_submodule_status(
            cstatus, self._repository._repo, to_bytes(name), ignore
        )
        check_error(err)
        return SubmoduleStatus(cstatus[0])

    def cache_all(self) -> None:
        """
        Load and cache all submodules in the repository.

        Because the `.gitmodules` file is unstructured, loading submodules is an
        O(N) operation.  Any operation that requires accessing all submodules is O(N^2)
        in the number of submodules, if it has to look each one up individually.
        This function loads all submodules and caches them so that subsequent
        submodule lookups by name are O(1).
        """
        err = C.git_repository_submodule_cache_all(self._repository._repo)
        check_error(err)

    def cache_clear(self) -> None:
        """
        Clear the submodule cache populated by `submodule_cache_all`.
        If there is no cache, do nothing.

        The cache incorporates data from the repository's configuration, as well
        as the state of the working tree, the index, and HEAD. So any time any
        of these has changed, the cache might become invalid.
        """
        err = C.git_repository_submodule_cache_clear(self._repository._repo)
        check_error(err)
