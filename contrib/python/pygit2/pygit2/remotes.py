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

import warnings
from collections.abc import Generator, Iterator
from typing import TYPE_CHECKING, Any, Literal

# Import from pygit2
from pygit2 import RemoteCallbacks

from . import utils
from ._pygit2 import Oid
from .callbacks import (
    git_fetch_options,
    git_proxy_options,
    git_push_options,
    git_remote_callbacks,
)
from .enums import FetchPrune
from .errors import check_error
from .ffi import C, ffi
from .refspec import Refspec
from .utils import StrArray, maybe_string, strarray_to_strings, to_bytes

# Need BaseRepository for type hints, but don't let it cause a circular dependency
if TYPE_CHECKING:
    from ._libgit2.ffi import GitRemoteC, char_pointer
    from .repository import BaseRepository


class RemoteHead:
    """
    Description of a reference advertised by a remote server,
    given out on `Remote.list_heads` calls.
    """

    local: bool
    """Available locally"""

    oid: Oid

    loid: Oid

    name: str | None

    symref_target: str | None
    """
    If the server sent a symref mapping for this ref, this will
    point to the target. 
    """

    def __init__(self, c_struct: Any) -> None:
        self.local = bool(c_struct.local)
        self.oid = Oid(raw=bytes(ffi.buffer(c_struct.oid.id)[:]))
        self.loid = Oid(raw=bytes(ffi.buffer(c_struct.loid.id)[:]))
        self.name = maybe_string(c_struct.name)
        self.symref_target = maybe_string(c_struct.symref_target)


class PushUpdate:
    """
    Represents an update which will be performed on the remote during push.
    """

    src_refname: str
    """The source name of the reference"""

    dst_refname: str
    """The name of the reference to update on the server"""

    src: Oid
    """The current target of the reference"""

    dst: Oid
    """The new target for the reference"""

    def __init__(self, c_struct: Any) -> None:
        src_refname = maybe_string(c_struct.src_refname)
        dst_refname = maybe_string(c_struct.dst_refname)
        assert src_refname is not None, 'libgit2 returned null src_refname'
        assert dst_refname is not None, 'libgit2 returned null dst_refname'
        self.src_refname = src_refname
        self.dst_refname = dst_refname
        self.src = Oid(raw=bytes(ffi.buffer(c_struct.src.id)[:]))
        self.dst = Oid(raw=bytes(ffi.buffer(c_struct.dst.id)[:]))


class TransferProgress:
    """Progress downloading and indexing data during a fetch."""

    total_objects: int
    indexed_objects: int
    received_objects: int
    local_objects: int
    total_deltas: int
    indexed_deltas: int
    received_bytes: int

    def __init__(self, tp: Any) -> None:
        self.total_objects = tp.total_objects
        """Total number of objects to download"""

        self.indexed_objects = tp.indexed_objects
        """Objects which have been indexed"""

        self.received_objects = tp.received_objects
        """Objects which have been received up to now"""

        self.local_objects = tp.local_objects
        """Local objects which were used to fix the thin pack"""

        self.total_deltas = tp.total_deltas
        """Total number of deltas in the pack"""

        self.indexed_deltas = tp.indexed_deltas
        """Deltas which have been indexed"""

        self.received_bytes = tp.received_bytes
        """"Number of bytes received up to now"""


class Remote:
    def __init__(self, repo: BaseRepository, ptr: 'GitRemoteC') -> None:
        """The constructor is for internal use only."""
        self._repo = repo
        self._remote = ptr
        self._stored_exception = None

    def __del__(self) -> None:
        C.git_remote_free(self._remote)

    @property
    def name(self) -> str | None:
        """Name of the remote"""

        return maybe_string(C.git_remote_name(self._remote))

    @property
    def url(self) -> str | None:
        """Url of the remote"""

        return maybe_string(C.git_remote_url(self._remote))

    @property
    def push_url(self) -> str | None:
        """Push url of the remote"""

        return maybe_string(C.git_remote_pushurl(self._remote))

    def connect(
        self,
        callbacks: RemoteCallbacks | None = None,
        direction: int = C.GIT_DIRECTION_FETCH,
        proxy: None | bool | str = None,
    ) -> None:
        """Connect to the remote.

        Parameters:

        proxy : None or True or str
            Proxy configuration. Can be one of:

            * `None` (the default) to disable proxy usage
            * `True` to enable automatic proxy detection
            * an url to a proxy (`http://proxy.example.org:3128/`)
        """
        with git_proxy_options(self, proxy=proxy) as proxy_opts:
            with git_remote_callbacks(callbacks) as payload:
                err = C.git_remote_connect(
                    self._remote,
                    direction,
                    payload.remote_callbacks,
                    proxy_opts,
                    ffi.NULL,
                )
                payload.check_error(err)

    def fetch(
        self,
        refspecs: list[str] | None = None,
        message: str | None = None,
        callbacks: RemoteCallbacks | None = None,
        prune: FetchPrune = FetchPrune.UNSPECIFIED,
        proxy: None | Literal[True] | str = None,
        depth: int = 0,
    ) -> TransferProgress:
        """Perform a fetch against this remote. Returns a <TransferProgress>
        object.

        Parameters:

        prune : enums.FetchPrune
            * `UNSPECIFIED`: use the configuration from the repository.
            * `PRUNE`: remove any remote branch in the local repository
               that does not exist in the remote.
            * `NO_PRUNE`: always keep the remote branches

        proxy : None or True or str
            Proxy configuration. Can be one of:

            * `None` (the default) to disable proxy usage
            * `True` to enable automatic proxy detection
            * an url to a proxy (`http://proxy.example.org:3128/`)

        depth : int
            Number of commits from the tip of each remote branch history to fetch.

            If non-zero, the number of commits from the tip of each remote
            branch history to fetch. If zero, all history is fetched.
            The default is 0 (all history is fetched).
        """
        with git_fetch_options(callbacks) as payload:
            opts = payload.fetch_options
            opts.prune = prune
            opts.depth = depth
            with git_proxy_options(self, payload.fetch_options.proxy_opts, proxy):
                with StrArray(refspecs) as arr:
                    err = C.git_remote_fetch(
                        self._remote, arr.ptr, opts, to_bytes(message)
                    )
                    payload.check_error(err)

        return TransferProgress(C.git_remote_stats(self._remote))

    def list_heads(
        self,
        callbacks: RemoteCallbacks | None = None,
        proxy: str | None | bool = None,
        connect: bool = True,
    ) -> list[RemoteHead]:
        """
        Get the list of references with which the server responds to a new
        connection.

        Parameters:

        callbacks : Passed to connect()

        proxy : Passed to connect()

        connect : Whether to connect to the remote first. You can pass False
        if the remote has already connected. The list remains available after
        disconnecting as long as a new connection is not initiated.
        """

        if connect:
            self.connect(callbacks=callbacks, proxy=proxy)

        refs_ptr = ffi.new('git_remote_head ***')
        size_ptr = ffi.new('size_t *')

        err = C.git_remote_ls(refs_ptr, size_ptr, self._remote)
        check_error(err)

        num_refs = int(size_ptr[0])
        results = [RemoteHead(refs_ptr[0][i]) for i in range(num_refs)]

        return results

    def ls_remotes(
        self,
        callbacks: RemoteCallbacks | None = None,
        proxy: str | None | bool = None,
        connect: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Deprecated interface to list_heads
        """
        warnings.warn('Use list_heads', DeprecationWarning)

        heads = self.list_heads(callbacks, proxy, connect)

        return [
            {
                'local': h.local,
                'oid': h.oid,
                'loid': h.loid if h.local else None,
                'name': h.name,
                'symref_target': h.symref_target,
            }
            for h in heads
        ]

    def prune(self, callbacks: RemoteCallbacks | None = None) -> None:
        """Perform a prune against this remote."""
        with git_remote_callbacks(callbacks) as payload:
            err = C.git_remote_prune(self._remote, payload.remote_callbacks)
            payload.check_error(err)

    @property
    def refspec_count(self) -> int:
        """Total number of refspecs in this remote"""

        return C.git_remote_refspec_count(self._remote)

    def get_refspec(self, n: int) -> Refspec:
        """Return the <Refspec> object at the given position."""
        spec = C.git_remote_get_refspec(self._remote, n)
        return Refspec(self, spec)

    @property
    def fetch_refspecs(self) -> list[str]:
        """Refspecs that will be used for fetching"""

        specs = ffi.new('git_strarray *')
        err = C.git_remote_get_fetch_refspecs(specs, self._remote)
        check_error(err)
        return strarray_to_strings(specs)

    @property
    def push_refspecs(self) -> list[str]:
        """Refspecs that will be used for pushing"""

        specs = ffi.new('git_strarray *')
        err = C.git_remote_get_push_refspecs(specs, self._remote)
        check_error(err)
        return strarray_to_strings(specs)

    def push(
        self,
        specs: list[str],
        callbacks: RemoteCallbacks | None = None,
        proxy: None | bool | str = None,
        push_options: None | list[str] = None,
        threads: int = 1,
    ) -> None:
        """
        Push the given refspec to the remote. Raises ``GitError`` on protocol
        error or unpack failure.

        When the remote has a githook installed, that denies the reference this
        function will return successfully. Thus it is strongly recommended to
        install a callback, that implements
        :py:meth:`RemoteCallbacks.push_update_reference` and check the passed
        parameters for successful operations.

        Parameters:

        specs : [str]
            Push refspecs to use.

        callbacks :

        proxy : None or True or str
            Proxy configuration. Can be one of:

            * `None` (the default) to disable proxy usage
            * `True` to enable automatic proxy detection
            * an url to a proxy (`http://proxy.example.org:3128/`)

        push_options : [str]
            Push options to send to the server, which passes them to the
            pre-receive as well as the post-receive hook.

        threads : int
            If the transport being used to push to the remote requires the
            creation of a pack file, this controls the number of worker threads
            used by the packbuilder when creating that pack file to be sent to
            the remote.

            If set to 0, the packbuilder will auto-detect the number of threads
            to create. The default value is 1.
        """
        with git_push_options(callbacks) as payload:
            opts = payload.push_options
            opts.pb_parallelism = threads
            with git_proxy_options(self, payload.push_options.proxy_opts, proxy):
                with StrArray(specs) as refspecs, StrArray(push_options) as pushopts:
                    pushopts.assign_to(opts.remote_push_options)
                    err = C.git_remote_push(self._remote, refspecs.ptr, opts)
                    payload.check_error(err)


class RemoteCollection:
    """Collection of configured remotes

    You can use this class to look up and manage the remotes configured
    in a repository.  You can access repositories using index
    access. E.g. to look up the "origin" remote, you can use

    >>> repo.remotes["origin"]
    """

    def __init__(self, repo: BaseRepository) -> None:
        self._repo = repo

    def __len__(self) -> int:
        with utils.new_git_strarray() as names:
            err = C.git_remote_list(names, self._repo._repo)
            check_error(err)
            return names.count

    def __iter__(self) -> Iterator[Remote]:
        cremote = ffi.new('git_remote **')
        for name in self._ffi_names():
            err = C.git_remote_lookup(cremote, self._repo._repo, name)
            check_error(err)

            yield Remote(self._repo, cremote[0])

    def __getitem__(self, name: str | int) -> Remote:
        if isinstance(name, int):
            return list(self)[name]

        cremote = ffi.new('git_remote **')
        err = C.git_remote_lookup(cremote, self._repo._repo, to_bytes(name))
        check_error(err)

        return Remote(self._repo, cremote[0])

    def _ffi_names(self) -> Generator['char_pointer', None, None]:
        with utils.new_git_strarray() as names:
            err = C.git_remote_list(names, self._repo._repo)
            check_error(err)
            for i in range(names.count):
                yield names.strings[i]  # type: ignore[index]

    def names(self) -> Generator[str | None, None, None]:
        """An iterator over the names of the available remotes."""
        for name in self._ffi_names():
            yield maybe_string(name)

    def create(self, name: str, url: str, fetch: str | None = None) -> Remote:
        """Create a new remote with the given name and url. Returns a <Remote>
        object.

        If 'fetch' is provided, this fetch refspec will be used instead of the
        default.
        """
        cremote = ffi.new('git_remote **')

        name_bytes = to_bytes(name)
        url_bytes = to_bytes(url)
        if fetch:
            fetch_bytes = to_bytes(fetch)
            err = C.git_remote_create_with_fetchspec(
                cremote, self._repo._repo, name_bytes, url_bytes, fetch_bytes
            )
        else:
            err = C.git_remote_create(cremote, self._repo._repo, name_bytes, url_bytes)

        check_error(err)

        return Remote(self._repo, cremote[0])

    def create_anonymous(self, url: str) -> Remote:
        """Create a new anonymous (in-memory only) remote with the given URL.
        Returns a <Remote> object.
        """
        cremote = ffi.new('git_remote **')
        url_bytes = to_bytes(url)
        err = C.git_remote_create_anonymous(cremote, self._repo._repo, url_bytes)
        check_error(err)
        return Remote(self._repo, cremote[0])

    def rename(self, name: str, new_name: str) -> list[str]:
        """Rename a remote in the configuration. The refspecs in standard
        format will be renamed.

        Returns a list of fetch refspecs (list of strings) which were not in
        the standard format and thus could not be remapped.
        """

        if not name:
            raise ValueError('Current remote name must be a non-empty string')

        if not new_name:
            raise ValueError('New remote name must be a non-empty string')

        problems = ffi.new('git_strarray *')
        err = C.git_remote_rename(
            problems, self._repo._repo, to_bytes(name), to_bytes(new_name)
        )
        check_error(err)
        return strarray_to_strings(problems)

    def delete(self, name: str) -> None:
        """Remove a remote from the configuration

        All remote-tracking branches and configuration settings for the remote will be removed.
        """
        err = C.git_remote_delete(self._repo._repo, to_bytes(name))
        check_error(err)

    def set_url(self, name: str, url: str) -> None:
        """Set the URL for a remote"""
        err = C.git_remote_set_url(self._repo._repo, to_bytes(name), to_bytes(url))
        check_error(err)

    def set_push_url(self, name: str, url: str) -> None:
        """Set the push-URL for a remote"""
        err = C.git_remote_set_pushurl(self._repo._repo, to_bytes(name), to_bytes(url))
        check_error(err)

    def add_fetch(self, name: str, refspec: str) -> None:
        """Add a fetch refspec (str) to the remote"""

        err = C.git_remote_add_fetch(
            self._repo._repo, to_bytes(name), to_bytes(refspec)
        )
        check_error(err)

    def add_push(self, name: str, refspec: str) -> None:
        """Add a push refspec (str) to the remote"""

        err = C.git_remote_add_push(self._repo._repo, to_bytes(name), to_bytes(refspec))
        check_error(err)
