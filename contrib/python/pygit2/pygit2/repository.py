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

import tarfile
import warnings
from collections.abc import Callable, Iterator
from io import BytesIO
from pathlib import Path
from string import hexdigits
from time import time
from typing import TYPE_CHECKING, Optional, overload

# Import from pygit2
from ._pygit2 import (
    GIT_OID_HEXSZ,
    GIT_OID_MINPREFIXLEN,
    Blob,
    Commit,
    Diff,
    InvalidSpecError,
    Object,
    Oid,
    Patch,
    Reference,
    Signature,
    Tree,
    init_file_backend,
)
from ._pygit2 import Repository as _Repository
from .blame import Blame
from .branches import Branches
from .callbacks import (
    StashApplyCallbacks,
    git_checkout_options,
    git_stash_apply_options,
)
from .config import Config
from .enums import (
    AttrCheck,
    BlameFlag,
    CheckoutStrategy,
    DescribeStrategy,
    DiffOption,
    FileMode,
    MergeFavor,
    MergeFileFlag,
    MergeFlag,
    ObjectType,
    RepositoryOpenFlag,
    RepositoryState,
)
from .errors import check_error
from .ffi import C, ffi
from .index import Index, IndexEntry, MergeFileResult
from .packbuilder import PackBuilder
from .references import References
from .remotes import RemoteCollection
from .submodules import SubmoduleCollection
from .transaction import ReferenceTransaction
from .utils import StrArray, to_bytes

if TYPE_CHECKING:
    from pygit2._libgit2.ffi import (
        ArrayC,
        GitMergeOptionsC,
        GitRepositoryC,
        _Pointer,
        char,
    )
    from pygit2._pygit2 import Odb, Refdb, RefdbBackend


class BaseRepository(_Repository):
    _pointer: '_Pointer[GitRepositoryC]'
    _repo: 'GitRepositoryC'
    backend: 'RefdbBackend'
    default_signature: Signature
    head: Reference
    head_is_detached: bool
    head_is_unborn: bool
    is_bare: bool
    is_empty: bool
    is_shallow: bool
    odb: 'Odb'
    path: str
    refdb: 'Refdb'
    workdir: str
    references: References
    remotes: RemoteCollection
    branches: Branches
    submodules: SubmoduleCollection

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._common_init()

    def _common_init(self) -> None:
        self.branches = Branches(self)
        self.references = References(self)
        self.remotes = RemoteCollection(self)
        self.submodules = SubmoduleCollection(self)
        self._active_transaction = None

        # Get the pointer as the contents of a buffer and store it for
        # later access
        repo_cptr = ffi.new('git_repository **')
        ffi.buffer(repo_cptr)[:] = self._pointer[:]
        self._repo = repo_cptr[0]

    # Backwards compatible ODB access
    def read(self, oid: Oid | str) -> tuple[int, bytes]:
        """read(oid) -> type, data, size

        Read raw object data from the repository.
        """
        return self.odb.read(oid)

    def write(self, type: int, data: bytes | str) -> Oid:
        """write(type, data) -> Oid

        Write raw object data into the repository. First arg is the object
        type, the second one a buffer with data. Return the Oid of the created
        object."""
        return self.odb.write(type, data)

    def pack(
        self,
        path: str | Path | None = None,
        pack_delegate: Callable[[PackBuilder], None] | None = None,
        n_threads: int | None = None,
    ) -> int:
        """Pack the objects in the odb chosen by the pack_delegate function
        and write `.pack` and `.idx` files for them.

        Returns: the number of objects written to the pack

        Parameters:

        path
            The path to which the `.pack` and `.idx` files should be written. `None` will write to the default location.

        pack_delegate
            The method which will provide add the objects to the pack builder. Defaults to all objects.

        n_threads
            The number of threads the `PackBuilder` will spawn. If set to 0, libgit2 will autodetect the number of CPUs.
        """

        def pack_all_objects(pack_builder):
            for obj in self.odb:
                pack_builder.add(obj)

        pack_delegate = pack_delegate or pack_all_objects

        builder = PackBuilder(self)
        if n_threads is not None:
            builder.set_threads(n_threads)
        pack_delegate(builder)
        builder.write(path=path)

        return builder.written_objects_count

    def hashfile(
        self,
        path: str,
        object_type: ObjectType = ObjectType.BLOB,
        as_path: str | None = None,
    ) -> Oid:
        """Calculate the hash of a file using repository filtering rules.

        If you simply want to calculate the hash of a file on disk with no filters,
        you can just use `pygit2.hashfile()`. However, if you want to hash a file
        in the repository and you want to apply filtering rules (e.g. crlf filters)
        before generating the SHA, then use this function.

        Note: if the repository has `core.safecrlf` set to fail and the filtering
        triggers that failure, then this function will raise an error and not
        calculate the hash of the file.

        Returns: Output value of calculated SHA (Oid)

        Parameters:

        path
            Path to file on disk whose contents should be hashed. This may be
            an absolute path or a relative path, in which case it will be treated
            as a path within the working directory.

        object_type
            The object type to hash (e.g. enums.ObjectType.BLOB)

        as_path
            The path to use to look up filtering rules. If this is an empty string
            then no filters will be applied when calculating the hash.
            If this is `None` and the `path` parameter is a file within the
            repository's working directory, then the `path` will be used.
        """
        c_path = to_bytes(path)

        c_as_path: ffi.NULL_TYPE | bytes
        if as_path is None:
            c_as_path = ffi.NULL
        else:
            c_as_path = to_bytes(as_path)

        c_oid = ffi.new('git_oid *')

        err = C.git_repository_hashfile(
            c_oid, self._repo, c_path, int(object_type), c_as_path
        )
        check_error(err)

        oid = Oid(raw=bytes(ffi.buffer(c_oid.id)[:]))
        return oid

    def __iter__(self) -> Iterator[Oid]:
        return iter(self.odb)

    #
    # Mapping interface
    #
    def get(self, key: Oid | str, default: Optional[Commit] = None) -> None | Object:
        value = self.git_object_lookup_prefix(key)
        return value if (value is not None) else default

    def __getitem__(self, key: str | Oid) -> Object:
        value = self.git_object_lookup_prefix(key)
        if value is None:
            raise KeyError(key)
        return value

    def __contains__(self, key: str | Oid) -> bool:
        return self.git_object_lookup_prefix(key) is not None

    def __repr__(self) -> str:
        return f'pygit2.Repository({repr(self.path)})'

    #
    # Configuration
    #
    @property
    def config(self) -> Config:
        """The configuration file for this repository.

        If a the configuration hasn't been set yet, the default config for
        repository will be returned, including global and system configurations
        (if they are available).
        """
        cconfig = ffi.new('git_config **')
        err = C.git_repository_config(cconfig, self._repo)
        check_error(err)

        return Config.from_c(self, cconfig[0])

    @property
    def config_snapshot(self):
        """A snapshot for this repositiory's configuration

        This allows reads over multiple values to use the same version
        of the configuration files.
        """
        cconfig = ffi.new('git_config **')
        err = C.git_repository_config_snapshot(cconfig, self._repo)
        check_error(err)

        return Config.from_c(self, cconfig[0])

    #
    # References
    #
    def create_reference(
        self,
        name: str,
        target: Oid | str,
        force: bool = False,
        message: str | None = None,
    ) -> 'Reference':
        """Create a new reference "name" which points to an object or to
        another reference.

        Based on the type and value of the target parameter, this method tries
        to guess whether it is a direct or a symbolic reference.

        Keyword arguments:

        force: bool
            If True references will be overridden, otherwise (the default) an
            exception is raised.

        message: str
            Optional message to use for the reflog.

        Examples::

            repo.create_reference('refs/heads/foo', repo.head.target)
            repo.create_reference('refs/tags/foo', 'refs/heads/master')
            repo.create_reference('refs/tags/foo', 'bbb78a9cec580')
        """
        direct = isinstance(target, Oid) or (
            all(c in hexdigits for c in target)
            and GIT_OID_MINPREFIXLEN <= len(target) <= GIT_OID_HEXSZ
        )

        # duplicate isinstance call for mypy
        if direct or isinstance(target, Oid):
            return self.create_reference_direct(name, target, force, message=message)

        return self.create_reference_symbolic(name, target, force, message=message)

    def listall_references(self) -> list[str]:
        """Return a list with all the references in the repository."""
        return list(x.name for x in self.references.iterator())

    def listall_reference_objects(self) -> list[Reference]:
        """Return a list with all the reference objects in the repository."""
        return list(x for x in self.references.iterator())

    def resolve_refish(self, refish: str) -> tuple[Commit, Reference]:
        """Convert a reference-like short name "ref-ish" to a valid
        (commit, reference) pair.

        If ref-ish points to a commit, the reference element of the result
        will be None.

        Examples::

            repo.resolve_refish('mybranch')
            repo.resolve_refish('sometag')
            repo.resolve_refish('origin/master')
            repo.resolve_refish('bbb78a9')
        """
        try:
            reference = self.lookup_reference_dwim(refish)
        except (KeyError, InvalidSpecError):
            reference = None
            commit = self.revparse_single(refish)
        else:
            commit = reference.peel(Commit)  # type: ignore

        return (commit, reference)  # type: ignore

    def transaction(self) -> ReferenceTransaction:
        """Create a new reference transaction.

        Returns a context manager that commits all reference updates atomically
        when the context exits successfully, or performs no updates if an exception
        is raised.

        Example::

            with repo.transaction() as txn:
                txn.lock_ref('refs/heads/master')
                txn.set_target('refs/heads/master', new_oid, message='Update')
        """
        txn = ReferenceTransaction(self)
        return txn

    #
    # Checkout
    #

    def checkout_head(self, **kwargs):
        """Checkout HEAD

        For arguments, see Repository.checkout().
        """
        with git_checkout_options(**kwargs) as payload:
            err = C.git_checkout_head(self._repo, payload.checkout_options)
            payload.check_error(err)

    def checkout_index(self, index=None, **kwargs):
        """Checkout the given index or the repository's index

        For arguments, see Repository.checkout().
        """
        with git_checkout_options(**kwargs) as payload:
            err = C.git_checkout_index(
                self._repo,
                index._index if index else ffi.NULL,
                payload.checkout_options,
            )
            payload.check_error(err)

    def checkout_tree(self, treeish, **kwargs):
        """Checkout the given treeish

        For arguments, see Repository.checkout().
        """
        with git_checkout_options(**kwargs) as payload:
            cptr = ffi.new('git_object **')
            ffi.buffer(cptr)[:] = treeish._pointer[:]
            err = C.git_checkout_tree(self._repo, cptr[0], payload.checkout_options)
            payload.check_error(err)

    def checkout(
        self,
        refname: str | None | Reference = None,
        **kwargs,
    ) -> None:
        """
        Checkout the given reference using the given strategy, and update the
        HEAD.
        The reference may be a reference name or a Reference object.
        The default strategy is SAFE | RECREATE_MISSING.

        If no reference is given, checkout from the index.

        Parameters:

        refname : str or Reference
            The reference to checkout. After checkout, the current branch will
            be switched to this one.

        strategy : CheckoutStrategy
            A ``CheckoutStrategy`` value. The default is ``SAFE | RECREATE_MISSING``.

        directory : str
            Alternative checkout path to workdir.

        paths : list[str]
            A list of files to checkout from the given reference.
            If paths is provided, HEAD will not be set to the reference.

        callbacks : CheckoutCallbacks
            Optional. Supply a `callbacks` object to get information about
            conflicted files, updated files, etc. as the checkout is being
            performed. The callbacks can also abort the checkout prematurely.

            The callbacks should be an object which inherits from
            `pyclass:CheckoutCallbacks`. It should implement the callbacks
            as overridden methods.

        Examples:

        * To checkout from the HEAD, just pass 'HEAD'::

            >>> checkout('HEAD')

          This is identical to calling checkout_head().
        """

        # Case 1: Checkout index
        if refname is None:
            return self.checkout_index(**kwargs)

        # Case 2: Checkout head
        if refname == 'HEAD':
            return self.checkout_head(**kwargs)

        # Case 3: Reference
        if isinstance(refname, Reference):
            reference = refname
            refname = refname.name
        else:
            reference = self.lookup_reference(refname)

        oid = reference.resolve().target
        treeish = self[oid]
        self.checkout_tree(treeish, **kwargs)

        if 'paths' not in kwargs:
            self.set_head(refname)

    #
    # Setting HEAD
    #
    def set_head(self, target: Oid | str) -> None:
        """
        Set HEAD to point to the given target.

        Parameters:

        target
            The new target for HEAD. Can be a string or Oid (to detach).
        """

        if isinstance(target, Oid):
            oid = ffi.new('git_oid *')
            ffi.buffer(oid)[:] = target.raw[:]
            err = C.git_repository_set_head_detached(self._repo, oid)
            check_error(err)
            return

        # if it's a string, then it's a reference name
        err = C.git_repository_set_head(self._repo, to_bytes(target))
        check_error(err)

    #
    # Diff
    #
    def __whatever_to_tree_or_blob(self, obj):
        if obj is None:
            return None

        # If it's a string, then it has to be valid revspec
        if isinstance(obj, str) or isinstance(obj, bytes):
            obj = self.revparse_single(obj)
        elif isinstance(obj, Oid):
            obj = self[obj]

        # First we try to get to a blob
        try:
            obj = obj.peel(Blob)
        except Exception:
            # And if that failed, try to get a tree, raising a type
            # error if that still doesn't work
            try:
                obj = obj.peel(Tree)
            except Exception:
                raise TypeError(f'unexpected "{type(obj)}"')

        return obj

    @overload
    def diff(
        self,
        a: None | str | bytes | Commit | Oid | Reference = None,
        b: None | str | bytes | Commit | Oid | Reference = None,
        cached: bool = False,
        flags: DiffOption = DiffOption.NORMAL,
        context_lines: int = 3,
        interhunk_lines: int = 0,
    ) -> Diff: ...
    @overload
    def diff(
        self,
        a: Blob | None = None,
        b: Blob | None = None,
        cached: bool = False,
        flags: DiffOption = DiffOption.NORMAL,
        context_lines: int = 3,
        interhunk_lines: int = 0,
    ) -> Patch: ...
    def diff(
        self,
        a: None | Blob | str | bytes | Commit | Oid | Reference = None,
        b: None | Blob | str | bytes | Commit | Oid | Reference = None,
        cached: bool = False,
        flags: DiffOption = DiffOption.NORMAL,
        context_lines: int = 3,
        interhunk_lines: int = 0,
    ) -> Diff | Patch:
        """
        Show changes between the working tree and the index or a tree,
        changes between the index and a tree, changes between two trees, or
        changes between two blobs.

        Keyword arguments:

        a
            None, a str (that refers to an Object, see revparse_single()) or a
            Reference object.
            If None, b must be None, too. In this case the working directory is
            compared with the index. Otherwise the referred object is compared to
            'b'.

        b
            None, a str (that refers to an Object, see revparse_single()) or a
            Reference object.
            If None, the working directory is compared to 'a'. (except
            'cached' is True, in which case the index is compared to 'a').
            Otherwise the referred object is compared to 'a'

        cached
            If 'b' is None, by default the working directory is compared to 'a'.
            If 'cached' is set to True, the index/staging area is used for comparing.

        flag
            A combination of enums.DiffOption constants.

        context_lines
            The number of unchanged lines that define the boundary of a hunk
            (and to display before and after)

        interhunk_lines
            The maximum number of unchanged lines between hunk boundaries
            before the hunks will be merged into a one

        Examples::

          # Changes in the working tree not yet staged for the next commit
          >>> diff()

          # Changes between the index and your last commit
          >>> diff(cached=True)

          # Changes in the working tree since your last commit
          >>> diff('HEAD')

          # Changes between commits
          >>> t0 = revparse_single('HEAD')
          >>> t1 = revparse_single('HEAD^')
          >>> diff(t0, t1)
          >>> diff('HEAD', 'HEAD^') # equivalent

        If you want to diff a tree against an empty tree, use the low level
        API (Tree.diff_to_tree()) directly.
        """

        a = self.__whatever_to_tree_or_blob(a)
        b = self.__whatever_to_tree_or_blob(b)

        options = {
            'flags': int(flags),
            'context_lines': context_lines,
            'interhunk_lines': interhunk_lines,
        }

        # Case 1: Diff tree to tree
        if isinstance(a, Tree) and isinstance(b, Tree):
            return a.diff_to_tree(b, **options)  # type: ignore[arg-type]

        # Case 2: Index to workdir
        elif a is None and b is None:
            return self.index.diff_to_workdir(**options)

        # Case 3: Diff tree to index or workdir
        elif isinstance(a, Tree) and b is None:
            if cached:
                return a.diff_to_index(self.index, **options)  # type: ignore[arg-type]
            else:
                return a.diff_to_workdir(**options)  # type: ignore[arg-type]

        # Case 4: Diff blob to blob
        if isinstance(a, Blob) and isinstance(b, Blob):
            return a.diff(b, **options)  # type: ignore[arg-type]

        raise ValueError('Only blobs and treeish can be diffed')

    def state(self) -> RepositoryState:
        """Determines the state of a git repository - ie, whether an operation
        (merge, cherry-pick, etc) is in progress.

        Returns a RepositoryState constant.
        """
        cstate: int = C.git_repository_state(self._repo)
        try:
            return RepositoryState(cstate)
        except ValueError:
            # Some value not in the IntEnum - newer libgit2 version?
            return cstate  # type: ignore[return-value]

    def state_cleanup(self) -> None:
        """Remove all the metadata associated with an ongoing command like
        merge, revert, cherry-pick, etc. For example: MERGE_HEAD, MERGE_MSG,
        etc.
        """
        C.git_repository_state_cleanup(self._repo)

    #
    # blame
    #
    def blame(
        self,
        path: str,
        flags: BlameFlag = BlameFlag.NORMAL,
        min_match_characters: int | None = None,
        newest_commit: Oid | str | None = None,
        oldest_commit: Oid | str | None = None,
        min_line: int | None = None,
        max_line: int | None = None,
    ) -> Blame:
        """
        Return a Blame object for a single file.

        Parameters:

        path
            Path to the file to blame.

        flags
            An enums.BlameFlag constant.

        min_match_characters
            The number of alphanum chars that must be detected as moving/copying
            within a file for it to associate those lines with the parent commit.

        newest_commit
            The id of the newest commit to consider.

        oldest_commit
            The id of the oldest commit to consider.

        min_line
            The first line in the file to blame.

        max_line
            The last line in the file to blame.

        Examples::

            repo.blame('foo.c', flags=enums.BlameFlag.IGNORE_WHITESPACE)
        """

        options = ffi.new('git_blame_options *')

        C.git_blame_options_init(options, C.GIT_BLAME_OPTIONS_VERSION)
        if flags:
            options.flags = int(flags)
        if min_match_characters:
            options.min_match_characters = min_match_characters
        if newest_commit:
            if not isinstance(newest_commit, Oid):
                newest_commit = Oid(hex=newest_commit)
            ffi.buffer(ffi.addressof(options, 'newest_commit'))[:] = newest_commit.raw
        if oldest_commit:
            if not isinstance(oldest_commit, Oid):
                oldest_commit = Oid(hex=oldest_commit)
            ffi.buffer(ffi.addressof(options, 'oldest_commit'))[:] = oldest_commit.raw
        if min_line:
            options.min_line = min_line
        if max_line:
            options.max_line = max_line

        cblame = ffi.new('git_blame **')
        err = C.git_blame_file(cblame, self._repo, to_bytes(path), options)
        check_error(err)

        return Blame._from_c(self, cblame[0])

    #
    # Index
    #
    @property
    def index(self):
        """Index representing the repository's index file."""
        cindex = ffi.new('git_index **')
        err = C.git_repository_index(cindex, self._repo)
        check_error(err, io=True)

        return Index.from_c(self, cindex)

    #
    # Merging
    #
    @staticmethod
    def _merge_options(
        favor: int | MergeFavor, flags: int | MergeFlag, file_flags: int | MergeFileFlag
    ) -> 'GitMergeOptionsC':
        """Return a 'git_merge_opts *'"""

        # Check arguments type
        if not isinstance(favor, (int, MergeFavor)):
            raise TypeError('favor argument must be MergeFavor')

        if not isinstance(flags, (int, MergeFlag)):
            raise TypeError('flags argument must be MergeFlag')

        if not isinstance(file_flags, (int, MergeFileFlag)):
            raise TypeError('file_flags argument must be MergeFileFlag')

        opts = ffi.new('git_merge_options *')
        err = C.git_merge_options_init(opts, C.GIT_MERGE_OPTIONS_VERSION)
        check_error(err)

        opts.file_favor = int(favor)
        opts.flags = int(flags)
        opts.file_flags = int(file_flags)

        return opts

    def merge_file_from_index(
        self,
        ancestor: 'IndexEntry | None',
        ours: 'IndexEntry | None',
        theirs: 'IndexEntry | None',
        use_deprecated: bool = True,
    ) -> 'str | MergeFileResult | None':
        """Merge files from index.

        Returns: A string with the content of the file containing
        possible conflicts if use_deprecated==True.
        If use_deprecated==False then it returns an instance of MergeFileResult.

        ancestor
            The index entry which will be used as a common
            ancestor.
        ours
            The index entry to take as "ours" or base.
        theirs
            The index entry which will be merged into "ours"
        use_deprecated
            This controls what will be returned. If use_deprecated==True (default),
            a string with the contents of the file will be returned.
            An instance of MergeFileResult will be returned otherwise.
        """
        cmergeresult = ffi.new('git_merge_file_result *')

        cancestor, ancestor_str_ref = (
            ancestor._to_c() if ancestor is not None else (ffi.NULL, ffi.NULL)
        )
        cours, ours_str_ref = ours._to_c() if ours is not None else (ffi.NULL, ffi.NULL)
        ctheirs, theirs_str_ref = (
            theirs._to_c() if theirs is not None else (ffi.NULL, ffi.NULL)
        )

        err = C.git_merge_file_from_index(
            cmergeresult, self._repo, cancestor, cours, ctheirs, ffi.NULL
        )
        check_error(err)

        mergeFileResult = MergeFileResult._from_c(cmergeresult)
        C.git_merge_file_result_free(cmergeresult)

        if use_deprecated:
            warnings.warn(
                'Getting an str from Repository.merge_file_from_index is deprecated. '
                'The method will later return an instance of MergeFileResult by default, instead. '
                'Check parameter use_deprecated.',
                DeprecationWarning,
            )
            return mergeFileResult.contents if mergeFileResult else ''

        return mergeFileResult

    def merge_commits(
        self,
        ours: str | Oid | Commit,
        theirs: str | Oid | Commit,
        favor: MergeFavor = MergeFavor.NORMAL,
        flags: MergeFlag = MergeFlag.FIND_RENAMES,
        file_flags: MergeFileFlag = MergeFileFlag.DEFAULT,
    ) -> 'Index':
        """
        Merge two arbitrary commits.

        Returns: an index with the result of the merge.

        Parameters:

        ours
            The commit to take as "ours" or base.

        theirs
            The commit which will be merged into "ours"

        favor
            An enums.MergeFavor constant specifying how to deal with file-level conflicts.
            For all but NORMAL, the index will not record a conflict.

        flags
            A combination of enums.MergeFlag constants.

        file_flags
            A combination of enums.MergeFileFlag constants.

        Both "ours" and "theirs" can be any object which peels to a commit or
        the id (string or Oid) of an object which peels to a commit.
        """
        ours_ptr = ffi.new('git_commit **')
        theirs_ptr = ffi.new('git_commit **')
        cindex = ffi.new('git_index **')

        if isinstance(ours, (str, Oid)):
            ours_object = self[ours]
            if not isinstance(ours_object, Commit):
                raise TypeError(f'expected Commit, got {type(ours_object)}')
            ours = ours_object
        if isinstance(theirs, (str, Oid)):
            theirs_object = self[theirs]
            if not isinstance(theirs_object, Commit):
                raise TypeError(f'expected Commit, got {type(theirs_object)}')
            theirs = theirs_object

        ours = ours.peel(Commit)
        theirs = theirs.peel(Commit)

        opts = self._merge_options(favor, flags, file_flags)

        ffi.buffer(ours_ptr)[:] = ours._pointer[:]
        ffi.buffer(theirs_ptr)[:] = theirs._pointer[:]

        err = C.git_merge_commits(cindex, self._repo, ours_ptr[0], theirs_ptr[0], opts)
        check_error(err)

        return Index.from_c(self, cindex)

    def merge_trees(
        self,
        ancestor: str | Oid | Tree,
        ours: str | Oid | Tree,
        theirs: str | Oid | Tree,
        favor: MergeFavor = MergeFavor.NORMAL,
        flags: MergeFlag = MergeFlag.FIND_RENAMES,
        file_flags: MergeFileFlag = MergeFileFlag.DEFAULT,
    ) -> 'Index':
        """
        Merge two trees.

        Returns: an Index that reflects the result of the merge.

        Parameters:

        ancestor
            The tree which is the common ancestor between 'ours' and 'theirs'.

        ours
            The commit to take as "ours" or base.

        theirs
            The commit which will be merged into "ours".

        favor
            An enums.MergeFavor constant specifying how to deal with file-level conflicts.
            For all but NORMAL, the index will not record a conflict.

        flags
            A combination of enums.MergeFlag constants.

        file_flags
            A combination of enums.MergeFileFlag constants.
        """
        ancestor_ptr = ffi.new('git_tree **')
        ours_ptr = ffi.new('git_tree **')
        theirs_ptr = ffi.new('git_tree **')
        cindex = ffi.new('git_index **')

        ancestor = self.__ensure_tree(ancestor)
        ours = self.__ensure_tree(ours)
        theirs = self.__ensure_tree(theirs)

        opts = self._merge_options(favor, flags, file_flags)

        ffi.buffer(ancestor_ptr)[:] = ancestor._pointer[:]
        ffi.buffer(ours_ptr)[:] = ours._pointer[:]
        ffi.buffer(theirs_ptr)[:] = theirs._pointer[:]

        err = C.git_merge_trees(
            cindex, self._repo, ancestor_ptr[0], ours_ptr[0], theirs_ptr[0], opts
        )
        check_error(err)

        return Index.from_c(self, cindex)

    def merge(
        self,
        source: Reference | Commit | Oid | str,
        favor: MergeFavor = MergeFavor.NORMAL,
        flags: MergeFlag = MergeFlag.FIND_RENAMES,
        file_flags: MergeFileFlag = MergeFileFlag.DEFAULT,
    ) -> None:
        """
        Merges the given Reference or Commit into HEAD.

        Merges the given commit into HEAD, writing the results into the working directory.
        Any changes are staged for commit and any conflicts are written to the index.
        Callers should inspect the repository's index after this completes,
        resolve any conflicts and prepare a commit.

        Parameters:

        source
            The Reference, Commit, or commit Oid to merge into HEAD.
            It is preferable to pass in a Reference, because this enriches the
            merge with additional information (for example, Repository.message will
            specify the name of the branch being merged).
            Previous versions of pygit2 allowed passing in a partial commit
            hash as a string; this is deprecated.

        favor
            An enums.MergeFavor constant specifying how to deal with file-level conflicts.
            For all but NORMAL, the index will not record a conflict.

        flags
            A combination of enums.MergeFlag constants.

        file_flags
            A combination of enums.MergeFileFlag constants.
        """

        if isinstance(source, Reference):
            # Annotated commit from ref
            cptr = ffi.new('struct git_reference **')
            ffi.buffer(cptr)[:] = source._pointer[:]  # type: ignore[attr-defined]
            commit_ptr = ffi.new('git_annotated_commit **')
            err = C.git_annotated_commit_from_ref(commit_ptr, self._repo, cptr[0])
            check_error(err)
        else:
            # Annotated commit from commit id
            if isinstance(source, str):
                # For backwards compatibility, parse a string as a partial commit hash
                warnings.warn(
                    'Passing str to Repository.merge is deprecated. '
                    'Pass Commit, Oid, or a Reference (such as a Branch) instead.',
                    DeprecationWarning,
                )
                oid = self[source].peel(Commit).id
            elif isinstance(source, Commit):
                oid = source.id
            elif isinstance(source, Oid):
                oid = source
            else:
                raise TypeError('expected Reference, Commit, or Oid')
            c_id = ffi.new('git_oid *')
            ffi.buffer(c_id)[:] = oid.raw[:]
            commit_ptr = ffi.new('git_annotated_commit **')
            err = C.git_annotated_commit_lookup(commit_ptr, self._repo, c_id)
            check_error(err)

        merge_opts = self._merge_options(favor, flags, file_flags)

        checkout_opts = ffi.new('git_checkout_options *')
        C.git_checkout_options_init(checkout_opts, 1)
        checkout_opts.checkout_strategy = int(
            CheckoutStrategy.SAFE | CheckoutStrategy.RECREATE_MISSING
        )

        err = C.git_merge(self._repo, commit_ptr, 1, merge_opts, checkout_opts)
        C.git_annotated_commit_free(commit_ptr[0])
        check_error(err)

    #
    # Prepared message (MERGE_MSG)
    #
    @property
    def raw_message(self) -> bytes:
        """
        Retrieve git's prepared message (bytes).
        See `Repository.message` for more information.
        """
        buf = ffi.new('git_buf *', (ffi.NULL, 0))
        try:
            err = C.git_repository_message(buf, self._repo)
            if err == C.GIT_ENOTFOUND:
                return b''
            check_error(err)
            return ffi.string(buf.ptr)
        finally:
            C.git_buf_dispose(buf)

    @property
    def message(self) -> str:
        """
        Retrieve git's prepared message.

        Operations such as git revert/cherry-pick/merge with the -n option stop
        just short of creating a commit with the changes and save their
        prepared message in .git/MERGE_MSG so the next git-commit execution can
        present it to the user for them to amend if they wish.

        Use this function to get the contents of this file. Don't forget to
        call `Repository.remove_message()` after you create the commit.

        Note that the message is also removed by `Repository.state_cleanup()`.

        If there is no such message, an empty string is returned.
        """
        return self.raw_message.decode('utf-8')

    def remove_message(self) -> None:
        """
        Remove git's prepared message.
        """
        err = C.git_repository_message_remove(self._repo)
        check_error(err)

    #
    # Describe
    #
    def describe(
        self,
        committish: str | Reference | Commit | None = None,
        max_candidates_tags: int | None = None,
        describe_strategy: DescribeStrategy = DescribeStrategy.DEFAULT,
        pattern: str | None = None,
        only_follow_first_parent: bool | None = None,
        show_commit_oid_as_fallback: bool | None = None,
        abbreviated_size: int | None = None,
        always_use_long_format: bool | None = None,
        dirty_suffix: str | None = None,
    ) -> str:
        """
        Describe a commit-ish or the current working tree.

        Returns: The description (str).

        Parameters:

        committish : `str`, :class:`~.Reference`, or :class:`~.Commit`
            Commit-ish object or object name to describe, or `None` to describe
            the current working tree.

        max_candidates_tags : int
            The number of candidate tags to consider. Increasing above 10 will
            take slightly longer but may produce a more accurate result. A
            value of 0 will cause only exact matches to be output.

        describe_strategy : DescribeStrategy
            Can be one of:

            * `DescribeStrategy.DEFAULT` - Only match annotated tags.
            * `DescribeStrategy.TAGS` - Match everything under refs/tags/
              (includes lightweight tags).
            * `DescribeStrategy.ALL` - Match everything under refs/ (includes
              branches).

        pattern : str
            Only consider tags matching the given `glob(7)` pattern, excluding
            the "refs/tags/" prefix.

        only_follow_first_parent : bool
            Follow only the first parent commit upon seeing a merge commit.

        show_commit_oid_as_fallback : bool
            Show uniquely abbreviated commit object as fallback.

        abbreviated_size : int
            The minimum number of hexadecimal digits to show for abbreviated
            object names. A value of 0 will suppress long format, only showing
            the closest tag.

        always_use_long_format : bool
            Always output the long format (the nearest tag, the number of
            commits, and the abbreviated commit name) even when the committish
            matches a tag.

        dirty_suffix : str
            A string to append if the working tree is dirty.

        Example::

            repo.describe(pattern='public/*', dirty_suffix='-dirty')
        """

        options = ffi.new('git_describe_options *')
        C.git_describe_options_init(options, C.GIT_DESCRIBE_OPTIONS_VERSION)

        if max_candidates_tags is not None:
            options.max_candidates_tags = max_candidates_tags
        if describe_strategy is not None:
            options.describe_strategy = int(describe_strategy)
        if pattern:
            # The returned pointer object has ownership on the allocated
            # memory. Make sure it is kept alive until git_describe_commit() or
            # git_describe_workdir() are called below.
            pattern_char = ffi.new('char[]', to_bytes(pattern))
            options.pattern = pattern_char
        if only_follow_first_parent is not None:
            options.only_follow_first_parent = only_follow_first_parent
        if show_commit_oid_as_fallback is not None:
            options.show_commit_oid_as_fallback = show_commit_oid_as_fallback

        result = ffi.new('git_describe_result **')
        if committish:
            committish_rev: Object | Reference | Commit
            if isinstance(committish, str):
                committish_rev = self.revparse_single(committish)
            else:
                committish_rev = committish

            commit = committish_rev.peel(Commit)

            cptr = ffi.new('git_object **')
            ffi.buffer(cptr)[:] = commit._pointer[:]

            err = C.git_describe_commit(result, cptr[0], options)
        else:
            err = C.git_describe_workdir(result, self._repo, options)
        check_error(err)

        try:
            format_options = ffi.new('git_describe_format_options *')
            C.git_describe_init_format_options(
                format_options, C.GIT_DESCRIBE_FORMAT_OPTIONS_VERSION
            )

            if abbreviated_size is not None:
                format_options.abbreviated_size = abbreviated_size
            if always_use_long_format is not None:
                format_options.always_use_long_format = always_use_long_format
            dirty_ptr = None
            if dirty_suffix:
                dirty_ptr = ffi.new('char[]', to_bytes(dirty_suffix))
                format_options.dirty_suffix = dirty_ptr

            buf = ffi.new('git_buf *', (ffi.NULL, 0))

            err = C.git_describe_format(buf, result[0], format_options)
            check_error(err)

            try:
                return ffi.string(buf.ptr).decode('utf-8')
            finally:
                C.git_buf_dispose(buf)
        finally:
            C.git_describe_result_free(result[0])

    #
    # Stash
    #
    def stash(
        self,
        stasher: Signature,
        message: str | None = None,
        keep_index: bool = False,
        include_untracked: bool = False,
        include_ignored: bool = False,
        keep_all: bool = False,
        paths: list[str] | None = None,
    ) -> Oid:
        """
        Save changes to the working directory to the stash.

        Returns: The Oid of the stash merge commit (Oid).

        Parameters:

        stasher : Signature
            The identity of the person doing the stashing.

        message : str
            An optional description of stashed state.

        keep_index : bool
            Leave changes already added to the index in the working directory.

        include_untracked : bool
            Also stash untracked files.

        include_ignored : bool
            Also stash ignored files.

        keep_all : bool
            All changes in the index and working directory are left intact.

        paths : list[str]
            An optional list of paths that control which files are stashed.

        Example::

            >>> repo = pygit2.Repository('.')
            >>> repo.stash(repo.default_signature(), 'WIP: stashing')
        """

        opts = ffi.new('git_stash_save_options *')
        C.git_stash_save_options_init(opts, C.GIT_STASH_SAVE_OPTIONS_VERSION)

        flags = 0
        flags |= keep_index * C.GIT_STASH_KEEP_INDEX
        flags |= keep_all * C.GIT_STASH_KEEP_ALL
        flags |= include_untracked * C.GIT_STASH_INCLUDE_UNTRACKED
        flags |= include_ignored * C.GIT_STASH_INCLUDE_IGNORED
        opts.flags = flags

        stasher_cptr = ffi.new('git_signature **')
        ffi.buffer(stasher_cptr)[:] = stasher._pointer[:]
        opts.stasher = stasher_cptr[0]

        if message:
            message_ref = ffi.new('char[]', to_bytes(message))
            opts.message = message_ref

        if paths:
            arr = StrArray(paths)
            opts.paths = arr.ptr[0]  # type: ignore[index]

        coid = ffi.new('git_oid *')
        err = C.git_stash_save_with_opts(coid, self._repo, opts)

        check_error(err)

        return Oid(raw=bytes(ffi.buffer(coid)[:]))

    def stash_apply(
        self,
        index: int = 0,
        reinstate_index: bool = False,
        strategy: CheckoutStrategy | None = None,
        callbacks: StashApplyCallbacks | None = None,
    ) -> None:
        """
        Apply a stashed state in the stash list to the working directory.

        Parameters:

        index : int
            The position within the stash list of the stash to apply. 0 is the
            most recent stash.

        reinstate_index : bool
            Try to reinstate stashed changes to the index.

        callbacks : StashApplyCallbacks
            Optional. Supply a `callbacks` object to get information about
            the progress of the stash application as it is being performed.

            The callbacks should be an object which inherits from
            `pyclass:StashApplyCallbacks`. It should implement the callbacks
            as overridden methods.

            Note that this class inherits from CheckoutCallbacks, so you can
            also get information from the checkout part of the unstashing
            process via the callbacks.

        The checkout options may be customized using the same arguments taken by
        Repository.checkout().

        Example::

            >>> repo = pygit2.Repository('.')
            >>> repo.stash(repo.default_signature(), 'WIP: stashing')
            >>> repo.stash_apply(strategy=CheckoutStrategy.ALLOW_CONFLICTS)
        """
        with git_stash_apply_options(
            reinstate_index=reinstate_index,
            strategy=strategy,
            callbacks=callbacks,
        ) as payload:
            err = C.git_stash_apply(self._repo, index, payload.stash_apply_options)
            payload.check_error(err)

    def stash_drop(self, index: int = 0) -> None:
        """
        Remove a stashed state from the stash list.

        Parameters:

        index : int
            The position within the stash list of the stash to remove. 0 is
            the most recent stash.
        """
        check_error(C.git_stash_drop(self._repo, index))

    def stash_pop(
        self,
        index: int = 0,
        reinstate_index: bool = False,
        strategy: CheckoutStrategy | None = None,
        callbacks: StashApplyCallbacks | None = None,
    ) -> None:
        """Apply a stashed state and remove it from the stash list.

        For arguments, see Repository.stash_apply().
        """
        with git_stash_apply_options(
            reinstate_index=reinstate_index,
            strategy=strategy,
            callbacks=callbacks,
        ) as payload:
            err = C.git_stash_pop(self._repo, index, payload.stash_apply_options)
            payload.check_error(err)

    #
    # Utility for writing a tree into an archive
    #
    def write_archive(
        self,
        treeish: str | Tree | Object | Oid,
        archive: tarfile.TarFile,
        timestamp: int | None = None,
        prefix: str = '',
    ) -> None:
        """
        Write treeish into an archive.

        If no timestamp is provided and 'treeish' is a commit, its committer
        timestamp will be used. Otherwise the current time will be used.

        All path names in the archive are added to 'prefix', which defaults to
        an empty string.

        Parameters:

        treeish
            The treeish to write.

        archive
            An archive from the 'tarfile' module.

        timestamp
            Timestamp to use for the files in the archive.

        prefix
            Extra prefix to add to the path names in the archive.

        Example::

            >>> import tarfile, pygit2
            >>> with tarfile.open('foo.tar', 'w') as archive:
            >>>     repo = pygit2.Repository('.')
            >>>     repo.write_archive(repo.head.target, archive)
        """

        # Try to get a tree form whatever we got
        if isinstance(treeish, (str, Oid)):
            treeish = self[treeish]

        tree = treeish.peel(Tree)

        # if we don't have a timestamp, try to get it from a commit
        if not timestamp:
            try:
                commit = treeish.peel(Commit)
                timestamp = commit.committer.time
            except Exception:
                pass

        # as a last resort, use the current timestamp
        if not timestamp:
            timestamp = int(time())

        index = Index()
        index.read_tree(tree)

        for entry in index:
            content = self[entry.id].read_raw()
            info = tarfile.TarInfo(prefix + entry.path)
            info.size = len(content)
            info.mtime = timestamp
            info.uname = info.gname = 'root'  # just because git does this
            if entry.mode == FileMode.LINK:
                info.type = tarfile.SYMTYPE
                info.linkname = content.decode('utf-8')
                info.mode = 0o777  # symlinks get placeholder
                info.size = 0
                archive.addfile(info)
            else:
                info.mode = entry.mode
                archive.addfile(info, BytesIO(content))

    #
    # Ahead-behind, which mostly lives on its own namespace
    #
    def ahead_behind(self, local: Oid | str, upstream: Oid | str) -> tuple[int, int]:
        """
        Calculate how many different commits are in the non-common parts of the
        history between the two given ids.

        Ahead is how many commits are in the ancestry of the `local` commit
        which are not in the `upstream` commit. Behind is the opposite.

        Returns: a tuple of two integers with the number of commits ahead and
        behind respectively.

        Parameters:

        local
            The commit which is considered the local or current state.

        upstream
            The commit which is considered the upstream.
        """

        if not isinstance(local, Oid):
            local = self.expand_id(local)

        if not isinstance(upstream, Oid):
            upstream = self.expand_id(upstream)

        ahead, behind = ffi.new('size_t*'), ffi.new('size_t*')
        oid1, oid2 = ffi.new('git_oid *'), ffi.new('git_oid *')
        ffi.buffer(oid1)[:] = local.raw[:]
        ffi.buffer(oid2)[:] = upstream.raw[:]
        err = C.git_graph_ahead_behind(ahead, behind, self._repo, oid1, oid2)
        check_error(err)

        return int(ahead[0]), int(behind[0])

    #
    # Git attributes
    #
    def get_attr(
        self,
        path: str | bytes | Path,
        name: str | bytes,
        flags: AttrCheck = AttrCheck.FILE_THEN_INDEX,
        commit: Oid | str | None = None,
    ) -> bool | None | str:
        """
        Retrieve an attribute for a file by path.

        Returns: a boolean, `None` if the value is unspecified, or string with
        the value of the attribute.

        Parameters:

        path
            The path of the file to look up attributes for, relative to the
            workdir root.

        name
            The name of the attribute to look up.

        flags
            A combination of enums.AttrCheck flags which determine the lookup order.

        commit
            Optional id of commit to load attributes from when the
            `INCLUDE_COMMIT` flag is specified.

        Examples::

            >>> print(repo.get_attr('splash.bmp', 'binary'))
            True
            >>> print(repo.get_attr('splash.bmp', 'unknown-attr'))
            None
            >>> repo.get_attr('test.h', 'whitespace')
            'tab-in-indent,trailing-space'
        """

        copts = ffi.new('git_attr_options *')
        copts.version = C.GIT_ATTR_OPTIONS_VERSION
        copts.flags = int(flags)
        if commit is not None:
            if not isinstance(commit, Oid):
                commit = Oid(hex=commit)
            ffi.buffer(ffi.addressof(copts, 'attr_commit_id'))[:] = commit.raw

        cvalue = ffi.new('char **')
        err = C.git_attr_get_ext(
            cvalue, self._repo, copts, to_bytes(path), to_bytes(name)
        )
        check_error(err)

        # Now let's see if we can figure out what the value is
        attr_kind = C.git_attr_value(cvalue[0])
        if attr_kind == C.GIT_ATTR_VALUE_UNSPECIFIED:
            return None
        elif attr_kind == C.GIT_ATTR_VALUE_TRUE:
            return True
        elif attr_kind == C.GIT_ATTR_VALUE_FALSE:
            return False
        elif attr_kind == C.GIT_ATTR_VALUE_STRING:
            return ffi.string(cvalue[0]).decode('utf-8')

        assert False, 'the attribute value from libgit2 is invalid'

    #
    # Identity for reference operations
    #
    @property
    def ident(self):
        cname = ffi.new('char **')
        cemail = ffi.new('char **')

        err = C.git_repository_ident(cname, cemail, self._repo)
        check_error(err)

        return (ffi.string(cname).decode('utf-8'), ffi.string(cemail).decode('utf-8'))

    def set_ident(self, name: str, email: str) -> None:
        """Set the identity to be used for reference operations.

        Updates to some references also append data to their
        reflog. You can use this method to set what identity will be
        used. If none is set, it will be read from the configuration.
        """

        err = C.git_repository_set_ident(self._repo, to_bytes(name), to_bytes(email))
        check_error(err)

    def revert(self, commit: Commit) -> None:
        """
        Revert the given commit, producing changes in the index and working
        directory.

        This operation updates the repository's state and prepared message
        (MERGE_MSG).
        """
        commit_ptr = ffi.new('git_commit **')
        ffi.buffer(commit_ptr)[:] = commit._pointer[:]
        err = C.git_revert(self._repo, commit_ptr[0], ffi.NULL)
        check_error(err)

    def revert_commit(
        self, revert_commit: Commit, our_commit: Commit, mainline: int = 0
    ) -> Index:
        """
        Revert the given Commit against the given "our" Commit, producing an
        Index that reflects the result of the revert.

        Returns: an Index with the result of the revert.

        Parameters:

        revert_commit
            The Commit to revert.

        our_commit
            The Commit to revert against (eg, HEAD).

        mainline
            The parent of the revert Commit, if it is a merge (i.e. 1, 2).
        """
        cindex = ffi.new('git_index **')
        revert_commit_ptr = ffi.new('git_commit **')
        our_commit_ptr = ffi.new('git_commit **')

        ffi.buffer(revert_commit_ptr)[:] = revert_commit._pointer[:]
        ffi.buffer(our_commit_ptr)[:] = our_commit._pointer[:]

        opts = ffi.new('git_merge_options *')
        err = C.git_merge_options_init(opts, C.GIT_MERGE_OPTIONS_VERSION)
        check_error(err)

        err = C.git_revert_commit(
            cindex, self._repo, revert_commit_ptr[0], our_commit_ptr[0], mainline, opts
        )
        check_error(err)

        return Index.from_c(self, cindex)

    #
    # Amend commit
    #
    def amend_commit(
        self,
        commit: Commit | Oid | str,
        refname: Reference | str | None,
        author: Signature | None = None,
        committer: Signature | None = None,
        message: str | None = None,
        tree: Tree | Oid | str | None = None,
        encoding: str = 'UTF-8',
    ) -> Oid:
        """
        Amend an existing commit by replacing only explicitly passed values,
        return the rewritten commit's oid.

        This creates a new commit that is exactly the same as the old commit,
        except that any explicitly passed values will be updated. The new
        commit has the same parents as the old commit.

        You may omit the `author`, `committer`, `message`, `tree`, and
        `encoding` parameters, in which case this will use the values
        from the original `commit`.

        Parameters:

        commit : Commit, Oid, or str
            The commit to amend.

        refname : Reference or str
            If not `None`, name of the reference that will be updated to point
            to the newly rewritten commit. Use "HEAD" to update the HEAD of the
            current branch and make it point to the rewritten commit.
            If you want to amend a commit that is not currently the tip of the
            branch and then rewrite the following commits to reach a ref, pass
            this as `None` and update the rest of the commit chain and ref
            separately.

        author : Signature
            If not None, replace the old commit's author signature with this
            one.

        committer : Signature
            If not None, replace the old commit's committer signature with this
            one.

        message : str
            If not None, replace the old commit's message with this one.

        tree : Tree, Oid, or str
            If not None, replace the old commit's tree with this one.

        encoding : str
            Optional encoding for `message`.
        """

        # Initialize parameters to pass on to C function git_commit_amend.
        # Note: the pointers are all initialized to NULL by default.
        coid = ffi.new('git_oid *')
        commit_cptr = ffi.new('git_commit **')
        refname_cstr: 'ArrayC[char]' | 'ffi.NULL_TYPE' = ffi.NULL
        author_cptr = ffi.new('git_signature **')
        committer_cptr = ffi.new('git_signature **')
        message_cstr: 'ArrayC[char]' | 'ffi.NULL_TYPE' = ffi.NULL
        encoding_cstr: 'ArrayC[char]' | 'ffi.NULL_TYPE' = ffi.NULL
        tree_cptr = ffi.new('git_tree **')

        # Get commit as pointer to git_commit.
        if isinstance(commit, (str, Oid)):
            commit_object = self[commit]
            commit_commit = commit_object.peel(Commit)
        elif isinstance(commit, Commit):
            commit_commit = commit
        elif commit is None:
            raise ValueError('the commit to amend cannot be None')
        else:
            raise TypeError('the commit to amend must be a Commit, str, or Oid')
        ffi.buffer(commit_cptr)[:] = commit_commit._pointer[:]

        # Get refname as C string.
        if isinstance(refname, Reference):
            refname_cstr = ffi.new('char[]', to_bytes(refname.name))
        elif type(refname) is str:
            refname_cstr = ffi.new('char[]', to_bytes(refname))
        elif refname is not None:
            raise TypeError('refname must be a str or Reference')

        # Get author as pointer to git_signature.
        if isinstance(author, Signature):
            ffi.buffer(author_cptr)[:] = author._pointer[:]
        elif author is not None:
            raise TypeError('author must be a Signature')

        # Get committer as pointer to git_signature.
        if isinstance(committer, Signature):
            ffi.buffer(committer_cptr)[:] = committer._pointer[:]
        elif committer is not None:
            raise TypeError('committer must be a Signature')

        # Get message and encoding as C strings.
        if message is not None:
            message_cstr = ffi.new('char[]', to_bytes(message, encoding))
            encoding_cstr = ffi.new('char[]', to_bytes(encoding))

        # Get tree as pointer to git_tree.
        if tree is not None:
            if isinstance(tree, (str, Oid)):
                tree_object = self[tree]
            else:
                tree_object = tree
            tree_tree = tree_object.peel(Tree)
            ffi.buffer(tree_cptr)[:] = tree_tree._pointer[:]

        # Amend the commit.
        err = C.git_commit_amend(
            coid,
            commit_cptr[0],
            refname_cstr,
            author_cptr[0],
            committer_cptr[0],
            encoding_cstr,
            message_cstr,
            tree_cptr[0],
        )
        check_error(err)

        return Oid(raw=bytes(ffi.buffer(coid)[:]))

    def __ensure_tree(self, maybe_tree: str | Oid | Tree) -> Tree:
        if isinstance(maybe_tree, Tree):
            return maybe_tree
        return self[maybe_tree].peel(Tree)


class Repository(BaseRepository):
    def __init__(
        self,
        path: str | bytes | None | Path = None,
        flags: RepositoryOpenFlag = RepositoryOpenFlag.DEFAULT,
    ):
        """
        The Repository constructor will commonly be called with one argument,
        the path of the repository to open.

        Alternatively, constructing a repository with no arguments will create
        a repository with no backends. You can use this path to create
        repositories with custom backends. Note that most operations on the
        repository are considered invalid and may lead to undefined behavior if
        attempted before providing an odb and refdb via set_odb and set_refdb.

        Parameters:

        path : str
        The path to open - if not provided, the repository will have no backend.

        flags : enums.RepositoryOpenFlag
        An optional combination of enums.RepositoryOpenFlag constants
        controlling how to open the repository.
        """

        if path is not None:
            if hasattr(path, '__fspath__'):
                path = path.__fspath__()
            if not isinstance(path, str):
                path = path.decode('utf-8')
            path_backend = init_file_backend(path, int(flags))
            super().__init__(path_backend)
        else:
            super().__init__()

    @classmethod
    def _from_c(cls, ptr: 'GitRepositoryC', owned: bool) -> 'Repository':
        cptr = ffi.new('git_repository **')
        cptr[0] = ptr
        repo = cls.__new__(cls)
        BaseRepository._from_c(repo, bytes(ffi.buffer(cptr)[:]), owned)  # type: ignore
        repo._common_init()
        return repo
