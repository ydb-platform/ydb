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

import typing
import warnings
from dataclasses import dataclass
from os import PathLike

# Import from pygit2
from ._pygit2 import Diff, Oid, Tree
from .enums import DiffOption, FileMode
from .errors import check_error
from .ffi import C, ffi
from .utils import GenericIterator, StrArray, to_bytes, to_str

if typing.TYPE_CHECKING:
    from .repository import Repository


class Index:
    # XXX Implement the basic features in C (_pygit2.Index) and make
    # pygit2.Index to inherit from _pygit2.Index? This would allow for
    # a proper implementation in some places: e.g. checking the index type
    # from C code (see Tree_diff_to_index)

    def __init__(self, path: str | PathLike[str] | None = None) -> None:
        """Create a new Index

        If path is supplied, the read and write methods will use that path
        to read from and write to.
        """
        cindex = ffi.new('git_index **')
        err = C.git_index_open(cindex, to_bytes(path))
        check_error(err)

        self._repo = None
        self._index = cindex[0]
        self._cindex = cindex

    @classmethod
    def from_c(cls, repo, ptr):
        index = cls.__new__(cls)
        index._repo = repo
        index._index = ptr[0]
        index._cindex = ptr

        return index

    @property
    def _pointer(self):
        return bytes(ffi.buffer(self._cindex)[:])

    def __del__(self) -> None:
        C.git_index_free(self._index)

    def __len__(self) -> int:
        return C.git_index_entrycount(self._index)

    def __contains__(self, path) -> bool:
        err = C.git_index_find(ffi.NULL, self._index, to_bytes(path))
        if err == C.GIT_ENOTFOUND:
            return False

        check_error(err)
        return True

    def __getitem__(self, key: str | int | PathLike[str]) -> 'IndexEntry':
        centry = ffi.NULL
        if isinstance(key, str) or hasattr(key, '__fspath__'):
            centry = C.git_index_get_bypath(self._index, to_bytes(key), 0)
        elif isinstance(key, int):
            if key >= 0:
                centry = C.git_index_get_byindex(self._index, key)
            else:
                raise ValueError(key)
        else:
            raise TypeError(f'Expected str or int, got {type(key)}')

        if centry == ffi.NULL:
            raise KeyError(key)

        return IndexEntry._from_c(centry)

    def __iter__(self):
        return GenericIterator(self)

    def read(self, force: bool = True) -> None:
        """
        Update the contents of the Index by reading from a file.

        Parameters:

        force
            If True (the default) always reload. If False, only if the file
            has changed.
        """

        err = C.git_index_read(self._index, force)
        check_error(err, io=True)

    def write(self) -> None:
        """Write the contents of the Index to disk."""
        err = C.git_index_write(self._index)
        check_error(err, io=True)

    def clear(self) -> None:
        err = C.git_index_clear(self._index)
        check_error(err)

    def read_tree(self, tree: Oid | Tree | str) -> None:
        """Replace the contents of the Index with those of the given tree,
        expressed either as a <Tree> object or as an oid (string or <Oid>).

        The tree will be read recursively and all its children will also be
        inserted into the Index.
        """
        repo = self._repo
        if isinstance(tree, str):
            if repo is None:
                raise TypeError('id given but no associated repository')
            tree = repo[tree]

        if isinstance(tree, Oid):
            if repo is None:
                raise TypeError('id given but no associated repository')

            tree = repo[tree]
        elif not isinstance(tree, Tree):
            raise TypeError('argument must be Oid, Tree or str')

        tree_cptr = ffi.new('git_tree **')
        ffi.buffer(tree_cptr)[:] = tree._pointer[:]
        err = C.git_index_read_tree(self._index, tree_cptr[0])
        check_error(err)

    def write_tree(self, repo: 'Repository | None' = None) -> Oid:
        """Create a tree out of the Index. Return the <Oid> object of the
        written tree.

        The contents of the index will be written out to the object
        database. If there is no associated repository, 'repo' must be
        passed. If there is an associated repository and 'repo' is
        passed, then that repository will be used instead.

        It returns the id of the resulting tree.
        """
        coid = ffi.new('git_oid *')

        repo = repo or self._repo

        if repo:
            err = C.git_index_write_tree_to(coid, self._index, repo._repo)
        else:
            err = C.git_index_write_tree(coid, self._index)

        check_error(err)
        return Oid(raw=bytes(ffi.buffer(coid)[:]))

    def remove(self, path: PathLike[str] | str, level: int = 0) -> None:
        """Remove an entry from the Index."""
        err = C.git_index_remove(self._index, to_bytes(path), level)
        check_error(err, io=True)

    def remove_directory(self, path: PathLike[str] | str, level: int = 0) -> None:
        """Remove a directory from the Index."""
        err = C.git_index_remove_directory(self._index, to_bytes(path), level)
        check_error(err, io=True)

    def remove_all(self, pathspecs: typing.Sequence[str | PathLike[str]]) -> None:
        """Remove all index entries matching pathspecs."""
        with StrArray(pathspecs) as arr:
            err = C.git_index_remove_all(self._index, arr.ptr, ffi.NULL, ffi.NULL)
            check_error(err, io=True)

    def add_all(self, pathspecs: None | list[str | PathLike[str]] = None) -> None:
        """Add or update index entries matching files in the working directory.

        If pathspecs are specified, only files matching those pathspecs will
        be added.
        """
        pathspecs = pathspecs or []
        with StrArray(pathspecs) as arr:
            err = C.git_index_add_all(self._index, arr.ptr, 0, ffi.NULL, ffi.NULL)
            check_error(err, io=True)

    def add(self, path_or_entry: 'IndexEntry | str | PathLike[str]') -> None:
        """Add or update an entry in the Index.

        If a path is given, that file will be added. The path must be relative
        to the root of the worktree and the Index must be associated with a
        repository.

        If an IndexEntry is given, that entry will be added or update in the
        Index without checking for the existence of the path or id.
        """
        if isinstance(path_or_entry, IndexEntry):
            entry = path_or_entry
            centry, str_ref = entry._to_c()
            err = C.git_index_add(self._index, centry)
        elif isinstance(path_or_entry, str) or hasattr(path_or_entry, '__fspath__'):
            path = path_or_entry
            err = C.git_index_add_bypath(self._index, to_bytes(path))
        else:
            raise TypeError('argument must be string, Path or IndexEntry')

        check_error(err, io=True)

    def add_conflict(
        self, ancestor: 'IndexEntry', ours: 'IndexEntry', theirs: 'IndexEntry | None'
    ) -> None:
        """
        Add or update index entries to represent a conflict. Any staged entries that
        exist at the given paths will be removed.

        Parameters:

        ancestor
            ancestor of the conflict
        ours
            ours side of the conflict
        theirs
            their side of the conflict
        """

        if ancestor and not isinstance(ancestor, IndexEntry):
            raise TypeError('ancestor has to be an instance of IndexEntry or None')
        if ours and not isinstance(ours, IndexEntry):
            raise TypeError('ours has to be an instance of IndexEntry or None')
        if theirs and not isinstance(theirs, IndexEntry):
            raise TypeError('theirs has to be an instance of IndexEntry or None')

        centry_ancestor: ffi.NULL_TYPE | ffi.GitIndexEntryC = ffi.NULL
        centry_ours: ffi.NULL_TYPE | ffi.GitIndexEntryC = ffi.NULL
        centry_theirs: ffi.NULL_TYPE | ffi.GitIndexEntryC = ffi.NULL
        if ancestor is not None:
            centry_ancestor, _ = ancestor._to_c()
        if ours is not None:
            centry_ours, _ = ours._to_c()
        if theirs is not None:
            centry_theirs, _ = theirs._to_c()
        err = C.git_index_conflict_add(
            self._index, centry_ancestor, centry_ours, centry_theirs
        )

        check_error(err, io=True)

    def diff_to_workdir(
        self,
        flags: DiffOption = DiffOption.NORMAL,
        context_lines: int = 3,
        interhunk_lines: int = 0,
    ) -> Diff:
        """
        Diff the index against the working directory. Return a <Diff> object
        with the differences between the index and the working copy.

        Parameters:

        flags
            A combination of enums.DiffOption constants.

        context_lines
            The number of unchanged lines that define the boundary of a hunk
            (and to display before and after).

        interhunk_lines
            The maximum number of unchanged lines between hunk boundaries
            before the hunks will be merged into a one.
        """
        repo = self._repo
        if repo is None:
            raise ValueError('diff needs an associated repository')

        copts = ffi.new('git_diff_options *')
        err = C.git_diff_options_init(copts, 1)
        check_error(err)

        copts.flags = int(flags)
        copts.context_lines = context_lines
        copts.interhunk_lines = interhunk_lines

        cdiff = ffi.new('git_diff **')
        err = C.git_diff_index_to_workdir(cdiff, repo._repo, self._index, copts)
        check_error(err)

        return Diff.from_c(bytes(ffi.buffer(cdiff)[:]), repo)

    def diff_to_tree(
        self,
        tree: Tree,
        flags: DiffOption = DiffOption.NORMAL,
        context_lines: int = 3,
        interhunk_lines: int = 0,
    ) -> Diff:
        """
        Diff the index against a tree.  Return a <Diff> object with the
        differences between the index and the given tree.

        Parameters:

        tree
            The tree to diff.

        flags
            A combination of enums.DiffOption constants.

        context_lines
            The number of unchanged lines that define the boundary of a hunk
            (and to display before and after).

        interhunk_lines
            The maximum number of unchanged lines between hunk boundaries
            before the hunks will be merged into a one.
        """
        repo = self._repo
        if repo is None:
            raise ValueError('diff needs an associated repository')

        if not isinstance(tree, Tree):
            raise TypeError('tree must be a Tree')

        copts = ffi.new('git_diff_options *')
        err = C.git_diff_options_init(copts, 1)
        check_error(err)

        copts.flags = int(flags)
        copts.context_lines = context_lines
        copts.interhunk_lines = interhunk_lines

        ctree = ffi.new('git_tree **')
        ffi.buffer(ctree)[:] = tree._pointer[:]

        cdiff = ffi.new('git_diff **')
        err = C.git_diff_tree_to_index(cdiff, repo._repo, ctree[0], self._index, copts)
        check_error(err)

        return Diff.from_c(bytes(ffi.buffer(cdiff)[:]), repo)

    #
    # Conflicts
    #

    @property
    def conflicts(self):
        """A collection of conflict information

        If there are no conflicts None is returned. Otherwise return an object
        that represents the conflicts in the index.

        This object presents a mapping interface with the paths as keys. You
        can use the ``del`` operator to remove a conflict from the Index.

        Each conflict is made up of three elements. Access or iteration
        of the conflicts returns a three-tuple of
        :py:class:`~pygit2.IndexEntry`. The first is the common
        ancestor, the second is the "ours" side of the conflict, and the
        third is the "theirs" side.

        These elements may be None depending on which sides exist for
        the particular conflict.
        """
        if not C.git_index_has_conflicts(self._index):
            return None

        return ConflictCollection(self)


@dataclass
class MergeFileResult:
    automergeable: bool
    'True if the output was automerged, false if the output contains conflict markers'

    path: str | None | PathLike[str]
    'The path that the resultant merge file should use, or None if a filename conflict would occur'

    mode: FileMode
    'The mode that the resultant merge file should use'

    contents: str
    'Contents of the file, which might include conflict markers'

    def __repr__(self):
        t = type(self)
        contents = (
            self.contents if len(self.contents) <= 20 else f'{self.contents[:20]}...'
        )
        return (
            f'<{t.__module__}.{t.__qualname__} "'
            f'automergeable={self.automergeable} "'
            f'path={self.path} '
            f'mode={self.mode} '
            f'contents={contents}>'
        )

    @classmethod
    def _from_c(cls, centry):
        if centry == ffi.NULL:
            return None

        automergeable = centry.automergeable != 0
        path = to_str(ffi.string(centry.path)) if centry.path else None
        mode = FileMode(centry.mode)
        contents = ffi.string(centry.ptr, centry.len).decode('utf-8')

        return MergeFileResult(automergeable, path, mode, contents)


class IndexEntry:
    path: str | PathLike[str]
    'The path of this entry'

    id: Oid
    'The id of the referenced object'

    mode: FileMode
    'The mode of this entry, a FileMode value'

    def __init__(
        self, path: str | PathLike[str], object_id: Oid, mode: FileMode
    ) -> None:
        self.path = path
        self.id = object_id
        self.mode = mode

    @property
    def oid(self):
        warnings.warn('Use entry.id', DeprecationWarning)
        return self.id

    def __str__(self):
        return f'<path={self.path} id={self.id} mode={self.mode}>'

    def __repr__(self):
        t = type(self)
        return f'<{t.__module__}.{t.__qualname__} path={self.path} id={self.id} mode={self.mode}>'

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, IndexEntry):
            return NotImplemented
        return (
            self.path == other.path and self.id == other.id and self.mode == other.mode
        )

    def _to_c(self) -> tuple['ffi.GitIndexEntryC', 'ffi.ArrayC[ffi.char]']:
        """Convert this entry into the C structure

        The first returned arg is the pointer, the second is the reference to
        the string we allocated, which we need to exist past this function
        """
        centry = ffi.new('git_index_entry *')
        # basically memcpy()
        ffi.buffer(ffi.addressof(centry, 'id'))[:] = self.id.raw[:]
        centry.mode = int(self.mode)
        path = ffi.new('char[]', to_bytes(self.path))
        centry.path = path

        return centry, path

    @classmethod
    def _from_c(cls, centry):
        if centry == ffi.NULL:
            return None

        entry = cls.__new__(cls)
        entry.path = to_str(ffi.string(centry.path))
        entry.mode = FileMode(centry.mode)
        entry.id = Oid(raw=bytes(ffi.buffer(ffi.addressof(centry, 'id'))[:]))

        return entry


class ConflictCollection:
    def __init__(self, index):
        self._index = index

    def __getitem__(self, path):
        cancestor = ffi.new('git_index_entry **')
        cours = ffi.new('git_index_entry **')
        ctheirs = ffi.new('git_index_entry **')

        err = C.git_index_conflict_get(
            cancestor, cours, ctheirs, self._index._index, to_bytes(path)
        )
        check_error(err)

        ancestor = IndexEntry._from_c(cancestor[0])
        ours = IndexEntry._from_c(cours[0])
        theirs = IndexEntry._from_c(ctheirs[0])

        return ancestor, ours, theirs

    def __delitem__(self, path):
        err = C.git_index_conflict_remove(self._index._index, to_bytes(path))
        check_error(err)

    def __iter__(self):
        return ConflictIterator(self._index)

    def __contains__(self, path):
        cancestor = ffi.new('git_index_entry **')
        cours = ffi.new('git_index_entry **')
        ctheirs = ffi.new('git_index_entry **')

        err = C.git_index_conflict_get(
            cancestor, cours, ctheirs, self._index._index, to_bytes(path)
        )
        if err == C.GIT_ENOTFOUND:
            return False

        check_error(err)
        return True


class ConflictIterator:
    def __init__(self, index):
        citer = ffi.new('git_index_conflict_iterator **')
        err = C.git_index_conflict_iterator_new(citer, index._index)
        check_error(err)
        self._index = index
        self._iter = citer[0]

    def __del__(self):
        C.git_index_conflict_iterator_free(self._iter)

    def __iter__(self):
        return self

    def __next__(self):
        cancestor = ffi.new('git_index_entry **')
        cours = ffi.new('git_index_entry **')
        ctheirs = ffi.new('git_index_entry **')

        err = C.git_index_conflict_next(cancestor, cours, ctheirs, self._iter)
        if err == C.GIT_ITEROVER:
            raise StopIteration

        check_error(err)

        ancestor = IndexEntry._from_c(cancestor[0])
        ours = IndexEntry._from_c(cours[0])
        theirs = IndexEntry._from_c(ctheirs[0])

        return ancestor, ours, theirs
