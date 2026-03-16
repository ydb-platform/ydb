from __future__ import annotations

import contextlib
import os

from typing import (
    Any,
    Iterable,
    Iterator,
)

from mercurial.interfaces.types import (
    MatcherT,
    TransactionT,
)
from mercurial.node import sha1nodeconstants
from mercurial import (
    dirstatemap,
    error,
    extensions,
    match as matchmod,
    pycompat,
    scmutil,
    util,
)
from mercurial.dirstateutils import (
    timestamp,
)
from mercurial.interfaces import (
    dirstate as intdirstate,
)

from . import gitutil


DirstateItem = dirstatemap.DirstateItem
propertycache = util.propertycache
pygit2 = gitutil.get_pygit2()


def readpatternfile(orig, filepath, warn, sourceinfo=False):
    if not (b'info/exclude' in filepath or filepath.endswith(b'.gitignore')):
        return orig(filepath, warn, sourceinfo=False)
    result = []
    warnings = []
    with open(filepath, 'rb') as fp:
        for l in fp:
            l = l.strip()
            if not l or l.startswith(b'#'):
                continue
            if l.startswith(b'!'):
                warnings.append(b'unsupported ignore pattern %s' % l)
                continue
            if l.startswith(b'/'):
                result.append(b'rootglob:' + l[1:])
            else:
                result.append(b'relglob:' + l)
    return result, warnings


extensions.wrapfunction(matchmod, 'readpatternfile', readpatternfile)


_STATUS_MAP = {}
if pygit2:
    _STATUS_MAP = {
        pygit2.GIT_STATUS_CONFLICTED: b'm',
        pygit2.GIT_STATUS_CURRENT: b'n',
        pygit2.GIT_STATUS_IGNORED: b'?',
        pygit2.GIT_STATUS_INDEX_DELETED: b'r',
        pygit2.GIT_STATUS_INDEX_MODIFIED: b'n',
        pygit2.GIT_STATUS_INDEX_NEW: b'a',
        pygit2.GIT_STATUS_INDEX_RENAMED: b'a',
        pygit2.GIT_STATUS_INDEX_TYPECHANGE: b'n',
        pygit2.GIT_STATUS_WT_DELETED: b'r',
        pygit2.GIT_STATUS_WT_MODIFIED: b'n',
        pygit2.GIT_STATUS_WT_NEW: b'?',
        pygit2.GIT_STATUS_WT_RENAMED: b'a',
        pygit2.GIT_STATUS_WT_TYPECHANGE: b'n',
        pygit2.GIT_STATUS_WT_UNREADABLE: b'?',
        pygit2.GIT_STATUS_INDEX_MODIFIED | pygit2.GIT_STATUS_WT_MODIFIED: b'm',
    }


class gitdirstate(intdirstate.idirstate):
    def __init__(self, ui, vfs, gitrepo, use_dirstate_v2):
        self._ui = ui
        self._root = os.path.dirname(vfs.base)
        self._opener = vfs
        self.git = gitrepo
        self._plchangecallbacks = {}
        # TODO: context.poststatusfixup is bad and uses this attribute
        self._dirty = False
        self._mapcls = dirstatemap.dirstatemap
        self._use_dirstate_v2 = use_dirstate_v2

    @propertycache
    def _map(self):
        """Return the dirstate contents (see documentation for dirstatemap)."""
        self._map = self._mapcls(
            self._ui,
            self._opener,
            self._root,
            sha1nodeconstants,
            self._use_dirstate_v2,
        )
        return self._map

    def p1(self) -> bytes:
        try:
            return self.git.head.peel().id.raw
        except pygit2.GitError:
            # Typically happens when peeling HEAD fails, as in an
            # empty repository.
            return sha1nodeconstants.nullid

    def p2(self) -> bytes:
        # TODO: MERGE_HEAD? something like that, right?
        return sha1nodeconstants.nullid

    def setparents(self, p1: bytes, p2: bytes | None = None):
        if p2 is None:
            p2 = sha1nodeconstants.nullid
        assert p2 == sha1nodeconstants.nullid, b'TODO merging support'
        self.git.head.set_target(gitutil.togitnode(p1))

    @util.propertycache
    def identity(self):
        return util.filestat.frompath(
            os.path.join(self._root, b'.git', b'index')
        )

    def branch(self) -> bytes:
        return b'default'

    def parents(self) -> list[bytes]:
        # TODO how on earth do we find p2 if a merge is in flight?
        return [self.p1(), sha1nodeconstants.nullid]

    def __iter__(self) -> Iterator[bytes]:
        return (pycompat.fsencode(f.path) for f in self.git.index)

    def items(self) -> Iterator[tuple[bytes, intdirstate.DirstateItemT]]:
        for ie in self.git.index:
            yield ie.path, None  # value should be a DirstateItem

    # py2,3 compat forward
    iteritems = items

    def __getitem__(self, filename):
        try:
            gs = self.git.status_file(filename)
        except KeyError:
            return b'?'
        return _STATUS_MAP[gs]

    def __contains__(self, filename: Any) -> bool:
        try:
            gs = self.git.status_file(filename)
            return _STATUS_MAP[gs] != b'?'
        except KeyError:
            return False

    def status(
        self,
        match: MatcherT,
        subrepos: bool,
        ignored: bool,
        clean: bool,
        unknown: bool,
    ) -> intdirstate.StatusReturnT:
        listclean = clean
        # TODO handling of clean files - can we get that from git.status()?
        modified, added, removed, deleted, unknown, ignored, clean = (
            [],
            [],
            [],
            [],
            [],
            [],
            [],
        )

        try:
            mtime_boundary = timestamp.get_fs_now(self._opener)
        except OSError:
            # In largefiles or readonly context
            mtime_boundary = None

        gstatus = self.git.status()
        for path, status in gstatus.items():
            path = pycompat.fsencode(path)
            if not match(path):
                continue
            if status == pygit2.GIT_STATUS_IGNORED:
                if path.endswith(b'/'):
                    continue
                ignored.append(path)
            elif status in (
                pygit2.GIT_STATUS_WT_MODIFIED,
                pygit2.GIT_STATUS_INDEX_MODIFIED,
                pygit2.GIT_STATUS_WT_MODIFIED
                | pygit2.GIT_STATUS_INDEX_MODIFIED,
            ):
                modified.append(path)
            elif status == pygit2.GIT_STATUS_INDEX_NEW:
                added.append(path)
            elif status == pygit2.GIT_STATUS_WT_NEW:
                unknown.append(path)
            elif status == pygit2.GIT_STATUS_WT_DELETED:
                deleted.append(path)
            elif status == pygit2.GIT_STATUS_INDEX_DELETED:
                removed.append(path)
            else:
                raise error.Abort(
                    b'unhandled case: status for %r is %r' % (path, status)
                )

        if listclean:
            observed = set(
                modified + added + removed + deleted + unknown + ignored
            )
            index = self.git.index
            index.read()
            for entry in index:
                path = pycompat.fsencode(entry.path)
                if not match(path):
                    continue
                if path in observed:
                    continue  # already in some other set
                if path[-1] == b'/':
                    continue  # directory
                clean.append(path)

        # TODO are we really always sure of status here?
        return (
            False,
            scmutil.status(
                modified, added, removed, deleted, unknown, ignored, clean
            ),
            mtime_boundary,
        )

    def flagfunc(
        self, buildfallback: intdirstate.FlagFuncFallbackT
    ) -> intdirstate.FlagFuncReturnT:
        # TODO we can do better
        return buildfallback()

    def getcwd(self) -> bytes:
        # TODO is this a good way to do this?
        return os.path.dirname(
            os.path.dirname(pycompat.fsencode(self.git.path))
        )

    def get_entry(self, path: bytes) -> intdirstate.DirstateItemT:
        """return a DirstateItem for the associated path"""
        entry = self._map.get(path)
        if entry is None:
            return DirstateItem()
        return entry

    def normalize(
        self, path: bytes, isknown: bool = False, ignoremissing: bool = False
    ) -> bytes:
        normed = util.normcase(path)
        assert normed == path, b"TODO handling of case folding: %s != %s" % (
            normed,
            path,
        )
        return path

    def is_changing_files(self) -> bool:
        raise NotImplementedError

    def _ignorefileandline(self, f: bytes) -> intdirstate.IgnoreFileAndLineT:
        raise NotImplementedError

    @property
    def _checklink(self) -> bool:
        return util.checklink(os.path.dirname(pycompat.fsencode(self.git.path)))

    def invalidate(self) -> None:
        raise NotImplementedError

    def copy(self, source: bytes | None, dest: bytes) -> None:
        raise NotImplementedError

    def copies(self) -> dict[bytes, bytes]:
        # TODO support copies?
        return {}

    # # TODO what the heck is this
    _filecache = set()

    @property
    def is_changing_parents(self) -> bool:
        # TODO: we need to implement the context manager bits and
        # correctly stage/revert index edits.
        return False

    @property
    def is_changing_any(self) -> bool:
        # TODO: we need to implement the context manager bits and
        # correctly stage/revert index edits.
        return False

    def clear(self) -> None:
        raise NotImplementedError

    def rebuild(
        self,
        parent: bytes,
        allfiles: Iterable[bytes],  # TODO: more than iterable? (uses len())
        changedfiles: Iterable[bytes] | None = None,
    ) -> None:
        raise NotImplementedError

    def write(self, tr: TransactionT | None) -> None:
        # TODO: call parent change callbacks

        if tr:

            def writeinner(category):
                self.git.index.write()

            tr.addpending(b'gitdirstate', writeinner)
        else:
            self.git.index.write()

    def pathto(self, f: bytes, cwd: bytes | None = None) -> bytes:
        if cwd is None:
            cwd = self.getcwd()
        # TODO core dirstate does something about slashes here
        assert isinstance(f, bytes)
        r = util.pathto(self._root, cwd, f)
        return r

    def matches(self, match: MatcherT) -> Iterable[bytes]:
        for x in self.git.index:
            p = pycompat.fsencode(x.path)
            if match(p):
                yield p  # TODO: return list instead of yielding?

    def set_clean(self, f, parentfiledata):
        """Mark a file normal and clean."""
        # TODO: for now we just let libgit2 re-stat the file. We can
        # clearly do better.

    def set_possibly_dirty(self, f):
        """Mark a file normal, but possibly dirty."""
        # TODO: for now we just let libgit2 re-stat the file. We can
        # clearly do better.

    def walk(
        self,
        match: MatcherT,
        subrepos: Any,
        unknown: bool,
        ignored: bool,
        full: bool = True,
    ) -> intdirstate.WalkReturnT:
        # TODO: we need to use .status() and not iterate the index,
        # because the index doesn't force a re-walk and so `hg add` of
        # a new file without an intervening call to status will
        # silently do nothing.
        r = {}
        cwd = self.getcwd()
        for path, status in self.git.status().items():
            if path.startswith('.hg/'):
                continue
            path = pycompat.fsencode(path)
            if not match(path):
                continue
            # TODO construct the stat info from the status object?
            try:
                s = os.stat(os.path.join(cwd, path))
            except FileNotFoundError:
                continue
            r[path] = s
        return r

    def set_tracked(self, f, reset_copy=False):
        # TODO: support copies and reset_copy=True
        uf = pycompat.fsdecode(f)
        if uf in self.git.index:
            return False
        index = self.git.index
        index.read()
        index.add(uf)
        index.write()
        return True

    def add(self, f):
        index = self.git.index
        index.read()
        index.add(pycompat.fsdecode(f))
        index.write()

    def drop(self, f):
        index = self.git.index
        index.read()
        fs = pycompat.fsdecode(f)
        if fs in index:
            index.remove(fs)
            index.write()

    def set_untracked(self, f):
        index = self.git.index
        index.read()
        fs = pycompat.fsdecode(f)
        if fs in index:
            index.remove(fs)
            index.write()
            return True
        return False

    def remove(self, f):
        index = self.git.index
        index.read()
        index.remove(pycompat.fsdecode(f))
        index.write()

    def copied(self, file: bytes) -> bytes | None:
        # TODO: track copies?
        return None

    def prefetch_parents(self):
        # TODO
        pass

    def update_file(self, *args, **kwargs):
        # TODO
        pass

    def _checkexec(self) -> bool:
        raise NotImplementedError

    @contextlib.contextmanager
    def changing_parents(self, repo):
        # TODO: track this maybe?
        yield

    @contextlib.contextmanager
    def changing_files(self, repo) -> Iterator:  # TODO: typehint this
        raise NotImplementedError

    def hasdir(self, d: bytes) -> bool:
        raise NotImplementedError

    def addparentchangecallback(
        self, category: bytes, callback: intdirstate.AddParentChangeCallbackT
    ) -> None:
        # TODO: should this be added to the dirstate interface?
        self._plchangecallbacks[category] = callback

    def setbranch(
        self, branch: bytes, transaction: TransactionT | None
    ) -> None:
        raise error.Abort(
            b'git repos do not support branches. try using bookmarks'
        )

    def verify(
        self, m1, m2, p1: bytes, narrow_matcher: Any | None = None
    ) -> Iterator[bytes]:
        raise NotImplementedError

    def running_status(self, repo):
        raise NotImplementedError

    def refresh(self):
        pass
