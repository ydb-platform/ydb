from __future__ import annotations

import typing

from typing import (
    Any,
    Collection,
    Iterable,
    Iterator,
)

from mercurial.node import sha1nodeconstants
from mercurial.interfaces.types import MatcherT

from mercurial import (
    match as matchmod,
    pathutil,
    pycompat,
    util,
)
from mercurial.interfaces import (
    repository,
)

from . import gitutil

if typing.TYPE_CHECKING:
    # We need to fully qualify the set primitive when typing the imanifestdict
    # class, so its set() method doesn't hide the primitive.
    import builtins

    from collections.abc import (
        ByteString,  # TODO: change to Buffer for 3.14
    )

pygit2 = gitutil.get_pygit2()


class gittreemanifest(repository.imanifestdict):
    """Expose git trees (and optionally a builder's overlay) as a manifestdict.

    Very similar to mercurial.manifest.treemanifest.
    """

    def __init__(self, git_repo, root_tree, pending_changes):
        """Initializer.

        Args:
          git_repo: The git_repo we're walking (required to look up child
              trees).
          root_tree: The root Git tree object for this manifest.
          pending_changes: A dict in which pending changes will be
              tracked. The enclosing memgittreemanifestctx will use this to
              construct any required Tree objects in Git during it's
              `write()` method.
        """
        self._git_repo = git_repo
        self._tree = root_tree
        if pending_changes is None:
            pending_changes = {}
        # dict of path: Optional[Tuple(node, flags)]
        self._pending_changes = pending_changes

    def _resolve_entry(self, path) -> tuple[bytes, bytes]:
        """Given a path, load its node and flags, or raise KeyError if missing.

        This takes into account any pending writes in the builder.
        """
        upath = pycompat.fsdecode(path)
        ent = None
        if path in self._pending_changes:
            val = self._pending_changes[path]
            if val is None:
                raise KeyError
            return val
        t = self._tree
        comps = upath.split('/')
        te = self._tree
        for comp in comps[:-1]:
            te = te[comp]
            t = self._git_repo[te.id]
        ent = t[comps[-1]]
        if ent.filemode == pygit2.GIT_FILEMODE_BLOB:
            flags = b''
        elif ent.filemode == pygit2.GIT_FILEMODE_BLOB_EXECUTABLE:
            flags = b'x'
        elif ent.filemode == pygit2.GIT_FILEMODE_LINK:
            flags = b'l'
        else:
            raise ValueError('unsupported mode %s' % oct(ent.filemode))
        return ent.id.raw, flags

    def __getitem__(self, path: bytes) -> bytes:
        return self._resolve_entry(path)[0]

    def find(self, path: bytes) -> tuple[bytes, bytes]:
        return self._resolve_entry(path)

    def __len__(self) -> int:
        return len(list(self.walk(matchmod.always())))

    def __nonzero__(self) -> bool:
        try:
            next(iter(self))
            return True
        except StopIteration:
            return False

    __bool__ = __nonzero__

    def __contains__(self, path: bytes) -> bool:
        try:
            self._resolve_entry(path)
            return True
        except KeyError:
            return False

    def iterkeys(self) -> Iterator[bytes]:
        return self.walk(matchmod.always())

    def keys(self) -> list[bytes]:
        return list(self.iterkeys())

    def __iter__(self) -> Iterator[bytes]:
        return self.iterkeys()

    def set(self, path: bytes, node: bytes, flags: bytes) -> None:
        raise NotImplementedError  # TODO: implement this

    def __setitem__(self, path: bytes, node: bytes) -> None:
        self._pending_changes[path] = node, self.flags(path)

    def __delitem__(self, path: bytes) -> None:
        # TODO: should probably KeyError for already-deleted  files?
        self._pending_changes[path] = None

    def filesnotin(self, other, match=None) -> builtins.set[bytes]:
        if match is not None:
            match = matchmod.badmatch(match, lambda path, msg: None)
            sm2 = set(other.walk(match))
            return {f for f in self.walk(match) if f not in sm2}
        return {f for f in self if f not in other}

    @util.propertycache
    def _dirs(self):
        return pathutil.dirs(self)

    def dirs(self) -> pathutil.dirs:
        return self._dirs  # TODO: why is there a prpoertycache?

    def hasdir(self, dir: bytes) -> bool:
        return dir in self._dirs

    def diff(
        self,
        other: Any,  # TODO: 'manifestdict' or (better) equivalent interface
        match: Any = lambda x: True,  # TODO: Optional[matchmod.basematcher] = None,
        clean: bool = False,
    ) -> dict[
        bytes,
        tuple[tuple[bytes | None, bytes], tuple[bytes | None, bytes]] | None,
    ]:
        """Finds changes between the current manifest and m2.

        The result is returned as a dict with filename as key and
        values of the form ((n1,fl1),(n2,fl2)), where n1/n2 is the
        nodeid in the current/other manifest and fl1/fl2 is the flag
        in the current/other manifest. Where the file does not exist,
        the nodeid will be None and the flags will be the empty
        string.
        """
        result = {}

        def _iterativediff(t1, t2, subdir):
            """compares two trees and appends new tree nodes to examine to
            the stack"""
            if t1 == t2:
                # If the trees are the same (git) object, then there are no
                # differences in this tree or any of its children.
                return

            if t1 is None:
                t1 = {}
            if t2 is None:
                t2 = {}

            for e1 in t1:
                realname = subdir + pycompat.fsencode(e1.name)

                if e1.type == pygit2.GIT_OBJ_TREE:
                    try:
                        e2 = t2[e1.name]
                        if e2.type != pygit2.GIT_OBJ_TREE:
                            e2 = None
                    except KeyError:
                        e2 = None

                    stack.append((realname + b'/', e1, e2))
                else:
                    n1, fl1 = self.find(realname)

                    try:
                        e2 = t2[e1.name]
                        n2, fl2 = other.find(realname)
                    except KeyError:
                        e2 = None
                        n2, fl2 = (None, b'')

                    if e2 is not None and e2.type == pygit2.GIT_OBJ_TREE:
                        stack.append((realname + b'/', None, e2))

                    if not match(realname):
                        continue

                    if n1 != n2 or fl1 != fl2:
                        result[realname] = ((n1, fl1), (n2, fl2))
                    elif clean:
                        result[realname] = None

            for e2 in t2:
                if e2.name in t1:
                    continue

                realname = subdir + pycompat.fsencode(e2.name)

                if e2.type == pygit2.GIT_OBJ_TREE:
                    stack.append((realname + b'/', None, e2))
                elif match(realname):
                    n2, fl2 = other.find(realname)
                    result[realname] = ((None, b''), (n2, fl2))

        stack = []
        _iterativediff(self._tree, other._tree, b'')
        while stack:
            subdir, t1, t2 = stack.pop()
            # stack is populated in the function call
            _iterativediff(t1, t2, subdir)

        return result

    def setflag(self, path: bytes, flag: bytes) -> None:
        node, unused_flag = self._resolve_entry(path)
        self._pending_changes[path] = node, flag

    def get(self, path: bytes, default=None) -> bytes | None:
        try:
            return self._resolve_entry(path)[0]
        except KeyError:
            return default

    def flags(self, path: bytes) -> bytes:
        try:
            return self._resolve_entry(path)[1]
        except KeyError:
            return b''

    def copy(self) -> gittreemanifest:
        return gittreemanifest(
            self._git_repo, self._tree, dict(self._pending_changes)
        )

    def items(self) -> Iterator[tuple[bytes, bytes]]:
        for f in self:
            # TODO: build a proper iterator version of this
            yield f, self[f]

    def iteritems(self) -> Iterator[tuple[bytes, bytes]]:
        return self.items()

    def iterentries(self) -> Iterator[tuple[bytes, bytes, bytes]]:
        for f in self:
            # TODO: build a proper iterator version of this
            yield f, *self._resolve_entry(f)

    def text(self) -> ByteString:
        # TODO can this method move out of the manifest iface?
        raise NotImplementedError

    def fastdelta(
        self, base: ByteString, changes: Iterable[tuple[bytes, bool]]
    ) -> tuple[ByteString, ByteString]:
        raise NotImplementedError  # TODO: implement this

    def _walkonetree(self, tree, match, subdir) -> Iterator[bytes]:
        for te in tree:
            # TODO: can we prune dir walks with the matcher?
            realname = subdir + pycompat.fsencode(te.name)
            if te.type == pygit2.GIT_OBJ_TREE:
                yield from self._walkonetree(
                    self._git_repo[te.id], match, realname + b'/'
                )
            elif match(realname):
                yield pycompat.fsencode(realname)

    def walk(self, match: MatcherT) -> Iterator[bytes]:
        # TODO: this is a very lazy way to merge in the pending
        # changes. There is absolutely room for optimization here by
        # being clever about walking over the sets...
        baseline = set(self._walkonetree(self._tree, match, b''))
        deleted = {p for p, v in self._pending_changes.items() if v is None}
        pend = {p for p in self._pending_changes if match(p)}
        return iter(sorted((baseline | pend) - deleted))


class gittreemanifestctx(repository.imanifestrevisionstored):
    def __init__(self, repo, gittree):
        self._repo = repo
        self._tree = gittree

    def read(self):
        return gittreemanifest(self._repo, self._tree, None)

    def readfast(self, shallow: bool = False):
        return self.read()

    def copy(self):
        # NB: it's important that we return a memgittreemanifestctx
        # because the caller expects a mutable manifest.
        return memgittreemanifestctx(self._repo, self._tree)

    def find(self, path: bytes) -> tuple[bytes, bytes]:
        return self.read().find(path)

    def node(self) -> bytes:
        raise NotImplementedError

    def readdelta(self, shallow: bool = False):
        raise NotImplementedError

    def read_any_fast_delta(
        self,
        valid_bases: Collection[int] | None = None,
        *,
        shallow: bool = False,
    ):
        raise NotImplementedError

    def read_delta_parents(self, *, shallow: bool = False, exact: bool = True):
        raise NotImplementedError

    def read_delta_new_entries(self, *, shallow: bool = False):
        raise NotImplementedError

    @property
    def parents(self):
        # FIXME: the 2 parent nodes
        return sha1nodeconstants.nullid, sha1nodeconstants.nullid


class memgittreemanifestctx(repository.imanifestrevisionwritable):
    def __init__(self, repo, tree):
        self._repo = repo
        self._tree = tree
        # dict of path: Optional[Tuple(node, flags)]
        self._pending_changes = {}

    def read(self):
        return gittreemanifest(self._repo, self._tree, self._pending_changes)

    def copy(self):
        # TODO: if we have a builder in play, what should happen here?
        # Maybe we can shuffle copy() into the immutable interface.
        return memgittreemanifestctx(self._repo, self._tree)

    def write(self, transaction, link, p1, p2, added, removed, match=None):
        # We're not (for now, anyway) going to audit filenames, so we
        # can ignore added and removed.

        # TODO what does this match argument get used for? hopefully
        # just narrow?
        assert not match or isinstance(match, matchmod.alwaysmatcher)

        touched_dirs = pathutil.dirs(list(self._pending_changes))
        trees = {
            b'': self._tree,
        }
        # path: treebuilder
        builders = {
            b'': self._repo.TreeBuilder(self._tree),
        }
        # get a TreeBuilder for every tree in the touched_dirs set
        for d in sorted(touched_dirs, key=lambda x: (len(x), x)):
            if d == b'':
                # loaded root tree above
                continue
            comps = d.split(b'/')
            full = b''
            for part in comps:
                parent = trees[full]
                try:
                    parent_tree_id = parent[pycompat.fsdecode(part)].id
                    new = self._repo[parent_tree_id]
                except KeyError:
                    # new directory
                    new = None
                full += b'/' + part
                if new is not None:
                    # existing directory
                    trees[full] = new
                    builders[full] = self._repo.TreeBuilder(new)
                else:
                    # new directory, use an empty dict to easily
                    # generate KeyError as any nested new dirs get
                    # created.
                    trees[full] = {}
                    builders[full] = self._repo.TreeBuilder()
        for f, info in self._pending_changes.items():
            if b'/' not in f:
                dirname = b''
                basename = f
            else:
                dirname, basename = f.rsplit(b'/', 1)
                dirname = b'/' + dirname
            if info is None:
                builders[dirname].remove(pycompat.fsdecode(basename))
            else:
                n, fl = info
                mode = {
                    b'': pygit2.GIT_FILEMODE_BLOB,
                    b'x': pygit2.GIT_FILEMODE_BLOB_EXECUTABLE,
                    b'l': pygit2.GIT_FILEMODE_LINK,
                }[fl]
                builders[dirname].insert(
                    pycompat.fsdecode(basename), gitutil.togitnode(n), mode
                )
        # This visits the buffered TreeBuilders in deepest-first
        # order, bubbling up the edits.
        for b in sorted(builders, key=len, reverse=True):
            if b == b'':
                break
            cb = builders[b]
            dn, bn = b.rsplit(b'/', 1)
            builders[dn].insert(
                pycompat.fsdecode(bn), cb.write(), pygit2.GIT_FILEMODE_TREE
            )
        return builders[b''].write().raw
