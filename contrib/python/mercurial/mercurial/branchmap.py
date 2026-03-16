# branchmap.py - logic to computes, maintain and stores branchmap for local repo
#
# Copyright 2005-2007 Olivia Mackall <olivia@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import annotations

from .node import (
    bin,
    hex,
    nullrev,
)

from typing import (
    Any,
    Callable,
    Iterable,
    TYPE_CHECKING,
    cast,
)

from . import (
    encoding,
    error,
    obsolete,
    scmutil,
    util,
)

from .utils import (
    repoviewutil,
    stringutil,
)

if TYPE_CHECKING:
    from . import localrepo

    assert [localrepo]

subsettable = repoviewutil.subsettable


class BranchMapCache:
    """mapping of filtered views of repo with their branchcache"""

    def __init__(self):
        self._per_filter = {}

    def __getitem__(self, repo):
        self.updatecache(repo)
        bcache = self._per_filter[repo.filtername]
        bcache._ensure_populated(repo)
        assert bcache._filtername == repo.filtername, (
            bcache._filtername,
            repo.filtername,
        )
        return bcache

    def update_disk(self, repo, detect_pure_topo=False):
        """ensure and up-to-date cache is (or will be) written on disk

        The cache for this repository view is updated  if needed and written on
        disk.

        If a transaction is in progress, the writing is schedule to transaction
        close. See the `BranchMapCache.write_dirty` method.

        This method exist independently of __getitem__ as it is sometime useful
        to signal that we have no intend to use the data in memory yet.
        """
        self.updatecache(repo)
        bcache = self._per_filter[repo.filtername]
        assert bcache._filtername == repo.filtername, (
            bcache._filtername,
            repo.filtername,
        )
        if detect_pure_topo:
            bcache._detect_pure_topo(repo)
        tr = repo.currenttransaction()
        if getattr(tr, 'finalized', True):
            bcache.sync_disk(repo)

    def updatecache(self, repo):
        """Update the cache for the given filtered view on a repository"""
        # This can trigger updates for the caches for subsets of the filtered
        # view, e.g. when there is no cache for this filtered view or the cache
        # is stale.

        cl = repo.changelog
        filtername = repo.filtername
        bcache = self._per_filter.get(filtername)
        if bcache is None or not bcache.validfor(repo):
            # cache object missing or cache object stale? Read from disk
            bcache = branch_cache_from_file(repo)

        revs = []
        if bcache is None:
            # no (fresh) cache available anymore, perhaps we can re-use
            # the cache for a subset, then extend that to add info on missing
            # revisions.
            subsetname = subsettable.get(filtername)
            if subsetname is not None:
                subset = repo.filtered(subsetname)
                self.updatecache(subset)
                bcache = self._per_filter[subset.filtername].inherit_for(repo)
                extrarevs = subset.changelog.filteredrevs - cl.filteredrevs
                revs.extend(r for r in extrarevs if r <= bcache.tiprev)
            else:
                # nothing to fall back on, start empty.
                bcache = new_branch_cache(repo)

        revs.extend(cl.revs(start=bcache.tiprev + 1))
        if revs:
            bcache.update(repo, revs)

        assert bcache.validfor(repo), filtername
        self._per_filter[repo.filtername] = bcache

    def replace(self, repo, remotebranchmap):
        """Replace the branchmap cache for a repo with a branch mapping.

        This is likely only called during clone with a branch map from a
        remote.

        """
        cl = repo.changelog
        clrev = cl.rev
        clbranchinfo = cl.branchinfo
        rbheads = []
        closed = set()
        for bheads in remotebranchmap.values():
            rbheads += bheads
            for h in bheads:
                r = clrev(h)
                b, c = clbranchinfo(r)
                if c:
                    closed.add(h)

        if rbheads:
            rtiprev = max(int(clrev(node)) for node in rbheads)
            cache = new_branch_cache(
                repo,
                remotebranchmap,
                repo[rtiprev].node(),
                rtiprev,
                closednodes=closed,
            )

            # Try to stick it as low as possible
            # filter above served are unlikely to be fetch from a clone
            for candidate in (b'base', b'immutable', b'served'):
                rview = repo.filtered(candidate)
                if cache.validfor(rview):
                    cache._filtername = candidate
                    self._per_filter[candidate] = cache
                    cache._state = STATE_DIRTY
                    cache.write(rview)
                    return

    def clear(self):
        self._per_filter.clear()

    def write_dirty(self, repo):
        unfi = repo.unfiltered()
        for filtername in repoviewutil.get_ordered_subset():
            cache = self._per_filter.get(filtername)
            if cache is None:
                continue
            if filtername is None:
                repo = unfi
            else:
                repo = unfi.filtered(filtername)
            cache.sync_disk(repo)


def _unknownnode(node):
    """raises ValueError when branchcache found a node which does not exists"""
    raise ValueError('node %s does not exist' % node.hex())


def _branchcachedesc(repo):
    if repo.filtername is not None:
        return b'branch cache (%s)' % repo.filtername
    else:
        return b'branch cache'


class _BaseBranchCache:
    """A dict like object that hold branches heads cache.

    This cache is used to avoid costly computations to determine all the
    branch heads of a repo.
    """

    def __init__(
        self,
        repo: localrepo.localrepository,
        entries: (
            dict[bytes, list[bytes]] | Iterable[tuple[bytes, list[bytes]]]
        ) = (),
        closed_nodes: set[bytes] | None = None,
    ) -> None:
        """hasnode is a function which can be used to verify whether changelog
        has a given node or not. If it's not provided, we assume that every node
        we have exists in changelog"""
        # closednodes is a set of nodes that close their branch. If the branch
        # cache has been updated, it may contain nodes that are no longer
        # heads.
        if closed_nodes is None:
            closed_nodes = set()
        self._closednodes = set(closed_nodes)
        self._entries = dict(entries)

    def __iter__(self):
        return iter(self._entries)

    def __setitem__(self, key, value):
        self._entries[key] = value

    def __getitem__(self, key):
        return self._entries[key]

    def __contains__(self, key):
        return key in self._entries

    def iteritems(self):
        return self._entries.items()

    items = iteritems

    def hasbranch(self, label):
        """checks whether a branch of this name exists or not"""
        return label in self._entries

    def _branchtip(self, heads):
        """Return tuple with last open head in heads and false,
        otherwise return last closed head and true."""
        tip = heads[-1]
        closed = True
        for h in reversed(heads):
            if h not in self._closednodes:
                tip = h
                closed = False
                break
        return tip, closed

    def branchtip(self, branch):
        """Return the tipmost open head on branch head, otherwise return the
        tipmost closed head on branch.
        Raise KeyError for unknown branch."""
        return self._branchtip(self[branch])[0]

    def iteropen(self, nodes):
        return (n for n in nodes if n not in self._closednodes)

    def branchheads(self, branch, closed=False):
        heads = self._entries[branch]
        if not closed:
            heads = list(self.iteropen(heads))
        return heads

    def iterbranches(self):
        for bn, heads in self.items():
            yield (bn, heads) + self._branchtip(heads)

    def iterheads(self):
        """returns all the heads"""
        return self._entries.values()

    def update(self, repo, revgen):
        """Given a branchhead cache, self, that may have extra nodes or be
        missing heads, and a generator of nodes that are strictly a superset of
        heads missing, this function updates self to be correct.
        """
        starttime = util.timer()
        cl = repo.changelog
        # Faster than using ctx.obsolete()
        obsrevs = obsolete.getrevs(repo, b'obsolete')
        # collect new branch entries
        newbranches = {}
        new_closed = set()
        obs_ignored = set()
        getbranchinfo = repo.revbranchcache().branchinfo
        max_rev = -1
        for r in revgen:
            max_rev = max(max_rev, r)
            if r in obsrevs:
                # We ignore obsolete changesets as they shouldn't be
                # considered heads.
                obs_ignored.add(r)
                continue
            branch, closesbranch = getbranchinfo(r)
            newbranches.setdefault(branch, []).append(r)
            if closesbranch:
                new_closed.add(r)
        if max_rev < 0:
            msg = "running branchcache.update without revision to update"
            raise error.ProgrammingError(msg)

        self._process_new(
            repo,
            newbranches,
            new_closed,
            obs_ignored,
            max_rev,
        )

        self._closednodes.update(cl.node(rev) for rev in new_closed)

        duration = util.timer() - starttime
        repo.ui.log(
            b'branchcache',
            b'updated %s in %.4f seconds\n',
            _branchcachedesc(repo),
            duration,
        )
        return max_rev

    def _process_new(
        self,
        repo,
        newbranches,
        new_closed,
        obs_ignored,
        max_rev,
    ):
        """update the branchmap from a set of new information"""
        # Delay fetching the topological heads until they are needed.
        # A repository without non-continous branches can skip this part.
        topoheads = None

        cl = repo.changelog
        getbranchinfo = repo.revbranchcache().branchinfo
        # Faster than using ctx.obsolete()
        obsrevs = obsolete.getrevs(repo, b'obsolete')

        # If a changeset is visible, its parents must be visible too, so
        # use the faster unfiltered parent accessor.
        parentrevs = cl._uncheckedparentrevs

        for branch, newheadrevs in newbranches.items():
            # For every branch, compute the new branchheads.
            # A branchhead is a revision such that no descendant is on
            # the same branch.
            #
            # The branchheads are computed iteratively in revision order.
            # This ensures topological order, i.e. parents are processed
            # before their children. Ancestors are inclusive here, i.e.
            # any revision is an ancestor of itself.
            #
            # Core observations:
            # - The current revision is always a branchhead for the
            #   repository up to that point.
            # - It is the first revision of the branch if and only if
            #   there was no branchhead before. In that case, it is the
            #   only branchhead as there are no possible ancestors on
            #   the same branch.
            # - If a parent is on the same branch, a branchhead can
            #   only be an ancestor of that parent, if it is parent
            #   itself. Otherwise it would have been removed as ancestor
            #   of that parent before.
            # - Therefore, if all parents are on the same branch, they
            #   can just be removed from the branchhead set.
            # - If one parent is on the same branch and the other is not
            #   and there was exactly one branchhead known, the existing
            #   branchhead can only be an ancestor if it is the parent.
            #   Otherwise it would have been removed as ancestor of
            #   the parent before. The other parent therefore can't have
            #   a branchhead as ancestor.
            # - In all other cases, the parents on different branches
            #   could have a branchhead as ancestor. Those parents are
            #   kept in the "uncertain" set. If all branchheads are also
            #   topological heads, they can't have descendants and further
            #   checks can be skipped. Otherwise, the ancestors of the
            #   "uncertain" set are removed from branchheads.
            #   This computation is heavy and avoided if at all possible.
            bheads = self._entries.get(branch, [])
            bheadset = {cl.rev(node) for node in bheads}
            uncertain = set()
            for newrev in sorted(newheadrevs):
                if not bheadset:
                    bheadset.add(newrev)
                    continue

                parents = [p for p in parentrevs(newrev) if p != nullrev]
                samebranch = set()
                otherbranch = set()
                obsparents = set()
                for p in parents:
                    if p in obsrevs:
                        # We ignored this obsolete changeset earlier, but now
                        # that it has non-ignored children, we need to make
                        # sure their ancestors are not considered heads. To
                        # achieve that, we will simply treat this obsolete
                        # changeset as a parent from other branch.
                        obsparents.add(p)
                    elif p in bheadset or getbranchinfo(p)[0] == branch:
                        samebranch.add(p)
                    else:
                        otherbranch.add(p)
                if not (len(bheadset) == len(samebranch) == 1):
                    uncertain.update(otherbranch)
                    uncertain.update(obsparents)
                bheadset.difference_update(samebranch)
                bheadset.add(newrev)

            if uncertain:
                if topoheads is None:
                    topoheads = set(cl.headrevs())
                if bheadset - topoheads:
                    floorrev = min(bheadset)
                    if floorrev <= max(uncertain):
                        ancestors = set(cl.ancestors(uncertain, floorrev))
                        bheadset -= ancestors
            if bheadset:
                self[branch] = [cl.node(rev) for rev in sorted(bheadset)]


STATE_CLEAN = 1
STATE_INHERITED = 2
STATE_DIRTY = 3


class _LocalBranchCache(_BaseBranchCache):
    """base class of branch-map info for a local repo or repoview"""

    _base_filename = None
    _default_key_hashes: tuple[bytes] = cast(tuple[bytes], ())

    def __init__(
        self,
        repo: localrepo.localrepository,
        entries: (
            dict[bytes, list[bytes]] | Iterable[tuple[bytes, list[bytes]]]
        ) = (),
        tipnode: bytes | None = None,
        tiprev: int | None = nullrev,
        key_hashes: tuple[bytes] | None = None,
        closednodes: set[bytes] | None = None,
        hasnode: Callable[[bytes], bool] | None = None,
        verify_node: bool = False,
        inherited: bool = False,
    ) -> None:
        """hasnode is a function which can be used to verify whether changelog
        has a given node or not. If it's not provided, we assume that every node
        we have exists in changelog"""
        self._filtername = repo.filtername
        if tipnode is None:
            self.tipnode = repo.nullid
        else:
            self.tipnode = tipnode
        self.tiprev = tiprev
        if key_hashes is None:
            self.key_hashes = self._default_key_hashes
        else:
            self.key_hashes = key_hashes
        self._state = STATE_CLEAN
        if inherited:
            self._state = STATE_INHERITED

        super().__init__(repo=repo, entries=entries, closed_nodes=closednodes)
        # closednodes is a set of nodes that close their branch. If the branch
        # cache has been updated, it may contain nodes that are no longer
        # heads.

        # Do we need to verify branch at all ?
        self._verify_node = verify_node
        # branches for which nodes are verified
        self._verifiedbranches = set()
        self._hasnode = None
        if self._verify_node:
            self._hasnode = repo.changelog.hasnode

    def _compute_key_hashes(self, repo) -> tuple[bytes]:
        raise NotImplementedError

    def _ensure_populated(self, repo):
        """make sure any lazily loaded values are fully populated"""

    def _detect_pure_topo(self, repo) -> None:
        pass

    def validfor(self, repo):
        """check that cache contents are valid for (a subset of) this repo

        - False when the order of changesets changed or if we detect a strip.
        - True when cache is up-to-date for the current repo or its subset."""
        try:
            node = repo.changelog.node(self.tiprev)
        except IndexError:
            # changesets were stripped and now we don't even have enough to
            # find tiprev
            return False
        if self.tipnode != node:
            # tiprev doesn't correspond to tipnode: repo was stripped, or this
            # repo has a different order of changesets
            return False
        repo_key_hashes = self._compute_key_hashes(repo)
        # hashes don't match if this repo view has a different set of filtered
        # revisions (e.g. due to phase changes) or obsolete revisions (e.g.
        # history was rewritten)
        return self.key_hashes == repo_key_hashes

    @classmethod
    def fromfile(cls, repo):
        f = None
        try:
            f = repo.cachevfs(cls._filename(repo))
            lineiter = iter(f)
            init_kwargs = cls._load_header(repo, lineiter)
            bcache = cls(
                repo,
                verify_node=True,
                **init_kwargs,
            )
            if not bcache.validfor(repo):
                # invalidate the cache
                raise ValueError('tip differs')
            bcache._load_heads(repo, lineiter)
        except OSError:
            return None

        except Exception as inst:
            if repo.ui.debugflag:
                msg = b'invalid %s: %s\n'
                msg %= (
                    _branchcachedesc(repo),
                    stringutil.forcebytestr(inst),
                )
                repo.ui.debug(msg)
            bcache = None

        finally:
            if f:
                f.close()

        return bcache

    @classmethod
    def _load_header(cls, repo, lineiter) -> dict[str, Any]:
        raise NotImplementedError

    def _load_heads(self, repo, lineiter):
        """fully loads the branchcache by reading from the file using the line
        iterator passed"""
        for line in lineiter:
            line = line.rstrip(b'\n')
            if not line:
                continue
            node, state, label = line.split(b" ", 2)
            if state not in b'oc':
                raise ValueError('invalid branch state')
            label = encoding.tolocal(label.strip())
            node = bin(node)
            self._entries.setdefault(label, []).append(node)
            if state == b'c':
                self._closednodes.add(node)

    @classmethod
    def _filename(cls, repo):
        """name of a branchcache file for a given repo or repoview"""
        filename = cls._base_filename
        assert filename is not None
        if repo.filtername:
            filename = b'%s-%s' % (filename, repo.filtername)
        return filename

    def inherit_for(self, repo):
        """return a deep copy of the branchcache object"""
        assert repo.filtername != self._filtername
        other = type(self)(
            repo=repo,
            # we always do a shally copy of self._entries, and the values is
            # always replaced, so no need to deepcopy until the above remains
            # true.
            entries=self._entries,
            tipnode=self.tipnode,
            tiprev=self.tiprev,
            key_hashes=self.key_hashes,
            closednodes=set(self._closednodes),
            verify_node=self._verify_node,
            inherited=True,
        )
        # also copy information about the current verification state
        other._verifiedbranches = set(self._verifiedbranches)
        return other

    def sync_disk(self, repo):
        """synchronise the on disk file with the cache state

        If new value specific to this filter level need to be written, the file
        will be updated, if the state of the branchcache is inherited from a
        subset, any stalled on disk file will be deleted.

        That method does nothing if there is nothing to do.
        """
        if self._state == STATE_DIRTY:
            self.write(repo)
        elif self._state == STATE_INHERITED:
            filename = self._filename(repo)
            repo.cachevfs.tryunlink(filename)

    def write(self, repo):
        assert self._filtername == repo.filtername, (
            self._filtername,
            repo.filtername,
        )
        assert self._state == STATE_DIRTY, self._state
        # This method should not be called during an open transaction
        tr = repo.currenttransaction()
        if not getattr(tr, 'finalized', True):
            msg = "writing branchcache in the middle of a transaction"
            raise error.ProgrammingError(msg)
        try:
            filename = self._filename(repo)
            with repo.cachevfs(filename, b"w", atomictemp=True) as f:
                self._write_header(f)
                nodecount = self._write_heads(repo, f)
            repo.ui.log(
                b'branchcache',
                b'wrote %s with %d labels and %d nodes\n',
                _branchcachedesc(repo),
                len(self._entries),
                nodecount,
            )
            self._state = STATE_CLEAN
        except (OSError, error.Abort) as inst:
            # Abort may be raised by read only opener, so log and continue
            repo.ui.debug(
                b"couldn't write branch cache: %s\n"
                % stringutil.forcebytestr(inst)
            )

    def _write_header(self, fp) -> None:
        raise NotImplementedError

    def _write_heads(self, repo, fp) -> int:
        """write list of heads to a file

        Return the number of heads written."""
        nodecount = 0
        for label, nodes in sorted(self._entries.items()):
            label = encoding.fromlocal(label)
            for node in nodes:
                nodecount += 1
                if node in self._closednodes:
                    state = b'c'
                else:
                    state = b'o'
                fp.write(b"%s %s %s\n" % (hex(node), state, label))
        return nodecount

    def _verifybranch(self, branch):
        """verify head nodes for the given branch."""
        if not self._verify_node:
            return
        if branch not in self._entries or branch in self._verifiedbranches:
            return
        assert self._hasnode is not None
        for n in self._entries[branch]:
            if not self._hasnode(n):
                _unknownnode(n)

        self._verifiedbranches.add(branch)

    def _verifyall(self):
        """verifies nodes of all the branches"""
        for b in self._entries.keys():
            if b not in self._verifiedbranches:
                self._verifybranch(b)

    def __getitem__(self, key):
        self._verifybranch(key)
        return super().__getitem__(key)

    def __contains__(self, key):
        self._verifybranch(key)
        return super().__contains__(key)

    def iteritems(self):
        self._verifyall()
        return super().iteritems()

    items = iteritems

    def iterheads(self):
        """returns all the heads"""
        self._verifyall()
        return super().iterheads()

    def hasbranch(self, label):
        """checks whether a branch of this name exists or not"""
        self._verifybranch(label)
        return super().hasbranch(label)

    def branchheads(self, branch, closed=False):
        self._verifybranch(branch)
        return super().branchheads(branch, closed=closed)

    def update(self, repo, revgen):
        assert self._filtername == repo.filtername, (
            self._filtername,
            repo.filtername,
        )
        cl = repo.changelog
        if self._hasnode is not None:
            self._hasnode = cl.hasnode
        max_rev = super().update(repo, revgen)
        # new tip revision which we found after iterating items from new
        # branches
        if max_rev is not None and max_rev > self.tiprev:
            self.tiprev = max_rev
            self.tipnode = cl.node(max_rev)
        else:
            # We should not be here is if this is false
            assert cl.node(self.tiprev) == self.tipnode

        if not self.validfor(repo):
            # the tiprev and tipnode should be aligned, so if the current repo
            # is not seens as valid this is because old cache key is now
            # invalid for the repo.
            #
            # However. we've just updated the cache and we assume it's valid,
            # so let's make the cache key valid as well by recomputing it from
            # the cached data
            self.key_hashes = self._compute_key_hashes(repo)
            self.filteredhash = scmutil.combined_filtered_and_obsolete_hash(
                repo,
                self.tiprev,
            )

        self._state = STATE_DIRTY
        tr = repo.currenttransaction()
        if getattr(tr, 'finalized', True):
            # Avoid premature writing.
            #
            # (The cache warming setup by localrepo will update the file later.)
            self.write(repo)


def branch_cache_from_file(repo) -> _LocalBranchCache | None:
    """Build a branch cache from on-disk data if possible

    Return a branch cache of the right format depending of the repository.
    """
    if repo.ui.configbool(b"experimental", b"branch-cache-v3"):
        return BranchCacheV3.fromfile(repo)
    else:
        return BranchCacheV2.fromfile(repo)


def new_branch_cache(repo, *args, **kwargs):
    """Build a new branch cache from argument

    Return a branch cache of the right format depending of the repository.
    """
    if repo.ui.configbool(b"experimental", b"branch-cache-v3"):
        return BranchCacheV3(repo, *args, **kwargs)
    else:
        return BranchCacheV2(repo, *args, **kwargs)


class BranchCacheV2(_LocalBranchCache):
    """a branch cache using version 2 of the format on disk

    The cache is serialized on disk in the following format:

    <tip hex node> <tip rev number> [optional filtered repo hex hash]
    <branch head hex node> <open/closed state> <branch name>
    <branch head hex node> <open/closed state> <branch name>
    ...

    The first line is used to check if the cache is still valid. If the
    branch cache is for a filtered repo view, an optional third hash is
    included that hashes the hashes of all filtered and obsolete revisions.

    The open/closed state is represented by a single letter 'o' or 'c'.
    This field can be used to avoid changelog reads when determining if a
    branch head closes a branch or not.
    """

    _base_filename = b"branch2"

    @classmethod
    def _load_header(cls, repo, lineiter) -> dict[str, Any]:
        """parse the head of a branchmap file

        return parameters to pass to a newly created class instance.
        """
        cachekey = next(lineiter).rstrip(b'\n').split(b" ", 2)
        last, lrev = cachekey[:2]
        last, lrev = bin(last), int(lrev)
        filteredhash = ()
        if len(cachekey) > 2:
            filteredhash = (bin(cachekey[2]),)
        return {
            "tipnode": last,
            "tiprev": lrev,
            "key_hashes": filteredhash,
        }

    def _write_header(self, fp) -> None:
        """write the branch cache header to a file"""
        cachekey = [hex(self.tipnode), b'%d' % self.tiprev]
        if self.key_hashes:
            cachekey.append(hex(self.key_hashes[0]))
        fp.write(b" ".join(cachekey) + b'\n')

    def _compute_key_hashes(self, repo) -> tuple[bytes]:
        """return the cache key hashes that match this repoview state"""
        filtered_hash = scmutil.combined_filtered_and_obsolete_hash(
            repo,
            self.tiprev,
            needobsolete=True,
        )
        keys: tuple[bytes] = cast(tuple[bytes], ())
        if filtered_hash is not None:
            keys: tuple[bytes] = (filtered_hash,)
        return keys


class BranchCacheV3(_LocalBranchCache):
    """a branch cache using version 3 of the format on disk

    This version is still EXPERIMENTAL and the format is subject to changes.

    The cache is serialized on disk in the following format:

    <cache-key-xxx>=<xxx-value> <cache-key-yyy>=<yyy-value> […]
    <branch head hex node> <open/closed state> <branch name>
    <branch head hex node> <open/closed state> <branch name>
    ...

    The first line is used to check if the cache is still valid. It is a series
    of key value pair. The following key are recognized:

    - tip-rev: the rev-num of the tip-most revision seen by this cache
    - tip-node: the node-id of the tip-most revision sen by this cache
    - filtered-hash: the hash of all filtered revisions (before tip-rev)
                     ignored by this cache.
    - obsolete-hash: the hash of all non-filtered obsolete revisions (before
                     tip-rev) ignored by this cache.

    The tip-rev is used to know how far behind the value in the file are
    compared to the current repository state.

    The tip-node, filtered-hash and obsolete-hash are used to detect if this
    cache can be used for this repository state at all.

    The open/closed state is represented by a single letter 'o' or 'c'.
    This field can be used to avoid changelog reads when determining if a
    branch head closes a branch or not.

    Topological heads are not included in the listing and should be dispatched
    on the right branch at read time. Obsolete topological heads should be
    ignored.
    """

    _base_filename = b"branch3-exp"
    _default_key_hashes = (None, None)

    def __init__(self, *args, pure_topo_branch=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._pure_topo_branch = pure_topo_branch
        self._needs_populate = self._pure_topo_branch is not None

    def inherit_for(self, repo):
        new = super().inherit_for(repo)
        new._pure_topo_branch = self._pure_topo_branch
        new._needs_populate = self._needs_populate
        return new

    def _get_topo_heads(self, repo):
        """returns the topological head of a repoview content up to self.tiprev"""
        cl = repo.changelog
        if self.tiprev == nullrev:
            return []
        elif self.tiprev == cl.tiprev():
            return cl.headrevs()
        else:
            heads = cl.headrevs(stop_rev=self.tiprev + 1)
            return heads

    def _write_header(self, fp) -> None:
        cache_keys = {
            b"tip-node": hex(self.tipnode),
            b"tip-rev": b'%d' % self.tiprev,
        }
        if self.key_hashes:
            if self.key_hashes[0] is not None:
                cache_keys[b"filtered-hash"] = hex(self.key_hashes[0])
            if self.key_hashes[1] is not None:
                cache_keys[b"obsolete-hash"] = hex(self.key_hashes[1])
        if self._pure_topo_branch is not None:
            cache_keys[b"topo-mode"] = b"pure"
        pieces = (b"%s=%s" % i for i in sorted(cache_keys.items()))
        fp.write(b" ".join(pieces) + b'\n')
        if self._pure_topo_branch is not None:
            label = encoding.fromlocal(self._pure_topo_branch)
            fp.write(label + b'\n')

    def _write_heads(self, repo, fp) -> int:
        """write list of heads to a file

        Return the number of heads written."""
        to_node = repo.changelog.node
        nodecount = 0
        topo_heads = None
        if self._pure_topo_branch is None:
            # we match using node because it is faster to built the set of node
            # than to resolve node → rev later.
            topo_heads = {to_node(r) for r in self._get_topo_heads(repo)}
        for label, nodes in sorted(self._entries.items()):
            if label == self._pure_topo_branch:
                # not need to write anything the header took care of that
                continue
            label = encoding.fromlocal(label)
            for node in nodes:
                if topo_heads is not None:
                    if node in topo_heads:
                        continue
                if node in self._closednodes:
                    state = b'c'
                else:
                    state = b'o'
                nodecount += 1
                fp.write(b"%s %s %s\n" % (hex(node), state, label))
        return nodecount

    @classmethod
    def _load_header(cls, repo, lineiter):
        header_line = next(lineiter)
        pieces = header_line.rstrip(b'\n').split(b" ")
        for p in pieces:
            if b'=' not in p:
                msg = b"invalid header_line: %r" % header_line
                raise ValueError(msg)
        cache_keys = dict(p.split(b'=', 1) for p in pieces)

        args = {}
        filtered_hash = None
        obsolete_hash = None
        has_pure_topo_heads = False
        for k, v in cache_keys.items():
            if k == b"tip-rev":
                args["tiprev"] = int(v)
            elif k == b"tip-node":
                args["tipnode"] = bin(v)
            elif k == b"filtered-hash":
                filtered_hash = bin(v)
            elif k == b"obsolete-hash":
                obsolete_hash = bin(v)
            elif k == b"topo-mode":
                if v == b"pure":
                    has_pure_topo_heads = True
                else:
                    msg = b"unknown topo-mode: %r" % v
                    raise ValueError(msg)
            else:
                msg = b"unknown cache key: %r" % k
                raise ValueError(msg)
        args["key_hashes"] = (filtered_hash, obsolete_hash)
        if has_pure_topo_heads:
            pure_line = next(lineiter).rstrip(b'\n')
            args["pure_topo_branch"] = encoding.tolocal(pure_line)
        return args

    def _load_heads(self, repo, lineiter):
        """fully loads the branchcache by reading from the file using the line
        iterator passed"""
        super()._load_heads(repo, lineiter)
        if self._pure_topo_branch is not None:
            # no need to read the repository heads, we know their value already.
            return
        cl = repo.changelog
        getbranchinfo = repo.revbranchcache().branchinfo
        obsrevs = obsolete.getrevs(repo, b'obsolete')
        to_node = cl.node
        touched_branch = set()
        for head in self._get_topo_heads(repo):
            if head in obsrevs:
                continue
            node = to_node(head)
            branch, closed = getbranchinfo(head)
            self._entries.setdefault(branch, []).append(node)
            if closed:
                self._closednodes.add(node)
            touched_branch.add(branch)
        to_rev = cl.index.rev
        for branch in touched_branch:
            self._entries[branch].sort(key=to_rev)

    def _compute_key_hashes(self, repo) -> tuple[bytes]:
        """return the cache key hashes that match this repoview state"""
        return scmutil.filtered_and_obsolete_hash(
            repo,
            self.tiprev,
        )

    def _process_new(
        self,
        repo,
        newbranches,
        new_closed,
        obs_ignored,
        max_rev,
    ) -> None:
        if (
            # note: the check about `obs_ignored` is too strict as the
            # obsolete revision could be non-topological, but lets keep
            # things simple for now
            #
            # The same apply to `new_closed` if the closed changeset are
            # not a head, we don't care that it is closed, but lets keep
            # things simple here too.
            not (obs_ignored or new_closed)
            and (
                not newbranches
                or (
                    len(newbranches) == 1
                    and (
                        self.tiprev == nullrev
                        or self._pure_topo_branch in newbranches
                    )
                )
            )
        ):
            if newbranches:
                assert len(newbranches) == 1
                self._pure_topo_branch = list(newbranches.keys())[0]
                self._needs_populate = True
                self._entries.pop(self._pure_topo_branch, None)
            return

        self._ensure_populated(repo)
        self._pure_topo_branch = None
        super()._process_new(
            repo,
            newbranches,
            new_closed,
            obs_ignored,
            max_rev,
        )

    def _ensure_populated(self, repo):
        """make sure any lazily loaded values are fully populated"""
        if self._needs_populate:
            assert self._pure_topo_branch is not None
            cl = repo.changelog
            to_node = cl.node
            topo_heads = self._get_topo_heads(repo)
            heads = [to_node(r) for r in topo_heads]
            self._entries[self._pure_topo_branch] = heads
            self._needs_populate = False

    def _detect_pure_topo(self, repo) -> None:
        if self._pure_topo_branch is not None:
            # we are pure topological already
            return
        to_node = repo.changelog.node
        topo_heads = [to_node(r) for r in self._get_topo_heads(repo)]
        if any(n in self._closednodes for n in topo_heads):
            return
        for branch, heads in self._entries.items():
            if heads == topo_heads:
                self._pure_topo_branch = branch
                self._state = STATE_DIRTY
                break


class remotebranchcache(_BaseBranchCache):
    """Branchmap info for a remote connection, should not write locally"""

    def __init__(
        self,
        repo: localrepo.localrepository,
        entries: (
            dict[bytes, list[bytes]] | Iterable[tuple[bytes, list[bytes]]]
        ) = (),
        closednodes: set[bytes] | None = None,
    ) -> None:
        super().__init__(repo=repo, entries=entries, closed_nodes=closednodes)
