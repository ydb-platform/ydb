# filelog.py - file history class for mercurial
#
# Copyright 2005-2007 Olivia Mackall <olivia@selenic.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import annotations

from typing import (
    Iterable,
    Iterator,
)

from .i18n import _
from .node import bin, nullrev
from . import (
    error,
    mdiff,
    revlog,
    revlogutils,
)
from .interfaces import (
    repository,
    types,
)
from .utils import storageutil
from .revlogutils import (
    constants as revlog_constants,
    deltas,
    rewrite,
)


class filelog(repository.ifilestorage):
    _revlog: revlog.revlog
    nullid: bytes
    _fix_issue6528: bool

    def __init__(
        self,
        opener: types.VfsT,
        path: types.HgPathT,
        writable: bool,
        *,
        try_split: bool = False,
    ):
        upper_bound_comp = None
        delta_config = opener.options.get(b'delta-config')
        if delta_config is not None and delta_config.file_max_comp_ratio > 0:
            upper_bound_comp = delta_config.file_max_comp_ratio
        self._revlog = revlog.revlog(
            opener,
            # XXX should use the unencoded path
            target=(revlog_constants.KIND_FILELOG, path),
            radix=b'/'.join((b'data', path)),
            censorable=True,
            upperboundcomp=upper_bound_comp,
            canonical_parent_order=False,  # see comment in revlog.py
            try_split=try_split,
            writable=writable,
        )
        # Full name of the user visible file, relative to the repository root.
        # Used by LFS.
        self._revlog.filename = path
        self.nullid = self._revlog.nullid
        opts = opener.options
        self._fix_issue6528 = opts.get(b'issue6528.fix-incoming', True)

    def get_revlog(self) -> revlog.revlog:
        """return an actual revlog instance if any

        This exist because a lot of code leverage the fact the underlying
        storage is a revlog for optimization, so giving simple way to access
        the revlog instance helps such code.
        """
        return self._revlog

    def __len__(self) -> int:
        return len(self._revlog)

    def __iter__(self) -> Iterator[int]:
        return self._revlog.__iter__()

    def hasnode(self, node):
        if node in (self.nullid, nullrev):
            return False

        try:
            self._revlog.rev(node)
            return True
        except (TypeError, ValueError, IndexError, error.LookupError):
            return False

    def revs(self, start=0, stop=None):
        return self._revlog.revs(start=start, stop=stop)

    def parents(self, node):
        return self._revlog.parents(node)

    def parentrevs(self, rev):
        return self._revlog.parentrevs(rev)

    def rev(self, node):
        return self._revlog.rev(node)

    def node(self, rev):
        return self._revlog.node(rev)

    def lookup(self, node):
        return storageutil.fileidlookup(
            self._revlog, node, self._revlog.display_id
        )

    def linkrev(self, rev):
        return self._revlog.linkrev(rev)

    def commonancestorsheads(self, node1, node2):
        return self._revlog.commonancestorsheads(node1, node2)

    # Used by dag_util.blockdescendants().
    def descendants(self, revs):
        return self._revlog.descendants(revs)

    def heads(self, start=None, stop=None):
        return self._revlog.heads(start, stop)

    # Used by hgweb, children extension.
    def children(self, node):
        return self._revlog.children(node)

    def iscensored(self, rev):
        return self._revlog.iscensored(rev)

    def revision(self, node):
        return self._revlog.revision(node)

    def rawdata(self, node):
        return self._revlog.rawdata(node)

    def emitrevisions(
        self,
        nodes,
        nodesorder=None,
        revisiondata=False,
        assumehaveparentrevisions=False,
        deltamode=repository.CG_DELTAMODE_STD,
        sidedata_helpers=None,
        debug_info=None,
        use_hasmeta_flag=False,
    ):
        all_revision_data = self._revlog.emitrevisions(
            nodes,
            nodesorder=nodesorder,
            revisiondata=revisiondata,
            assumehaveparentrevisions=assumehaveparentrevisions,
            deltamode=deltamode,
            sidedata_helpers=sidedata_helpers,
            debug_info=debug_info,
        )
        revlog_hasmeta_flag = (
            self._revlog._format_flags & revlog.FLAG_FILELOG_META
        )
        if use_hasmeta_flag and not revlog_hasmeta_flag:
            for d in all_revision_data:
                # XXX this is going to be very slow, but this get the format
                # rolling, it need to be taken care of before getting out of
                # experimental.
                #
                # XXX we should use "rev" for faster lookup
                # (or better have emit revision yield the right data)
                if self.has_meta(d.node):
                    if d.p1node == self.nullid and d.p2node != self.nullid:
                        d.p1node, d.p2node = d.p2node, d.p1node
                    d.flags |= revlog_constants.REVIDX_HASMETA
                yield d
        elif (not use_hasmeta_flag) and revlog_hasmeta_flag:
            for d in all_revision_data:
                if d.flags & revlog_constants.REVIDX_HASMETA:
                    d.p1node, d.p2node = d.p2node, d.p1node
                    d.flags &= ~revlog_constants.REVIDX_HASMETA
                yield d
        else:
            yield from all_revision_data

    def addrevision(
        self,
        revisiondata,
        transaction,
        linkrev,
        p1,
        p2,
        node=None,
        flags=revlog_constants.REVIDX_DEFAULT_FLAGS,
        cachedelta=None,
    ):
        if (
            revlog_constants.REVIDX_HASMETA & flags
            and not self._revlog._format_flags & revlog.FLAG_FILELOG_META
        ):
            assert p2 == self.nullid
            p1, p2 = self.nullid, p1
            flags &= ~revlog_constants.REVIDX_HASMETA

        return self._revlog.addrevision(
            revisiondata,
            transaction,
            linkrev,
            p1,
            p2,
            node=node,
            flags=flags,
            cachedelta=cachedelta,
        )

    def addgroup(
        self,
        deltas,
        linkmapper,
        transaction,
        addrevisioncb=None,
        duplicaterevisioncb=None,
        maybemissingparents=False,
        debug_info=None,
        delta_base_reuse_policy=None,
        use_hasmeta_flag=True,
    ):
        if maybemissingparents:
            raise error.Abort(
                _(
                    b'revlog storage does not support missing '
                    b'parents write mode'
                )
            )

        with self._revlog._writing(transaction):
            revlog_hasmeta_flag = (
                self._revlog._format_flags & revlog.FLAG_FILELOG_META
            )
            if use_hasmeta_flag:
                if not revlog_hasmeta_flag:
                    deltas = self._hasmeta_group_to_old_format(deltas)
            else:
                if revlog_hasmeta_flag:
                    deltas = self._oldformat_to_hasmeta_group(deltas)
                elif self._fix_issue6528:
                    deltas = rewrite.filter_delta_issue6528(
                        self._revlog, deltas
                    )

            return self._revlog.addgroup(
                deltas,
                linkmapper,
                transaction,
                addrevisioncb=addrevisioncb,
                duplicaterevisioncb=duplicaterevisioncb,
                debug_info=debug_info,
                delta_base_reuse_policy=delta_base_reuse_policy,
            )

    def _hasmeta_group_to_old_format(self, deltas_iter):
        """translate a group of delta with "hasmeta" information to old format

        If the format does not support the "REVIDX_HASMETA" flag, we need to
        store them differently.

        This direction is fairly easy, we just need to drop the flag and use
        "nullid" for p1 when relevant.
        (And mark the delta as not using the flag).
        """
        for d in deltas_iter:
            if d.flags & revlog_constants.REVIDX_HASMETA:
                d.flags &= ~revlog_constants.REVIDX_HASMETA
                if d.p1 == self.nullid and d.p2 != self.nullid:
                    d.p1, d.p2 = d.p2, d.p1
            d.has_filelog_hasmeta_flag = False
            yield d

    def _oldformat_to_hasmeta_group(self, deltas_iter):
        """translate a delta group without "hasmeta" information to new format

        If the inbound group does not support carry the "REVIDX_HASMETA" flag,
        but the destination storage do, we need to store them differently.
        store them differently.

        This direction is harder as we have to check for metadata present in
        the bundle.  We currently do it in a very slow way, but faster solution
        are available.
        """
        deltacomputer = deltas.deltacomputer(self._revlog)
        for d in deltas_iter:
            # XXX there are multiple option to speed this up as we can rely on
            # previous revision, and we only need to check the start of the
            # delta. However, we keep things simple for now.
            if d.raw_text is None:
                delta_base_rev = self._revlog.rev(d.delta_base)
                base_size = self._revlog.size(delta_base_rev)
                textlen = mdiff.patchedsize(base_size, d.delta)
                revinfo = revlogutils.revisioninfo(
                    d.node,
                    d.p1,
                    d.p2,
                    None,
                    textlen,
                    revlogutils.CachedDelta(delta_base_rev, d.delta),
                    d.flags,
                )
                d.raw_text = deltacomputer.buildtext(revinfo)
            if (
                d.raw_text[: revlog_constants.META_MARKER_SIZE]
                == revlog_constants.META_MARKER
            ):
                d.flags |= revlog_constants.REVIDX_HASMETA
            if d.p1 == self.nullid and d.p2 != self.nullid:
                d.p1, d.p2 = d.p2, d.p1
            d.has_filelog_hasmeta_flag = True
            yield d

    def getstrippoint(self, minlink):
        return self._revlog.getstrippoint(minlink)

    def strip(self, minlink, transaction):
        return self._revlog.strip(minlink, transaction)

    def censorrevision(self, tr, node, tombstone=b''):
        return self._revlog.censorrevision(tr, node, tombstone=tombstone)

    def files(self):
        return self._revlog.files()

    def read(self, node):
        return storageutil.filtermetadata(self.revision(node))

    def add(self, text, meta, transaction, link, p1=None, p2=None):
        flags = revlog_constants.REVIDX_DEFAULT_FLAGS
        if meta or text.startswith(b'\1\n'):
            text = storageutil.packmeta(meta, text)
            flags |= revlog_constants.REVIDX_HASMETA
        rev = self.addrevision(text, transaction, link, p1, p2, flags=flags)
        return self.node(rev)

    def has_meta(self, node):
        rev = self._revlog.rev(node)
        if self._revlog._format_flags & revlog.FLAG_FILELOG_META:
            return self._revlog.flags(rev) & revlog_constants.REVIDX_HASMETA
        else:
            if self.parentrevs(rev)[0] != nullrev:
                return False
            return self._revlog.rawdata(rev)[:2] == b'\x01\n'

    def renamed(self, node):
        """Resolve file revision copy metadata.

        Returns ``None`` if the file has no copy metadata. Otherwise a
        2-tuple of the source filename and node.
        """
        rev = self.rev(node)
        if (
            self._revlog._format_flags & revlog.FLAG_FILELOG_META
            and not self.has_meta(node)
        ):
            return None
        elif self.parentrevs(rev)[0] != nullrev:
            # When creating a copy or move we set filelog parents to null,
            # because contents are probably unrelated and making a delta
            # would not be useful.
            # Conversely, if filelog p1 is non-null we know
            # there is no copy metadata.
            # In the presence of merges, this reasoning becomes invalid
            # if we reorder parents. See tests/test-issue6528.t.
            return None

        meta = storageutil.parsemeta(self.revision(rev))[0]

        # copy and copyrev occur in pairs. In rare cases due to old bugs,
        # one can occur without the other. So ensure both are present to flag
        # as a copy.
        if meta and b'copy' in meta and b'copyrev' in meta:
            return meta[b'copy'], bin(meta[b'copyrev'])

        return None

    def size(self, rev):
        """return the size of a given revision"""

        # for revisions with renames, we have to go the slow way
        node = self.node(rev)
        if self.iscensored(rev):
            return 0
        if self.has_meta(node):
            return len(self.read(node))

        # XXX if self.read(node).startswith("\1\n"), this returns (size+4)
        # XXX See also basefilectx.cmp.
        return self._revlog.size(rev)

    def cmp(self, node, text):
        """compare text with a given file revision

        returns True if text is different than what is stored.
        """
        return not storageutil.filedataequivalent(self, node, text)

    def verifyintegrity(self, state) -> Iterable[repository.iverifyproblem]:
        return self._revlog.verifyintegrity(state)

    def storageinfo(
        self,
        exclusivefiles=False,
        sharedfiles=False,
        revisionscount=False,
        trackedsize=False,
        storedsize=False,
    ):
        return self._revlog.storageinfo(
            exclusivefiles=exclusivefiles,
            sharedfiles=sharedfiles,
            revisionscount=revisionscount,
            trackedsize=trackedsize,
            storedsize=storedsize,
        )

    # Used by repo upgrade.
    def clone(self, tr, destrevlog, **kwargs):
        if not isinstance(destrevlog, filelog):
            msg = b'expected filelog to clone(), not %r'
            msg %= destrevlog
            raise error.ProgrammingError(msg)

        src_meta = bool(self._revlog._format_flags & revlog.FLAG_FILELOG_META)
        dst_meta = bool(
            destrevlog._revlog._format_flags & revlog.FLAG_FILELOG_META
        )
        kw = kwargs
        if src_meta and not dst_meta:
            kw['hasmeta_change'] = revlog_constants.FILELOG_HASMETA_DOWNGRADE
        elif dst_meta and not src_meta:
            kw['hasmeta_change'] = revlog_constants.FILELOG_HASMETA_UPGRADE

        return self._revlog.clone(tr, destrevlog._revlog, **kwargs)


class narrowfilelog(filelog):
    """Filelog variation to be used with narrow stores."""

    def __init__(self, opener, path, narrowmatch, writable, *, try_split=False):
        super().__init__(opener, path, writable=writable, try_split=try_split)
        self._narrowmatch = narrowmatch

    def renamed(self, node):
        res = super().renamed(node)

        # Renames that come from outside the narrowspec are problematic
        # because we may lack the base text for the rename. This can result
        # in code attempting to walk the ancestry or compute a diff
        # encountering a missing revision. We address this by silently
        # removing rename metadata if the source file is outside the
        # narrow spec.
        #
        # A better solution would be to see if the base revision is available,
        # rather than assuming it isn't.
        #
        # An even better solution would be to teach all consumers of rename
        # metadata that the base revision may not be available.
        #
        # TODO consider better ways of doing this.
        if res and not self._narrowmatch(res[0]):
            return None

        return res

    def size(self, rev):
        # Because we have a custom renamed() that may lie, we need to call
        # the base renamed() to report accurate results.
        node = self.node(rev)
        if super().renamed(node):
            return len(self.read(node))
        else:
            return super().size(rev)

    def cmp(self, node, text):
        # We don't call `super` because narrow parents can be buggy in case of a
        # ambiguous dirstate. Always take the slow path until there is a better
        # fix, see issue6150.

        # Censored files compare against the empty file.
        if self.iscensored(self.rev(node)):
            return text != b''

        return self.read(node) != text
