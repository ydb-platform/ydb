# censor code related to censoring revision
#
# Copyright 2021 Pierre-Yves David <pierre-yves.david@octobus.net>
# Copyright 2015 Google, Inc <martinvonz@google.com>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import annotations

import binascii
import contextlib
import os

from ..node import (
    nullrev,
)
from .constants import (
    COMP_MODE_PLAIN,
    ENTRY_DATA_COMPRESSED_LENGTH,
    ENTRY_DATA_COMPRESSION_MODE,
    ENTRY_DATA_OFFSET,
    ENTRY_DATA_UNCOMPRESSED_LENGTH,
    ENTRY_DELTA_BASE,
    ENTRY_LINK_REV,
    ENTRY_NODE_ID,
    ENTRY_PARENT_1,
    ENTRY_PARENT_2,
    ENTRY_SIDEDATA_COMPRESSED_LENGTH,
    ENTRY_SIDEDATA_COMPRESSION_MODE,
    ENTRY_SIDEDATA_OFFSET,
    META_MARKER,
    META_MARKER_SIZE,
    REVIDX_ISCENSORED,
    REVLOGV0,
    REVLOGV1,
)
from ..i18n import _

from .. import (
    error,
    mdiff,
    pycompat,
    revlogutils,
    util,
)
from ..utils import (
    storageutil,
)
from . import (
    constants,
    deltas,
)


def v1_censor(rl, tr, censor_nodes, tombstone=b''):
    """censors a revision in a "version 1" revlog"""
    assert rl._format_version == constants.REVLOGV1, rl._format_version

    # avoid cycle
    from .. import revlog

    censor_revs = {rl.rev(node) for node in censor_nodes}
    tombstone = storageutil.packmeta({b'censored': tombstone}, b'')

    # Rewriting the revlog in place is hard. Our strategy for censoring is
    # to create a new revlog, copy all revisions to it, then replace the
    # revlogs on transaction close.
    #
    # This is a bit dangerous. We could easily have a mismatch of state.
    newrl = revlog.revlog(
        rl.opener,
        target=rl.target,
        radix=rl.radix,
        postfix=b'tmpcensored',
        censorable=True,
        data_config=rl.data_config,
        delta_config=rl.delta_config,
        feature_config=rl.feature_config,
        may_inline=rl._inline,
        writable=True,
    )
    # inline splitting will prepare some transaction work that will get
    # confused by the final file move. So if there is a risk of not being
    # inline at the end, we prevent the new revlog to be inline in the first
    # place.
    assert not (newrl._inline and not rl._inline)

    for rev in rl.revs():
        node = rl.node(rev)
        p1, p2 = rl.parents(node)

        if rev in censor_revs:
            newrl.addrawrevision(
                tombstone,
                tr,
                rl.linkrev(rev),
                p1,
                p2,
                node,
                constants.REVIDX_ISCENSORED,
            )

            if newrl.deltaparent(rev) != nullrev:
                m = _(b'censored revision stored as delta; cannot censor')
                h = _(
                    b'censoring of revlogs is not fully implemented;'
                    b' please report this bug'
                )
                raise error.Abort(m, hint=h)
            continue

        if rl.iscensored(rev):
            if rl.deltaparent(rev) != nullrev:
                m = _(
                    b'cannot censor due to censored '
                    b'revision having delta stored'
                )
                raise error.Abort(m)
            rawtext = rl._inner._chunk(rev)
        else:
            rawtext = rl.rawdata(rev)

        newrl.addrawrevision(
            rawtext, tr, rl.linkrev(rev), p1, p2, node, rl.flags(rev)
        )

    tr.addbackup(rl._indexfile, location=b'store')
    if not rl._inline:
        tr.addbackup(rl._datafile, location=b'store')

    rl.opener.rename(newrl._indexfile, rl._indexfile)
    if newrl._inline:
        assert rl._inline
    else:
        assert not rl._inline
        rl.opener.rename(newrl._datafile, rl._datafile)

    rl.clearcaches()
    index, chunk_cache = rl._loadindex()
    rl._load_inner(index, chunk_cache)


def v2_censor(revlog, tr, censor_nodes, tombstone=b''):
    """censors a revision in a "version 2" revlog"""
    assert revlog._format_version != REVLOGV0, revlog._format_version
    assert revlog._format_version != REVLOGV1, revlog._format_version

    censor_revs = {revlog.rev(node) for node in censor_nodes}
    _rewrite_v2(revlog, tr, censor_revs, tombstone)


def _rewrite_v2(revlog, tr, censor_revs, tombstone=b''):
    """rewrite a revlog to censor some of its content

    General principle

    We create new revlog files (index/data/sidedata) to copy the content of
    the existing data without the censored data.

    We need to recompute new delta for any revision that used the censored
    revision as delta base. As the cumulative size of the new delta may be
    large, we store them in a temporary file until they are stored in their
    final destination.

    All data before the censored data can be blindly copied. The rest needs
    to be copied as we go and the associated index entry needs adjustement.
    """
    assert revlog._format_version != REVLOGV0, revlog._format_version
    assert revlog._format_version != REVLOGV1, revlog._format_version

    old_index = revlog.index
    docket = revlog._docket

    tombstone = storageutil.packmeta({b'censored': tombstone}, b'')

    first_excl_rev = min(censor_revs)

    first_excl_entry = revlog.index[first_excl_rev]
    index_cutoff = revlog.index.entry_size * first_excl_rev
    data_cutoff = first_excl_entry[ENTRY_DATA_OFFSET] >> 16
    sidedata_cutoff = revlog.sidedata_cut_off(first_excl_rev)

    with pycompat.unnamedtempfile(mode=b"w+b") as tmp_storage:
        # rev → (new_base, data_start, data_end, compression_mode)
        rewritten_entries = _precompute_rewritten_delta(
            revlog,
            old_index,
            censor_revs,
            tmp_storage,
        )

        all_files = _setup_new_files(
            revlog,
            index_cutoff,
            data_cutoff,
            sidedata_cutoff,
        )

        # we dont need to open the old index file since its content already
        # exist in a usable form in `old_index`.
        with all_files() as open_files:
            (
                old_data_file,
                old_sidedata_file,
                new_index_file,
                new_data_file,
                new_sidedata_file,
            ) = open_files

            # writing the censored revision

            # Writing all subsequent revisions
            for rev in range(first_excl_rev, len(old_index)):
                if rev in censor_revs:
                    _rewrite_censor(
                        revlog,
                        old_index,
                        open_files,
                        rev,
                        tombstone,
                    )
                else:
                    _rewrite_simple(
                        revlog,
                        old_index,
                        open_files,
                        rev,
                        rewritten_entries,
                        tmp_storage,
                    )
    docket.write(transaction=None, stripping=True)


def _precompute_rewritten_delta(
    revlog,
    old_index,
    excluded_revs,
    tmp_storage,
):
    """Compute new delta for revisions whose delta is based on revision that
    will not survive as is.

    Return a mapping: {rev → (new_base, data_start, data_end, compression_mode)}
    """
    dc = deltas.deltacomputer(revlog)
    rewritten_entries = {}
    first_excl_rev = min(excluded_revs)
    with revlog.reading():
        for rev in range(first_excl_rev, len(old_index)):
            if rev in excluded_revs:
                # this revision will be preserved as is, so we don't need to
                # consider recomputing a delta.
                continue
            entry = old_index[rev]
            if entry[ENTRY_DELTA_BASE] not in excluded_revs:
                continue
            # This is a revision that use the censored revision as the base
            # for its delta. We need a need new deltas
            if entry[ENTRY_DATA_UNCOMPRESSED_LENGTH] == 0:
                # this revision is empty, we can delta against nullrev
                rewritten_entries[rev] = (nullrev, 0, 0, COMP_MODE_PLAIN)
            else:
                text = revlog.rawdata(rev)
                info = revlogutils.revisioninfo(
                    node=entry[ENTRY_NODE_ID],
                    p1=revlog.node(entry[ENTRY_PARENT_1]),
                    p2=revlog.node(entry[ENTRY_PARENT_2]),
                    btext=text,
                    textlen=len(text),
                    cachedelta=None,
                    flags=entry[ENTRY_DATA_OFFSET] & 0xFFFF,
                )
                d = dc.finddeltainfo(
                    info, excluded_bases=excluded_revs, target_rev=rev
                )
                default_comp = revlog._docket.default_compression_header
                comp_mode, d = deltas.delta_compression(default_comp, d)
                # using `tell` is a bit lazy, but we are not here for speed
                start = tmp_storage.tell()
                tmp_storage.write(d.data[1])
                end = tmp_storage.tell()
                rewritten_entries[rev] = (d.base, start, end, comp_mode)
    return rewritten_entries


def _setup_new_files(
    revlog,
    index_cutoff,
    data_cutoff,
    sidedata_cutoff,
):
    """

    return a context manager to open all the relevant files:
    - old_data_file,
    - old_sidedata_file,
    - new_index_file,
    - new_data_file,
    - new_sidedata_file,

    The old_index_file is not here because it is accessed through the
    `old_index` object if the caller function.
    """
    docket = revlog._docket
    old_index_filepath = revlog.opener.join(docket.index_filepath())
    old_data_filepath = revlog.opener.join(docket.data_filepath())
    old_sidedata_filepath = revlog.opener.join(docket.sidedata_filepath())

    new_index_filepath = revlog.opener.join(docket.new_index_file())
    new_data_filepath = revlog.opener.join(docket.new_data_file())
    new_sidedata_filepath = revlog.opener.join(docket.new_sidedata_file())

    util.copyfile(old_index_filepath, new_index_filepath, nb_bytes=index_cutoff)
    util.copyfile(old_data_filepath, new_data_filepath, nb_bytes=data_cutoff)
    util.copyfile(
        old_sidedata_filepath,
        new_sidedata_filepath,
        nb_bytes=sidedata_cutoff,
    )
    revlog.opener.register_file(docket.index_filepath())
    revlog.opener.register_file(docket.data_filepath())
    revlog.opener.register_file(docket.sidedata_filepath())

    docket.index_end = index_cutoff
    docket.data_end = data_cutoff
    docket.sidedata_end = sidedata_cutoff

    # reload the revlog internal information
    revlog.clearcaches()
    index, chunk_cache = revlog._loadindex(docket=docket)
    revlog._load_inner(index, chunk_cache)

    @contextlib.contextmanager
    def all_files_opener():
        # hide opening in an helper function to please check-code, black
        # and various python version at the same time
        with open(old_data_filepath, 'rb') as old_data_file:
            with open(old_sidedata_filepath, 'rb') as old_sidedata_file:
                with open(new_index_filepath, 'r+b') as new_index_file:
                    with open(new_data_filepath, 'r+b') as new_data_file:
                        with open(
                            new_sidedata_filepath, 'r+b'
                        ) as new_sidedata_file:
                            new_index_file.seek(0, os.SEEK_END)
                            assert new_index_file.tell() == index_cutoff
                            new_data_file.seek(0, os.SEEK_END)
                            assert new_data_file.tell() == data_cutoff
                            new_sidedata_file.seek(0, os.SEEK_END)
                            assert new_sidedata_file.tell() == sidedata_cutoff
                            yield (
                                old_data_file,
                                old_sidedata_file,
                                new_index_file,
                                new_data_file,
                                new_sidedata_file,
                            )

    return all_files_opener


def _rewrite_simple(
    revlog,
    old_index,
    all_files,
    rev,
    rewritten_entries,
    tmp_storage,
):
    """append a normal revision to the index after the rewritten one(s)"""
    (
        old_data_file,
        old_sidedata_file,
        new_index_file,
        new_data_file,
        new_sidedata_file,
    ) = all_files
    entry = old_index[rev]
    flags = entry[ENTRY_DATA_OFFSET] & 0xFFFF
    old_data_offset = entry[ENTRY_DATA_OFFSET] >> 16

    if rev not in rewritten_entries:
        old_data_file.seek(old_data_offset)
        new_data_size = entry[ENTRY_DATA_COMPRESSED_LENGTH]
        new_data = old_data_file.read(new_data_size)
        data_delta_base = entry[ENTRY_DELTA_BASE]
        d_comp_mode = entry[ENTRY_DATA_COMPRESSION_MODE]
    else:
        (
            data_delta_base,
            start,
            end,
            d_comp_mode,
        ) = rewritten_entries[rev]
        new_data_size = end - start
        tmp_storage.seek(start)
        new_data = tmp_storage.read(new_data_size)

    # It might be faster to group continuous read/write operation,
    # however, this is censor, an operation that is not focussed
    # around stellar performance. So I have not written this
    # optimisation yet.
    new_data_offset = new_data_file.tell()
    new_data_file.write(new_data)

    sidedata_size = entry[ENTRY_SIDEDATA_COMPRESSED_LENGTH]
    new_sidedata_offset = new_sidedata_file.tell()
    if 0 < sidedata_size:
        old_sidedata_offset = entry[ENTRY_SIDEDATA_OFFSET]
        old_sidedata_file.seek(old_sidedata_offset)
        new_sidedata = old_sidedata_file.read(sidedata_size)
        new_sidedata_file.write(new_sidedata)

    data_uncompressed_length = entry[ENTRY_DATA_UNCOMPRESSED_LENGTH]
    sd_com_mode = entry[ENTRY_SIDEDATA_COMPRESSION_MODE]
    assert data_delta_base <= rev, (data_delta_base, rev)

    new_entry = revlogutils.entry(
        flags=flags,
        data_offset=new_data_offset,
        data_compressed_length=new_data_size,
        data_uncompressed_length=data_uncompressed_length,
        data_delta_base=data_delta_base,
        link_rev=entry[ENTRY_LINK_REV],
        parent_rev_1=entry[ENTRY_PARENT_1],
        parent_rev_2=entry[ENTRY_PARENT_2],
        node_id=entry[ENTRY_NODE_ID],
        sidedata_offset=new_sidedata_offset,
        sidedata_compressed_length=sidedata_size,
        data_compression_mode=d_comp_mode,
        sidedata_compression_mode=sd_com_mode,
    )
    revlog.index.append(new_entry)
    entry_bin = revlog.index.entry_binary(rev)
    new_index_file.write(entry_bin)

    revlog._docket.index_end = new_index_file.tell()
    revlog._docket.data_end = new_data_file.tell()
    revlog._docket.sidedata_end = new_sidedata_file.tell()


def _rewrite_censor(
    revlog,
    old_index,
    all_files,
    rev,
    tombstone,
):
    """rewrite and append a censored revision"""
    (
        old_data_file,
        old_sidedata_file,
        new_index_file,
        new_data_file,
        new_sidedata_file,
    ) = all_files
    entry = old_index[rev]

    # XXX consider trying the default compression too
    new_data_size = len(tombstone)
    new_data_offset = new_data_file.tell()
    new_data_file.write(tombstone)

    # we are not adding any sidedata as they might leak info about the censored version

    link_rev = entry[ENTRY_LINK_REV]

    p1 = entry[ENTRY_PARENT_1]
    p2 = entry[ENTRY_PARENT_2]

    new_entry = revlogutils.entry(
        flags=constants.REVIDX_ISCENSORED,
        data_offset=new_data_offset,
        data_compressed_length=new_data_size,
        data_uncompressed_length=new_data_size,
        data_delta_base=rev,
        link_rev=link_rev,
        parent_rev_1=p1,
        parent_rev_2=p2,
        node_id=entry[ENTRY_NODE_ID],
        sidedata_offset=0,
        sidedata_compressed_length=0,
        data_compression_mode=COMP_MODE_PLAIN,
        sidedata_compression_mode=COMP_MODE_PLAIN,
    )
    revlog.index.append(new_entry)
    entry_bin = revlog.index.entry_binary(rev)
    new_index_file.write(entry_bin)
    revlog._docket.index_end = new_index_file.tell()
    revlog._docket.data_end = new_data_file.tell()


def _get_filename_from_filelog_index(path):
    # Drop the extension and the `data/` prefix
    path_part = path.rsplit(b'.', 1)[0].split(b'/', 1)
    if len(path_part) < 2:
        msg = _(b"cannot recognize filelog from filename: '%s'")
        msg %= path
        raise error.Abort(msg)

    return path_part[1]


def _write_swapped_parents(repo, rl, rev, offset, fp):
    """Swaps p1 and p2 and overwrites the revlog entry for `rev` in `fp`"""
    from ..pure import parsers  # avoid cycle

    if repo._currentlock(repo._lockref) is None:
        # Let's be paranoid about it
        msg = "repo needs to be locked to rewrite parents"
        raise error.ProgrammingError(msg)

    index_format = parsers.IndexObject.index_format
    entry = rl.index[rev]
    new_entry = list(entry)
    new_entry[5], new_entry[6] = entry[6], entry[5]
    packed = index_format.pack(*new_entry[:8])
    fp.seek(offset)
    fp.write(packed)


### Constant used for `delta_has_meta` function
#
# Can't know if the revision has meta from this delta
HM_UNKNOWN = -2
# We know the revision is similar to the delta parent
HM_INHERIT = -1
# We know the revision  does not have meta
HM_NO_META = 0
# we know the revision has meta information
HM_META = 1


def delta_has_meta(delta: bytes, has_base: bool = True) -> int:
    """determine if a revision has metadata from its delta"""
    if not has_base:
        if delta[:META_MARKER_SIZE] == META_MARKER:
            return HM_META
        else:
            return HM_NO_META
    before, local_bytes, after = mdiff.first_bytes(delta, META_MARKER_SIZE)
    if before == META_MARKER_SIZE:
        # We can't trust bytes from `after` as it could mean some initial
        # content was deleted.
        #
        # This is not the case with `before` as it garanteed to be untouched
        # from the delta base.
        return HM_INHERIT
    elif len(local_bytes) == META_MARKER_SIZE:
        if local_bytes == META_MARKER:
            return HM_META
        else:
            return HM_NO_META
    else:
        return HM_UNKNOWN


def _reorder_filelog_parents(repo, fl, to_fix):
    """
    Swaps p1 and p2 for all `to_fix` revisions of filelog `fl` and writes the
    new version to disk, overwriting the old one with a rename.
    """
    from ..pure import parsers  # avoid cycle

    ui = repo.ui
    assert len(to_fix) > 0
    rl = fl._revlog
    if rl._format_version != constants.REVLOGV1:
        msg = "expected version 1 revlog, got version '%d'" % rl._format_version
        raise error.ProgrammingError(msg)

    index_file = rl._indexfile
    new_file_path = index_file + b'.tmp-parents-fix'
    repaired_msg = _(b"repaired revision %d of 'filelog %s'\n")

    with ui.uninterruptible():
        try:
            util.copyfile(
                rl.opener.join(index_file),
                rl.opener.join(new_file_path),
                checkambig=rl.data_config.check_ambig,
            )

            with rl.opener(new_file_path, mode=b"r+") as fp:
                if rl._inline:
                    index = parsers.InlinedIndexObject(fp.read())
                    for rev in fl.revs():
                        if rev in to_fix:
                            offset = index._calculate_index(rev)
                            _write_swapped_parents(repo, rl, rev, offset, fp)
                            ui.write(repaired_msg % (rev, index_file))
                else:
                    index_format = parsers.IndexObject.index_format
                    for rev in to_fix:
                        offset = rev * index_format.size
                        _write_swapped_parents(repo, rl, rev, offset, fp)
                        ui.write(repaired_msg % (rev, index_file))

            rl.opener.rename(new_file_path, index_file)
            rl.clearcaches()
            index, chunk_cache = rl._loadindex()
            rl._load_inner(index, chunk_cache)
        finally:
            util.tryunlink(new_file_path)


def _has_bad_parents(has_meta, p1, p2):
    """return True if the parent of revision are badly ordered

    >>> _has_bad_parents(True, -1, -1)
    False
    >>> _has_bad_parents(True, -1, 42)
    False
    >>> _has_bad_parents(True, 42, -1)
    True
    >>> _has_bad_parents(True, 18, 42)
    False
    >>> _has_bad_parents(False, -1, -1)
    False
    >>> _has_bad_parents(False, -1, 42)
    True
    >>> _has_bad_parents(False, 42, -1)
    False
    >>> _has_bad_parents(False, 18, 42)
    False
    """
    if has_meta:
        return p1 != nullrev and p2 == nullrev
    else:
        return p1 == nullrev and p2 != nullrev


def _is_revision_affected(fl, filerev, metadata_cache=None):
    full_text = lambda: fl._revlog.rawdata(filerev)
    parent_revs = lambda: fl._revlog.parentrevs(filerev)
    return _is_revision_affected_inner(
        full_text, parent_revs, filerev, metadata_cache
    )


def _is_revision_affected_inner(
    full_text,
    parents_revs,
    filerev,
    metadata_cache=None,
):
    """Mercurial currently (5.9rc0) uses `p1 == nullrev and p2 != nullrev` as a
    special meaning compared to the reverse in the context of filelog-based
    copytracing. issue6528 exists because new code assumed that parent ordering
    didn't matter, so this detects if the revision contains metadata (since
    it's only used for filelog-based copytracing) and its parents are in the
    "wrong" order."""
    try:
        raw_text = full_text()
    except error.CensoredNodeError:
        # We don't care about censored nodes as they never carry metadata
        return False

    has_meta = raw_text[:META_MARKER_SIZE] == META_MARKER
    if metadata_cache is not None:
        metadata_cache[filerev] = has_meta
    return _has_bad_parents(has_meta, *parents_revs())


def _is_revision_affected_fast(repo, fl, filerev, metadata_cache):
    rl = fl._revlog
    is_censored = lambda: rl.iscensored(filerev)
    delta_base = lambda: rl.deltaparent(filerev)
    delta = lambda: rl._inner._chunk(filerev)
    full_text = lambda: rl.rawdata(filerev)
    parent_revs = lambda: rl.parentrevs(filerev)
    # This function is used by repair_issue6528, but not by
    # filter_delta_issue6528. As such, we do not want to trust
    # parent revisions of the delta base to decide whether
    # the delta base has metadata.
    return _is_revision_affected_fast_inner(
        is_censored,
        delta_base,
        delta,
        full_text,
        parent_revs,
        filerev,
        metadata_cache,
    )


def _is_revision_affected_fast_inner(
    is_censored,
    delta_base,
    delta,
    full_text,
    parent_revs,
    filerev,
    metadata_cache,
):
    """Optimization fast-path for `_is_revision_affected`.

    `metadata_cache` is a dict of `{rev: has_metadata}` which allows any
    revision to check if its base has metadata, saving computation of the full
    text, instead looking at the current delta.

    This optimization only works if the revisions are looked at in order."""

    if is_censored():
        # Censored revisions don't contain metadata, so they cannot be affected
        metadata_cache[filerev] = False
        return False

    delta_parent = delta_base()
    if delta_parent >= 0:
        d_meta = delta_has_meta(delta())
    else:
        # the content of `delta()` can still be a delta against nullrev when
        # filtering incoming delta, so we keep things simple and explicitly
        # rely on the code path using `full_text`.
        d_meta = HM_UNKNOWN
    if d_meta == HM_NO_META:
        metadata_cache[filerev] = False
    elif d_meta == HM_META:
        metadata_cache[filerev] = True
    elif d_meta == HM_INHERIT and delta_parent in metadata_cache:
        # The diff did not remove or add the metadata header, it's then in the
        # same situation as its parent
        metadata_cache[filerev] = metadata_cache[delta_parent]
    else:
        # This delta does *something* to the metadata marker (if any).
        # Check it the slow way
        return _is_revision_affected_inner(
            full_text,
            parent_revs,
            filerev,
            metadata_cache,
        )

    return _has_bad_parents(metadata_cache[filerev], *parent_revs())


def _from_report(ui, repo, context, from_report, dry_run):
    """
    Fix the revisions given in the `from_report` file, but still checks if the
    revisions are indeed affected to prevent an unfortunate cyclic situation
    where we'd swap well-ordered parents again.

    See the doc for `debug_fix_issue6528` for the format documentation.
    """
    ui.write(_(b"loading report file '%s'\n") % from_report)

    with context(), open(from_report, mode='rb') as f:
        for line in f.read().split(b'\n'):
            if not line:
                continue
            filenodes, filename = line.split(b' ', 1)
            fl = repo.file(filename, writable=True)
            to_fix = {
                fl.rev(binascii.unhexlify(n)) for n in filenodes.split(b',')
            }
            excluded = set()

            for filerev in to_fix:
                if _is_revision_affected(fl, filerev):
                    msg = b"found affected revision %d for filelog '%s'\n"
                    ui.warn(msg % (filerev, filename))
                else:
                    msg = _(b"revision %s of file '%s' is not affected\n")
                    msg %= (binascii.hexlify(fl.node(filerev)), filename)
                    ui.warn(msg)
                    excluded.add(filerev)

            to_fix = to_fix - excluded
            if not to_fix:
                msg = _(b"no affected revisions were found for '%s'\n")
                ui.write(msg % filename)
                continue
            if not dry_run:
                _reorder_filelog_parents(repo, fl, sorted(to_fix))


def filter_delta_issue6528(revlog, deltas_iter):
    """filter incomind deltas to repaire issue 6528 on the fly"""
    metadata_cache = {nullrev: False}

    deltacomputer = deltas.deltacomputer(revlog)

    for rev, d in enumerate(deltas_iter, len(revlog)):
        if not revlog.index.has_node(d.delta_base):
            raise error.LookupError(
                d.delta_base, revlog.radix, _(b'unknown parent')
            )
        base_rev = revlog.rev(d.delta_base)
        if not revlog.index.has_node(d.p1):
            raise error.LookupError(d.p1, revlog.radix, _(b'unknown parent'))
        p1_rev = revlog.rev(d.p1)
        if not revlog.index.has_node(d.p2):
            raise error.LookupError(d.p2, revlog.radix, _(b'unknown parent'))
        p2_rev = revlog.rev(d.p2)

        is_censored = lambda: bool(d.flags & REVIDX_ISCENSORED)
        delta_base = lambda: base_rev
        parent_revs = lambda: (p1_rev, p2_rev)

        def full_text():
            if d.raw_text is None:
                textlen = mdiff.patchedsize(revlog.size(base_rev), d.delta)

                revinfo = revlogutils.revisioninfo(
                    d.node,
                    d.p1,
                    d.p2,
                    None,
                    textlen,
                    revlogutils.CachedDelta(base_rev, d.delta),
                    d.flags,
                )
                d.raw_text = deltacomputer.buildtext(revinfo)
            return d.raw_text

        is_affected = _is_revision_affected_fast_inner(
            is_censored,
            delta_base,
            lambda: d.delta,
            full_text,
            parent_revs,
            rev,
            metadata_cache,
        )
        if is_affected:
            d.p2, d.p1 = d.p1, d.p2
        yield d


def repair_issue6528(
    ui, repo, dry_run=False, to_report=None, from_report=None, paranoid=False
):
    @contextlib.contextmanager
    def context():
        if dry_run or to_report:  # No need for locking
            yield
        else:
            with repo.wlock(), repo.lock():
                yield

    if from_report:
        return _from_report(ui, repo, context, from_report, dry_run)

    report_entries = []

    with context():
        files = list(
            entry
            for entry in repo.store.data_entries()
            if entry.is_revlog and entry.is_filelog
        )

        progress = ui.makeprogress(
            _(b"looking for affected revisions"),
            unit=_(b"filelogs"),
            total=len(files),
        )
        found_nothing = True

        for entry in files:
            progress.increment()
            filename = entry.target_id
            fl = repo.file(entry.target_id, writable=not dry_run)

            # Set of filerevs (or hex filenodes if `to_report`) that need fixing
            to_fix = set()
            metadata_cache = {nullrev: False}
            for filerev in fl.revs():
                affected = _is_revision_affected_fast(
                    repo, fl, filerev, metadata_cache
                )
                if paranoid:
                    slow = _is_revision_affected(fl, filerev)
                    if slow != affected:
                        msg = _(b"paranoid check failed for '%s' at node %s")
                        node = binascii.hexlify(fl.node(filerev))
                        raise error.Abort(msg % (filename, node))
                if affected:
                    msg = b"found affected revision %d for file '%s'\n"
                    ui.warn(msg % (filerev, filename))
                    found_nothing = False
                    if not dry_run:
                        if to_report:
                            to_fix.add(binascii.hexlify(fl.node(filerev)))
                        else:
                            to_fix.add(filerev)

            if to_fix:
                to_fix = sorted(to_fix)
                if to_report:
                    report_entries.append((filename, to_fix))
                else:
                    _reorder_filelog_parents(repo, fl, to_fix)

        if found_nothing:
            ui.write(_(b"no affected revisions were found\n"))

        if to_report and report_entries:
            with open(to_report, mode="wb") as f:
                for path, to_fix in report_entries:
                    f.write(b"%s %s\n" % (b",".join(to_fix), path))

        progress.complete()


def _find_all_revs_with_meta(rl):
    meta = {}
    for filerev in rl:
        if rl.iscensored(filerev):
            continue
        delta_parent = rl.deltaparent(filerev)
        has_base = delta_parent >= 0

        delta = rl._inner._chunk(filerev)

        hm = delta_has_meta(delta, has_base)
        if hm == HM_META:
            meta[filerev] = True
        elif hm == HM_NO_META:
            meta[filerev] = False
        elif hm == HM_INHERIT and delta_parent in meta:
            meta[filerev] = meta[delta_parent]
        else:
            try:
                revdata = rl._revisiondata(
                    filerev,
                    validate=False,
                )
            except error.CensoredNodeError:
                meta[filerev] = False
            else:
                meta[filerev] = revdata[:META_MARKER_SIZE] == META_MARKER

    return {k for k, v in meta.items() if v}


def _find_all_snapshots(rl):
    snapshots = set()
    if hasattr(rl.index, 'findsnapshots'):
        cache: dict[int, set[int]] = {}
        rl.index.findsnapshots(cache, 0, len(rl))
        for b, c in cache.items():
            snapshots.add(b)
            snapshots.update(c)
    else:
        for rev in rl:
            if rl.issnapshot(rev):
                snapshots.add(rev)
    return snapshots


def quick_upgrade(rl, upgrade_meta, upgrade_delta_info):
    assert rl._format_flags & constants.FLAG_GENERALDELTA

    if rl.target[0] != constants.KIND_FILELOG:
        upgrade_meta = False

    if upgrade_meta and rl._format_flags & constants.FLAG_FILELOG_META:
        upgrade_meta = False

    if rl.target[0] not in (constants.KIND_FILELOG, constants.KIND_MANIFESTLOG):
        upgrade_delta_info = False

    if upgrade_delta_info and rl._format_flags & constants.FLAG_DELTA_INFO:
        upgrade_delta_info = False

    if not (upgrade_meta or upgrade_delta_info):
        return

    new_flags = 0

    if upgrade_meta:
        revs_with_meta = _find_all_revs_with_meta(rl)
        new_flags |= constants.FLAG_FILELOG_META
    else:
        revs_with_meta = set()

    if upgrade_delta_info:
        revs_with_snapshot = _find_all_snapshots(rl)
        new_flags |= constants.FLAG_DELTA_INFO
    else:
        revs_with_snapshot = set()

    if not (revs_with_meta or revs_with_snapshot):
        # We just need to write the header flag
        # XXX do we need to sort the parent anyway?
        with rl.opener(rl._indexfile, b'br+') as n:
            n.seek(0)
            first_entry = rl.index.entry_binary(0)
            header = rl._format_flags
            header |= rl._format_version
            header |= new_flags
            header = rl.index.pack_header(header)
            first_entry = header + first_entry
            n.write(first_entry)
        return

    index = rl.index
    new_index = rl._parse_index(
        b'',
        False,
        True,
        True,
    )[0]

    for filerev in rl:
        (
            data_offset_and_flag,
            data_compressed_length,
            data_uncompressed_length,
            delta_base,
            link_rev,
            parent_1,
            parent_2,
            node_id,
            side_data_offset,
            side_data_compressed_length,
            data_compression_mode,
            sidedata_compression_mode,
            rank,
        ) = index[filerev]
        flags = data_offset_and_flag & 0xFFFF
        dataoffset = int(data_offset_and_flag >> 16)

        if parent_1 == nullrev and parent_2 != nullrev:
            parent_1, parent_2 = parent_2, parent_1

        if filerev in revs_with_meta:
            flags |= constants.REVIDX_HASMETA

        if filerev in revs_with_snapshot:
            flags |= constants.REVIDX_DELTA_IS_SNAPSHOT

        e = revlogutils.entry(
            flags=flags,
            data_offset=dataoffset,
            data_compressed_length=data_compressed_length,
            data_uncompressed_length=data_uncompressed_length,
            data_compression_mode=data_compression_mode,
            data_delta_base=delta_base,
            link_rev=link_rev,
            parent_rev_1=parent_1,
            parent_rev_2=parent_2,
            node_id=node_id,
            sidedata_offset=side_data_offset,
            sidedata_compressed_length=side_data_compressed_length,
            sidedata_compression_mode=sidedata_compression_mode,
            rank=rank,
        )
        new_index.append(e)

    # write data
    with rl.opener(rl._indexfile, b'wb', atomictemp=True) as n:
        for rev in range(len(new_index)):
            idx = new_index.entry_binary(rev)
            if rev == 0:
                header = rl._format_flags
                header |= rl._format_version
                header |= new_flags
                header = new_index.pack_header(header)
                idx = header + idx
            n.write(idx)
            if rl._inline:
                n.write(rl._inner.get_segment_for_revs(rev, rev)[1])
