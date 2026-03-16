# mercurial.revlogutils -- basic utilities for revlog
#
# Copyright 2019 Pierre-Yves David <pierre-yves.david@octobus.net>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import annotations

from typing import (
    Optional,
    TYPE_CHECKING,
)

from ..thirdparty import attr

# Force pytype to use the non-vendored package
if TYPE_CHECKING:
    # noinspection PyPackageRequirements
    import attr

from ..interfaces.types import (
    NodeIdT,
    RevnumT,
)
from ..interfaces import repository

# See mercurial.revlogutils.constants for doc
COMP_MODE_INLINE = 2
RANK_UNKNOWN = -1


def offset_type(offset, type):
    if (type & ~repository.REVISION_FLAGS_KNOWN) != 0:
        raise ValueError(b'unknown revlog index flags: %d' % type)
    return int(int(offset) << 16 | type)


def entry(
    data_offset,
    data_compressed_length,
    data_delta_base,
    link_rev,
    parent_rev_1,
    parent_rev_2,
    node_id,
    flags=0,
    data_uncompressed_length=-1,
    data_compression_mode=COMP_MODE_INLINE,
    sidedata_offset=0,
    sidedata_compressed_length=0,
    sidedata_compression_mode=COMP_MODE_INLINE,
    rank=RANK_UNKNOWN,
):
    """Build one entry from symbolic name

    This is useful to abstract the actual detail of how we build the entry
    tuple for caller who don't care about it.

    This should always be called using keyword arguments. Some arguments have
    default value, this match the value used by index version that does not store such data.
    """
    return (
        offset_type(data_offset, flags),
        data_compressed_length,
        data_uncompressed_length,
        data_delta_base,
        link_rev,
        parent_rev_1,
        parent_rev_2,
        node_id,
        sidedata_offset,
        sidedata_compressed_length,
        data_compression_mode,
        sidedata_compression_mode,
        rank,
    )


@attr.s(slots=True)
class CachedDelta:
    base = attr.ib(type=RevnumT)
    delta = attr.ib(type=bytes)
    reuse_policy = attr.ib(type=Optional[int], default=None)
    snapshot_level = attr.ib(type=Optional[int], default=None)


@attr.s(slots=True)
class revisioninfo:
    """Information about a revision that allows building its fulltext
    node:       expected hash of the revision
    p1, p2:     parent revs of the revision (as node)
    btext:      built text cache
    cachedelta: (baserev, uncompressed_delta, usage_mode) or None
    flags:      flags associated to the revision storage

    One of btext or cachedelta must be set.
    """

    node = attr.ib(type=NodeIdT)
    p1 = attr.ib(type=NodeIdT)
    p2 = attr.ib(type=NodeIdT)
    btext = attr.ib(type=Optional[bytes])
    textlen = attr.ib(type=int)
    cachedelta = attr.ib(type=Optional[CachedDelta])
    flags = attr.ib(type=int)


@attr.s(slots=True)
class InboundRevision:
    """Data retrieved for a changegroup like data (used in revlog.addgroup)
    node:        the revision node
    p1, p2:      the parents (as node)
    linknode:    the linkrev information
    delta_base:  the node to which apply the delta informaiton
    data:        the data from the revision
    flags:       revision flags
    sidedata:    sidedata for the revision
    proto_flags: protocol related flag affecting this revision
    """

    node = attr.ib()
    p1 = attr.ib()
    p2 = attr.ib()
    link_node = attr.ib()
    delta_base = attr.ib()
    delta = attr.ib()
    flags = attr.ib()
    sidedata = attr.ib()
    protocol_flags = attr.ib(default=0)
    snapshot_level = attr.ib(default=None, type=Optional[int])
    raw_text = attr.ib(default=None)
    has_censor_flag = attr.ib(default=False)
    has_filelog_hasmeta_flag = attr.ib(default=False)
