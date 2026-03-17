# scmutil.py - Mercurial core utility functions
#
#  Copyright Olivia Mackall <olivia@selenic.com> and other
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import annotations

from .interfaces.types import (
    HgPathT,
    RepoT,
)

from .utils import repoviewutil


def cachetocopy(srcrepo: RepoT) -> list[HgPathT]:
    """return the list of cache file valuable to copy during a clone"""
    # In local clones we're copying all nodes, not just served
    # ones. Therefore copy all branch caches over.
    subsets = [s for s in repoviewutil.get_ordered_subset() if s is not None]
    cachefiles = [b'branch2']
    cachefiles += [b'branch2-%s' % f for f in subsets]
    cachefiles += [b'branch3-exp']
    cachefiles += [b'branch3-exp-%s' % f for f in subsets]
    cachefiles += [b'rbc-names-v2', b'rbc-revs-v2']
    cachefiles += [b'tags2']
    cachefiles += [b'tags2-%s' % f for f in subsets]
    cachefiles += [b'hgtagsfnodes1']
    return cachefiles
