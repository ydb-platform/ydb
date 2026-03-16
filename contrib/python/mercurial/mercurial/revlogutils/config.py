# revlogutils/config.py - small config class for Revlogs
#
# Copyright 2023 Pierre-Yves David <pierre-yves.david@octobus,net>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import annotations

import typing
from typing import Optional

from ..thirdparty import attr

# Force pytype to use the non-vendored package
if typing.TYPE_CHECKING:
    # noinspection PyPackageRequirements
    import attr


class _Config:
    """Abstract Revlog Config class"""

    def copy(self):
        return self.__class__(**self.__dict__)


@attr.s()
class FeatureConfig(_Config):
    """Hold configuration values about the available revlog features"""

    # the default compression engine
    compression_engine = attr.ib(default=b'zlib', type=bytes)
    # compression engines options
    compression_engine_options = attr.ib(
        default=attr.Factory(dict),
        type=dict[bytes, Optional[int]],
    )

    # can we use censor on this revlog
    censorable = attr.ib(default=False, type=bool)
    # do we ignore filelog censored revisions and return an empty string
    ignore_filelog_censored_revisions = attr.ib(default=False)
    # does this revlog use the "side data" feature
    has_side_data = attr.ib(default=False, type=bool)
    # might remove rank configuration once the computation has no impact
    compute_rank = attr.ib(default=False, type=bool)
    # parent order is supposed to be semantically irrelevant, so we
    # normally resort parents to ensure that the first parent is non-null,
    # if there is a non-null parent at all.
    # filelog abuses the parent order as flag to mark some instances of
    # meta-encoded files, so allow it to disable this behavior.
    canonical_parent_order = attr.ib(default=False, type=bool)
    # can ellipsis commit be used
    enable_ellipsis = attr.ib(default=False, type=bool)

    # use a flag to signal that a filerevision constains metadata
    hasmeta_flag = attr.ib(default=False, type=bool)

    def copy(self):
        new = super().copy()
        new.compression_engine_options = self.compression_engine_options.copy()
        return new


@attr.s()
class DataConfig(_Config):
    """Hold configuration value about how the revlog data are read"""

    # should we try to open the "pending" version of the revlog
    try_pending = attr.ib(default=False, type=bool)
    # should we try to open the "splitted" version of the revlog
    try_split = attr.ib(default=False, type=bool)
    #  When True, indexfile should be opened with checkambig=True at writing,
    #  to avoid file stat ambiguity.
    check_ambig = attr.ib(default=False, type=bool)

    # If true, use mmap instead of reading to deal with large index
    mmap_large_index = attr.ib(default=False, type=bool)
    # how much data is large
    mmap_index_threshold = attr.ib(default=None, type=Optional[int])
    # How much data to read and cache into the raw revlog data cache.
    chunk_cache_size = attr.ib(default=65536, type=int)

    # The size of the uncompressed cache compared to the largest revision seen.
    uncompressed_cache_factor = attr.ib(default=None, type=Optional[int])

    # The number of chunk cached
    uncompressed_cache_count = attr.ib(default=None, type=Optional[int])

    # Allow sparse reading of the revlog data
    with_sparse_read = attr.ib(default=False, type=bool)
    # minimal density of a sparse read chunk
    sr_density_threshold = attr.ib(default=0.50, type=float)
    # minimal size of data we skip when performing sparse read
    sr_min_gap_size = attr.ib(default=262144, type=int)

    # are delta encoded against arbitrary bases.
    generaldelta = attr.ib(default=False, type=bool)

    # index contains extra delta information
    #
    # (It is useful to have it here in addition to the one in DeltaConfig when
    # we need to exchange related information)
    delta_info = attr.ib(default=False)


@attr.s()
class DeltaConfig(_Config):
    """Hold configuration value about how new delta are computed

    Some attributes are duplicated from DataConfig to help havign each object
    self contained.
    """

    # can delta be encoded against arbitrary bases.
    general_delta = attr.ib(default=False, type=bool)
    # Allow sparse writing of the revlog data
    sparse_revlog = attr.ib(default=False, type=bool)
    # index contains extra delta information
    delta_info = attr.ib(default=False)
    # maximum length of a delta chain
    max_chain_len = attr.ib(default=None, type=Optional[int])
    # Maximum distance between delta chain base start and end
    max_deltachain_span = attr.ib(default=-1, type=int)
    # If `upper_bound_comp` is not None, this is the expected maximal gain from
    # compression for the data content.
    upper_bound_comp = attr.ib(default=None, type=int)
    # Should we try a delta against both parent
    delta_both_parents = attr.ib(default=True, type=bool)
    # Test delta base candidate group by chunk of this maximal size.
    candidate_group_chunk_size = attr.ib(default=0, type=int)
    # Should we display debug information about delta computation
    debug_delta = attr.ib(default=False, type=bool)
    # trust incoming delta by default
    lazy_delta = attr.ib(default=True, type=bool)
    # trust the base of incoming delta by default
    lazy_delta_base = attr.ib(default=False, type=bool)
    # don't used incoming delta if they don't look optimal
    filter_suspicious_delta = attr.ib(default=False, type=bool)
    # check integrity of candidate bases before computing a delta against them
    validate_base = attr.ib(default=False, type=bool)
    # A theoretical maximum compression ratio for file content
    # Used to estimate delta size before compression. value <= 0 disable such
    # estimate.
    file_max_comp_ratio = attr.ib(default=10, type=int)
    # Use delta folding to estimate the size of a delta before actually
    # computing it.
    delta_fold_estimate = attr.ib(default=True, type=bool)
    # the maximal ratio between the original delta and a new delta optimized by folding.
    #
    # a value of None means the feature is disabled.
    delta_fold_tolerance = attr.ib(default=True, type=Optional[float])
