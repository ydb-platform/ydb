# -*- coding: utf-8 -*-
"""Logical disk geometry used by the functional NBS suites.

Values match ``ydb/core/nbs/cloud/blockstore/libs/common/constants.h``
and the in-memory PDisk vchunk size from ``KikimrConfigGenerator``.
"""

# Default volume block size.
DEFAULT_BLOCK_SIZE = 4096

# Supported volume block sizes (constants.h: DefaultBlockSize .. MaxBlockSize).
# Max volume is 2^31 blocks at every size.
SUPPORTED_BLOCK_SIZES = (
    4 * 1024,
    8 * 1024,
    16 * 1024,
    32 * 1024,
    64 * 1024,
    128 * 1024,
)
MAX_BLOCKS_PER_DISK = 1 << 31


def max_disk_bytes(block_size):
    """Maximum volume size in bytes for ``block_size``."""
    return MAX_BLOCKS_PER_DISK * block_size


# Stripe size (TStorageServiceConfig.StripeSize).
STRIPE_SIZE = 512 * 1024

# Region size.
REGION_SIZE = 4 * 1024 * 1024 * 1024

# In-memory PDisks use 32 MiB chunks; on-disk PDisks use 128 MiB.
VCHUNK_SIZE_IN_MEMORY = 32 * 1024 * 1024
VCHUNK_SIZE_ON_DISK = 128 * 1024 * 1024

# Default disk in NbsTestBase is exactly one region.
DEFAULT_DISK_BLOCKS_COUNT = REGION_SIZE // DEFAULT_BLOCK_SIZE


def blocks_per_stripe(block_size=DEFAULT_BLOCK_SIZE):
    """Number of blocks in one stripe."""
    return STRIPE_SIZE // block_size


def blocks_per_vchunk(vchunk_size=VCHUNK_SIZE_IN_MEMORY, block_size=DEFAULT_BLOCK_SIZE):
    """Number of logical blocks in one vchunk's address span.

    Consecutive stripes are striped across vchunks, so a 32 MiB logical
    offset is a convenient vchunk-boundary probe even though each stripe
    already lands in a different vchunk.
    """
    return vchunk_size // block_size


def blocks_per_region(block_size=DEFAULT_BLOCK_SIZE):
    """Number of blocks in one region."""
    return REGION_SIZE // block_size


def stripe_boundary_block(block_size=DEFAULT_BLOCK_SIZE):
    """First block of the second stripe (the write that crosses a stripe)."""
    return blocks_per_stripe(block_size) - 1


def vchunk_boundary_block(vchunk_size=VCHUNK_SIZE_IN_MEMORY, block_size=DEFAULT_BLOCK_SIZE):
    """Block index of a write that crosses a 32 MiB logical vchunk span."""
    return blocks_per_vchunk(vchunk_size, block_size) - 1


def region_boundary_block(block_size=DEFAULT_BLOCK_SIZE):
    """Last block of the first region."""
    return blocks_per_region(block_size) - 1
