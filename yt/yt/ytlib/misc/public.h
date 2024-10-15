#pragma once

#include <yt/yt/client/misc/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(INodeMemoryTracker)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMemoryCategory,
    ((Footprint)                   (0))
    ((BlockCache)                  (1))
    ((ChunkMeta)                   (2))
    ((ChunkBlockMeta)             (10))
    ((ChunkBlocksExt)             (16))
    ((ChunkJournalIndex)          (20))
    ((Rpc)                        (21))
    ((UserJobs)                    (3))
    ((TabletStatic)                (4))
    ((TabletDynamic)               (5))
    // COMPAT(babenko): drop
    ((BlobSession)                 (6))
    ((PendingDiskRead)            (22))
    ((PendingDiskWrite)           (23))
    ((VersionedChunkMeta)          (7))
    ((SystemJobs)                  (8))
    ((Query)                       (9))
    ((TmpfsLayers)                (11))
    ((MasterCache)                (12))
    ((Lookup)                     (24))
    ((LookupRowsCache)            (13))
    ((AllocFragmentation)         (14))
    ((P2P)                        (15))
    ((Unknown)                    (17))
    ((Mixed)                      (18))
    ((TabletBackground)           (19))
    ((JobInputBlockCache)         (25))
    ((JobInputChunkMetaCache)     (26))
    ((TableReplication)           (27))
    ((ChaosReplicationIncoming)   (28))
    ((ChaosReplicationOutgoing)   (29))
    ((ReadTable)                  (30))
    ((Other)                      (31))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
