#pragma once

<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/range_translate.h
#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/volume_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
=======
#include "vchunk_config.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range/block_range.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
>>>>>>> 78a20188184 (Field map set optimization (#52565)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/region_geometry.h

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/range_translate.h
<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/range_translate.h
=======
size_t GetVChunksPerRegion(ui64 vChunkSize);
=======
// Returns the region size for the specified vchunk size.
ui64 GetRegionSize(ui64 vChunkSize);

// Returns the number of blocks in one vchunk.
ui64 GetVChunkBlockCount(ui32 blockSize, ui64 vChunkSize);

// Returns the number of blocks in one region.
ui64 GetRegionBlockCount(ui32 blockSize, ui64 vChunkSize);

// Returns the number of regions needed for a disk with the specified geometry.
size_t GetRegionCount(ui64 blockCount, ui32 blockSize, ui64 vChunkSize);

// Returns the number of vchunks allocated for all disk regions.
size_t GetVChunkCount(ui64 blockCount, ui32 blockSize, ui64 vChunkSize);

// Returns the number of vchunks assigned to one DirectBlockGroup.
ui32 GetVChunkCountPerDirectBlockGroup(
    size_t regionCount,
    size_t directBlockGroupInVolumeCount);
>>>>>>> f32af13259a (Refactor partition direct disk geometry calculations (#53169)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/region_geometry.h

// The group that serves this vchunk. Load-time compaction relies on the same
// answer as the vchunk creation, so both go through here.
size_t GetDirectBlockGroupIndex(
    size_t vChunkIndex,
    size_t directBlockGroupInVolumeCount);

>>>>>>> ed0d0ddf015 (NBS-6784: Remove host from dbg (#46061)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/region_geometry.h
size_t GetRegionIndex(const TVolumeConfig& volumeConfig, TBlockRange64 range);

<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/range_translate.h
=======
size_t GetRegionIndexByVChunk(size_t vChunkIndex);

size_t GetVChunkIndexInRegion(size_t vChunkIndex);

// Returns the absolute vchunk index for its region-local position.
size_t GetVChunkIndex(size_t regionIndex, size_t vChunkIndexInRegion);

>>>>>>> f32af13259a (Refactor partition direct disk geometry calculations (#53169)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/region_geometry.h
TBlockRange64 TranslateToRegion(
    const TVolumeConfig& volumeConfig,
    TBlockRange64 range);

size_t GetVChunkIndex(
    const TVolumeConfig& volumeConfig,
    TBlockRange64 regionRange);

TBlockRange16 TranslateToVChunk(
    const TVolumeConfig& volumeConfig,
    TBlockRange64 regionRange);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
