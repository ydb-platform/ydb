#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/volume_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/range_translate.h
=======
size_t GetVChunksPerRegion(ui64 vChunkSize);

// The group that serves this vchunk. Load-time compaction relies on the same
// answer as the vchunk creation, so both go through here.
size_t GetDirectBlockGroupIndex(
    size_t vChunkIndex,
    size_t directBlockGroupCount);

>>>>>>> ed0d0ddf015 (NBS-6784: Remove host from dbg (#46061)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/region_geometry.h
size_t GetRegionIndex(const TVolumeConfig& volumeConfig, TBlockRange64 range);

TBlockRange64 TranslateToRegion(
    const TVolumeConfig& volumeConfig,
    TBlockRange64 range);

size_t GetVChunkIndex(
    const TVolumeConfig& volumeConfig,
    TBlockRange64 regionRange);

TBlockRange64 TranslateToVChunk(
    const TVolumeConfig& volumeConfig,
    TBlockRange64 regionRange);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
