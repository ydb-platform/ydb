#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/volume_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

size_t GetVChunksPerRegion(ui64 vChunkSize);

size_t GetRegionIndex(const TVolumeConfig& volumeConfig, TBlockRange64 range);

size_t GetRegionIndexByVChunk(
    const TVolumeConfig& volumeConfig,
    size_t vChunkIndex);

size_t GetVChunkIndexInRegion(
    const TVolumeConfig& volumeConfig,
    size_t vChunkIndex);

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
