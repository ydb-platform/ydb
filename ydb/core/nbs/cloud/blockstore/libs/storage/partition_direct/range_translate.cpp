#include "range_translate.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/range_translate.cpp
=======
size_t GetDirectBlockGroupIndex(
    size_t vChunkIndex,
    size_t directBlockGroupCount)
{
    return vChunkIndex % directBlockGroupCount;
}

size_t GetVChunksPerRegion(ui64 vChunkSize)
{
    Y_ABORT_UNLESS(vChunkSize > 0 && vChunkSize <= RegionSize);
    Y_ABORT_UNLESS(RegionSize % vChunkSize == 0);
    return RegionSize / vChunkSize;
}

>>>>>>> ed0d0ddf015 (NBS-6784: Remove host from dbg (#46061)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/region_geometry.cpp
size_t GetRegionIndex(const TVolumeConfig& volumeConfig, TBlockRange64 range)
{
    const ui64 blocksPerRegion = RegionSize / volumeConfig.BlockSize;
    return range.Start / blocksPerRegion;
}

TBlockRange64 TranslateToRegion(
    const TVolumeConfig& volumeConfig,
    TBlockRange64 range)
{
    const ui64 blocksPerRegion = RegionSize / volumeConfig.BlockSize;
    const size_t regionOffset = range.Start % blocksPerRegion;
    return TBlockRange64::WithLength(regionOffset, range.Size());
}

size_t GetVChunkIndex(
    const TVolumeConfig& volumeConfig,
    TBlockRange64 regionRange)
{
    Y_ABORT_UNLESS(
        volumeConfig.VChunkSize > 0 && volumeConfig.VChunkSize <= RegionSize);
    Y_ABORT_UNLESS(RegionSize % volumeConfig.VChunkSize == 0);
    Y_ABORT_UNLESS(volumeConfig.BlockSize > 0);
    Y_ABORT_UNLESS(volumeConfig.BlocksPerStripe >= regionRange.Size());

    const size_t blocksPerStripe = volumeConfig.BlocksPerStripe;
    const size_t stripeIndex = regionRange.Start / blocksPerStripe;
    const ui32 vChunksPerRegionCount = RegionSize / volumeConfig.VChunkSize;
    return stripeIndex % vChunksPerRegionCount;
}

TBlockRange16 TranslateToVChunk(
    const TVolumeConfig& volumeConfig,
    TBlockRange64 regionRange)
{
    Y_ABORT_UNLESS(
        volumeConfig.VChunkSize > 0 && volumeConfig.VChunkSize <= RegionSize);
    Y_ABORT_UNLESS(RegionSize % volumeConfig.VChunkSize == 0);
    Y_ABORT_UNLESS(volumeConfig.BlockSize > 0);
    Y_ABORT_UNLESS(volumeConfig.BlocksPerStripe >= regionRange.Size());

    const size_t blocksPerStripe = volumeConfig.BlocksPerStripe;
    const size_t stripeIndex = regionRange.Start / blocksPerStripe;
    const ui32 vChunksPerRegionCount = RegionSize / volumeConfig.VChunkSize;
    const size_t stripeIndexInVChunk = stripeIndex / vChunksPerRegionCount;
    const size_t blockIndexInStripe = regionRange.Start % blocksPerStripe;
    const ui64 vChunkStart =
        stripeIndexInVChunk * blocksPerStripe + blockIndexInStripe;

    return TBlockRange16::WithLength(
        IntegerCast<ui16>(vChunkStart),
        IntegerCast<ui16>(regionRange.Size()));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
