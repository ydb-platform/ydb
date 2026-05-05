#include "range_translate.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

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

    const size_t blocksPerStripe = DefaultStripeSize / volumeConfig.BlockSize;
    const size_t stripeIndex = regionRange.Start / blocksPerStripe;
    const ui32 vChunksPerRegionCount = RegionSize / volumeConfig.VChunkSize;
    return stripeIndex % vChunksPerRegionCount;
}

TBlockRange64 TranslateToVChunk(
    const TVolumeConfig& volumeConfig,
    TBlockRange64 regionRange)
{
    Y_ABORT_UNLESS(
        volumeConfig.VChunkSize > 0 && volumeConfig.VChunkSize <= RegionSize);
    Y_ABORT_UNLESS(RegionSize % volumeConfig.VChunkSize == 0);
    Y_ABORT_UNLESS(volumeConfig.BlockSize > 0);

    const size_t blocksPerStripe = DefaultStripeSize / volumeConfig.BlockSize;
    const size_t stripeIndex = regionRange.Start / blocksPerStripe;
    const ui32 vChunksPerRegionCount = RegionSize / volumeConfig.VChunkSize;
    const size_t stripeIndexInVChunk = stripeIndex / vChunksPerRegionCount;
    const size_t blockIndexInStripe = regionRange.Start % blocksPerStripe;

    return TBlockRange64::WithLength(
        stripeIndexInVChunk * blocksPerStripe + blockIndexInStripe,
        regionRange.Size());
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
