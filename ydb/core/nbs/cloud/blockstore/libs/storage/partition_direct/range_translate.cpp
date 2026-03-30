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
    const size_t blocksPerStripe = DefaultStripeSize / volumeConfig.BlockSize;
    const size_t stripeIndex = regionRange.Start / blocksPerStripe;
    return stripeIndex % VChunksPerRegionCount;
}

TBlockRange64 TranslateToVChunk(
    const TVolumeConfig& volumeConfig,
    TBlockRange64 regionRange)
{
    const size_t blocksPerStripe = DefaultStripeSize / volumeConfig.BlockSize;
    const size_t stripeIndex = regionRange.Start / blocksPerStripe;
    const size_t stripeIndexInVChunk = stripeIndex / VChunksPerRegionCount;
    const size_t blockIndexInStripe = regionRange.Start % blocksPerStripe;

    return TBlockRange64::WithLength(
        stripeIndexInVChunk * blocksPerStripe + blockIndexInStripe,
        regionRange.Size());
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
