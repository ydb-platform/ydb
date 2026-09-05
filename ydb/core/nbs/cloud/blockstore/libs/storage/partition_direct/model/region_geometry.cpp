#include "region_geometry.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/volume_config.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

// FastPathService routes a range to a single vchunk from its start block.
// Callers (vhost via the split wrapper, the load actor adapter) must keep
// each request inside one stripe.
void CheckStripeContained(
    const TVolumeConfig& volumeConfig,
    TBlockRange64 regionRange)
{
    Y_ABORT_UNLESS(volumeConfig.BlockSize > 0);
    const size_t blocksPerStripe = volumeConfig.BlocksPerStripe;
    Y_ABORT_UNLESS(blocksPerStripe > 0);
    Y_ABORT_UNLESS(
        regionRange.Start / blocksPerStripe ==
            regionRange.End / blocksPerStripe,
        "range %s crosses a stripe boundary; the caller must split it",
        regionRange.Print().c_str());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

size_t GetVChunksPerRegion(ui64 vChunkSize)
{
    Y_ABORT_UNLESS(vChunkSize > 0 && vChunkSize <= RegionSize);
    Y_ABORT_UNLESS(RegionSize % vChunkSize == 0);
    return RegionSize / vChunkSize;
}

size_t GetRegionIndex(const TVolumeConfig& volumeConfig, TBlockRange64 range)
{
    const ui64 blocksPerRegion = RegionSize / volumeConfig.BlockSize;
    return range.Start / blocksPerRegion;
}

size_t GetRegionIndexByVChunk(
    const TVolumeConfig& volumeConfig,
    size_t vChunkIndex)
{
    return vChunkIndex / GetVChunksPerRegion(volumeConfig.VChunkSize);
}

size_t GetVChunkIndexInRegion(
    const TVolumeConfig& volumeConfig,
    size_t vChunkIndex)
{
    return vChunkIndex % GetVChunksPerRegion(volumeConfig.VChunkSize);
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
    CheckStripeContained(volumeConfig, regionRange);

    const size_t blocksPerStripe = volumeConfig.BlocksPerStripe;
    const size_t stripeIndex = regionRange.Start / blocksPerStripe;
    const size_t vChunksPerRegionCount =
        GetVChunksPerRegion(volumeConfig.VChunkSize);
    return stripeIndex % vChunksPerRegionCount;
}

TBlockRange64 TranslateToVChunk(
    const TVolumeConfig& volumeConfig,
    TBlockRange64 regionRange)
{
    CheckStripeContained(volumeConfig, regionRange);

    const size_t blocksPerStripe = volumeConfig.BlocksPerStripe;
    const size_t stripeIndex = regionRange.Start / blocksPerStripe;
    const size_t vChunksPerRegionCount =
        GetVChunksPerRegion(volumeConfig.VChunkSize);
    const size_t stripeIndexInVChunk = stripeIndex / vChunksPerRegionCount;
    const size_t blockIndexInStripe = regionRange.Start % blocksPerStripe;

    return TBlockRange64::WithLength(
        stripeIndexInVChunk * blocksPerStripe + blockIndexInStripe,
        regionRange.Size());
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
