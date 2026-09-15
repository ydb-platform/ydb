#include "range_translate.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/range_translate.cpp
=======
namespace {

size_t GetCountByBlocks(ui64 blockCount, ui64 itemBlockCount)
{
    return blockCount / itemBlockCount + (blockCount % itemBlockCount != 0);
}

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

>>>>>>> f32af13259a (Refactor partition direct disk geometry calculations (#53169)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/region_geometry.cpp
////////////////////////////////////////////////////////////////////////////////

<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/range_translate.cpp
=======
size_t GetDirectBlockGroupIndex(
    size_t vChunkIndex,
    size_t /*directBlockGroupInVolumeCount*/)
{
    return vChunkIndex % VChunkPerRegionCount;
}

ui64 GetRegionSize(ui64 vChunkSize)
{
    return vChunkSize * VChunkPerRegionCount;
}

ui64 GetVChunkBlockCount(ui32 blockSize, ui64 vChunkSize)
{
    Y_ABORT_UNLESS(blockSize > 0 && vChunkSize % blockSize == 0);
    return vChunkSize / blockSize;
}

ui64 GetRegionBlockCount(ui32 blockSize, ui64 vChunkSize)
{
    const ui64 regionSize = GetRegionSize(vChunkSize);
    Y_ABORT_UNLESS(blockSize > 0 && regionSize % blockSize == 0);
    return regionSize / blockSize;
}

size_t GetRegionCount(ui64 blockCount, ui32 blockSize, ui64 vChunkSize)
{
    return GetCountByBlocks(
        blockCount,
        GetRegionBlockCount(blockSize, vChunkSize));
}

size_t GetVChunkCount(ui64 blockCount, ui32 blockSize, ui64 vChunkSize)
{
    return GetCountByBlocks(
        blockCount,
        GetVChunkBlockCount(blockSize, vChunkSize));
}

ui32 GetVChunkCountPerDirectBlockGroup(
    size_t regionCount,
    size_t directBlockGroupInVolumeCount)
{
    Y_ABORT_UNLESS(directBlockGroupInVolumeCount > 0);
    return IntegerCast<ui32>(
        regionCount * VChunkPerRegionCount / directBlockGroupInVolumeCount);
}

>>>>>>> ed0d0ddf015 (NBS-6784: Remove host from dbg (#46061)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/region_geometry.cpp
size_t GetRegionIndex(const TVolumeConfig& volumeConfig, TBlockRange64 range)
{
    const ui64 blocksPerRegion =
        GetRegionBlockCount(volumeConfig.BlockSize, volumeConfig.VChunkSize);
    return range.Start / blocksPerRegion;
}

<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/range_translate.cpp
=======
size_t GetRegionIndexByVChunk(size_t vChunkIndex)
{
    return vChunkIndex / VChunkPerRegionCount;
}

size_t GetVChunkIndexInRegion(size_t vChunkIndex)
{
    return vChunkIndex % VChunkPerRegionCount;
}

size_t GetVChunkIndex(size_t regionIndex, size_t vChunkIndexInRegion)
{
    return regionIndex * VChunkPerRegionCount + vChunkIndexInRegion;
}

>>>>>>> f32af13259a (Refactor partition direct disk geometry calculations (#53169)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/region_geometry.cpp
TBlockRange64 TranslateToRegion(
    const TVolumeConfig& volumeConfig,
    TBlockRange64 range)
{
    const ui64 blocksPerRegion =
        GetRegionBlockCount(volumeConfig.BlockSize, volumeConfig.VChunkSize);
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
<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/range_translate.cpp
    const ui32 vChunksPerRegionCount = RegionSize / volumeConfig.VChunkSize;
    return stripeIndex % vChunksPerRegionCount;
=======
    return stripeIndex % VChunkPerRegionCount;
>>>>>>> f32af13259a (Refactor partition direct disk geometry calculations (#53169)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/region_geometry.cpp
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
<<<<<<< HEAD:ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/range_translate.cpp
    const ui32 vChunksPerRegionCount = RegionSize / volumeConfig.VChunkSize;
    const size_t stripeIndexInVChunk = stripeIndex / vChunksPerRegionCount;
=======
    const size_t stripeIndexInVChunk = stripeIndex / VChunkPerRegionCount;
>>>>>>> f32af13259a (Refactor partition direct disk geometry calculations (#53169)):ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/region_geometry.cpp
    const size_t blockIndexInStripe = regionRange.Start % blocksPerStripe;
    const ui64 vChunkStart =
        stripeIndexInVChunk * blocksPerStripe + blockIndexInStripe;

    return TBlockRange16::WithLength(
        IntegerCast<ui16>(vChunkStart),
        IntegerCast<ui16>(regionRange.Size()));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
