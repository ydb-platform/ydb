#include "region_geometry.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/volume_config.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

TVolumeConfig MakeVolumeConfig()
{
    return {
        .DiskId = "disk",
        .BlockSize = DefaultBlockSize,
        .BlockCount = 0,
        .BlocksPerStripe = 8,
        .VChunkSize = MaxVChunkSize,
    };
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRegionGeometryTest)
{
    Y_UNIT_TEST(ShouldCalculateRegionAndVChunkCounts)
    {
        constexpr ui64 regionSize = MaxVChunkSize * VChunkPerRegionCount;
        constexpr ui64 blocksPerRegion = regionSize / DefaultBlockSize;
        constexpr size_t vChunksPerRegion = VChunkPerRegionCount;
        constexpr ui64 blocksPerVChunk = MaxVChunkSize / DefaultBlockSize;

        UNIT_ASSERT_VALUES_EQUAL(regionSize, GetRegionSize(MaxVChunkSize));
        UNIT_ASSERT_VALUES_EQUAL(
            blocksPerVChunk,
            GetVChunkBlockCount(DefaultBlockSize, MaxVChunkSize));
        UNIT_ASSERT_VALUES_EQUAL(
            blocksPerRegion,
            GetRegionBlockCount(DefaultBlockSize, MaxVChunkSize));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            GetRegionCount(0, DefaultBlockSize, MaxVChunkSize));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            GetRegionCount(blocksPerRegion, DefaultBlockSize, MaxVChunkSize));
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            GetRegionCount(
                blocksPerRegion + 1,
                DefaultBlockSize,
                MaxVChunkSize));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            GetVChunkCount(0, DefaultBlockSize, MaxVChunkSize));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            GetVChunkCount(1, DefaultBlockSize, MaxVChunkSize));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            GetVChunkCount(blocksPerVChunk, DefaultBlockSize, MaxVChunkSize));
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            GetVChunkCount(
                blocksPerVChunk + 1,
                DefaultBlockSize,
                MaxVChunkSize));
        UNIT_ASSERT_VALUES_EQUAL(
            vChunksPerRegion + 1,
            GetVChunkCount(
                blocksPerRegion + 1,
                DefaultBlockSize,
                MaxVChunkSize));
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            GetVChunkCountPerDirectBlockGroup(
                3,
                DefaultVolumeDirectBlockGroupCount));
        UNIT_ASSERT_VALUES_EQUAL(6, GetVChunkCountPerDirectBlockGroup(3, 16));

        constexpr size_t regionIndex = 2;
        constexpr size_t vChunkIndexInRegion = 3;
        const size_t vChunkIndex =
            GetVChunkIndex(regionIndex, vChunkIndexInRegion);
        UNIT_ASSERT_VALUES_EQUAL(
            regionIndex,
            GetRegionIndexByVChunk(vChunkIndex));
        UNIT_ASSERT_VALUES_EQUAL(
            vChunkIndexInRegion,
            GetVChunkIndexInRegion(vChunkIndex));
    }

    Y_UNIT_TEST(ShouldTranslateRegionRangeToVChunkRange)
    {
        const auto volumeConfig = MakeVolumeConfig();
        const TBlockRange64 regionRange =
            TBlockRange64::MakeClosedInterval(523, 525);

        const TBlockRange16 result =
            TranslateToVChunk(volumeConfig, regionRange);

        UNIT_ASSERT_VALUES_EQUAL(19, result.Start);
        UNIT_ASSERT_VALUES_EQUAL(21, result.End);
    }

    Y_UNIT_TEST(ShouldTranslateLastVChunkBlock)
    {
        const auto volumeConfig = MakeVolumeConfig();
        constexpr ui64 vChunksPerRegion = VChunkPerRegionCount;
        constexpr ui64 stripesPerVChunk = MaxVChunkBlockCount / 8;
        constexpr ui64 lastStripe =
            (stripesPerVChunk - 1) * vChunksPerRegion + (vChunksPerRegion - 1);
        const TBlockRange64 regionRange = TBlockRange64::MakeOneBlock(
            lastStripe * volumeConfig.BlocksPerStripe +
            volumeConfig.BlocksPerStripe - 1);

        const TBlockRange16 result =
            TranslateToVChunk(volumeConfig, regionRange);

        UNIT_ASSERT_VALUES_EQUAL(MaxVChunkBlockCount - 1, result.Start);
        UNIT_ASSERT_VALUES_EQUAL(MaxVChunkBlockCount - 1, result.End);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
