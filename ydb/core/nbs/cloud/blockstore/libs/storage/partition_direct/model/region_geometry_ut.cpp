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
        constexpr ui64 vChunksPerRegion = RegionSize / MaxVChunkSize;
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
