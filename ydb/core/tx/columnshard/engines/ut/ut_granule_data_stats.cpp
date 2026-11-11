#include <ydb/core/tx/columnshard/engines/storage/data_stats/granule_data_stats.h>
#include <ydb/core/tx/columnshard/engines/storage/data_stats/shard_data_stats.h>
#include <ydb/core/tx/columnshard/test_helper/portion_test_helper.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap {

Y_UNIT_TEST_SUITE(TGranuleDataStatsTests) {
    const TInternalPathId PathId = TInternalPathId::FromRawValue(1);
    const TSnapshot Committed(1, 1);
    const TSnapshot Removed(2, 1);
    const TString Tier = "tier1";

    std::shared_ptr<TPortionInfo> MakeCompacted(const ui64 portionId, const ui32 recordsCount, const std::optional<TSnapshot>& removeSnapshot,
        const TString& tierName = {}, const ui32 blobStorageIndexBytes = 0) {
        return NTest::MakeTestCompactedPortion(
            PathId, portionId, 1, 2, recordsCount, Committed, removeSnapshot, tierName, blobStorageIndexBytes);
    }

    Y_UNIT_TEST(MissingStorageHasEmptyStats) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        UNIT_ASSERT(stats.GetStorageStats(NBlobOperations::TGlobal::DefaultStorageId).IsEmpty());
        UNIT_ASSERT(stats.GetStorageStats(Tier).IsEmpty());
    }

    Y_UNIT_TEST(EachPortionGoesToItsStorageAndGroup) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        stats.AddPortion(NTest::MakeTestWrittenPortion(PathId, 1, 1, Committed));
        stats.AddPortion(MakeCompacted(2, 2, std::nullopt));
        stats.AddPortion(NTest::MakeTestWrittenPortion(PathId, 3, 4, Committed, Tier));
        stats.AddPortion(MakeCompacted(4, 8, std::nullopt, Tier));
        stats.AddPortion(MakeCompacted(5, 16, Removed));
        stats.AddPortion(MakeCompacted(6, 32, Removed, Tier));

        const TString blobStorage = NBlobOperations::TGlobal::DefaultStorageId;
        UNIT_ASSERT_VALUES_EQUAL(stats.GetGranuleStats(PathId).GetWritten(blobStorage).GetRecordsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetGranuleStats(PathId).GetCompacted(blobStorage).GetRecordsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetGranuleStats(PathId).GetRemoved(blobStorage).GetRecordsCount(), 16);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetGranuleStats(PathId).GetWritten(Tier).GetRecordsCount(), 4);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetGranuleStats(PathId).GetCompacted(Tier).GetRecordsCount(), 8);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetGranuleStats(PathId).GetRemoved(Tier).GetRecordsCount(), 32);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetGranuleStats(PathId).GetAll().GetRecordsCount(), 1 + 2 + 4 + 8 + 16 + 32);
    }

    Y_UNIT_TEST(EmptyAfterRemovingEveryPortion) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        const auto written = NTest::MakeTestWrittenPortion(PathId, 1, 1, Committed);
        const auto removed = MakeCompacted(2, 2, Removed, Tier, 50);
        stats.AddPortion(written);
        stats.AddPortion(removed);
        stats.RemovePortion(written);
        UNIT_ASSERT(!stats.GetGranuleStats(PathId).IsEmpty());
        stats.RemovePortion(removed);
        UNIT_ASSERT(stats.GetGranuleStats(PathId).IsEmpty());
    }

    Y_UNIT_TEST(PortionBelowThresholdIsSmallBlob) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        stats.AddPortion(MakeCompacted(1, 1, std::nullopt, {}, 50));
        UNIT_ASSERT_VALUES_EQUAL(stats.GetGranuleStats(PathId).GetSmallBlobs().VolumeBytes, 150);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetGranuleStats(PathId).GetSmallBlobs().Count, 1);
    }

    Y_UNIT_TEST(EvictedPortionIndexesAreBlobStorageSmallBlobs) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        stats.AddPortion(MakeCompacted(1, 1, std::nullopt, Tier, 50));
        UNIT_ASSERT_VALUES_EQUAL(stats.GetGranuleStats(PathId).GetSmallBlobs(NBlobOperations::TGlobal::DefaultStorageId).VolumeBytes, 50);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetGranuleStats(PathId).GetSmallBlobs(Tier).VolumeBytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetStorageStats(Tier).GetSmallBlobs().Count, 0);
    }

    Y_UNIT_TEST(ShardTotalsSumGranulesByStorage) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        const auto secondPathId = TInternalPathId::FromRawValue(2);
        stats.AddPortion(NTest::MakeTestWrittenPortion(PathId, 1, 1, Committed));
        stats.AddPortion(NTest::MakeTestWrittenPortion(secondPathId, 2, 2, Committed));
        stats.AddPortion(MakeCompacted(3, 4, std::nullopt, Tier, 50));
        stats.AddPortion(NTest::MakeTestCompactedPortion(secondPathId, 4, 1, 2, 8, Committed, Removed, Tier, 25));

        const auto& blobStorage = stats.GetStorageStats(NBlobOperations::TGlobal::DefaultStorageId);
        UNIT_ASSERT_VALUES_EQUAL(blobStorage.GetWritten().GetRecordsCount(), 3);
        UNIT_ASSERT_VALUES_EQUAL(blobStorage.GetSmallBlobs().Count, 4);
        UNIT_ASSERT_VALUES_EQUAL(blobStorage.GetSmallBlobs().VolumeBytes, 275);
        const auto& tier = stats.GetStorageStats(Tier);
        UNIT_ASSERT_VALUES_EQUAL(tier.GetCompacted().GetRecordsCount(), 4);
        UNIT_ASSERT_VALUES_EQUAL(tier.GetRemoved().GetRecordsCount(), 8);
        UNIT_ASSERT_VALUES_EQUAL(tier.GetSmallBlobs().Count, 0);
    }

    Y_UNIT_TEST(ShardTotalsDropRemovedGranules) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        const auto portion = MakeCompacted(1, 1, std::nullopt, Tier, 50);
        stats.AddPortion(portion);
        UNIT_ASSERT(!stats.GetStorageStats(Tier).IsEmpty());

        stats.RemovePortion(portion);
        UNIT_ASSERT(stats.GetStorageStats(Tier).IsEmpty());
        UNIT_ASSERT(stats.GetStorageStats(NBlobOperations::TGlobal::DefaultStorageId).IsEmpty());
        stats.AddPortion(portion);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetStorageStats(NBlobOperations::TGlobal::DefaultStorageId).GetSmallBlobs().Count, 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetGranuleStats(PathId).GetSmallBlobs().Count, 1);
    }
}

}   // namespace NKikimr::NOlap
