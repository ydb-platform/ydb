#include <ydb/core/tx/columnshard/engines/storage/data_stats/shard_data_stats.h>
#include <ydb/core/tx/columnshard/test_helper/portion_test_helper.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap {

Y_UNIT_TEST_SUITE(TShardDataStatsTests) {
    const TInternalPathId PathId = TInternalPathId::FromRawValue(1);
    const TSnapshot Committed(1, 1);
    const TSnapshot Removed(2, 1);
    const TString BlobStorage = NBlobOperations::TGlobal::DefaultStorageId;
    const TString Tier = "tier1";
    using NTest::TestPortionColumnBlobBytes;

    std::shared_ptr<TPortionInfo> MakeCompacted(const ui64 portionId, const ui32 recordsCount, const std::optional<TSnapshot>& removeSnapshot,
        const TString& tierName = {}, const ui32 blobStorageIndexBytes = 0) {
        return NTest::MakeTestCompactedPortion(
            PathId, portionId, 1, 2, recordsCount, Committed, removeSnapshot, tierName, blobStorageIndexBytes);
    }

    Y_UNIT_TEST(EachPortionGoesToItsStorageAndGroup) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        stats.AddPortion(NTest::MakeTestWrittenPortion(PathId, 1, 1, Committed));
        stats.AddPortion(MakeCompacted(2, 2, std::nullopt));
        stats.AddPortion(NTest::MakeTestWrittenPortion(PathId, 3, 4, Committed, Tier));
        stats.AddPortion(MakeCompacted(4, 8, std::nullopt, Tier));
        stats.AddPortion(MakeCompacted(5, 16, Removed));
        stats.AddPortion(MakeCompacted(6, 32, Removed, Tier));

        const auto& granule = stats.GetGranuleStats(PathId);
        UNIT_ASSERT_VALUES_EQUAL(granule.GetWritten(BlobStorage).GetRecordsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(granule.GetCompacted(BlobStorage).GetRecordsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(granule.GetRemoved(BlobStorage).GetRecordsCount(), 16);
        UNIT_ASSERT_VALUES_EQUAL(granule.GetWritten(Tier).GetRecordsCount(), 4);
        UNIT_ASSERT_VALUES_EQUAL(granule.GetCompacted(Tier).GetRecordsCount(), 8);
        UNIT_ASSERT_VALUES_EQUAL(granule.GetRemoved(Tier).GetRecordsCount(), 32);
        UNIT_ASSERT_VALUES_EQUAL(granule.GetAll().GetRecordsCount(), 1 + 2 + 4 + 8 + 16 + 32);
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
        UNIT_ASSERT_VALUES_EQUAL(stats.GetGranuleStats(PathId).GetSmallBlobs().VolumeBytes, TestPortionColumnBlobBytes + 50);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetGranuleStats(PathId).GetSmallBlobs().Count, 1);
    }

    Y_UNIT_TEST(PortionAboveThresholdIsNotSmallBlob) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        stats.AddPortion(MakeCompacted(1, 1, std::nullopt, {}, 1 << 20));
        UNIT_ASSERT_VALUES_EQUAL(stats.GetGranuleStats(PathId).GetSmallBlobs().Count, 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetStorageStats(BlobStorage).GetSmallBlobs().Count, 0);
    }

    Y_UNIT_TEST(EvictedPortionIndexesAreBlobStorageSmallBlobs) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        stats.AddPortion(MakeCompacted(1, 1, std::nullopt, Tier, 50));
        UNIT_ASSERT_VALUES_EQUAL(stats.GetGranuleStats(PathId).GetSmallBlobs(BlobStorage).VolumeBytes, 50);
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

        const auto& blobStorage = stats.GetStorageStats(BlobStorage);
        UNIT_ASSERT_VALUES_EQUAL(blobStorage.GetWritten().GetRecordsCount(), 3);
        UNIT_ASSERT_VALUES_EQUAL(blobStorage.GetSmallBlobs().Count, 4);
        UNIT_ASSERT_VALUES_EQUAL(blobStorage.GetSmallBlobs().VolumeBytes, 2 * TestPortionColumnBlobBytes + 50 + 25);
        const auto& tier = stats.GetStorageStats(Tier);
        UNIT_ASSERT_VALUES_EQUAL(tier.GetCompacted().GetRecordsCount(), 4);
        UNIT_ASSERT_VALUES_EQUAL(tier.GetRemoved().GetRecordsCount(), 8);
        UNIT_ASSERT_VALUES_EQUAL(tier.GetSmallBlobs().Count, 0);
    }

    Y_UNIT_TEST(ReAddedPortionCountsOnce) {
        NColumnShard::TShardDataStats stats(TTestStoragesManager::GetInstance(), 1);
        const auto portion = MakeCompacted(1, 1, std::nullopt, Tier, 50);
        stats.AddPortion(portion);
        stats.RemovePortion(portion);
        UNIT_ASSERT(stats.GetGranuleStats(PathId).IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(stats.GetStorageStats(Tier).GetAll().GetCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetStorageStats(BlobStorage).GetSmallBlobs().Count, 0);

        stats.AddPortion(portion);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetStorageStats(Tier).GetAll().GetCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetStorageStats(BlobStorage).GetSmallBlobs().Count, 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetGranuleStats(PathId).GetSmallBlobs().Count, 1);
    }
}

}   // namespace NKikimr::NOlap
