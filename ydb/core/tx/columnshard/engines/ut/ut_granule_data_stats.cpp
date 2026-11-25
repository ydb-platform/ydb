#include <ydb/core/tx/columnshard/engines/storage/data_stats/granule_data_stats.h>
#include <ydb/core/tx/columnshard/test_helper/portion_test_helper.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap {

Y_UNIT_TEST_SUITE(TGranuleDataStatsTests) {
    const TInternalPathId PathId = TInternalPathId::FromRawValue(1);
    const TSnapshot Committed(1, 1);
    const TSnapshot Removed(2, 1);
    const TString Tier = "tier1";

    std::shared_ptr<TPortionInfo> MakeCompacted(
        const ui64 portionId, const ui32 recordsCount, const std::optional<TSnapshot>& removeSnapshot, const TString& tierName = {}) {
        return NTest::MakeTestCompactedPortion(PathId, portionId, 1, 2, recordsCount, Committed, removeSnapshot, tierName);
    }

    Y_UNIT_TEST(EachPortionGoesToItsGroup) {
        NColumnShard::TGranuleDataStats stats;
        stats.AddPortion(*NTest::MakeTestWrittenPortion(PathId, 1, 1, Committed), 0);
        stats.AddPortion(*MakeCompacted(2, 2, std::nullopt), 0);
        stats.AddPortion(*NTest::MakeTestWrittenPortion(PathId, 3, 4, Committed, Tier), 0);
        stats.AddPortion(*MakeCompacted(4, 8, std::nullopt, Tier), 0);
        stats.AddPortion(*MakeCompacted(5, 16, Removed), 0);
        stats.AddPortion(*MakeCompacted(6, 32, Removed, Tier), 0);

        UNIT_ASSERT_VALUES_EQUAL(stats.GetWritten().GetRecordsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetCompacted().GetRecordsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetEvicted().GetRecordsCount(), 4 + 8);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetRemoved().GetRecordsCount(), 16 + 32);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetDefaultStorage().GetRecordsCount(), 1 + 2 + 16);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetActiveWritten().GetRecordsCount(), 1 + 4);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetActiveCompacted().GetRecordsCount(), 2 + 8);
    }

    Y_UNIT_TEST(EmptyAfterRemovingEveryPortion) {
        NColumnShard::TGranuleDataStats stats;
        const auto written = NTest::MakeTestWrittenPortion(PathId, 1, 1, Committed);
        const auto removed = MakeCompacted(2, 2, Removed, Tier);
        stats.AddPortion(*written, 0);
        stats.AddPortion(*removed, 0);
        stats.RemovePortion(*written, 0);
        UNIT_ASSERT(!stats.IsEmpty());
        stats.RemovePortion(*removed, 0);
        UNIT_ASSERT(stats.IsEmpty());
    }

    Y_UNIT_TEST(PortionBelowThresholdIsSmallBlob) {
        NColumnShard::TGranuleDataStats stats;
        stats.AddPortion(*MakeCompacted(1, 1, std::nullopt), 1000);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetSmallBlobs().VolumeBytes, 100);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetSmallBlobs().Count, 1);
    }

    Y_UNIT_TEST(SmallBlobsClampedAtZeroWhenThresholdGrowsBeforeRemove) {
        NColumnShard::TGranuleDataStats stats;
        const auto portion = MakeCompacted(1, 1, std::nullopt);
        stats.AddPortion(*portion, 0);
        stats.RemovePortion(*portion, 1000);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetSmallBlobs().VolumeBytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetSmallBlobs().Count, 0);
    }
}

}   // namespace NKikimr::NOlap
