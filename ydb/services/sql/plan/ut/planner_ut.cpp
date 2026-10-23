#include <library/cpp/testing/unittest/registar.h>

#include <ydb/services/sql/plan/plan.h>

using namespace NKikimr::NSql;

namespace {

TTableShape TwoShards() {
    TTableShape table;
    table.KeyColumns = {"id"};
    table.Partitions.push_back({1, "", "m"});
    table.Partitions.push_back({2, "m", "", true});
    table.SchemaVersion = 3;
    table.RowsPerPartition = 10;
    table.BytesPerRow = 8;
    return table;
}

} // namespace

Y_UNIT_TEST_SUITE(Planner) {
    Y_UNIT_TEST(PointIsCheaperThanScan) {
        TPlannerConfig config;
        config.EnableFullScan = true;
        const TPlanResult point = ChoosePlan(TwoShards(), {{"id", true, false, false, "a", {}}}, config);
        UNIT_ASSERT(point.Ok);
        UNIT_ASSERT_VALUES_EQUAL(point.Plan->Reads.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(point.Plan->Reads[0].TabletId, 1u);
        UNIT_ASSERT(point.Plan->Reads[0].Shape == EReadShape::Keys);
        UNIT_ASSERT_VALUES_EQUAL(point.Plan->Cost.Calls, 1u);

        const TPlanResult scan = ChoosePlan(TwoShards(), {}, config);
        UNIT_ASSERT(scan.Ok);
        UNIT_ASSERT(scan.Plan->Cost.Total > point.Plan->Cost.Total);
        UNIT_ASSERT(scan.Plan->Cost.Calls > point.Plan->Cost.Calls);
    }

    Y_UNIT_TEST(RangeCrossesThePartitionBoundary) {
        const TPlanResult plan = ChoosePlan(TwoShards(), {{"id", false, true, true, "a", "z"}}, {});
        UNIT_ASSERT(plan.Ok);
        UNIT_ASSERT_VALUES_EQUAL(plan.Plan->Reads.size(), 2u);
        UNIT_ASSERT(plan.Plan->Reads[0].Shape == EReadShape::Ranges);
        UNIT_ASSERT_VALUES_EQUAL(plan.Plan->Reads[0].TabletId, 1u);
        UNIT_ASSERT_VALUES_EQUAL(plan.Plan->Reads[1].TabletId, 2u);
    }

    Y_UNIT_TEST(MoreCallsLoseAndScanStaysOff) {
        const TPlanResult unbounded = ChoosePlan(TwoShards(), {}, {});
        UNIT_ASSERT(!unbounded.Ok);
        UNIT_ASSERT_VALUES_EQUAL(unbounded.SqlState, "0A000");

        TTableShape column = TwoShards();
        column.ColumnTable = true;
        const TPlanResult rejected = ChoosePlan(column, {{"id", true, false, false, "a", {}}}, {});
        UNIT_ASSERT(!rejected.Ok);
        UNIT_ASSERT_VALUES_EQUAL(rejected.SqlState, "0A000");
        UNIT_ASSERT(!rejected.Plan);
    }

    Y_UNIT_TEST(CacheDropsOnSchemaError) {
        TPlannerConfig config;
        config.CacheTtlMs = 1000;
        TPlanCache cache(config);
        TPlan plan;
        plan.SchemaVersion = 3;
        plan.Cost.Calls = 1;
        cache.Store("/dc-1", "select 1", {1}, plan, 10);
        UNIT_ASSERT(cache.Lookup("/dc-1", "select 1", {1}, 3, 20));
        UNIT_ASSERT(!cache.Lookup("/dc-1", "select 1", {1}, 4, 20));
        cache.Store("/dc-1", "select 1", {1}, plan, 10);
        cache.ForgetSchema("/dc-1", "select 1");
        UNIT_ASSERT(!cache.Lookup("/dc-1", "select 1", {1}, 3, 20));
    }
}
