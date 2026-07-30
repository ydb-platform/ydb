#include "node_database_metrics_aggregator.h"
#include "ut_helpers.h"

#include <library/cpp/monlib/dynamic_counters/encode.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/array_size.h>
#include <util/string/cast.h>

using namespace NKikimr;
using namespace NKikimr::NDetailedMetricsTests;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString DATABASE_PATH = "/Root/db";
const TString MONITORING_PROJECT_ID = "my-project";

const TString TABLE_PATH = "/Root/db/dir/table";
const TString RELATIVE_TABLE_PATH = "dir/table";

const TPathId TABLE_ID(72057594046644480ull, 42);

constexpr TTabletTypes::EType TABLET_TYPE = TTabletTypes::DataShard;

////////////////////////////////////////////////////////////////////////////////
// A small stand-in for the low level counters of Data Shard. The real counter set
// has hundreds of counters, which would make the assertions unreadable without
// covering anything, which is not covered by these few counters.

constexpr const char* SIMPLE_COUNTER_NAMES[] = {
    "UniqueRows",
    "UniqueBytes",
};

constexpr const char* CUMULATIVE_COUNTER_NAMES[] = {
    "ConsumedCPU",
};

constexpr const char* PERCENTILE_COUNTER_NAMES[] = {
    // A histogram aggregate: it is NOT filled by the tablet, it collects
    // one observation per tablet from the "ConsumedCPU" cumulative counter
    "HIST(ConsumedCPU)",

    // An ordinary percentile counter, which the tablet fills itself
    "TxLatency",
};

constexpr TTabletPercentileCounter::TRangeDef PERCENTILE_RANGES[] = {
    {  0,   "0"},
    { 10,  "10"},
    {100, "100"},
};

enum ESimpleCounter : ui32 {
    UNIQUE_ROWS = 0,
    UNIQUE_BYTES = 1,
};

enum ECumulativeCounter : ui32 {
    CONSUMED_CPU = 0,
};

////////////////////////////////////////////////////////////////////////////////

/**
 * A single tablet, which reports the low level counters above.
 */
struct TFakeTablet {
    TFakeTablet(ui64 tabletId, ui32 followerId)
        : TabletId(tabletId)
        , FollowerId(followerId)
        , AppCounters(
            Y_ARRAY_SIZE(SIMPLE_COUNTER_NAMES),
            Y_ARRAY_SIZE(CUMULATIVE_COUNTER_NAMES),
            Y_ARRAY_SIZE(PERCENTILE_COUNTER_NAMES),
            SIMPLE_COUNTER_NAMES,
            CUMULATIVE_COUNTER_NAMES,
            PERCENTILE_COUNTER_NAMES
        )
    {
        for (ui32 i = 0; i < Y_ARRAY_SIZE(PERCENTILE_COUNTER_NAMES); ++i) {
            AppCounters.Percentile()[i].Initialize(PERCENTILE_RANGES, true /* integral */);
        }
    }

    TFakeTablet& SetSimple(ESimpleCounter counter, ui64 value) {
        AppCounters.Simple()[counter].Set(value);
        return *this;
    }

    TFakeTablet& AddCumulative(ECumulativeCounter counter, ui64 delta) {
        AppCounters.Cumulative()[counter] += delta;
        return *this;
    }

    void Report(
        const TNodeDatabaseMetricsAggregatorPtr& aggregator,
        EDetailedMetricsLevel level,
        TInstant now
    ) {
        TDetailedMetricsTableInfo table;
        table.TableId = TABLE_ID;
        table.TablePath = TABLE_PATH;
        table.SchemaVersion = 1;
        table.MetricsLevel = level;

        aggregator->AddCounters(
            table,
            TabletId,
            FollowerId,
            TABLET_TYPE,
            ExecutorCounters,
            AppCounters,
            now
        );
    }

    const ui64 TabletId;
    const ui32 FollowerId;

    TTabletCountersBase ExecutorCounters;
    TTabletCountersBase AppCounters;
};

////////////////////////////////////////////////////////////////////////////////

/**
 * @return The counter group of the table (or nullptr if there is none)
 */
NMonitoring::TDynamicCounterPtr FindTableGroup(NMonitoring::TDynamicCounterPtr rootGroup) {
    auto databaseGroup = rootGroup->FindSubgroup("database", DATABASE_PATH);
    if (!databaseGroup) {
        return nullptr;
    }

    auto projectGroup = databaseGroup->FindSubgroup("monitoring_project_id", MONITORING_PROJECT_ID);
    if (!projectGroup) {
        return nullptr;
    }

    return projectGroup->FindSubgroup("table", RELATIVE_TABLE_PATH);
}

/**
 * @param[in] bucketGroup The counter group of a role bucket or of a leaf
 *
 * @return The counter group of the application counters (or nullptr if there is none)
 */
NMonitoring::TDynamicCounterPtr FindAppCountersGroup(NMonitoring::TDynamicCounterPtr bucketGroup) {
    if (!bucketGroup) {
        return nullptr;
    }

    auto typeGroup = bucketGroup->FindSubgroup("type", TTabletTypes::TypeToStr(TABLET_TYPE));
    if (!typeGroup) {
        return nullptr;
    }

    return typeGroup->FindSubgroup("category", "app");
}

/**
 * @param[in] rootGroup The counter group where the whole tree is created
 * @param[in] followerId The follower ID of the tablet (0 for the leader)
 *
 * @return The application counters of the role bucket (or nullptr if there is none)
 */
NMonitoring::TDynamicCounterPtr FindRoleBucketCounters(
    NMonitoring::TDynamicCounterPtr rootGroup,
    ui32 followerId
) {
    auto tableGroup = FindTableGroup(rootGroup);
    if (!tableGroup) {
        return nullptr;
    }

    return FindAppCountersGroup(
        tableGroup->FindSubgroup("role", followerId == 0 ? "leader" : "follower")
    );
}

/**
 * @param[in] rootGroup The counter group where the whole tree is created
 * @param[in] tabletId The ID of the tablet
 * @param[in] followerId The follower ID of the tablet (0 for the leader)
 *
 * @return The application counters of the leaf (or nullptr if there is none)
 */
NMonitoring::TDynamicCounterPtr FindLeafCounters(
    NMonitoring::TDynamicCounterPtr rootGroup,
    ui64 tabletId,
    ui32 followerId
) {
    auto tableGroup = FindTableGroup(rootGroup);
    if (!tableGroup) {
        return nullptr;
    }

    auto perPartitionGroup = tableGroup->FindSubgroup("detailed_metrics", "per_partition");
    if (!perPartitionGroup) {
        return nullptr;
    }

    auto tabletGroup = perPartitionGroup->FindSubgroup("tablet_id", ToString(tabletId));
    if (!tabletGroup) {
        return nullptr;
    }

    return FindAppCountersGroup(tabletGroup->FindSubgroup("follower_id", ToString(followerId)));
}

/**
 * @param[in] countersGroup The counter group to read the counter from
 * @param[in] name The name of the counter
 *
 * @return The value of the corresponding counter
 */
ui64 GetCounterValue(NMonitoring::TDynamicCounterPtr countersGroup, const TString& name) {
    UNIT_ASSERT_C(countersGroup, "no counter group for the counter " << name);

    auto counter = countersGroup->FindNamedCounter("sensor", name);
    UNIT_ASSERT_C(counter, "no counter " << name);

    return counter->Val();
}

void DumpCounters(const TString& title, NMonitoring::TDynamicCounterPtr rootGroup) {
    Cerr << "TEST " << title << ":" << Endl
         << NormalizeJson(NMonitoring::ToJson(*rootGroup)) << Endl;
}

} // namespace <anonymous>

////////////////////////////////////////////////////////////////////////////////

/**
 * Unit tests for the node database metrics aggregator (TNodeDatabaseMetricsAggregator).
 */
Y_UNIT_TEST_SUITE(TNodeDatabaseMetricsAggregatorTest) {
    /**
     * Verify that at the table level all same-node partitions of the table are collapsed
     * into the two role buckets and no per-partition counters are created.
     */
    Y_UNIT_TEST(TableLevelCollapsesPartitions) {
        NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

        auto aggregator = CreateNodeDatabaseMetricsAggregator(
            rootGroup,
            DATABASE_PATH,
            MONITORING_PROJECT_ID
        );

        const TInstant now = TInstant::Seconds(100);

        // 3 leader partitions of the same table on this node
        TFakeTablet leader1(1000, 0);
        TFakeTablet leader2(2000, 0);
        TFakeTablet leader3(3000, 0);

        leader1.SetSimple(UNIQUE_ROWS, 1).SetSimple(UNIQUE_BYTES, 10).AddCumulative(CONSUMED_CPU, 100);
        leader2.SetSimple(UNIQUE_ROWS, 2).SetSimple(UNIQUE_BYTES, 20).AddCumulative(CONSUMED_CPU, 200);
        leader3.SetSimple(UNIQUE_ROWS, 4).SetSimple(UNIQUE_BYTES, 40).AddCumulative(CONSUMED_CPU, 400);

        // 2 followers of the very same partition: they must NOT collide with each other
        TFakeTablet follower1(1000, 1);
        TFakeTablet follower2(1000, 2);

        follower1.SetSimple(UNIQUE_ROWS, 8).AddCumulative(CONSUMED_CPU, 800);
        follower2.SetSimple(UNIQUE_ROWS, 16).AddCumulative(CONSUMED_CPU, 1600);

        for (auto* tablet : {&leader1, &leader2, &leader3, &follower1, &follower2}) {
            tablet->Report(aggregator, EDetailedMetricsLevel::Table, now);
        }

        aggregator->RecalculateAllCounters();

        DumpCounters("Table level counters", rootGroup);

        // The leader role bucket holds the 3 leader partitions
        auto leaderCounters = FindRoleBucketCounters(rootGroup, 0);
        UNIT_ASSERT(leaderCounters);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "SUM(UniqueRows)"), 1 + 2 + 4);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "MAX(UniqueRows)"), 4);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "SUM(UniqueBytes)"), 10 + 20 + 40);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "MAX(UniqueBytes)"), 40);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "ConsumedCPU"), 100 + 200 + 400);

        // The follower role bucket holds the 2 followers
        auto followerCounters = FindRoleBucketCounters(rootGroup, 1);
        UNIT_ASSERT(followerCounters);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(followerCounters, "SUM(UniqueRows)"), 8 + 16);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(followerCounters, "MAX(UniqueRows)"), 16);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(followerCounters, "ConsumedCPU"), 800 + 1600);

        // No per-partition counters at the table level
        auto tableGroup = FindTableGroup(rootGroup);
        UNIT_ASSERT(tableGroup);
        UNIT_ASSERT(!tableGroup->FindSubgroup("detailed_metrics", "per_partition"));
    }

    /**
     * Verify that at the partition level every (tablet_id, follower_id) leaf is kept
     * verbatim and no on-node rollup of any kind is created.
     */
    Y_UNIT_TEST(PartitionLevelKeepsLeaves) {
        NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

        auto aggregator = CreateNodeDatabaseMetricsAggregator(
            rootGroup,
            DATABASE_PATH,
            MONITORING_PROJECT_ID
        );

        const TInstant now = TInstant::Seconds(100);

        // 2 partitions of the same table, a leader and 2 followers each
        const TVector<ui64> tabletIds = {1000, 2000};
        const TVector<ui32> followerIds = {0, 1, 2};

        ui64 value = 0;
        THashMap<std::pair<ui64, ui32>, ui64> expectedValues;

        for (ui64 tabletId : tabletIds) {
            for (ui32 followerId : followerIds) {
                value += 1;
                expectedValues[std::make_pair(tabletId, followerId)] = value;

                TFakeTablet tablet(tabletId, followerId);
                tablet.SetSimple(UNIQUE_ROWS, value).AddCumulative(CONSUMED_CPU, value * 100);
                tablet.Report(aggregator, EDetailedMetricsLevel::Partition, now);
            }
        }

        aggregator->RecalculateAllCounters();

        DumpCounters("Partition level counters", rootGroup);

        // Every leaf holds exactly what its tablet has reported
        for (const auto& [tablet, expectedValue] : expectedValues) {
            const auto& [tabletId, followerId] = tablet;

            auto leafCounters = FindLeafCounters(rootGroup, tabletId, followerId);
            UNIT_ASSERT_C(leafCounters, "no leaf for " << tabletId << ":" << followerId);

            UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leafCounters, "SUM(UniqueRows)"), expectedValue);
            UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leafCounters, "MAX(UniqueRows)"), expectedValue);
            UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leafCounters, "ConsumedCPU"), expectedValue * 100);
        }

        // No role buckets and no on-node rollup at the partition level
        auto tableGroup = FindTableGroup(rootGroup);
        UNIT_ASSERT(tableGroup);
        UNIT_ASSERT(!tableGroup->FindSubgroup("role", "leader"));
        UNIT_ASSERT(!tableGroup->FindSubgroup("role", "follower"));
        UNIT_ASSERT(!FindAppCountersGroup(tableGroup));

        auto perPartitionGroup = tableGroup->FindSubgroup("detailed_metrics", "per_partition");
        UNIT_ASSERT(perPartitionGroup);
        UNIT_ASSERT(!FindAppCountersGroup(perPartitionGroup));

        for (ui64 tabletId : tabletIds) {
            auto tabletGroup = perPartitionGroup->FindSubgroup("tablet_id", ToString(tabletId));
            UNIT_ASSERT(tabletGroup);
            UNIT_ASSERT(!FindAppCountersGroup(tabletGroup));
        }
    }

    /**
     * Verify that forgetting a tablet of a table level table drops its contribution
     * from the role bucket and removes the counter groups, which become empty.
     */
    Y_UNIT_TEST(ForgetTabletAtTableLevel) {
        NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

        auto aggregator = CreateNodeDatabaseMetricsAggregator(
            rootGroup,
            DATABASE_PATH,
            MONITORING_PROJECT_ID
        );

        const TInstant now = TInstant::Seconds(100);

        TFakeTablet leader1(1000, 0);
        TFakeTablet leader2(2000, 0);
        TFakeTablet follower(1000, 1);

        leader1.SetSimple(UNIQUE_ROWS, 1);
        leader2.SetSimple(UNIQUE_ROWS, 2);
        follower.SetSimple(UNIQUE_ROWS, 8);

        for (auto* tablet : {&leader1, &leader2, &follower}) {
            tablet->Report(aggregator, EDetailedMetricsLevel::Table, now);
        }

        aggregator->RecalculateAllCounters();

        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindRoleBucketCounters(rootGroup, 0), "SUM(UniqueRows)"),
            1 + 2
        );

        // TEST 1: The role bucket is recomputed from the surviving partitions
        aggregator->ForgetTablet(TABLE_ID, leader2.TabletId, leader2.FollowerId);
        aggregator->RecalculateAllCounters();

        DumpCounters("Table level counters after forgetting one leader", rootGroup);

        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindRoleBucketCounters(rootGroup, 0), "SUM(UniqueRows)"),
            1
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindRoleBucketCounters(rootGroup, 0), "MAX(UniqueRows)"),
            1
        );

        // TEST 2: The role bucket is removed as soon as its last partition is gone
        aggregator->ForgetTablet(TABLE_ID, leader1.TabletId, leader1.FollowerId);
        aggregator->RecalculateAllCounters();

        auto tableGroup = FindTableGroup(rootGroup);
        UNIT_ASSERT(tableGroup);
        UNIT_ASSERT(!tableGroup->FindSubgroup("role", "leader"));
        UNIT_ASSERT(tableGroup->FindSubgroup("role", "follower"));

        // TEST 3: The table is removed as soon as its last tablet is gone
        aggregator->ForgetTablet(TABLE_ID, follower.TabletId, follower.FollowerId);

        UNIT_ASSERT(!FindTableGroup(rootGroup));

        // TEST 4: Forgetting an unknown tablet is not an error
        aggregator->ForgetTablet(TABLE_ID, leader1.TabletId, leader1.FollowerId);
    }

    /**
     * Verify that forgetting a tablet of a partition level table removes its leaf
     * and all the counter groups, which become empty.
     */
    Y_UNIT_TEST(ForgetTabletAtPartitionLevel) {
        NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

        auto aggregator = CreateNodeDatabaseMetricsAggregator(
            rootGroup,
            DATABASE_PATH,
            MONITORING_PROJECT_ID
        );

        const TInstant now = TInstant::Seconds(100);

        TFakeTablet leader1(1000, 0);
        TFakeTablet follower1(1000, 1);
        TFakeTablet leader2(2000, 0);

        for (auto* tablet : {&leader1, &follower1, &leader2}) {
            tablet->SetSimple(UNIQUE_ROWS, 1);
            tablet->Report(aggregator, EDetailedMetricsLevel::Partition, now);
        }

        aggregator->RecalculateAllCounters();

        auto tableGroup = FindTableGroup(rootGroup);
        UNIT_ASSERT(tableGroup);

        auto perPartitionGroup = tableGroup->FindSubgroup("detailed_metrics", "per_partition");
        UNIT_ASSERT(perPartitionGroup);

        // TEST 1: Only the leaf of the forgotten tablet is removed, its sibling survives
        aggregator->ForgetTablet(TABLE_ID, follower1.TabletId, follower1.FollowerId);

        DumpCounters("Partition level counters after forgetting one follower", rootGroup);

        UNIT_ASSERT(!FindLeafCounters(rootGroup, follower1.TabletId, follower1.FollowerId));
        UNIT_ASSERT(FindLeafCounters(rootGroup, leader1.TabletId, leader1.FollowerId));
        UNIT_ASSERT(perPartitionGroup->FindSubgroup("tablet_id", ToString(leader1.TabletId)));

        // TEST 2: The tablet group is removed as soon as its last follower is gone
        aggregator->ForgetTablet(TABLE_ID, leader1.TabletId, leader1.FollowerId);

        UNIT_ASSERT(!perPartitionGroup->FindSubgroup("tablet_id", ToString(leader1.TabletId)));
        UNIT_ASSERT(tableGroup->FindSubgroup("detailed_metrics", "per_partition"));

        // TEST 3: The table is removed as soon as its last tablet is gone
        aggregator->ForgetTablet(TABLE_ID, leader2.TabletId, leader2.FollowerId);

        UNIT_ASSERT(!FindTableGroup(rootGroup));
    }

    /**
     * Verify that the tables, which do not collect detailed metrics, are ignored.
     */
    Y_UNIT_TEST(IgnoresTablesWithoutDetailedMetrics) {
        NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

        auto aggregator = CreateNodeDatabaseMetricsAggregator(
            rootGroup,
            DATABASE_PATH,
            MONITORING_PROJECT_ID
        );

        const TInstant now = TInstant::Seconds(100);

        TFakeTablet tablet(1000, 0);
        tablet.SetSimple(UNIQUE_ROWS, 1);

        tablet.Report(aggregator, EDetailedMetricsLevel::Unspecified, now);
        tablet.Report(aggregator, EDetailedMetricsLevel::Disabled, now);

        aggregator->RecalculateAllCounters();

        // Not a single counter group is created for such tables
        UNIT_ASSERT(!rootGroup->FindSubgroup("database", DATABASE_PATH));
    }
}
