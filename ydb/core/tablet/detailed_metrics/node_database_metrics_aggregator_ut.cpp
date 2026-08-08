#include "node_database_metrics_aggregator.h"
#include "ut_helpers.h"

#include <library/cpp/monlib/dynamic_counters/encode.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/array_size.h>
#include <util/string/builder.h>
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

enum EPercentileCounter : ui32 {
    HIST_CONSUMED_CPU = 0,
    TX_LATENCY = 1,
};

////////////////////////////////////////////////////////////////////////////////

/**
 * A single tablet, which reports the low level counters above.
 *
 * @note Reporting goes through MakeDiffForAggr()/RememberCurrentStateAsBaseline(),
 *       exactly like the Executor does it, so what the aggregator sees is what it
 *       sees in production: the simple counters are absolute, the cumulative ones
 *       are the delta since the previous report of THIS tablet, and the integral
 *       percentile counters are absolute.
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

    TFakeTablet& IncrementPercentile(EPercentileCounter counter, ui64 value) {
        AppCounters.Percentile()[counter].IncrementFor(value);
        return *this;
    }

    /**
     * Send everything accumulated since the previous report, the way the Executor does.
     */
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

        // An empty baseline (the very first report) makes the diff a plain copy
        auto appDiff = AppCounters.MakeDiffForAggr(AppBaseline);
        auto executorDiff = ExecutorCounters.MakeDiffForAggr(ExecutorBaseline);

        aggregator->AddCounters(
            table,
            TabletId,
            FollowerId,
            TABLET_TYPE,
            *executorDiff,
            *appDiff,
            now
        );

        AppCounters.RememberCurrentStateAsBaseline(AppBaseline);
        ExecutorCounters.RememberCurrentStateAsBaseline(ExecutorBaseline);
    }

    const ui64 TabletId;
    const ui32 FollowerId;

    TTabletCountersBase ExecutorCounters;
    TTabletCountersBase AppCounters;

    // The state as of the previous report, subtracted from the cumulative counters
    TTabletCountersBase ExecutorBaseline;
    TTabletCountersBase AppBaseline;
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

/**
 * @param[in] countersGroup The counter group to read the histogram from
 * @param[in] name The name of the histogram
 *
 * @return The total number of the observations in all the buckets of the histogram
 */
ui64 GetHistogramTotal(NMonitoring::TDynamicCounterPtr countersGroup, const TString& name) {
    UNIT_ASSERT_C(countersGroup, "no counter group for the histogram " << name);

    auto histogram = countersGroup->FindHistogram(name);
    UNIT_ASSERT_C(histogram, "no histogram " << name);

    auto snapshot = histogram->Snapshot();

    ui64 total = 0;
    for (ui32 i = 0; i < snapshot->Count(); ++i) {
        total += snapshot->Value(i);
    }

    return total;
}

/**
 * @param[in] countersGroup The counter group to read the histogram from
 * @param[in] name The name of the histogram
 *
 * @return The value of every bucket of the histogram, comma separated
 *
 * @note A string rather than a vector, so that a failed assertion prints
 *       both the expected and the actual buckets.
 */
TString GetHistogramBuckets(NMonitoring::TDynamicCounterPtr countersGroup, const TString& name) {
    UNIT_ASSERT_C(countersGroup, "no counter group for the histogram " << name);

    auto histogram = countersGroup->FindHistogram(name);
    UNIT_ASSERT_C(histogram, "no histogram " << name);

    auto snapshot = histogram->Snapshot();

    TStringBuilder buckets;
    for (ui32 i = 0; i < snapshot->Count(); ++i) {
        if (i > 0) {
            buckets << ",";
        }
        buckets << snapshot->Value(i);
    }

    return buckets;
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
            tablet->Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);
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
                tablet.Report(aggregator, TDetailedMetricsSettings::MetricsLevelPartition, now);
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
     * Verify that the cumulative counters, which the Executor sends as the delta since
     * the previous report, are ACCUMULATED rather than replaced, and that the derived
     * per second rate is recomputed from the delta of the latest report only.
     *
     * @note This is the whole point of the cumulative counters: a monotonically growing
     *       series. Replacing the accumulated value with the latest delta would look
     *       like a counter reset to the consumer.
     */
    Y_UNIT_TEST(CumulativeCountersAccumulateAcrossReports) {
        NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

        auto aggregator = CreateNodeDatabaseMetricsAggregator(
            rootGroup,
            DATABASE_PATH,
            MONITORING_PROJECT_ID
        );

        TFakeTablet leader1(1000, 0);
        TFakeTablet leader2(2000, 0);

        // TICK 1: both partitions report a non-zero delta
        TInstant now = TInstant::Seconds(100);

        leader1.AddCumulative(CONSUMED_CPU, 100);
        leader2.AddCumulative(CONSUMED_CPU, 200);

        for (auto* tablet : {&leader1, &leader2}) {
            tablet->Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);
        }

        aggregator->RecalculateAllCounters();

        auto leaderCounters = FindRoleBucketCounters(rootGroup, 0);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "ConsumedCPU"), 100 + 200);

        // TICK 2: 10 seconds later only the first partition does any work
        now += TDuration::Seconds(10);

        leader1.AddCumulative(CONSUMED_CPU, 300);

        for (auto* tablet : {&leader1, &leader2}) {
            tablet->Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);
        }

        aggregator->RecalculateAllCounters();

        DumpCounters("Table level counters after the second report", rootGroup);

        // The accumulated value keeps growing and never goes backwards
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "ConsumedCPU"), 100 + 200 + 300);

        // MAX() of a cumulative counter is the maximum per second rate over the bucket:
        // 300 over the 10 seconds since the previous report of that very tablet
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "MAX(ConsumedCPU)"), 300 / 10);

        // TICK 3: nobody does any work at all
        now += TDuration::Seconds(10);

        for (auto* tablet : {&leader1, &leader2}) {
            tablet->Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);
        }

        aggregator->RecalculateAllCounters();

        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "ConsumedCPU"), 100 + 200 + 300);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "MAX(ConsumedCPU)"), 0);
    }

    /**
     * Verify that both kinds of the percentile counters end up in the counter tree:
     * the ordinary ones, which the tablet fills itself, and the histogram aggregates
     * named HIST(x), which are filled here from the counter named x.
     */
    Y_UNIT_TEST(PercentileCountersAreAggregated) {
        NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

        auto aggregator = CreateNodeDatabaseMetricsAggregator(
            rootGroup,
            DATABASE_PATH,
            MONITORING_PROJECT_ID
        );

        const TInstant now = TInstant::Seconds(100);

        TFakeTablet leader1(1000, 0);
        TFakeTablet leader2(2000, 0);

        // 3 transactions on the first partition, 2 on the second one
        leader1
            .IncrementPercentile(TX_LATENCY, 5)
            .IncrementPercentile(TX_LATENCY, 50)
            .IncrementPercentile(TX_LATENCY, 500)
            .AddCumulative(CONSUMED_CPU, 100);

        leader2
            .IncrementPercentile(TX_LATENCY, 5)
            .IncrementPercentile(TX_LATENCY, 5)
            .AddCumulative(CONSUMED_CPU, 200);

        for (auto* tablet : {&leader1, &leader2}) {
            tablet->Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);
        }

        aggregator->RecalculateAllCounters();

        DumpCounters("Table level counters with the percentile counters", rootGroup);

        auto leaderCounters = FindRoleBucketCounters(rootGroup, 0);
        UNIT_ASSERT(leaderCounters);

        // The ordinary percentile counter holds the observations of both partitions
        UNIT_ASSERT_VALUES_EQUAL(GetHistogramTotal(leaderCounters, "TxLatency"), 3 + 2);

        // The histogram aggregate holds one observation per partition, taken from
        // the "ConsumedCPU" cumulative counter. The tablets do NOT fill it themselves,
        // so an empty histogram here would mean the aggregate is never fed.
        UNIT_ASSERT_VALUES_EQUAL(GetHistogramTotal(leaderCounters, "HIST(ConsumedCPU)"), 2);
    }

    /**
     * Verify that forgetting a tablet drops its observations from the percentile
     * counters of the role bucket, while the accumulated cumulative counters keep
     * the work the tablet had already done.
     *
     * @note The two kinds of the percentile counters are dropped by two different
     *       mechanisms: an ordinary one is subtracted bucket by bucket right away,
     *       while a HIST(x) aggregate is rebuilt from scratch on the next recalculation.
     */
    Y_UNIT_TEST(ForgetTabletDropsPercentileObservations) {
        NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

        auto aggregator = CreateNodeDatabaseMetricsAggregator(
            rootGroup,
            DATABASE_PATH,
            MONITORING_PROJECT_ID
        );

        const TInstant now = TInstant::Seconds(100);

        TFakeTablet leader1(1000, 0);
        TFakeTablet leader2(2000, 0);

        // The observations of the two partitions land in DIFFERENT buckets, so that
        // a misaligned subtraction is not mistaken for a correct one
        leader1
            .IncrementPercentile(TX_LATENCY, 5)
            .IncrementPercentile(TX_LATENCY, 5)
            .AddCumulative(CONSUMED_CPU, 100);

        leader2
            .IncrementPercentile(TX_LATENCY, 500)
            .IncrementPercentile(TX_LATENCY, 500)
            .IncrementPercentile(TX_LATENCY, 500)
            .AddCumulative(CONSUMED_CPU, 200);

        for (auto* tablet : {&leader1, &leader2}) {
            tablet->Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);
        }

        aggregator->RecalculateAllCounters();

        auto leaderCounters = FindRoleBucketCounters(rootGroup, 0);
        UNIT_ASSERT(leaderCounters);

        // The ranges {0, 10, 100} become the 4 buckets <=0, (0;10], (10;100], (100;inf],
        // so the 2 observations of the first partition land in the second bucket and
        // the 3 observations of the second one in the last
        UNIT_ASSERT_VALUES_EQUAL(GetHistogramBuckets(leaderCounters, "TxLatency"), "0,2,0,3");
        UNIT_ASSERT_VALUES_EQUAL(GetHistogramTotal(leaderCounters, "HIST(ConsumedCPU)"), 2);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "ConsumedCPU"), 100 + 200);

        // The second partition is gone
        aggregator->ForgetTablet(TABLE_ID, leader2.TabletId, leader2.FollowerId);
        aggregator->RecalculateAllCounters();

        DumpCounters("Table level counters after forgetting the second partition", rootGroup);

        // Only the observations of the forgotten partition are subtracted, and only
        // from its own bucket
        UNIT_ASSERT_VALUES_EQUAL(GetHistogramBuckets(leaderCounters, "TxLatency"), "0,2,0,0");

        // The histogram aggregate is rebuilt from the surviving partitions only
        UNIT_ASSERT_VALUES_EQUAL(GetHistogramTotal(leaderCounters, "HIST(ConsumedCPU)"), 1);

        // The accumulated cumulative counter is NOT reduced: the CPU the forgotten
        // partition had burnt has still been burnt, and the series must not go backwards
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "ConsumedCPU"), 100 + 200);

        // The last partition is gone too, so the whole table leaves the tree
        aggregator->ForgetTablet(TABLE_ID, leader1.TabletId, leader1.FollowerId);

        UNIT_ASSERT(!FindTableGroup(rootGroup));
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
            tablet->Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);
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
            tablet->Report(aggregator, TDetailedMetricsSettings::MetricsLevelPartition, now);
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

        tablet.Report(aggregator, TDetailedMetricsSettings::MetricsLevelUnspecified, now);
        tablet.Report(aggregator, TDetailedMetricsSettings::MetricsLevelDisabled, now);

        aggregator->RecalculateAllCounters();

        // Not a single counter group is created for such tables
        UNIT_ASSERT(!rootGroup->FindSubgroup("database", DATABASE_PATH));
    }

    /**
     * Verify that the "table" label holds the path of the table relative to
     * the database, and that only a whole path component is ever stripped.
     */
    Y_UNIT_TEST(TablePathIsRelativeToTheDatabase) {
        NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

        // NOTE: /Root/db1 is a PREFIX of /Root/db10, but not a parent of it
        auto aggregator = CreateNodeDatabaseMetricsAggregator(
            rootGroup,
            "/Root/db1",
            MONITORING_PROJECT_ID
        );

        const TInstant now = TInstant::Seconds(100);

        struct TCase {
            TPathId TableId;
            TString TablePath;
            TString ExpectedLabel;
        };

        const TVector<TCase> cases = {
            // Within the database: the database path and the separator are stripped
            {TPathId(1, 1), "/Root/db1/dir/table", "dir/table"},
            {TPathId(1, 2), "/Root/db1/table",     "table"},

            // NOT within the database: the path is reported as is, so that the odd
            // looking label is noticed instead of the counters being silently misplaced
            {TPathId(1, 3), "/Root/db10/table",    "/Root/db10/table"},
            {TPathId(1, 4), "/Root/other/table",   "/Root/other/table"},
        };

        ui64 tabletId = 1000;

        for (const auto& testCase : cases) {
            TDetailedMetricsTableInfo table;
            table.TableId = testCase.TableId;
            table.TablePath = testCase.TablePath;
            table.SchemaVersion = 1;
            table.MetricsLevel = TDetailedMetricsSettings::MetricsLevelTable;

            TFakeTablet tablet(tabletId++, 0);
            tablet.SetSimple(UNIQUE_ROWS, 1);

            aggregator->AddCounters(
                table,
                tablet.TabletId,
                tablet.FollowerId,
                TABLET_TYPE,
                tablet.ExecutorCounters,
                tablet.AppCounters,
                now
            );
        }

        aggregator->RecalculateAllCounters();

        DumpCounters("Table level counters of several tables", rootGroup);

        auto databaseGroup = rootGroup->FindSubgroup("database", "/Root/db1");
        UNIT_ASSERT(databaseGroup);

        auto projectGroup = databaseGroup->FindSubgroup("monitoring_project_id", MONITORING_PROJECT_ID);
        UNIT_ASSERT(projectGroup);

        for (const auto& testCase : cases) {
            UNIT_ASSERT_C(
                projectGroup->FindSubgroup("table", testCase.ExpectedLabel),
                "no table group " << testCase.ExpectedLabel << " for " << testCase.TablePath
            );
        }
    }
}
