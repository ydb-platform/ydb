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

const TString TABLE_PATH = "/Root/db/dir/table";
const TString RELATIVE_TABLE_PATH = "dir/table";

const TPathId TABLE_ID(72057594046644480ull, 42);

// The same schemeshard owner as TABLE_ID, but a different local id, reporting under
// the very same TABLE_PATH: models a table dropped and recreated at the same path,
// or an ESchemeOpMoveTable rename that moves the old table away and a new one is
// created at the vacated path.
const TPathId RECREATED_TABLE_ID(72057594046644480ull, 44);

// Another table of the very same database
const TString OTHER_TABLE_PATH = "/Root/db/dir/other_table";
const TString OTHER_RELATIVE_TABLE_PATH = "dir/other_table";

const TPathId OTHER_TABLE_ID(72057594046644480ull, 43);

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
        TInstant now,
        const TPathId& tableId = TABLE_ID,
        const TString& tablePath = TABLE_PATH
    ) {
        TDetailedMetricsTableInfo table;
        table.TableId = tableId;
        table.TablePath = tablePath;
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
NMonitoring::TDynamicCounterPtr FindTableGroup(
    NMonitoring::TDynamicCounterPtr rootGroup,
    const TString& relativeTablePath = RELATIVE_TABLE_PATH
) {
    auto databaseGroup = rootGroup->FindSubgroup("database", DATABASE_PATH);
    if (!databaseGroup) {
        return nullptr;
    }

    return databaseGroup->FindSubgroup("table", relativeTablePath);
}

/**
 * @param[in] bucketGroup The counter group of a table bucket or of a leaf
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
 *
 * @return The application counters of the table bucket (or nullptr if there is none)
 *
 * @note At the table level the collapsed counters live directly in the table group:
 *       the role is the caller's partition of the tree, not a label within it.
 */
NMonitoring::TDynamicCounterPtr FindTableBucketCounters(
    NMonitoring::TDynamicCounterPtr rootGroup,
    const TString& relativeTablePath = RELATIVE_TABLE_PATH
) {
    return FindAppCountersGroup(FindTableGroup(rootGroup, relativeTablePath));
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
    ui32 followerId,
    const TString& relativeTablePath = RELATIVE_TABLE_PATH
) {
    auto tableGroup = FindTableGroup(rootGroup, relativeTablePath);
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

/**
 * The two role subtrees of the private "ydb_detailed_raw" group, built the way the two
 * Tablet Counters Aggregator actors of a node build them: one aggregator per role,
 * each owning everything below its own role= node, both hanging off one shared root.
 */
struct TRoleTrees {
    NMonitoring::TDynamicCounterPtr Root = MakeIntrusive<NMonitoring::TDynamicCounters>();

    NMonitoring::TDynamicCounterPtr LeaderRoot = Root->GetSubgroup("role", "leader");
    NMonitoring::TDynamicCounterPtr FollowerRoot = Root->GetSubgroup("role", "follower");

    TNodeDatabaseMetricsAggregatorPtr Leaders = CreateNodeDatabaseMetricsAggregator(
        LeaderRoot,
        DATABASE_PATH,
        false /* isFollowerRole */
    );

    TNodeDatabaseMetricsAggregatorPtr Followers = CreateNodeDatabaseMetricsAggregator(
        FollowerRoot,
        DATABASE_PATH,
        true /* isFollowerRole */
    );

    void RecalculateAllCounters() {
        Leaders->RecalculateAllCounters();
        Followers->RecalculateAllCounters();
    }
};

} // namespace <anonymous>

////////////////////////////////////////////////////////////////////////////////

/**
 * Unit tests for the node database metrics aggregator (TNodeDatabaseMetricsAggregator).
 */
Y_UNIT_TEST_SUITE(TNodeDatabaseMetricsAggregatorTest) {
    /**
     * Verify that at the table level all same-node partitions of the table are collapsed
     * into a single bucket per role tree, and no per-partition counters are created.
     */
    Y_UNIT_TEST(TableLevelCollapsesPartitions) {
        TRoleTrees trees;

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

        for (auto* tablet : {&leader1, &leader2, &leader3}) {
            tablet->Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelTable, now);
        }

        for (auto* tablet : {&follower1, &follower2}) {
            tablet->Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelTable, now);
        }

        trees.RecalculateAllCounters();

        DumpCounters("Table level counters", trees.Root);

        // The bucket of the leader tree holds the 3 leader partitions
        auto leaderCounters = FindTableBucketCounters(trees.LeaderRoot);
        UNIT_ASSERT(leaderCounters);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "SUM(UniqueRows)"), 1 + 2 + 4);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "MAX(UniqueRows)"), 4);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "SUM(UniqueBytes)"), 10 + 20 + 40);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "MAX(UniqueBytes)"), 40);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "ConsumedCPU"), 100 + 200 + 400);

        // The bucket of the follower tree holds the 2 followers
        auto followerCounters = FindTableBucketCounters(trees.FollowerRoot);
        UNIT_ASSERT(followerCounters);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(followerCounters, "SUM(UniqueRows)"), 8 + 16);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(followerCounters, "MAX(UniqueRows)"), 16);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(followerCounters, "ConsumedCPU"), 800 + 1600);

        // No per-partition counters at the table level, in either tree
        for (auto roleRoot : {trees.LeaderRoot, trees.FollowerRoot}) {
            auto tableGroup = FindTableGroup(roleRoot);
            UNIT_ASSERT(tableGroup);
            UNIT_ASSERT(!tableGroup->FindSubgroup("detailed_metrics", "per_partition"));
        }
    }

    /**
     * Verify that at the partition level every (tablet_id, follower_id) leaf is kept
     * verbatim and no on-node rollup of any kind is created.
     */
    Y_UNIT_TEST(PartitionLevelKeepsLeaves) {
        TRoleTrees trees;

        const TInstant now = TInstant::Seconds(100);

        // 2 partitions of the same table, a leader and 2 followers each. The leader goes
        // to the leader tree and both followers to the follower one, exactly the way
        // the two Tablet Counters Aggregator actors of a node are fed.
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
                tablet.Report(
                    followerId == 0 ? trees.Leaders : trees.Followers,
                    TDetailedMetricsSettings::MetricsLevelPartition,
                    now
                );
            }
        }

        trees.RecalculateAllCounters();

        DumpCounters("Partition level counters", trees.Root);

        // Every leaf holds exactly what its tablet has reported, in the tree of its role
        for (const auto& [tablet, expectedValue] : expectedValues) {
            const auto& [tabletId, followerId] = tablet;

            auto roleRoot = followerId == 0 ? trees.LeaderRoot : trees.FollowerRoot;

            auto leafCounters = FindLeafCounters(roleRoot, tabletId, followerId);
            UNIT_ASSERT_C(leafCounters, "no leaf for " << tabletId << ":" << followerId);

            UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leafCounters, "SUM(UniqueRows)"), expectedValue);
            UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leafCounters, "MAX(UniqueRows)"), expectedValue);
            UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leafCounters, "ConsumedCPU"), expectedValue * 100);
        }

        // A leaf of one role never lands in the tree of the other one
        UNIT_ASSERT(!FindLeafCounters(trees.LeaderRoot, 1000, 1));
        UNIT_ASSERT(!FindLeafCounters(trees.FollowerRoot, 1000, 0));

        // No table bucket and no on-node rollup at the partition level, in either tree
        for (auto roleRoot : {trees.LeaderRoot, trees.FollowerRoot}) {
            auto tableGroup = FindTableGroup(roleRoot);
            UNIT_ASSERT(tableGroup);
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
            false /* isFollowerRole */
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

        auto leaderCounters = FindTableBucketCounters(rootGroup);
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
            false /* isFollowerRole */
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

        auto leaderCounters = FindTableBucketCounters(rootGroup);
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
     * counters of the table bucket, while the accumulated cumulative counters keep
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
            false /* isFollowerRole */
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

        auto leaderCounters = FindTableBucketCounters(rootGroup);
        UNIT_ASSERT(leaderCounters);

        // The ranges {0, 10, 100} become the 4 buckets <=0, (0;10], (10;100], (100;inf],
        // so the 2 observations of the first partition land in the second bucket and
        // the 3 observations of the second one in the last
        UNIT_ASSERT_VALUES_EQUAL(GetHistogramBuckets(leaderCounters, "TxLatency"), "0,2,0,3");
        UNIT_ASSERT_VALUES_EQUAL(GetHistogramTotal(leaderCounters, "HIST(ConsumedCPU)"), 2);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "ConsumedCPU"), 100 + 200);

        // The second partition is gone
        aggregator->ForgetTablet(leader2.TabletId, leader2.FollowerId);
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
        aggregator->ForgetTablet(leader1.TabletId, leader1.FollowerId);

        UNIT_ASSERT(!FindTableGroup(rootGroup));
    }

    /**
     * Verify that forgetting a tablet of a table level table drops its contribution
     * from the table bucket and removes the counter groups, which become empty.
     *
     * @note The two role trees are torn down independently. That is the whole reason
     *       the role is the caller's partition of the tree rather than a label within
     *       it: an aggregator only ever removes groups it exclusively owns, so emptying
     *       one role tree can never detach the live counters of the other one from
     *       the shared root.
     */
    Y_UNIT_TEST(ForgetTabletAtTableLevel) {
        TRoleTrees trees;

        const TInstant now = TInstant::Seconds(100);

        TFakeTablet leader1(1000, 0);
        TFakeTablet leader2(2000, 0);
        TFakeTablet follower(1000, 1);

        leader1.SetSimple(UNIQUE_ROWS, 1);
        leader2.SetSimple(UNIQUE_ROWS, 2);
        follower.SetSimple(UNIQUE_ROWS, 8);

        for (auto* tablet : {&leader1, &leader2}) {
            tablet->Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelTable, now);
        }
        follower.Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelTable, now);

        trees.RecalculateAllCounters();

        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindTableBucketCounters(trees.LeaderRoot), "SUM(UniqueRows)"),
            1 + 2
        );

        // TEST 1: The table bucket is recomputed from the surviving partitions
        trees.Leaders->ForgetTablet(leader2.TabletId, leader2.FollowerId);
        trees.RecalculateAllCounters();

        DumpCounters("Table level counters after forgetting one leader", trees.Root);

        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindTableBucketCounters(trees.LeaderRoot), "SUM(UniqueRows)"),
            1
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindTableBucketCounters(trees.LeaderRoot), "MAX(UniqueRows)"),
            1
        );

        // TEST 2: The table leaves the leader tree as soon as its last leader is gone,
        //         and the follower tree keeps its own counters, still reachable from
        //         the shared root
        trees.Leaders->ForgetTablet(leader1.TabletId, leader1.FollowerId);
        trees.RecalculateAllCounters();

        DumpCounters("Counters after forgetting the last leader", trees.Root);

        UNIT_ASSERT(!FindTableGroup(trees.LeaderRoot));
        UNIT_ASSERT(!trees.Root->FindSubgroup("role", "leader")->FindSubgroup("database", DATABASE_PATH));

        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(
                FindTableBucketCounters(trees.Root->FindSubgroup("role", "follower")),
                "SUM(UniqueRows)"
            ),
            8
        );

        // TEST 3: The table is removed as soon as its last tablet is gone
        trees.Followers->ForgetTablet(follower.TabletId, follower.FollowerId);

        UNIT_ASSERT(!FindTableGroup(trees.FollowerRoot));

        // TEST 4: Forgetting an unknown tablet is not an error
        trees.Leaders->ForgetTablet(leader1.TabletId, leader1.FollowerId);
    }

    /**
     * Verify that forgetting a tablet of a partition level table removes its leaf
     * and all the counter groups, which become empty.
     */
    Y_UNIT_TEST(ForgetTabletAtPartitionLevel) {
        TRoleTrees trees;

        const TInstant now = TInstant::Seconds(100);

        // Two followers of the same partition plus one of another partition, so that
        // the tablet group is only removed once its LAST follower is gone
        TFakeTablet follower1(1000, 1);
        TFakeTablet follower2(1000, 2);
        TFakeTablet follower3(2000, 1);

        for (auto* tablet : {&follower1, &follower2, &follower3}) {
            tablet->SetSimple(UNIQUE_ROWS, 1);
            tablet->Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelPartition, now);
        }

        trees.RecalculateAllCounters();

        auto rootGroup = trees.FollowerRoot;

        auto tableGroup = FindTableGroup(rootGroup);
        UNIT_ASSERT(tableGroup);

        auto perPartitionGroup = tableGroup->FindSubgroup("detailed_metrics", "per_partition");
        UNIT_ASSERT(perPartitionGroup);

        // TEST 1: Only the leaf of the forgotten tablet is removed, its sibling survives
        trees.Followers->ForgetTablet(follower1.TabletId, follower1.FollowerId);

        DumpCounters("Partition level counters after forgetting one follower", trees.Root);

        UNIT_ASSERT(!FindLeafCounters(rootGroup, follower1.TabletId, follower1.FollowerId));
        UNIT_ASSERT(FindLeafCounters(rootGroup, follower2.TabletId, follower2.FollowerId));
        UNIT_ASSERT(perPartitionGroup->FindSubgroup("tablet_id", ToString(follower2.TabletId)));

        // TEST 2: The tablet group is removed as soon as its last follower is gone
        trees.Followers->ForgetTablet(follower2.TabletId, follower2.FollowerId);

        UNIT_ASSERT(!perPartitionGroup->FindSubgroup("tablet_id", ToString(follower2.TabletId)));
        UNIT_ASSERT(tableGroup->FindSubgroup("detailed_metrics", "per_partition"));

        // TEST 3: The table is removed as soon as its last tablet is gone
        trees.Followers->ForgetTablet(follower3.TabletId, follower3.FollowerId);

        UNIT_ASSERT(!FindTableGroup(rootGroup));
    }

    /**
     * Verify that the database counter groups do not pile up empty: the database node
     * is removed together with the last table of the database and recreated on demand.
     */
    Y_UNIT_TEST(DatabaseGroupIsRemovedWithTheLastTable) {
        NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

        auto aggregator = CreateNodeDatabaseMetricsAggregator(
            rootGroup,
            DATABASE_PATH,
            false /* isFollowerRole */
        );

        const TInstant now = TInstant::Seconds(100);

        TFakeTablet leader(1000, 0);
        leader.SetSimple(UNIQUE_ROWS, 1);
        leader.Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);

        auto databaseGroup = rootGroup->FindSubgroup("database", DATABASE_PATH);
        UNIT_ASSERT(databaseGroup);
        UNIT_ASSERT(databaseGroup->FindSubgroup("table", RELATIVE_TABLE_PATH));

        // TEST 1: Nothing of the database is left behind once its last table is gone
        aggregator->ForgetTablet(leader.TabletId, leader.FollowerId);

        DumpCounters("Counters after forgetting the last table", rootGroup);

        UNIT_ASSERT(!rootGroup->FindSubgroup("database", DATABASE_PATH));
        UNIT_ASSERT(!databaseGroup->FindSubgroup("table", RELATIVE_TABLE_PATH));

        // TEST 2: The database node is recreated by the next table
        leader.SetSimple(UNIQUE_ROWS, 2);
        leader.Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);
        aggregator->RecalculateAllCounters();

        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindTableBucketCounters(rootGroup), "SUM(UniqueRows)"),
            2
        );
    }

    /**
     * Verify that the tables, which do not collect detailed metrics, are ignored.
     */
    Y_UNIT_TEST(IgnoresTablesWithoutDetailedMetrics) {
        NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

        auto aggregator = CreateNodeDatabaseMetricsAggregator(
            rootGroup,
            DATABASE_PATH,
            false /* isFollowerRole */
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
     * Verify that splitting the counters across two role trees prevents the
     * double-counting of leader-only metrics at the table level.
     *
     * A follower's executor counters are real and equal its leader's (the rows
     * are physically the same), which is why the corresponding public metric
     * (table.datashard.row_count) must be leader-only. If the roles were
     * aggregated into one bucket, the collapsed metric would read the follower's
     * value twice: once from the leader (100) and once from the follower (100),
     * yielding 200 against the true 100. This is unrecoverable downstream, because
     * the raw DataShard counters carry no leader-only marking, and the filter
     * runs on the processor after aggregation (decision S4).
     *
     * The role split ensures that the true 100 is preserved in the separate trees
     * and never summed with itself. A legitimate multi-role metric (one that
     * counts all copies of the work, not just the leaders) is undamaged: the
     * consumer adds the two streams and gets the correct total.
     *
     * @note Two independent mechanisms defend this, and this test pins the second.
     *       Feeding both roles to ONE instance, which is what a merged bucket would
     *       mean in production, never reaches the arithmetic at all: CheckSingleRole()
     *       aborts first. What this test guards is the shape — that the two trees stay
     *       separate and each reads the truth — so that a change that aliases them onto
     *       a shared counter group, which the invariant cannot see, still fails here.
     *
     * Reference: exchange/review-response-issue1.md (Tree B vs Tree C).
     */
    Y_UNIT_TEST(RoleSplitKeepsLeaderOnlyMetricsUninflated) {
        TRoleTrees trees;

        const TInstant now = TInstant::Seconds(100);

        // One partition: the leader reports a row count of 100, which is the truth.
        // The follower's executor counters hold the same value (the rows are the same),
        // and that is precisely why the corresponding public metric is leader-only.
        TFakeTablet leader(1000, 0);
        leader.SetSimple(UNIQUE_ROWS, 100).AddCumulative(CONSUMED_CPU, 7);

        TFakeTablet follower(1000, 1);
        follower.SetSimple(UNIQUE_ROWS, 100).AddCumulative(CONSUMED_CPU, 3);

        leader.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelTable, now);
        follower.Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelTable, now);

        trees.RecalculateAllCounters();

        DumpCounters("Role split and leader-only metrics", trees.Root);

        // The leader tree's table bucket reads 100: the true row count
        auto leaderCounters = FindTableBucketCounters(trees.LeaderRoot);
        UNIT_ASSERT(leaderCounters);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "SUM(UniqueRows)"), 100);

        // The follower tree's table bucket also reads 100: separate aggregation
        auto followerCounters = FindTableBucketCounters(trees.FollowerRoot);
        UNIT_ASSERT(followerCounters);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(followerCounters, "SUM(UniqueRows)"), 100);

        // Cumulative counters (non-leader-only) are undamaged by the split: they are
        // legitimately counted in both roles, and the consumer adds them: 7 + 3 = 10
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "ConsumedCPU"), 7);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(followerCounters, "ConsumedCPU"), 3);
    }

    /**
     * Verify that at the partition level, leaves of both roles are kept in their
     * respective trees and never merged, even on the same node.
     *
     * This is deliberate on the node: a leaf is single-owner and passes through
     * verbatim, and there is no on-node rollup at partition granularity. However,
     * the leader-only filter for leaf series is an open decision owned by steps 09/13,
     * not by this class. The rectified plans state the per-metric role selection only
     * at the table level.
     *
     * Consequence: until that decision is made, a consumer summing published leaves
     * across roles will double-count the eight leader-only metrics (the raw DataShard
     * subset), exactly as in the role-split test above. The difference is that here
     * the arithmetic is unavoidable on the consumer side, because the leaves are the
     * published units and have no aggregation on the node to apply the filter to.
     *
     * This test documents the boundary rather than asserting a behaviour we have not
     * chosen.
     */
    Y_UNIT_TEST(PartitionLeavesCarryBothRolesByDesign) {
        TRoleTrees trees;

        const TInstant now = TInstant::Seconds(100);

        // The leader holds one value, the follower another, so we can verify they
        // land in their own trees and not in the other's
        TFakeTablet leader(1000, 0);
        leader.SetSimple(UNIQUE_ROWS, 42);

        TFakeTablet follower(1000, 1);
        follower.SetSimple(UNIQUE_ROWS, 99);

        leader.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, now);
        follower.Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelPartition, now);

        trees.RecalculateAllCounters();

        DumpCounters("Partition level leaves, both roles", trees.Root);

        // The leader's leaf is in the leader tree only, carrying the leader's value
        auto leaderLeaf = FindLeafCounters(trees.LeaderRoot, 1000, 0);
        UNIT_ASSERT(leaderLeaf);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderLeaf, "SUM(UniqueRows)"), 42);

        // The follower's leaf is in the follower tree only, carrying the follower's value
        auto followerLeaf = FindLeafCounters(trees.FollowerRoot, 1000, 1);
        UNIT_ASSERT(followerLeaf);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(followerLeaf, "SUM(UniqueRows)"), 99);

        // Cross-tree lookup finds nothing: the roles are split at the tree root
        UNIT_ASSERT(!FindLeafCounters(trees.LeaderRoot, 1000, 1));
        UNIT_ASSERT(!FindLeafCounters(trees.FollowerRoot, 1000, 0));
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
            false /* isFollowerRole */
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

        for (const auto& testCase : cases) {
            UNIT_ASSERT_C(
                databaseGroup->FindSubgroup("table", testCase.ExpectedLabel),
                "no table group " << testCase.ExpectedLabel << " for " << testCase.TablePath
            );
        }
    }

    /**
     * Verify that a tablet, which is re-reported under another table, is MOVED rather than
     * copied: its contribution to the previous table is dropped together with the counter
     * groups, which become empty.
     *
     * @note The reverse map holds one table per tablet, because the forget event carries no
     *       table identity. Overwriting the entry without cleaning up would leave the old
     *       table's contribution in the tree with nothing left able to reach it: neither
     *       ForgetTablet, which now routes to the new table, nor the next report.
     */
    Y_UNIT_TEST(TabletReportedUnderAnotherTableIsMoved) {
        const TInstant now = TInstant::Seconds(100);

        // TEST 1: The table level, where the tablet contributes to a shared bucket
        {
            NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

            auto aggregator = CreateNodeDatabaseMetricsAggregator(
                rootGroup,
                DATABASE_PATH,
                false /* isFollowerRole */
            );

            TFakeTablet leader(1000, 0);

            leader.SetSimple(UNIQUE_ROWS, 5);
            leader.Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);
            aggregator->RecalculateAllCounters();

            UNIT_ASSERT_VALUES_EQUAL(
                GetCounterValue(FindTableBucketCounters(rootGroup), "SUM(UniqueRows)"),
                5
            );

            // The very same tablet now reports another table of the same database
            leader.SetSimple(UNIQUE_ROWS, 7);
            leader.Report(
                aggregator,
                TDetailedMetricsSettings::MetricsLevelTable,
                now,
                OTHER_TABLE_ID,
                OTHER_TABLE_PATH
            );
            aggregator->RecalculateAllCounters();

            DumpCounters("Counters after the tablet moved to another table", rootGroup);

            // Nothing of the old table is left behind, and the new one holds the counters
            UNIT_ASSERT(!FindTableGroup(rootGroup));
            UNIT_ASSERT_VALUES_EQUAL(
                GetCounterValue(
                    FindTableBucketCounters(rootGroup, OTHER_RELATIVE_TABLE_PATH),
                    "SUM(UniqueRows)"
                ),
                7
            );

            // The reverse map points at the new table, so forgetting the tablet empties
            // the whole tree rather than the table it no longer belongs to
            aggregator->ForgetTablet(leader.TabletId, leader.FollowerId);

            UNIT_ASSERT(!rootGroup->FindSubgroup("database", DATABASE_PATH));
        }

        // TEST 2: The partition level, where the tablet owns a leaf of its own
        {
            NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

            auto aggregator = CreateNodeDatabaseMetricsAggregator(
                rootGroup,
                DATABASE_PATH,
                false /* isFollowerRole */
            );

            TFakeTablet leader(1000, 0);

            leader.SetSimple(UNIQUE_ROWS, 5);
            leader.Report(aggregator, TDetailedMetricsSettings::MetricsLevelPartition, now);
            aggregator->RecalculateAllCounters();

            UNIT_ASSERT(FindLeafCounters(rootGroup, leader.TabletId, leader.FollowerId));

            leader.SetSimple(UNIQUE_ROWS, 7);
            leader.Report(
                aggregator,
                TDetailedMetricsSettings::MetricsLevelPartition,
                now,
                OTHER_TABLE_ID,
                OTHER_TABLE_PATH
            );
            aggregator->RecalculateAllCounters();

            DumpCounters("Leaves after the tablet moved to another table", rootGroup);

            // The old leaf and its whole table are gone, the new leaf holds the counters
            UNIT_ASSERT(!FindTableGroup(rootGroup));

            auto leafCounters = FindLeafCounters(
                rootGroup,
                leader.TabletId,
                leader.FollowerId,
                OTHER_RELATIVE_TABLE_PATH
            );
            UNIT_ASSERT(leafCounters);
            UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leafCounters, "SUM(UniqueRows)"), 7);

            aggregator->ForgetTablet(leader.TabletId, leader.FollowerId);

            UNIT_ASSERT(!rootGroup->FindSubgroup("database", DATABASE_PATH));
        }
    }

    /**
     * Verify that two different TPathIds, which report the very same table path, share
     * ONE counter group and ONE aggregate rather than fragmenting the tree between them.
     *
     * @note In production this happens whenever a table is dropped and a new one is
     *       created at the same path, or when an ESchemeOpMoveTable rename moves the
     *       old table away and a new table is created at the vacated old path: the
     *       schemeshard hands out a fresh PathId, but the "table" label of the counter
     *       tree is keyed by path, not by PathId. Before this fix, the state map was
     *       keyed by PathId, so the two reports created two TTableEntry-s aliasing one
     *       GetSubgroup() result: their TAggregatedTabletCounters overwrote each other's
     *       sums, and forgetting the last tablet of the OLDER entry unconditionally
     *       removed the shared group, detaching the counters the SURVIVING entry still
     *       wrote into.
     */
    Y_UNIT_TEST(SamePathUnderDifferentPathIdsSharesOneTable) {
        const TInstant now = TInstant::Seconds(100);

        // TEST 1: The table level, where both tablets contribute to a shared bucket
        {
            NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

            auto aggregator = CreateNodeDatabaseMetricsAggregator(
                rootGroup,
                DATABASE_PATH,
                false /* isFollowerRole */
            );

            TFakeTablet oldTablet(1000, 0);
            TFakeTablet newTablet(1001, 0);

            oldTablet.SetSimple(UNIQUE_ROWS, 5);
            oldTablet.Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now, TABLE_ID, TABLE_PATH);

            newTablet.SetSimple(UNIQUE_ROWS, 7);
            newTablet.Report(
                aggregator,
                TDetailedMetricsSettings::MetricsLevelTable,
                now,
                RECREATED_TABLE_ID,
                TABLE_PATH
            );

            aggregator->RecalculateAllCounters();

            DumpCounters("Table level counters of two PathIds at one path", rootGroup);

            // Exactly one table= group holds the sum of both PathIds' contributions:
            // under the bug this would read 5 or 7 (whichever entry recalculated last),
            // never their sum
            UNIT_ASSERT(FindTableGroup(rootGroup));
            UNIT_ASSERT_VALUES_EQUAL(
                GetCounterValue(FindTableBucketCounters(rootGroup), "SUM(UniqueRows)"),
                5 + 7
            );

            // The tablet of the OLDER PathId is forgotten: the group must stay reachable,
            // because the surviving PathId's entry still owns it
            aggregator->ForgetTablet(oldTablet.TabletId, oldTablet.FollowerId);
            aggregator->RecalculateAllCounters();

            DumpCounters("Table level counters after forgetting the older PathId's tablet", rootGroup);

            UNIT_ASSERT(FindTableGroup(rootGroup));
            UNIT_ASSERT_VALUES_EQUAL(
                GetCounterValue(FindTableBucketCounters(rootGroup), "SUM(UniqueRows)"),
                7
            );
        }

        // TEST 2: The partition level, where both tablets own a leaf of their own
        {
            NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

            auto aggregator = CreateNodeDatabaseMetricsAggregator(
                rootGroup,
                DATABASE_PATH,
                false /* isFollowerRole */
            );

            TFakeTablet oldTablet(1000, 0);
            TFakeTablet newTablet(1001, 0);

            oldTablet.SetSimple(UNIQUE_ROWS, 5);
            oldTablet.Report(
                aggregator,
                TDetailedMetricsSettings::MetricsLevelPartition,
                now,
                TABLE_ID,
                TABLE_PATH
            );

            newTablet.SetSimple(UNIQUE_ROWS, 7);
            newTablet.Report(
                aggregator,
                TDetailedMetricsSettings::MetricsLevelPartition,
                now,
                RECREATED_TABLE_ID,
                TABLE_PATH
            );

            aggregator->RecalculateAllCounters();

            DumpCounters("Partition level leaves of two PathIds at one path", rootGroup);

            // Both leaves live under the very same table= group
            UNIT_ASSERT(FindTableGroup(rootGroup));

            auto oldLeaf = FindLeafCounters(rootGroup, oldTablet.TabletId, oldTablet.FollowerId);
            UNIT_ASSERT(oldLeaf);
            UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(oldLeaf, "SUM(UniqueRows)"), 5);

            auto newLeaf = FindLeafCounters(rootGroup, newTablet.TabletId, newTablet.FollowerId);
            UNIT_ASSERT(newLeaf);
            UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(newLeaf, "SUM(UniqueRows)"), 7);

            // Forgetting the OLDER PathId's tablet must not detach the surviving leaf
            aggregator->ForgetTablet(oldTablet.TabletId, oldTablet.FollowerId);

            DumpCounters("Partition level leaves after forgetting the older PathId's tablet", rootGroup);

            UNIT_ASSERT(!FindLeafCounters(rootGroup, oldTablet.TabletId, oldTablet.FollowerId));

            auto survivingLeaf = FindLeafCounters(rootGroup, newTablet.TabletId, newTablet.FollowerId);
            UNIT_ASSERT(survivingLeaf);
            UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(survivingLeaf, "SUM(UniqueRows)"), 7);
        }
    }

    /**
     * Pin the CURRENT behaviour of a metrics level change: the level of a table is frozen
     * at its very first report, so a changed level does not re-route the counters.
     *
     * @note This is a deliberate deferral, not the target behaviour: reconciling a table
     *       entry on a schema version or a metrics level change is the scope of the level
     *       and rename step, which is expected to REPLACE the assertions below with
     *       the transition ones (drop the leaves on PARTITION -> TABLE, drop the bucket on
     *       TABLE -> PARTITION, drop the table on -> DISABLED). The test exists so that
     *       the step has to flip an explicit assertion instead of silently changing
     *       behaviour, which nothing pins.
     */
    Y_UNIT_TEST(MetricsLevelChangeIsIgnoredUntilReconciliation) {
        const TInstant now = TInstant::Seconds(100);

        // TEST 1: A table, which was first seen at the table level, keeps collapsing
        {
            NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

            auto aggregator = CreateNodeDatabaseMetricsAggregator(
                rootGroup,
                DATABASE_PATH,
                false /* isFollowerRole */
            );

            TFakeTablet leader(1000, 0);

            leader.SetSimple(UNIQUE_ROWS, 5);
            leader.Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);

            leader.SetSimple(UNIQUE_ROWS, 7);
            leader.Report(aggregator, TDetailedMetricsSettings::MetricsLevelPartition, now);

            aggregator->RecalculateAllCounters();

            DumpCounters("Counters after the level changed to the partition one", rootGroup);

            UNIT_ASSERT_VALUES_EQUAL(
                GetCounterValue(FindTableBucketCounters(rootGroup), "SUM(UniqueRows)"),
                7
            );
            UNIT_ASSERT(!FindTableGroup(rootGroup)->FindSubgroup("detailed_metrics", "per_partition"));
        }

        // TEST 2: A table, which was first seen at the partition level, keeps its leaves
        {
            NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

            auto aggregator = CreateNodeDatabaseMetricsAggregator(
                rootGroup,
                DATABASE_PATH,
                false /* isFollowerRole */
            );

            TFakeTablet leader(1000, 0);

            leader.SetSimple(UNIQUE_ROWS, 5);
            leader.Report(aggregator, TDetailedMetricsSettings::MetricsLevelPartition, now);

            leader.SetSimple(UNIQUE_ROWS, 7);
            leader.Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);

            aggregator->RecalculateAllCounters();

            DumpCounters("Counters after the level changed to the table one", rootGroup);

            UNIT_ASSERT_VALUES_EQUAL(
                GetCounterValue(
                    FindLeafCounters(rootGroup, leader.TabletId, leader.FollowerId),
                    "SUM(UniqueRows)"
                ),
                7
            );
            UNIT_ASSERT(!FindAppCountersGroup(FindTableGroup(rootGroup)));
        }
    }
}
