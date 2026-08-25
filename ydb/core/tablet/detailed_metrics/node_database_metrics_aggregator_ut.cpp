#include "node_database_metrics_aggregator.h"
#include "ut_helpers.h"

#include <library/cpp/monlib/dynamic_counters/encode.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/array_size.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/system/mutex.h>

#include <atomic>
#include <thread>

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

// The very same table after an ESchemeOpMoveTable rename: another path, a fresh
// PathId and a bumped schema version, reported by the very same tablets
const TString RENAMED_TABLE_PATH = "/Root/db/dir/renamed_table";
const TString RENAMED_RELATIVE_TABLE_PATH = "dir/renamed_table";

const TPathId RENAMED_TABLE_ID(72057594046644480ull, 45);

constexpr TTabletTypes::EType TABLET_TYPE = TTabletTypes::DataShard;

////////////////////////////////////////////////////////////////////////////////
// A small stand-in for the low level counters of Data Shard. The real counter set
// has hundreds of counters, which would make the assertions unreadable without
// covering anything, which is not covered by these few counters.

constexpr const char* EXECUTOR_SIMPLE_COUNTER_NAMES[] = {
    "DbUniqueRowsTotal",
    "DbUniqueDataBytes",
    // Absent from the DataShard allow-list (ydb/core/protos/counters_detailed_datashard.proto),
    // used to verify Initialize()'s nameFilter is honored (see NameFilterDropsUnlistedCounters)
    "NotInTheAllowList",
};

constexpr const char* EXECUTOR_CUMULATIVE_COUNTER_NAMES[] = {
    "ConsumedCPU",
};

constexpr const char* EXECUTOR_PERCENTILE_COUNTER_NAMES[] = {
    // A histogram aggregate: it is NOT filled by the tablet, it collects
    // one observation per tablet from the "ConsumedCPU" cumulative counter.
    // It is DataShard's only percentile: there is no ordinary one in the allow-list.
    "HIST(ConsumedCPU)",
};

constexpr const char* APP_CUMULATIVE_COUNTER_NAMES[] = {
    "DataShard/EngineHostRowUpdates",
    "DataShard/EngineHostRowUpdateBytes",
};

constexpr TTabletPercentileCounter::TRangeDef PERCENTILE_RANGES[] = {
    {  0,   "0"},
    { 10,  "10"},
    {100, "100"},
};

enum ESimpleCounter : ui32 {
    DB_UNIQUE_ROWS_TOTAL = 0,
    DB_UNIQUE_DATA_BYTES = 1,
    NOT_IN_ALLOW_LIST = 2,
};

enum ECumulativeCounter : ui32 {
    CONSUMED_CPU = 0,
};

enum EAppCumulativeCounter : ui32 {
    ENGINE_HOST_ROW_UPDATES = 0,
    ENGINE_HOST_ROW_UPDATE_BYTES = 1,
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
        , ExecutorCounters(
            Y_ARRAY_SIZE(EXECUTOR_SIMPLE_COUNTER_NAMES),
            Y_ARRAY_SIZE(EXECUTOR_CUMULATIVE_COUNTER_NAMES),
            Y_ARRAY_SIZE(EXECUTOR_PERCENTILE_COUNTER_NAMES),
            EXECUTOR_SIMPLE_COUNTER_NAMES,
            EXECUTOR_CUMULATIVE_COUNTER_NAMES,
            EXECUTOR_PERCENTILE_COUNTER_NAMES
        )
        , AppCounters(
            0,
            Y_ARRAY_SIZE(APP_CUMULATIVE_COUNTER_NAMES),
            0,
            nullptr,
            APP_CUMULATIVE_COUNTER_NAMES,
            nullptr
        )
    {
        for (ui32 i = 0; i < Y_ARRAY_SIZE(EXECUTOR_PERCENTILE_COUNTER_NAMES); ++i) {
            ExecutorCounters.Percentile()[i].Initialize(PERCENTILE_RANGES, true /* integral */);
        }
    }

    TFakeTablet& SetSimple(ESimpleCounter counter, ui64 value) {
        ExecutorCounters.Simple()[counter].Set(value);
        return *this;
    }

    TFakeTablet& AddCumulative(ECumulativeCounter counter, ui64 delta) {
        ExecutorCounters.Cumulative()[counter] += delta;
        return *this;
    }

    TFakeTablet& AddAppCumulative(EAppCumulativeCounter counter, ui64 delta) {
        AppCounters.Cumulative()[counter] += delta;
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
        const TString& tablePath = TABLE_PATH,
        TTabletTypes::EType tabletType = TABLET_TYPE,
        ui64 schemaVersion = 1
    ) {
        TDetailedMetricsTableInfo table;
        table.TableId = tableId;
        table.TablePath = tablePath;
        table.SchemaVersion = schemaVersion;
        table.MetricsLevel = level;

        // An empty baseline (the very first report) makes the diff a plain copy
        auto appDiff = AppCounters.MakeDiffForAggr(AppBaseline);
        auto executorDiff = ExecutorCounters.MakeDiffForAggr(ExecutorBaseline);

        aggregator->AddCounters(
            table,
            TabletId,
            FollowerId,
            tabletType,
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
 * @param[in] category "executor" or "app"
 *
 * @return The counter group of the given category (or nullptr if there is none)
 */
NMonitoring::TDynamicCounterPtr FindCategoryCountersGroup(
    NMonitoring::TDynamicCounterPtr bucketGroup,
    const TString& category
) {
    if (!bucketGroup) {
        return nullptr;
    }

    auto typeGroup = bucketGroup->FindSubgroup("type", TTabletTypes::TypeToStr(TABLET_TYPE));
    if (!typeGroup) {
        return nullptr;
    }

    return typeGroup->FindSubgroup("category", category);
}

/**
 * @param[in] bucketGroup The counter group of a table bucket or of a leaf
 *
 * @return The counter group of the executor counters (or nullptr if there is none)
 *
 * @note The renamed fixture counters (DbUniqueRowsTotal, ConsumedCPU, ...) live here.
 */
NMonitoring::TDynamicCounterPtr FindExecutorCountersGroup(NMonitoring::TDynamicCounterPtr bucketGroup) {
    return FindCategoryCountersGroup(bucketGroup, "executor");
}

/**
 * @param[in] bucketGroup The counter group of a table bucket or of a leaf
 *
 * @return The counter group of the application counters (or nullptr if there is none)
 */
NMonitoring::TDynamicCounterPtr FindAppCountersGroup(NMonitoring::TDynamicCounterPtr bucketGroup) {
    return FindCategoryCountersGroup(bucketGroup, "app");
}

/**
 * @param[in] rootGroup The counter group where the whole tree is created
 *
 * @return The executor counters of the table bucket (or nullptr if there is none)
 *
 * @note At the table level the collapsed counters live directly in the table group:
 *       the role is the caller's partition of the tree, not a label within it.
 */
NMonitoring::TDynamicCounterPtr FindTableBucketCounters(
    NMonitoring::TDynamicCounterPtr rootGroup,
    const TString& relativeTablePath = RELATIVE_TABLE_PATH
) {
    return FindExecutorCountersGroup(FindTableGroup(rootGroup, relativeTablePath));
}

/**
 * @param[in] rootGroup The counter group where the whole tree is created
 *
 * @return The application counters of the table bucket (or nullptr if there is none)
 */
NMonitoring::TDynamicCounterPtr FindAppTableBucketCounters(
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
 * @return The counter group of the leaf itself (or nullptr if there is none)
 */
NMonitoring::TDynamicCounterPtr FindLeafGroup(
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

    return tabletGroup->FindSubgroup("follower_id", ToString(followerId));
}

NMonitoring::TDynamicCounterPtr FindLeafCounters(
    NMonitoring::TDynamicCounterPtr rootGroup,
    ui64 tabletId,
    ui32 followerId,
    const TString& relativeTablePath = RELATIVE_TABLE_PATH
) {
    return FindExecutorCountersGroup(FindLeafGroup(rootGroup, tabletId, followerId, relativeTablePath));
}

/**
 * @param[in] rootGroup The counter group where the whole tree is created
 * @param[in] tabletId The ID of the tablet
 * @param[in] followerId The follower ID of the tablet (0 for the leader)
 *
 * @return The application counters of the leaf (or nullptr if there is none)
 */
NMonitoring::TDynamicCounterPtr FindAppLeafCounters(
    NMonitoring::TDynamicCounterPtr rootGroup,
    ui64 tabletId,
    ui32 followerId,
    const TString& relativeTablePath = RELATIVE_TABLE_PATH
) {
    return FindAppCountersGroup(FindLeafGroup(rootGroup, tabletId, followerId, relativeTablePath));
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
 * @param[in] countersGroup The counter group to check
 * @param[in] name The name of the counter
 *
 * @return Whether a counter of the given name is present in the group
 */
bool HasCounter(NMonitoring::TDynamicCounterPtr countersGroup, const TString& name) {
    UNIT_ASSERT_C(countersGroup, "no counter group to look for the counter " << name);

    return countersGroup->FindNamedCounter("sensor", name) != nullptr;
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
 * The private "ydb_detailed_raw" group with the two aggregators of a node built off it,
 * the way the two Tablet Counters Aggregator actors do: ONE shared root, no role label,
 * one aggregator per role writing into the very same tree.
 */
struct TRoleTrees {
    NMonitoring::TDynamicCounterPtr Root = MakeIntrusive<NMonitoring::TDynamicCounters>();

    TNodeDatabaseMetricsAggregatorPtr Leaders = CreateNodeDatabaseMetricsAggregator(
        Root,
        DATABASE_PATH,
        false /* isFollowerRole */
    );

    TNodeDatabaseMetricsAggregatorPtr Followers = CreateNodeDatabaseMetricsAggregator(
        Root,
        DATABASE_PATH,
        true /* isFollowerRole */
    );

    void RecalculateAllCounters() {
        Leaders->RecalculateAllCounters();
        Followers->RecalculateAllCounters();
    }
};

////////////////////////////////////////////////////////////////////////////////

/**
 * A reader thread, which runs the given check over and over under the shared tree lock,
 * the way the SysView Service actor reads the tree off its own mailbox, until the writer
 * on the main thread says it is done.
 *
 * @param[in] check Returns the description of the very first violation it finds, or an
 *                   empty string when the tree looks whole
 *
 * @note The check does NOT assert on its own. UNIT_ASSERT off the unittest thread does
 *       not throw: it panics ("assertion failed in non-unittest thread"), which aborts
 *       the whole test chunk instead of failing this one test. So the failure is handed
 *       back to the main thread by Join(), which is where the assertion happens.
 */
class TLockedReaderThread {
public:
    template <typename TCheck>
    explicit TLockedReaderThread(TCheck check)
        : Thread([this, check]() {
              while (!Stopped.load(std::memory_order_acquire)) {
                  try {
                      TGuard<TMutex> guard(DetailedMetricsLock());
                      Failure = check();
                  } catch (...) {
                      // Nothing here is expected to throw, but a panic on this thread
                      // would be even less readable than a reported failure
                      Failure = CurrentExceptionMessage();
                  }

                  if (Failure) {
                      return;
                  }

                  ++Reads;

                  // TMutex is not fair, and a reader, which relocks the very moment it
                  // unlocks, can starve the writer for the whole test
                  std::this_thread::yield();
              }
          })
    {}

    /**
     * @note Stops the thread as well, so that an assertion, which fails on the writer
     *       side, does not leave a joinable thread behind and terminate the process
     *       instead of failing the test.
     */
    ~TLockedReaderThread() {
        Stop();
    }

    /**
     * @return The number of the completed reads, asserting that none of them failed
     */
    ui64 Join() {
        Stop();

        UNIT_ASSERT_C(Failure.empty(), Failure);

        return Reads;
    }

private:
    void Stop() {
        Stopped.store(true, std::memory_order_release);

        if (Thread.joinable()) {
            Thread.join();
        }
    }

private:
    TString Failure;
    ui64 Reads = 0;
    std::atomic<bool> Stopped = false;

    // Declared last on purpose: the thread starts as soon as it is constructed, and it
    // touches every member above
    std::thread Thread;
};

} // namespace <anonymous>

////////////////////////////////////////////////////////////////////////////////

/**
 * Unit tests for the node database metrics aggregator (TNodeDatabaseMetricsAggregator).
 */
Y_UNIT_TEST_SUITE(TNodeDatabaseMetricsAggregatorTest) {
    /**
     * Verify that at the table level all same-node LEADER partitions of the table are
     * collapsed into a single bucket, that the followers contribute nothing to it, and
     * that no per-partition counters are created.
     *
     * @note The bucket belongs to the aggregator of the leaders alone. Both aggregators
     *       write into one shared tree, and two TAggregatedTabletCounters pointed at one
     *       counter group assign rather than sum, so a follower side bucket would simply
     *       overwrite the leader values on every recalculation.
     */
    Y_UNIT_TEST(TableLevelCollapsesPartitions) {
        TRoleTrees trees;

        const TInstant now = TInstant::Seconds(100);

        // 3 leader partitions of the same table on this node
        TFakeTablet leader1(1000, 0);
        TFakeTablet leader2(2000, 0);
        TFakeTablet leader3(3000, 0);

        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1).SetSimple(DB_UNIQUE_DATA_BYTES, 10).AddCumulative(CONSUMED_CPU, 100)
            .AddAppCumulative(ENGINE_HOST_ROW_UPDATES, 1000);
        leader2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 2).SetSimple(DB_UNIQUE_DATA_BYTES, 20).AddCumulative(CONSUMED_CPU, 200)
            .AddAppCumulative(ENGINE_HOST_ROW_UPDATES, 2000);
        leader3.SetSimple(DB_UNIQUE_ROWS_TOTAL, 4).SetSimple(DB_UNIQUE_DATA_BYTES, 40).AddCumulative(CONSUMED_CPU, 400)
            .AddAppCumulative(ENGINE_HOST_ROW_UPDATES, 4000);

        // 2 followers of the very same partition: they must NOT collide with each other
        TFakeTablet follower1(1000, 1);
        TFakeTablet follower2(1000, 2);

        follower1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 8).AddCumulative(CONSUMED_CPU, 800)
            .AddAppCumulative(ENGINE_HOST_ROW_UPDATES, 8000);
        follower2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 16).AddCumulative(CONSUMED_CPU, 1600)
            .AddAppCumulative(ENGINE_HOST_ROW_UPDATES, 16000);

        for (auto* tablet : {&leader1, &leader2, &leader3}) {
            tablet->Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelTable, now);
        }

        for (auto* tablet : {&follower1, &follower2}) {
            tablet->Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelTable, now);
        }

        trees.RecalculateAllCounters();

        DumpCounters("Table level counters", trees.Root);

        // The single bucket holds the 3 leader partitions and nothing else
        auto leaderCounters = FindTableBucketCounters(trees.Root);
        UNIT_ASSERT(leaderCounters);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "SUM(DbUniqueRowsTotal)"), 1 + 2 + 4);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "MAX(DbUniqueRowsTotal)"), 4);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "SUM(DbUniqueDataBytes)"), 10 + 20 + 40);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "MAX(DbUniqueDataBytes)"), 40);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "ConsumedCPU"), 100 + 200 + 400);

        // The app category counters (category=app, SCC_TABLET) collapse the very same way
        auto leaderAppCounters = FindAppTableBucketCounters(trees.Root);
        UNIT_ASSERT(leaderAppCounters);
        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(leaderAppCounters, "DataShard/EngineHostRowUpdates"),
            1000 + 2000 + 4000
        );

        // The regression this whole arrangement exists to prevent: recalculating the
        // follower side must not touch a single value of the bucket
        trees.Followers->RecalculateAllCounters();

        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "SUM(DbUniqueRowsTotal)"), 1 + 2 + 4);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "SUM(DbUniqueDataBytes)"), 10 + 20 + 40);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "ConsumedCPU"), 100 + 200 + 400);
        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(leaderAppCounters, "DataShard/EngineHostRowUpdates"),
            1000 + 2000 + 4000
        );

        // No per-partition counters at the table level
        auto tableGroup = FindTableGroup(trees.Root);
        UNIT_ASSERT(tableGroup);
        UNIT_ASSERT(!tableGroup->FindSubgroup("detailed_metrics", "per_partition"));
    }

    /**
     * Verify that at the partition level every (tablet_id, follower_id) leaf is kept
     * verbatim and no on-node rollup of any kind is created.
     */
    Y_UNIT_TEST(PartitionLevelKeepsLeaves) {
        TRoleTrees trees;

        const TInstant now = TInstant::Seconds(100);

        // 2 partitions of the same table, a leader and 2 followers each. The leader goes
        // to the aggregator of the leaders and both followers to the other one, exactly
        // the way the two Tablet Counters Aggregator actors of a node are fed. All of
        // them land in ONE shared tree, told apart by follower_id alone.
        const TVector<ui64> tabletIds = {1000, 2000};
        const TVector<ui32> followerIds = {0, 1, 2};

        ui64 value = 0;
        THashMap<std::pair<ui64, ui32>, ui64> expectedValues;

        for (ui64 tabletId : tabletIds) {
            for (ui32 followerId : followerIds) {
                value += 1;
                expectedValues[std::make_pair(tabletId, followerId)] = value;

                TFakeTablet tablet(tabletId, followerId);
                tablet
                    .SetSimple(DB_UNIQUE_ROWS_TOTAL, value)
                    .AddCumulative(CONSUMED_CPU, value * 100)
                    .AddAppCumulative(ENGINE_HOST_ROW_UPDATES, value * 10);
                tablet.Report(
                    followerId == 0 ? trees.Leaders : trees.Followers,
                    TDetailedMetricsSettings::MetricsLevelPartition,
                    now
                );
            }
        }

        trees.RecalculateAllCounters();

        DumpCounters("Partition level counters", trees.Root);

        // Every leaf holds exactly what its tablet has reported, all in the one tree
        for (const auto& [tablet, expectedValue] : expectedValues) {
            const auto& [tabletId, followerId] = tablet;

            auto leafCounters = FindLeafCounters(trees.Root, tabletId, followerId);
            UNIT_ASSERT_C(leafCounters, "no leaf for " << tabletId << ":" << followerId);

            UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leafCounters, "SUM(DbUniqueRowsTotal)"), expectedValue);
            UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leafCounters, "MAX(DbUniqueRowsTotal)"), expectedValue);
            UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leafCounters, "ConsumedCPU"), expectedValue * 100);

            auto appLeafCounters = FindAppLeafCounters(trees.Root, tabletId, followerId);
            UNIT_ASSERT_C(appLeafCounters, "no app leaf for " << tabletId << ":" << followerId);
            UNIT_ASSERT_VALUES_EQUAL(
                GetCounterValue(appLeafCounters, "DataShard/EngineHostRowUpdates"),
                expectedValue * 10
            );
        }

        // The leader and the followers of one partition share a single tablet_id= node,
        // written by the two different aggregators
        auto tableGroup = FindTableGroup(trees.Root);
        UNIT_ASSERT(tableGroup);

        auto perPartitionGroup = tableGroup->FindSubgroup("detailed_metrics", "per_partition");
        UNIT_ASSERT(perPartitionGroup);

        auto sharedTabletGroup = perPartitionGroup->FindSubgroup("tablet_id", "1000");
        UNIT_ASSERT(sharedTabletGroup);
        for (ui32 followerId : followerIds) {
            UNIT_ASSERT_C(
                sharedTabletGroup->FindSubgroup("follower_id", ToString(followerId)),
                "no follower_id=" << followerId << " under the shared tablet_id=1000"
            );
        }

        // No table bucket and no on-node rollup at the partition level
        UNIT_ASSERT(!FindExecutorCountersGroup(tableGroup));
        UNIT_ASSERT(!FindExecutorCountersGroup(perPartitionGroup));

        for (ui64 tabletId : tabletIds) {
            auto tabletGroup = perPartitionGroup->FindSubgroup("tablet_id", ToString(tabletId));
            UNIT_ASSERT(tabletGroup);
            UNIT_ASSERT(!FindExecutorCountersGroup(tabletGroup));

            // No replicas_only aggregate is synthesized on the node
            UNIT_ASSERT(!tabletGroup->FindSubgroup("follower_id", "replicas_only"));
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
     * Verify that the histogram aggregate named HIST(x) ends up in the counter tree,
     * filled here from the counter named x. DataShard's allow-list has no ordinary
     * percentile counter, only this synthesized one.
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

        leader1.AddCumulative(CONSUMED_CPU, 100);
        leader2.AddCumulative(CONSUMED_CPU, 200);

        for (auto* tablet : {&leader1, &leader2}) {
            tablet->Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);
        }

        aggregator->RecalculateAllCounters();

        DumpCounters("Table level counters with the percentile counters", rootGroup);

        auto leaderCounters = FindTableBucketCounters(rootGroup);
        UNIT_ASSERT(leaderCounters);

        // The histogram aggregate holds one observation per partition, taken from
        // the "ConsumedCPU" cumulative counter. The tablets do NOT fill it themselves,
        // so an empty histogram here would mean the aggregate is never fed.
        UNIT_ASSERT_VALUES_EQUAL(GetHistogramTotal(leaderCounters, "HIST(ConsumedCPU)"), 2);
    }

    /**
     * Verify that forgetting a tablet drops its observations from the HIST(x) percentile
     * aggregate of the table bucket, while the accumulated cumulative counters keep
     * the work the tablet had already done.
     *
     * @note A HIST(x) aggregate is rebuilt from scratch on the next recalculation, rather
     *       than subtracted bucket by bucket right away.
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

        leader1.AddCumulative(CONSUMED_CPU, 100);
        leader2.AddCumulative(CONSUMED_CPU, 200);

        for (auto* tablet : {&leader1, &leader2}) {
            tablet->Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);
        }

        aggregator->RecalculateAllCounters();

        auto leaderCounters = FindTableBucketCounters(rootGroup);
        UNIT_ASSERT(leaderCounters);

        // The very first report of a tablet contributes a 0 observation (there is no
        // previous report of it to derive a per second rate from), so both partitions'
        // observations land in the <=0 bucket of the ranges {0, 10, 100}
        UNIT_ASSERT_VALUES_EQUAL(GetHistogramBuckets(leaderCounters, "HIST(ConsumedCPU)"), "2,0,0,0");
        UNIT_ASSERT_VALUES_EQUAL(GetHistogramTotal(leaderCounters, "HIST(ConsumedCPU)"), 2);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "ConsumedCPU"), 100 + 200);

        // The second partition is gone
        aggregator->ForgetTablet(leader2.TabletId, leader2.FollowerId);
        aggregator->RecalculateAllCounters();

        DumpCounters("Table level counters after forgetting the second partition", rootGroup);

        // The histogram aggregate is rebuilt from the surviving partitions only
        UNIT_ASSERT_VALUES_EQUAL(GetHistogramBuckets(leaderCounters, "HIST(ConsumedCPU)"), "1,0,0,0");
        UNIT_ASSERT_VALUES_EQUAL(GetHistogramTotal(leaderCounters, "HIST(ConsumedCPU)"), 1);

        // The accumulated cumulative counter is NOT reduced: the CPU the forgotten
        // partition had burnt has still been burnt, and the series must not go backwards
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderCounters, "ConsumedCPU"), 100 + 200);

        // The last partition is gone too, so the bucket takes its own type= subtree with
        // it and the emptied table= and database= nodes above it follow
        aggregator->ForgetTablet(leader1.TabletId, leader1.FollowerId);

        UNIT_ASSERT(!FindTableBucketCounters(rootGroup));
        UNIT_ASSERT(!FindTableGroup(rootGroup));
        UNIT_ASSERT(!rootGroup->FindSubgroup("database", DATABASE_PATH));
    }

    /**
     * Verify that forgetting a tablet of a table level table drops its contribution
     * from the table bucket and removes the counter groups, which become empty.
     *
     * @note The table bucket belongs to the aggregator of the leaders alone, so the
     *       followers of a table level table contribute nothing at all. The table= node
     *       itself is shared spine and outlives the bucket.
     */
    Y_UNIT_TEST(ForgetTabletAtTableLevel) {
        TRoleTrees trees;

        const TInstant now = TInstant::Seconds(100);

        TFakeTablet leader1(1000, 0);
        TFakeTablet leader2(2000, 0);
        TFakeTablet follower(1000, 1);

        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);
        leader2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 2);
        follower.SetSimple(DB_UNIQUE_ROWS_TOTAL, 8);

        for (auto* tablet : {&leader1, &leader2}) {
            tablet->Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelTable, now);
        }
        follower.Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelTable, now);

        trees.RecalculateAllCounters();

        // The follower of a table level table is dropped on the floor: only the leaders
        // are in the bucket
        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindTableBucketCounters(trees.Root), "SUM(DbUniqueRowsTotal)"),
            1 + 2
        );

        // TEST 1: The table bucket is recomputed from the surviving partitions
        trees.Leaders->ForgetTablet(leader2.TabletId, leader2.FollowerId);
        trees.RecalculateAllCounters();

        DumpCounters("Table level counters after forgetting one leader", trees.Root);

        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindTableBucketCounters(trees.Root), "SUM(DbUniqueRowsTotal)"),
            1
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindTableBucketCounters(trees.Root), "MAX(DbUniqueRowsTotal)"),
            1
        );

        // TEST 2: The bucket takes its own type= subtree with it once its last tablet is
        //         gone, and the table= and database= nodes it emptied go with it. Nothing
        //         else is under them: the follower of a table level table never built
        //         anything of its own
        trees.Leaders->ForgetTablet(leader1.TabletId, leader1.FollowerId);
        trees.RecalculateAllCounters();

        DumpCounters("Counters after forgetting the last leader", trees.Root);

        UNIT_ASSERT(!FindTableBucketCounters(trees.Root));
        UNIT_ASSERT(!FindTableGroup(trees.Root));
        UNIT_ASSERT(!trees.Root->FindSubgroup("database", DATABASE_PATH));

        // TEST 3: Forgetting the follower, which never contributed, is not an error
        trees.Followers->ForgetTablet(follower.TabletId, follower.FollowerId);

        // TEST 4: Forgetting an unknown tablet is not an error
        trees.Leaders->ForgetTablet(leader1.TabletId, leader1.FollowerId);

        // TEST 5: A tablet reported after the teardown rebuilds the tree, rather than
        //         filling the database= node this instance used to hold a pointer to
        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 3);
        leader1.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelTable, now);
        trees.RecalculateAllCounters();

        DumpCounters("Counters after the table came back", trees.Root);

        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindTableBucketCounters(trees.Root), "SUM(DbUniqueRowsTotal)"),
            3
        );
    }

    /**
     * Verify that forgetting a tablet of a partition level table removes its own leaf
     * and ONLY its own leaf.
     *
     * @note The tablet_id= node above the leaf is shared: the leader of a partition and
     *       its followers may run on one node and are reported by the two different
     *       aggregators. It survives for exactly as long as either of them is still there,
     *       and is reclaimed together with detailed_metrics=, table= and database= once
     *       the last leaf below it goes.
     */
    Y_UNIT_TEST(ForgetTabletAtPartitionLevel) {
        TRoleTrees trees;

        const TInstant now = TInstant::Seconds(100);

        // Two followers of the same partition plus one of another partition
        TFakeTablet follower1(1000, 1);
        TFakeTablet follower2(1000, 2);
        TFakeTablet follower3(2000, 1);

        // ... and the leader of the first partition, on this very node, reported by the
        // OTHER aggregator into the very same tablet_id= node
        TFakeTablet leader1(1000, 0);

        for (auto* tablet : {&follower1, &follower2, &follower3}) {
            tablet->SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);
            tablet->Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelPartition, now);
        }

        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 7);
        leader1.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, now);

        trees.RecalculateAllCounters();

        auto tableGroup = FindTableGroup(trees.Root);
        UNIT_ASSERT(tableGroup);

        auto perPartitionGroup = tableGroup->FindSubgroup("detailed_metrics", "per_partition");
        UNIT_ASSERT(perPartitionGroup);

        // TEST 1: Only the leaf of the forgotten tablet is removed, its siblings survive
        trees.Followers->ForgetTablet(follower1.TabletId, follower1.FollowerId);

        DumpCounters("Partition level counters after forgetting one follower", trees.Root);

        UNIT_ASSERT(!FindLeafCounters(trees.Root, follower1.TabletId, follower1.FollowerId));
        UNIT_ASSERT(FindLeafCounters(trees.Root, follower2.TabletId, follower2.FollowerId));
        UNIT_ASSERT(FindLeafCounters(trees.Root, leader1.TabletId, leader1.FollowerId));

        // TEST 2: Emptying the followers of a partition must NOT take the shared
        //         tablet_id= node with it — the leader of that partition is still live
        //         there, reported by the other aggregator
        trees.Followers->ForgetTablet(follower2.TabletId, follower2.FollowerId);
        trees.Followers->ForgetTablet(follower3.TabletId, follower3.FollowerId);

        DumpCounters("Counters after forgetting every follower", trees.Root);

        auto sharedTabletGroup = perPartitionGroup->FindSubgroup("tablet_id", ToString(leader1.TabletId));
        UNIT_ASSERT(sharedTabletGroup);

        auto leaderLeaf = FindLeafCounters(trees.Root, leader1.TabletId, leader1.FollowerId);
        UNIT_ASSERT(leaderLeaf);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderLeaf, "SUM(DbUniqueRowsTotal)"), 7);

        // ... and it is still reachable from the SHARED root, not merely alive by
        // reference count
        UNIT_ASSERT(
            trees.Root
                ->FindSubgroup("database", DATABASE_PATH)
                ->FindSubgroup("table", RELATIVE_TABLE_PATH)
                ->FindSubgroup("detailed_metrics", "per_partition")
                ->FindSubgroup("tablet_id", ToString(leader1.TabletId))
                ->FindSubgroup("follower_id", "0")
        );

        // TEST 3: The last leaf goes too, and everything it emptied goes with it, all the
        //         way up to the database= node. Nothing of this database is left on the
        //         node, so nothing of it is left in the tree either
        trees.Leaders->ForgetTablet(leader1.TabletId, leader1.FollowerId);

        DumpCounters("Counters after forgetting the last tablet of the database", trees.Root);

        UNIT_ASSERT(!FindLeafCounters(trees.Root, leader1.TabletId, leader1.FollowerId));
        UNIT_ASSERT(!FindTableGroup(trees.Root));
        UNIT_ASSERT(!trees.Root->FindSubgroup("database", DATABASE_PATH));

        // TEST 4: The two instances rebuild the tree from scratch afterwards, neither of
        //         them filling a node it used to hold a pointer to
        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 5);
        leader1.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, now);

        follower1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 6);
        follower1.Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelPartition, now);

        trees.RecalculateAllCounters();

        DumpCounters("Counters after the partitions came back", trees.Root);

        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(
                FindLeafCounters(trees.Root, leader1.TabletId, leader1.FollowerId),
                "SUM(DbUniqueRowsTotal)"
            ),
            5
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(
                FindLeafCounters(trees.Root, follower1.TabletId, follower1.FollowerId),
                "SUM(DbUniqueRowsTotal)"
            ),
            6
        );
    }

    /**
     * Verify that emptying one table of a database reclaims that table's node alone: the
     * teardown walks upwards only for as long as the nodes it empties come out empty.
     */
    Y_UNIT_TEST(ForgetTabletKeepsTheOtherTablesOfTheDatabase) {
        TRoleTrees trees;

        const TInstant now = TInstant::Seconds(100);

        TFakeTablet leader(1000, 0);
        TFakeTablet otherLeader(2000, 0);

        leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);
        leader.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, now);

        otherLeader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 2);
        otherLeader.Report(
            trees.Leaders,
            TDetailedMetricsSettings::MetricsLevelPartition,
            now,
            OTHER_TABLE_ID,
            OTHER_TABLE_PATH
        );

        trees.RecalculateAllCounters();

        // The only tablet of the first table is gone: that table= node goes with it, and
        // the walk stops at database=, which the second table still occupies
        trees.Leaders->ForgetTablet(leader.TabletId, leader.FollowerId);

        DumpCounters("Counters after emptying one table of the database", trees.Root);

        UNIT_ASSERT(!FindTableGroup(trees.Root));
        UNIT_ASSERT(trees.Root->FindSubgroup("database", DATABASE_PATH));

        auto survivingLeaf = FindLeafCounters(
            trees.Root,
            otherLeader.TabletId,
            otherLeader.FollowerId,
            OTHER_RELATIVE_TABLE_PATH
        );
        UNIT_ASSERT(survivingLeaf);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(survivingLeaf, "SUM(DbUniqueRowsTotal)"), 2);

        // The second table goes too, and now the database= node has nothing left to hold
        trees.Leaders->ForgetTablet(otherLeader.TabletId, otherLeader.FollowerId);

        UNIT_ASSERT(!trees.Root->FindSubgroup("database", DATABASE_PATH));
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
        tablet.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);

        tablet.Report(aggregator, TDetailedMetricsSettings::MetricsLevelUnspecified, now);
        tablet.Report(aggregator, TDetailedMetricsSettings::MetricsLevelDisabled, now);

        aggregator->RecalculateAllCounters();

        // Not a single counter group is created for such tables
        UNIT_ASSERT(!rootGroup->FindSubgroup("database", DATABASE_PATH));
    }

    /**
     * Verify that the table level bucket holds the leaders and the leaders alone, so
     * that the leader-only metrics are neither inflated nor overwritten.
     *
     * A follower's executor counters are real and equal its leader's (the rows are
     * physically the same), which is why the corresponding public metric
     * (table.datashard.row_count) must be leader-only. The raw DataShard counters carry
     * no leader-only marking, and the filter runs on the processor after aggregation
     * (decision S4), so whatever the node collapses into one bucket is what the filter
     * has to work with.
     *
     * Both aggregators of a node write into ONE tree, and the collapsed bucket lives
     * directly on the shared table= node. A follower side bucket would therefore be the
     * very same counter group, and TAggregatedTabletCounters ASSIGNS the simple SUM/MAX,
     * the cumulative MAX and the histograms from its own contributors rather than adding
     * to what is there. So the two would not sum to a wrong 200 — they would take turns
     * overwriting each other, and row_count would flap to whatever the last recalculation
     * saw. Leaving the bucket to the aggregator of the leaders is what removes the
     * collision, and it agrees with the rule that the high level table.datashard.*
     * metrics are computed from the leaders alone.
     *
     * @note Two independent mechanisms defend this, and this test pins the second.
     *       Feeding both roles to ONE instance never reaches the arithmetic at all:
     *       CheckSingleRole() aborts first. What this test guards is the shape.
     */
    Y_UNIT_TEST(RoleSplitKeepsLeaderOnlyMetricsUninflated) {
        TRoleTrees trees;

        const TInstant now = TInstant::Seconds(100);

        // One partition: the leader reports a row count of 100, which is the truth.
        // The follower's executor counters hold the same value (the rows are the same),
        // and that is precisely why the corresponding public metric is leader-only.
        TFakeTablet leader(1000, 0);
        leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 100).AddCumulative(CONSUMED_CPU, 7);

        TFakeTablet follower(1000, 1);
        follower.SetSimple(DB_UNIQUE_ROWS_TOTAL, 100).AddCumulative(CONSUMED_CPU, 3);

        leader.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelTable, now);
        follower.Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelTable, now);

        trees.RecalculateAllCounters();

        DumpCounters("Leader-only metrics at the table level", trees.Root);

        // The single table bucket reads 100: the true row count, neither doubled
        // by the follower nor overwritten by it
        auto tableCounters = FindTableBucketCounters(trees.Root);
        UNIT_ASSERT(tableCounters);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(tableCounters, "SUM(DbUniqueRowsTotal)"), 100);

        // The follower contributed nothing at all, not even its cumulative counters:
        // at the table level its work is simply not collected on the node
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(tableCounters, "ConsumedCPU"), 7);

        // Recalculating the follower side leaves every value exactly as it was. This is
        // the assertion that fails the day a follower side table bucket is reintroduced
        // onto the shared table= node.
        trees.Followers->RecalculateAllCounters();

        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(tableCounters, "SUM(DbUniqueRowsTotal)"), 100);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(tableCounters, "ConsumedCPU"), 7);
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

        // The leader holds one value, the follower another, so we can verify each lands
        // in its own follower_id= leaf of the ONE shared tree
        TFakeTablet leader(1000, 0);
        leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 42);

        TFakeTablet follower(1000, 1);
        follower.SetSimple(DB_UNIQUE_ROWS_TOTAL, 99);

        leader.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, now);
        follower.Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelPartition, now);

        trees.RecalculateAllCounters();

        DumpCounters("Partition level leaves, both roles", trees.Root);

        // follower_id=0 is the leader, carrying the leader's value
        auto leaderLeaf = FindLeafCounters(trees.Root, 1000, 0);
        UNIT_ASSERT(leaderLeaf);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leaderLeaf, "SUM(DbUniqueRowsTotal)"), 42);

        // follower_id=1 is the replica, carrying the replica's own value
        auto followerLeaf = FindLeafCounters(trees.Root, 1000, 1);
        UNIT_ASSERT(followerLeaf);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(followerLeaf, "SUM(DbUniqueRowsTotal)"), 99);

        // The two are separate leaves of ONE tablet_id= node: the label carries the role,
        // so nothing above the leaf has to
        auto tabletGroup = FindTableGroup(trees.Root)
            ->FindSubgroup("detailed_metrics", "per_partition")
            ->FindSubgroup("tablet_id", "1000");
        UNIT_ASSERT(tabletGroup);
        UNIT_ASSERT(tabletGroup->FindSubgroup("follower_id", "0"));
        UNIT_ASSERT(tabletGroup->FindSubgroup("follower_id", "1"));
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
            tablet.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);

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

            leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 5);
            leader.Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);
            aggregator->RecalculateAllCounters();

            UNIT_ASSERT_VALUES_EQUAL(
                GetCounterValue(FindTableBucketCounters(rootGroup), "SUM(DbUniqueRowsTotal)"),
                5
            );

            // The very same tablet now reports another table of the same database
            leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 7);
            leader.Report(
                aggregator,
                TDetailedMetricsSettings::MetricsLevelTable,
                now,
                OTHER_TABLE_ID,
                OTHER_TABLE_PATH
            );
            aggregator->RecalculateAllCounters();

            DumpCounters("Counters after the tablet moved to another table", rootGroup);

            // Nothing of the old table is left behind — its emptied table= node goes with
            // its counters, and so does the database= node until the new table recreates
            // it — and the new table holds the counters instead
            UNIT_ASSERT(!FindTableBucketCounters(rootGroup));
            UNIT_ASSERT_VALUES_EQUAL(
                GetCounterValue(
                    FindTableBucketCounters(rootGroup, OTHER_RELATIVE_TABLE_PATH),
                    "SUM(DbUniqueRowsTotal)"
                ),
                7
            );

            // The reverse map points at the new table, so forgetting the tablet drops
            // the counters of the NEW table rather than of the one it no longer belongs to
            aggregator->ForgetTablet(leader.TabletId, leader.FollowerId);

            UNIT_ASSERT(!FindTableBucketCounters(rootGroup, OTHER_RELATIVE_TABLE_PATH));
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

            leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 5);
            leader.Report(aggregator, TDetailedMetricsSettings::MetricsLevelPartition, now);
            aggregator->RecalculateAllCounters();

            UNIT_ASSERT(FindLeafCounters(rootGroup, leader.TabletId, leader.FollowerId));

            leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 7);
            leader.Report(
                aggregator,
                TDetailedMetricsSettings::MetricsLevelPartition,
                now,
                OTHER_TABLE_ID,
                OTHER_TABLE_PATH
            );
            aggregator->RecalculateAllCounters();

            DumpCounters("Leaves after the tablet moved to another table", rootGroup);

            // The old leaf is gone together with the table= node it emptied, and the new
            // leaf holds the counters
            UNIT_ASSERT(!FindLeafCounters(rootGroup, leader.TabletId, leader.FollowerId));
            UNIT_ASSERT(!FindTableGroup(rootGroup));

            auto leafCounters = FindLeafCounters(
                rootGroup,
                leader.TabletId,
                leader.FollowerId,
                OTHER_RELATIVE_TABLE_PATH
            );
            UNIT_ASSERT(leafCounters);
            UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leafCounters, "SUM(DbUniqueRowsTotal)"), 7);

            aggregator->ForgetTablet(leader.TabletId, leader.FollowerId);

            UNIT_ASSERT(!FindLeafCounters(
                rootGroup,
                leader.TabletId,
                leader.FollowerId,
                OTHER_RELATIVE_TABLE_PATH
            ));
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

            oldTablet.SetSimple(DB_UNIQUE_ROWS_TOTAL, 5);
            oldTablet.Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now, TABLE_ID, TABLE_PATH);

            newTablet.SetSimple(DB_UNIQUE_ROWS_TOTAL, 7);
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
                GetCounterValue(FindTableBucketCounters(rootGroup), "SUM(DbUniqueRowsTotal)"),
                5 + 7
            );

            // The tablet of the OLDER PathId is forgotten: the group must stay reachable,
            // because the surviving PathId's entry still owns it
            aggregator->ForgetTablet(oldTablet.TabletId, oldTablet.FollowerId);
            aggregator->RecalculateAllCounters();

            DumpCounters("Table level counters after forgetting the older PathId's tablet", rootGroup);

            UNIT_ASSERT(FindTableGroup(rootGroup));
            UNIT_ASSERT_VALUES_EQUAL(
                GetCounterValue(FindTableBucketCounters(rootGroup), "SUM(DbUniqueRowsTotal)"),
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

            oldTablet.SetSimple(DB_UNIQUE_ROWS_TOTAL, 5);
            oldTablet.Report(
                aggregator,
                TDetailedMetricsSettings::MetricsLevelPartition,
                now,
                TABLE_ID,
                TABLE_PATH
            );

            newTablet.SetSimple(DB_UNIQUE_ROWS_TOTAL, 7);
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
            UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(oldLeaf, "SUM(DbUniqueRowsTotal)"), 5);

            auto newLeaf = FindLeafCounters(rootGroup, newTablet.TabletId, newTablet.FollowerId);
            UNIT_ASSERT(newLeaf);
            UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(newLeaf, "SUM(DbUniqueRowsTotal)"), 7);

            // Forgetting the OLDER PathId's tablet must not detach the surviving leaf
            aggregator->ForgetTablet(oldTablet.TabletId, oldTablet.FollowerId);

            DumpCounters("Partition level leaves after forgetting the older PathId's tablet", rootGroup);

            UNIT_ASSERT(!FindLeafCounters(rootGroup, oldTablet.TabletId, oldTablet.FollowerId));

            auto survivingLeaf = FindLeafCounters(rootGroup, newTablet.TabletId, newTablet.FollowerId);
            UNIT_ASSERT(survivingLeaf);
            UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(survivingLeaf, "SUM(DbUniqueRowsTotal)"), 7);
        }
    }

    /**
     * Verify that a metrics level change with NO schema version bump — an
     * ALTER DATABASE ... TABLES_METRICS_LEVEL, which reaches the node through the
     * subdomain publish rather than through the schema — re-routes the counters of the
     * table from the per-partition leaves into the collapse bucket.
     *
     * @note This is the transition, which watching the schema version alone would miss
     *       entirely, leaving the table emitting per-partition leaves forever.
     */
    Y_UNIT_TEST(LevelChangeWithoutSchemaBumpReconciles) {
        TRoleTrees trees;

        const TInstant now = TInstant::Seconds(100);

        TFakeTablet leader1(1000, 0);
        TFakeTablet leader2(2000, 0);
        TFakeTablet follower(1000, 1);

        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);
        leader2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 2);
        follower.SetSimple(DB_UNIQUE_ROWS_TOTAL, 8);

        for (auto* tablet : {&leader1, &leader2}) {
            tablet->Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, now);
        }
        follower.Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelPartition, now);

        trees.RecalculateAllCounters();

        UNIT_ASSERT(FindLeafCounters(trees.Root, leader1.TabletId, leader1.FollowerId));
        UNIT_ASSERT(FindLeafCounters(trees.Root, leader2.TabletId, leader2.FollowerId));
        UNIT_ASSERT(FindLeafCounters(trees.Root, follower.TabletId, follower.FollowerId));

        // The database default drops to the table level: the very same schema version 1,
        // the very same tablets, only the level of the report changes
        for (auto* tablet : {&leader1, &leader2}) {
            tablet->Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelTable, now);
        }
        follower.Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelTable, now);

        trees.RecalculateAllCounters();

        DumpCounters("Counters after ALTER DATABASE dropped the level to the table one", trees.Root);

        // Not a single leaf is left, of either role
        auto tableGroup = FindTableGroup(trees.Root);
        UNIT_ASSERT(tableGroup);
        UNIT_ASSERT(!tableGroup->FindSubgroup("detailed_metrics", "per_partition"));

        // ... and the collapse bucket holds the leaders alone
        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindTableBucketCounters(trees.Root), "SUM(DbUniqueRowsTotal)"),
            1 + 2
        );
    }

    /**
     * Verify the opposite transition: a table, which collapsed into one bucket, starts
     * emitting a leaf per partition and drops the bucket it no longer fills.
     *
     * @note The table level series of a partition level table is produced on the
     *       processor by summing the leaves across the nodes, so keeping the bucket here
     *       as well would publish the very same table twice.
     */
    Y_UNIT_TEST(LevelChangeToPartitionDropsTheTableBucket) {
        NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

        auto aggregator = CreateNodeDatabaseMetricsAggregator(
            rootGroup,
            DATABASE_PATH,
            false /* isFollowerRole */
        );

        const TInstant now = TInstant::Seconds(100);

        TFakeTablet leader1(1000, 0);
        TFakeTablet leader2(2000, 0);

        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);
        leader2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 2);

        for (auto* tablet : {&leader1, &leader2}) {
            tablet->Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);
        }

        aggregator->RecalculateAllCounters();

        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindTableBucketCounters(rootGroup), "SUM(DbUniqueRowsTotal)"),
            1 + 2
        );

        // The level is raised to the partition one
        for (auto* tablet : {&leader1, &leader2}) {
            tablet->Report(aggregator, TDetailedMetricsSettings::MetricsLevelPartition, now);
        }

        aggregator->RecalculateAllCounters();

        DumpCounters("Counters after the level was raised to the partition one", rootGroup);

        // The bucket took its type= subtree with it, and every partition has a leaf now
        UNIT_ASSERT(!FindTableBucketCounters(rootGroup));

        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(
                FindLeafCounters(rootGroup, leader1.TabletId, leader1.FollowerId),
                "SUM(DbUniqueRowsTotal)"
            ),
            1
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(
                FindLeafCounters(rootGroup, leader2.TabletId, leader2.FollowerId),
                "SUM(DbUniqueRowsTotal)"
            ),
            2
        );
    }

    /**
     * Verify that a table, which stops collecting detailed metrics, is dropped whole:
     * its groups go, and its reports create nothing afterwards.
     */
    Y_UNIT_TEST(LevelChangeToDisabledDropsTheTable) {
        const TInstant now = TInstant::Seconds(100);

        // TEST 1: From the table level, where the collapse bucket has to go
        {
            NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

            auto aggregator = CreateNodeDatabaseMetricsAggregator(
                rootGroup,
                DATABASE_PATH,
                false /* isFollowerRole */
            );

            TFakeTablet leader(1000, 0);
            leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 5);

            leader.Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);
            aggregator->RecalculateAllCounters();

            UNIT_ASSERT(FindTableBucketCounters(rootGroup));

            leader.Report(aggregator, TDetailedMetricsSettings::MetricsLevelDisabled, now);

            DumpCounters("Counters after the table level table was disabled", rootGroup);

            UNIT_ASSERT(!FindTableGroup(rootGroup));
            UNIT_ASSERT(!rootGroup->FindSubgroup("database", DATABASE_PATH));

            // The reports of a disabled table keep creating nothing at all
            leader.Report(aggregator, TDetailedMetricsSettings::MetricsLevelDisabled, now);
            aggregator->RecalculateAllCounters();

            UNIT_ASSERT(!rootGroup->FindSubgroup("database", DATABASE_PATH));
        }

        // TEST 2: From the partition level, where the leaves have to go. The level is
        //         cleared rather than disabled: a table with no override of its own
        //         follows a database default, which collects nothing
        {
            NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

            auto aggregator = CreateNodeDatabaseMetricsAggregator(
                rootGroup,
                DATABASE_PATH,
                false /* isFollowerRole */
            );

            TFakeTablet leader1(1000, 0);
            TFakeTablet leader2(2000, 0);

            for (auto* tablet : {&leader1, &leader2}) {
                tablet->SetSimple(DB_UNIQUE_ROWS_TOTAL, 5);
                tablet->Report(aggregator, TDetailedMetricsSettings::MetricsLevelPartition, now);
            }
            aggregator->RecalculateAllCounters();

            UNIT_ASSERT(FindLeafCounters(rootGroup, leader1.TabletId, leader1.FollowerId));

            // The first partition to notice takes ONLY its own table's leaves with it,
            // including the leaf of the partition, which has not reported yet
            leader1.Report(aggregator, TDetailedMetricsSettings::MetricsLevelUnspecified, now);

            DumpCounters("Counters after the partition level table stopped collecting", rootGroup);

            UNIT_ASSERT(!FindLeafCounters(rootGroup, leader1.TabletId, leader1.FollowerId));
            UNIT_ASSERT(!FindLeafCounters(rootGroup, leader2.TabletId, leader2.FollowerId));
            UNIT_ASSERT(!FindTableGroup(rootGroup));
            UNIT_ASSERT(!rootGroup->FindSubgroup("database", DATABASE_PATH));
        }
    }

    /**
     * Verify that a per-table ALTER, which enables the detailed metrics of a table that
     * collected none, starts emitting the leaves.
     *
     * @note Unlike the database default change above, this one DOES bump the schema
     *       version, and it is the level, which decides what is built either way.
     */
    Y_UNIT_TEST(SchemaBumpEnablesPartitionLevel) {
        NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

        auto aggregator = CreateNodeDatabaseMetricsAggregator(
            rootGroup,
            DATABASE_PATH,
            false /* isFollowerRole */
        );

        const TInstant now = TInstant::Seconds(100);

        TFakeTablet leader(1000, 0);
        leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 5);

        // The table follows a database default, which collects nothing
        leader.Report(
            aggregator,
            TDetailedMetricsSettings::MetricsLevelUnspecified,
            now,
            TABLE_ID,
            TABLE_PATH,
            TABLET_TYPE,
            1 /* schemaVersion */
        );
        aggregator->RecalculateAllCounters();

        UNIT_ASSERT(!rootGroup->FindSubgroup("database", DATABASE_PATH));

        // ALTER TABLE ... SET (DETAILED_METRICS_LEVEL = PARTITION)
        leader.Report(
            aggregator,
            TDetailedMetricsSettings::MetricsLevelPartition,
            now,
            TABLE_ID,
            TABLE_PATH,
            TABLET_TYPE,
            2 /* schemaVersion */
        );
        aggregator->RecalculateAllCounters();

        DumpCounters("Counters after the ALTER enabled the partition level", rootGroup);

        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(
                FindLeafCounters(rootGroup, leader.TabletId, leader.FollowerId),
                "SUM(DbUniqueRowsTotal)"
            ),
            5
        );
    }

    /**
     * Verify that a schema version bump, which does NOT change the effective level,
     * keeps the counters of the table exactly where they are.
     *
     * @note The level is the only thing, which decides the shape of the entry, so a
     *       plain ALTER has nothing to reconcile. Rebuilding the table on every schema
     *       bump instead would restart the accumulated cumulative counters from zero —
     *       a consumer reads that as a counter reset — and would blank the leaves of
     *       the partitions, which have not reported since the ALTER.
     */
    Y_UNIT_TEST(SchemaBumpAtTheSameLevelKeepsTheCounters) {
        NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

        auto aggregator = CreateNodeDatabaseMetricsAggregator(
            rootGroup,
            DATABASE_PATH,
            false /* isFollowerRole */
        );

        const TInstant now = TInstant::Seconds(100);

        TFakeTablet leader1(1000, 0);
        TFakeTablet leader2(2000, 0);

        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1).AddCumulative(CONSUMED_CPU, 100);
        leader2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 2).AddCumulative(CONSUMED_CPU, 200);

        for (auto* tablet : {&leader1, &leader2}) {
            tablet->Report(aggregator, TDetailedMetricsSettings::MetricsLevelPartition, now);
        }
        aggregator->RecalculateAllCounters();

        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(
                FindLeafCounters(rootGroup, leader1.TabletId, leader1.FollowerId),
                "ConsumedCPU"
            ),
            100
        );

        // The ALTER bumps the schema version, and only the first partition has noticed
        // it so far
        leader1.AddCumulative(CONSUMED_CPU, 50);
        leader1.Report(
            aggregator,
            TDetailedMetricsSettings::MetricsLevelPartition,
            now,
            TABLE_ID,
            TABLE_PATH,
            TABLET_TYPE,
            2 /* schemaVersion */
        );
        aggregator->RecalculateAllCounters();

        DumpCounters("Counters after a schema bump at the very same level", rootGroup);

        // The accumulated counter keeps growing rather than restarting from the delta
        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(
                FindLeafCounters(rootGroup, leader1.TabletId, leader1.FollowerId),
                "ConsumedCPU"
            ),
            100 + 50
        );

        // ... and the partition, which has not reported since, keeps its own leaf
        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(
                FindLeafCounters(rootGroup, leader2.TabletId, leader2.FollowerId),
                "ConsumedCPU"
            ),
            200
        );
    }

    /**
     * Verify that the two instances of a node converge on a new level INDEPENDENTLY,
     * one report each, and that neither of them detaches a shared node the other still
     * writes under while they disagree.
     *
     * @note A level change reaches the instances through their own tablets' reports, so
     *       there is always a window where the leader instance has already switched and
     *       the follower one has not. The removals are routed through
     *       RemoveSubgroupChain, which tests every node it empties within that node's
     *       own lock, so the walk stops at the shared tablet_id= node for as long as
     *       either instance still has a leaf there.
     */
    Y_UNIT_TEST(LevelChangeConvergesBothInstances) {
        TRoleTrees trees;

        const TInstant now = TInstant::Seconds(100);

        // The leader of a partition and its follower, on one node, under ONE tablet_id=
        TFakeTablet leader(1000, 0);
        TFakeTablet follower(1000, 1);

        leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 42);
        follower.SetSimple(DB_UNIQUE_ROWS_TOTAL, 99);

        leader.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, now);
        follower.Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelPartition, now);

        trees.RecalculateAllCounters();

        UNIT_ASSERT(FindLeafCounters(trees.Root, leader.TabletId, leader.FollowerId));
        UNIT_ASSERT(FindLeafCounters(trees.Root, follower.TabletId, follower.FollowerId));

        // TEST 1: The leader instance notices the new level first
        leader.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelTable, now);
        trees.RecalculateAllCounters();

        DumpCounters("Counters while only the leader instance has switched", trees.Root);

        UNIT_ASSERT(!FindLeafCounters(trees.Root, leader.TabletId, leader.FollowerId));

        // The follower's leaf is untouched and still reachable from the SHARED root:
        // the leader instance removed its own leaf, and the walk stopped at the
        // tablet_id= node, which the follower instance still occupies
        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(
                FindLeafCounters(trees.Root, follower.TabletId, follower.FollowerId),
                "SUM(DbUniqueRowsTotal)"
            ),
            99
        );
        UNIT_ASSERT(
            trees.Root
                ->FindSubgroup("database", DATABASE_PATH)
                ->FindSubgroup("table", RELATIVE_TABLE_PATH)
                ->FindSubgroup("detailed_metrics", "per_partition")
                ->FindSubgroup("tablet_id", ToString(leader.TabletId))
                ->FindSubgroup("follower_id", ToString(follower.FollowerId))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindTableBucketCounters(trees.Root), "SUM(DbUniqueRowsTotal)"),
            42
        );

        // TEST 2: The follower instance converges one report later, and the last leaf
        //         takes the whole per-partition subtree with it — but not the table=
        //         node, which the collapse bucket of the other instance still holds
        follower.Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelTable, now);
        trees.RecalculateAllCounters();

        DumpCounters("Counters after both instances converged on the table level", trees.Root);

        auto tableGroup = FindTableGroup(trees.Root);
        UNIT_ASSERT(tableGroup);
        UNIT_ASSERT(!tableGroup->FindSubgroup("detailed_metrics", "per_partition"));

        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindTableBucketCounters(trees.Root), "SUM(DbUniqueRowsTotal)"),
            42
        );
    }

    /**
     * Verify that a straggler report from a tablet of a table PREVIOUS to the one now
     * living at this path is dropped whole rather than reconciled: it carries the old
     * (PathId, SchemaVersion), so nothing about it — its MetricsLevel included — says
     * anything about the table that lives at this path now.
     *
     * @note The scenario from the PR review: a table is dropped and recreated at the
     *       same path with a new PathId AND a different level. The new table's own
     *       tablet reports and builds the correct state. A tablet of the OLD table is
     *       still alive (draining) and keeps sending its own 5s report, which still
     *       carries the old PathId and the old level. Without the ordering guard in
     *       front of the level check, that stale report hits the level-mismatch branch,
     *       DropTableEntry wipes the NEW table's counters, and GetOrCreateTable rebuilds
     *       the entry from the stale (and now current) Info — the tree flaps between the
     *       two shapes every 5s until the old tablet is forgotten.
     */
    Y_UNIT_TEST(StaleReportOfARecreatedTableDoesNotDropTheNewState) {
        NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

        auto aggregator = CreateNodeDatabaseMetricsAggregator(
            rootGroup,
            DATABASE_PATH,
            false /* isFollowerRole */
        );

        const TInstant now = TInstant::Seconds(100);

        // TABLE_ID at the partition level: a per_partition leaf
        TFakeTablet straggler(1000, 0);
        straggler.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);
        straggler.Report(aggregator, TDetailedMetricsSettings::MetricsLevelPartition, now, TABLE_ID, TABLE_PATH);
        aggregator->RecalculateAllCounters();

        UNIT_ASSERT(FindLeafCounters(rootGroup, straggler.TabletId, straggler.FollowerId));

        // The table is dropped and recreated at the same path: a newer PathId, and the
        // level moves to TABLE. Its own tablet builds the correct collapse bucket.
        TFakeTablet newTablet(2000, 0);
        newTablet.SetSimple(DB_UNIQUE_ROWS_TOTAL, 7).AddCumulative(CONSUMED_CPU, 100);
        newTablet.Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now, RECREATED_TABLE_ID, TABLE_PATH);
        aggregator->RecalculateAllCounters();

        DumpCounters("Counters after the recreated table's own tablet reported", rootGroup);

        auto tableCounters = FindTableBucketCounters(rootGroup);
        UNIT_ASSERT(tableCounters);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(tableCounters, "SUM(DbUniqueRowsTotal)"), 7);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(tableCounters, "ConsumedCPU"), 100);
        UNIT_ASSERT(!FindLeafCounters(rootGroup, straggler.TabletId, straggler.FollowerId));

        // The OLD table's tablet is still alive and reports again, carrying the OLD
        // PathId and the OLD (partition) level — the straggler
        straggler.SetSimple(DB_UNIQUE_ROWS_TOTAL, 999);
        straggler.Report(aggregator, TDetailedMetricsSettings::MetricsLevelPartition, now, TABLE_ID, TABLE_PATH);
        aggregator->RecalculateAllCounters();

        DumpCounters("Counters after the straggler of the old table reported", rootGroup);

        // The new table's collapse bucket survives, its VALUES are exactly as they were
        // — a rebuilt-from-stale entry would have the right shape with zeroed contents,
        // which a presence-only check would not catch
        auto tableCountersAfterStraggler = FindTableBucketCounters(rootGroup);
        UNIT_ASSERT(tableCountersAfterStraggler);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(tableCountersAfterStraggler, "SUM(DbUniqueRowsTotal)"), 7);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(tableCountersAfterStraggler, "ConsumedCPU"), 100);

        // No per_partition subtree reappeared: the straggler's report was dropped whole,
        // never reaching GetOrCreateTable
        auto tableGroup = FindTableGroup(rootGroup);
        UNIT_ASSERT(tableGroup);
        UNIT_ASSERT(!tableGroup->FindSubgroup("detailed_metrics", "per_partition"));

        // The bucket is still the SAME live object, not a fresh one: a further report of
        // the new table's own tablet keeps accumulating rather than restarting from zero
        newTablet.AddCumulative(CONSUMED_CPU, 50);
        newTablet.Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now, RECREATED_TABLE_ID, TABLE_PATH);
        aggregator->RecalculateAllCounters();

        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(FindTableBucketCounters(rootGroup), "ConsumedCPU"), 100 + 50);

        // Forgetting the straggler's tablet finds no leaf and no source ID in the live
        // bucket: it never contributed to it, so this is a no-op, and the live table's
        // bucket and its table= group survive
        aggregator->ForgetTablet(straggler.TabletId, straggler.FollowerId);

        UNIT_ASSERT(FindTableGroup(rootGroup));
        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindTableBucketCounters(rootGroup), "ConsumedCPU"),
            100 + 50
        );
    }

    /**
     * Verify that a renamed table manifests as drop-old/create-new, with no stale
     * table= group left behind once the last of its tablets has reported the new path.
     *
     * @note A rename bumps the schema version and hands out a fresh PathId, but the key
     *       of the whole per-table state is the RELATIVE PATH, so the new path simply
     *       creates a new entry, and the tablets drain the old one as they move over —
     *       the very same path a tablet re-reported under another table takes.
     */
    Y_UNIT_TEST(TableRenameCreatesTheNewGroupAndDropsTheOld) {
        const TInstant now = TInstant::Seconds(100);

        // TEST 1: The table level, where the tablets share one collapse bucket
        {
            NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

            auto aggregator = CreateNodeDatabaseMetricsAggregator(
                rootGroup,
                DATABASE_PATH,
                false /* isFollowerRole */
            );

            TFakeTablet leader1(1000, 0);
            TFakeTablet leader2(2000, 0);

            leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);
            leader2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 2);

            for (auto* tablet : {&leader1, &leader2}) {
                tablet->Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);
            }
            aggregator->RecalculateAllCounters();

            UNIT_ASSERT_VALUES_EQUAL(
                GetCounterValue(FindTableBucketCounters(rootGroup), "SUM(DbUniqueRowsTotal)"),
                1 + 2
            );

            // The first partition reports the new path: the old table keeps the second
            // one for as long as it has not moved over
            leader1.Report(
                aggregator,
                TDetailedMetricsSettings::MetricsLevelTable,
                now,
                RENAMED_TABLE_ID,
                RENAMED_TABLE_PATH,
                TABLET_TYPE,
                2 /* schemaVersion */
            );
            aggregator->RecalculateAllCounters();

            DumpCounters("Counters while only one partition has reported the new path", rootGroup);

            UNIT_ASSERT_VALUES_EQUAL(
                GetCounterValue(FindTableBucketCounters(rootGroup), "SUM(DbUniqueRowsTotal)"),
                2
            );
            UNIT_ASSERT_VALUES_EQUAL(
                GetCounterValue(
                    FindTableBucketCounters(rootGroup, RENAMED_RELATIVE_TABLE_PATH),
                    "SUM(DbUniqueRowsTotal)"
                ),
                1
            );

            // The second one follows, and nothing of the old path is left
            leader2.Report(
                aggregator,
                TDetailedMetricsSettings::MetricsLevelTable,
                now,
                RENAMED_TABLE_ID,
                RENAMED_TABLE_PATH,
                TABLET_TYPE,
                2 /* schemaVersion */
            );
            aggregator->RecalculateAllCounters();

            DumpCounters("Counters after the rename was fully reported", rootGroup);

            UNIT_ASSERT(!FindTableGroup(rootGroup));
            UNIT_ASSERT_VALUES_EQUAL(
                GetCounterValue(
                    FindTableBucketCounters(rootGroup, RENAMED_RELATIVE_TABLE_PATH),
                    "SUM(DbUniqueRowsTotal)"
                ),
                1 + 2
            );

            // The reverse map points at the new path, so forgetting the tablets drops
            // the group of the renamed table and the database= node above it
            for (auto* tablet : {&leader1, &leader2}) {
                aggregator->ForgetTablet(tablet->TabletId, tablet->FollowerId);
            }

            UNIT_ASSERT(!FindTableGroup(rootGroup, RENAMED_RELATIVE_TABLE_PATH));
            UNIT_ASSERT(!rootGroup->FindSubgroup("database", DATABASE_PATH));
        }

        // TEST 2: The partition level, where every tablet owns a leaf of its own
        {
            NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

            auto aggregator = CreateNodeDatabaseMetricsAggregator(
                rootGroup,
                DATABASE_PATH,
                false /* isFollowerRole */
            );

            TFakeTablet leader(1000, 0);
            leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 5);

            leader.Report(aggregator, TDetailedMetricsSettings::MetricsLevelPartition, now);
            aggregator->RecalculateAllCounters();

            UNIT_ASSERT(FindLeafCounters(rootGroup, leader.TabletId, leader.FollowerId));

            leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 7);
            leader.Report(
                aggregator,
                TDetailedMetricsSettings::MetricsLevelPartition,
                now,
                RENAMED_TABLE_ID,
                RENAMED_TABLE_PATH,
                TABLET_TYPE,
                2 /* schemaVersion */
            );
            aggregator->RecalculateAllCounters();

            DumpCounters("Leaves after the rename", rootGroup);

            UNIT_ASSERT(!FindTableGroup(rootGroup));

            auto renamedLeaf = FindLeafCounters(
                rootGroup,
                leader.TabletId,
                leader.FollowerId,
                RENAMED_RELATIVE_TABLE_PATH
            );
            UNIT_ASSERT(renamedLeaf);
            UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(renamedLeaf, "SUM(DbUniqueRowsTotal)"), 7);

            aggregator->ForgetTablet(leader.TabletId, leader.FollowerId);

            UNIT_ASSERT(!rootGroup->FindSubgroup("database", DATABASE_PATH));
        }
    }

    /**
     * Verify that a tablet type with no detailed metrics allow-list publishes nothing.
     *
     * @note This is the production path: the aggregator falls back to
     *       GetDetailedMetricsCounterNames(tabletType), which returns nullptr for
     *       ColumnShard.
     */
    Y_UNIT_TEST(UnsupportedTabletTypePublishesNothing) {
        NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

        auto aggregator = CreateNodeDatabaseMetricsAggregator(
            rootGroup,
            DATABASE_PATH,
            false /* isFollowerRole */
        );

        const TInstant now = TInstant::Seconds(100);

        TFakeTablet tablet(1000, 0);
        tablet.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);
        tablet.Report(
            aggregator,
            TDetailedMetricsSettings::MetricsLevelPartition,
            now,
            TABLE_ID,
            TABLE_PATH,
            TTabletTypes::ColumnShard
        );

        aggregator->RecalculateAllCounters();

        UNIT_ASSERT(!rootGroup->FindSubgroup("database", DATABASE_PATH));
    }

    /**
     * Verify that a counter absent from the DataShard allow-list (NotInTheAllowList,
     * a simple counter deliberately outside SourceCounters) is published nowhere: not at
     * the table level bucket, not at the partition level leaf, neither under its raw name
     * nor under its SUM(...)/MAX(...) aggregates. An allow-listed counter right next to it
     * is still published, so the absence is the filter's doing, not an empty tree.
     */
    Y_UNIT_TEST(NameFilterDropsUnlistedCounters) {
        NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

        auto aggregator = CreateNodeDatabaseMetricsAggregator(
            rootGroup,
            DATABASE_PATH,
            false /* isFollowerRole */
        );

        const TInstant now = TInstant::Seconds(100);

        // Table level
        TFakeTablet tableTablet(1000, 0);
        tableTablet.SetSimple(DB_UNIQUE_ROWS_TOTAL, 5).SetSimple(NOT_IN_ALLOW_LIST, 123);
        tableTablet.Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);

        // Partition level, a different table so it gets its own leaf
        TFakeTablet partitionTablet(2000, 0);
        partitionTablet.SetSimple(DB_UNIQUE_ROWS_TOTAL, 7).SetSimple(NOT_IN_ALLOW_LIST, 456);
        partitionTablet.Report(
            aggregator,
            TDetailedMetricsSettings::MetricsLevelPartition,
            now,
            OTHER_TABLE_ID,
            OTHER_TABLE_PATH
        );

        aggregator->RecalculateAllCounters();

        DumpCounters("Counters with an unlisted counter reported", rootGroup);

        // Table level: the allow-listed neighbour is there, the unlisted counter is not,
        // neither raw nor as SUM(...)/MAX(...)
        auto tableCounters = FindTableBucketCounters(rootGroup);
        UNIT_ASSERT(tableCounters);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(tableCounters, "SUM(DbUniqueRowsTotal)"), 5);
        UNIT_ASSERT(!HasCounter(tableCounters, "NotInTheAllowList"));
        UNIT_ASSERT(!HasCounter(tableCounters, "SUM(NotInTheAllowList)"));
        UNIT_ASSERT(!HasCounter(tableCounters, "MAX(NotInTheAllowList)"));

        // Partition level: same story for the leaf
        auto leafCounters = FindLeafCounters(
            rootGroup,
            partitionTablet.TabletId,
            partitionTablet.FollowerId,
            OTHER_RELATIVE_TABLE_PATH
        );
        UNIT_ASSERT(leafCounters);
        UNIT_ASSERT_VALUES_EQUAL(GetCounterValue(leafCounters, "SUM(DbUniqueRowsTotal)"), 7);
        UNIT_ASSERT(!HasCounter(leafCounters, "NotInTheAllowList"));
        UNIT_ASSERT(!HasCounter(leafCounters, "SUM(NotInTheAllowList)"));
        UNIT_ASSERT(!HasCounter(leafCounters, "MAX(NotInTheAllowList)"));
    }

    /**
     * Verify that a reader, which holds the shared tree lock, never observes a partially
     * rebuilt histogram while the writer recalculates the aggregates.
     *
     * @note This is the regression test for the guard in RecalculateAllCounters().
     *       TAggregatedTabletCounters republishes a HIST(x) aggregate by resetting the
     *       histogram and refilling it one tablet at a time, so without that guard the
     *       reader sees a total anywhere between 0 and the number of the partitions. A
     *       torn histogram is a perfectly valid looking snapshot, which is why this
     *       asserts the contents rather than the absence of a crash.
     */
    Y_UNIT_TEST(ConcurrentReadDuringRecalculationSeesWholeHistogram) {
        constexpr ui32 PARTITION_COUNT = 8;
        constexpr ui32 WRITER_ITERATIONS = 2000;

        NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

        auto aggregator = CreateNodeDatabaseMetricsAggregator(
            rootGroup,
            DATABASE_PATH,
            false /* isFollowerRole */
        );

        TInstant now = TInstant::Seconds(100);

        // The set of the partitions never changes, and neither do their simple counters:
        // everything the reader asserts below is a constant of the whole test
        TVector<THolder<TFakeTablet>> partitions;
        ui64 expectedRowsSum = 0;
        for (ui32 i = 0; i < PARTITION_COUNT; ++i) {
            auto& partition = partitions.emplace_back(MakeHolder<TFakeTablet>(1000 + i, 0));
            partition->SetSimple(DB_UNIQUE_ROWS_TOTAL, i + 1);
            expectedRowsSum += i + 1;
        }

        // Report once up front, so that the reader finds the whole tree in place from its
        // very first iteration
        for (auto& partition : partitions) {
            partition->Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);
        }
        aggregator->RecalculateAllCounters();

        auto bucketCounters = FindTableBucketCounters(rootGroup);
        UNIT_ASSERT(bucketCounters);
        UNIT_ASSERT_VALUES_EQUAL(
            GetHistogramTotal(bucketCounters, "HIST(ConsumedCPU)"),
            PARTITION_COUNT
        );

        TLockedReaderThread reader([&]() -> TString {
            // Looked up by hand rather than through GetHistogramTotal()/GetCounterValue(),
            // which UNIT_ASSERT_C on a missing histogram/counter: an assert off this
            // thread panics and aborts the whole test chunk instead of just failing this
            // one check (see TLockedReaderThread's own comment)
            auto histogram = bucketCounters->FindHistogram("HIST(ConsumedCPU)");
            if (!histogram) {
                return "no histogram HIST(ConsumedCPU)";
            }

            // Every partition contributes exactly one observation, so any other total
            // means the reader landed inside the reset-then-refill window of a
            // recalculation
            auto snapshot = histogram->Snapshot();
            ui64 observations = 0;
            for (ui32 i = 0; i < snapshot->Count(); ++i) {
                observations += snapshot->Value(i);
            }
            if (observations != PARTITION_COUNT) {
                return TStringBuilder() << "a torn HIST(ConsumedCPU): " << observations
                    << " observations instead of " << PARTITION_COUNT;
            }

            // The simple counter aggregates are assigned rather than rebuilt, and their
            // sources never change, so they must not budge either
            auto rowsSumCounter = bucketCounters->FindNamedCounter("sensor", "SUM(DbUniqueRowsTotal)");
            if (!rowsSumCounter) {
                return "no counter SUM(DbUniqueRowsTotal)";
            }

            const ui64 rowsSum = rowsSumCounter->Val();
            if (rowsSum != expectedRowsSum) {
                return TStringBuilder() << "SUM(DbUniqueRowsTotal) is " << rowsSum
                    << " instead of " << expectedRowsSum;
            }

            return {};
        });

        for (ui32 iteration = 0; iteration < WRITER_ITERATIONS; ++iteration) {
            // The recalculation rebuilds a histogram only for a counter, whose value has
            // actually changed, so the per second rate of ConsumedCPU has to differ from
            // the one of the previous iteration, or there would be no window to hit
            now += TDuration::Seconds(1);
            const ui64 consumedCpu = 1 + iteration % 3;

            for (auto& partition : partitions) {
                partition->AddCumulative(CONSUMED_CPU, consumedCpu);
                partition->Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);
            }

            aggregator->RecalculateAllCounters();
        }

        UNIT_ASSERT(reader.Join() > 0);
    }

    /**
     * Verify that a reader, which holds the shared tree lock, never observes a half built
     * leaf while the writer creates and drops the leaves of a partition level table.
     *
     * @note A leaf group and the low level counters underneath it are created in two
     *       steps, and this is what asserts that both steps are inside one critical
     *       section: a leaf, which the reader can see, always carries its counters.
     */
    Y_UNIT_TEST(ConcurrentStructuralChurnKeepsTheTreeConsistent) {
        constexpr ui32 PARTITION_COUNT = 16;
        constexpr ui32 WRITER_ITERATIONS = 200;

        NMonitoring::TDynamicCounterPtr rootGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();

        auto aggregator = CreateNodeDatabaseMetricsAggregator(
            rootGroup,
            DATABASE_PATH,
            false /* isFollowerRole */
        );

        TInstant now = TInstant::Seconds(100);

        TVector<THolder<TFakeTablet>> partitions;
        for (ui32 i = 0; i < PARTITION_COUNT; ++i) {
            auto& partition = partitions.emplace_back(MakeHolder<TFakeTablet>(1000 + i, 0));
            partition->SetSimple(DB_UNIQUE_ROWS_TOTAL, i + 1);
        }

        TLockedReaderThread reader([&]() -> TString {
            for (ui32 i = 0; i < PARTITION_COUNT; ++i) {
                const ui64 tabletId = 1000 + i;

                auto leafGroup = FindLeafGroup(rootGroup, tabletId, 0);
                if (!leafGroup) {
                    // The writer has not created this leaf yet, or has already dropped it
                    continue;
                }

                // A leaf, which exists at all, is fully built: its type=/category=
                // subtree is there and the low level counters are already published.
                // Their VALUES are not checked, because a freshly created leaf carries
                // its aggregates only from the next recalculation on
                auto leafCounters = FindExecutorCountersGroup(leafGroup);
                if (!leafCounters || !leafCounters->FindNamedCounter("sensor", "SUM(DbUniqueRowsTotal)")) {
                    return TStringBuilder() << "a half built leaf: the tablet " << tabletId
                        << " has no executor counters";
                }

                auto leafAppCounters = FindAppCountersGroup(leafGroup);
                if (!leafAppCounters
                    || !leafAppCounters->FindNamedCounter("sensor", "DataShard/EngineHostRowUpdates"))
                {
                    return TStringBuilder() << "a half built leaf: the tablet " << tabletId
                        << " has no application counters";
                }
            }

            return {};
        });

        for (ui32 iteration = 0; iteration < WRITER_ITERATIONS; ++iteration) {
            now += TDuration::Seconds(1);

            for (ui32 i = 0; i < PARTITION_COUNT; ++i) {
                auto& partition = partitions[i];
                partition->AddCumulative(CONSUMED_CPU, 1 + i);
                partition->Report(aggregator, TDetailedMetricsSettings::MetricsLevelPartition, now);

                // Drop the previous leaf right after creating this one, so that the whole
                // per_partition subtree — and, on the wrap around, the table= and
                // database= nodes above it — is torn down and rebuilt under the reader
                const ui32 previous = (i + PARTITION_COUNT - 1) % PARTITION_COUNT;
                aggregator->ForgetTablet(partitions[previous]->TabletId, 0);
            }

            aggregator->RecalculateAllCounters();

            // Now and then drop the very last leaf too, so that the emptied table= and
            // database= nodes above it are reclaimed and rebuilt under the reader
            if (iteration % 8 == 0) {
                for (auto& partition : partitions) {
                    aggregator->ForgetTablet(partition->TabletId, 0);
                }

                UNIT_ASSERT(!rootGroup->FindSubgroup("database", DATABASE_PATH));
            }
        }

        UNIT_ASSERT(reader.Join() > 0);
    }
}
