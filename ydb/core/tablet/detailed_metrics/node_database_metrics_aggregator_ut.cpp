#include "node_database_metrics_aggregator.h"
#include "detailed_metrics_binding.h"
#include "detailed_metrics_counter_set.h"
#include "detailed_values_accumulator.h"
#include "ut_helpers.h"

#include <ydb/core/sys_view/service/db_counters_codec.h>
#include <ydb/core/tablet/private/aggregated_tablet_counters.h>
#include <ydb/core/tablet/tablet_counters_app.h>
#include <ydb/core/tablet_flat/flat_executor_counters.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/array_size.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/system/align.h>

#include <atomic>
#include <thread>
#include <tuple>

using namespace NKikimr;
using namespace NKikimr::NDetailedMetricsTests;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString DATABASE_PATH = "/Root/db";

const TString TABLE_PATH = "/Root/db/dir/table";

// Another table of the very same database
const TString OTHER_TABLE_PATH = "/Root/db/dir/other_table";

// The very same table after an ESchemeOpMoveTable rename: another path, reported by
// the very same tablets. Whatever changes upstream (a fresh PathId, a bumped schema
// version) is the Tablet Counters Aggregator's concern, not this class's — see the
// class comment in the header. All this layer ever sees is a different path.
const TString RENAMED_TABLE_PATH = "/Root/db/dir/renamed_table";

constexpr TTabletTypes::EType TABLET_TYPE = TTabletTypes::DataShard;

////////////////////////////////////////////////////////////////////////////////
// A small stand-in for the low level counters of Data Shard. The real counter set
// has hundreds of counters, which would make the assertions unreadable without
// covering anything, which is not covered by these few counters.

constexpr const char* EXECUTOR_SIMPLE_COUNTER_NAMES[] = {
    "DbUniqueRowsTotal",
    "DbUniqueDataBytes",
    // Absent from the DataShard allow-list (ydb/core/protos/counters_detailed_datashard.proto),
    // used to verify that a counter outside the public metrics is reported nowhere
    // (see NameFilterDropsUnlistedCounters)
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
        const TString& tablePath = TABLE_PATH,
        TTabletTypes::EType tabletType = TABLET_TYPE
    ) {
        // An empty baseline (the very first report) makes the diff a plain copy
        auto appDiff = AppCounters.MakeDiffForAggr(AppBaseline);
        auto executorDiff = ExecutorCounters.MakeDiffForAggr(ExecutorBaseline);

        aggregator->AddCounters(
            tablePath,
            level,
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
 * @param[in] tabletId The ID of the tablet
 * @param[in] followerId The follower ID of the tablet (0 for the leader)
 * @param[in] tablePath The absolute path of the table, as the tablet reports it
 *
 * @return The PARTITION leaf of the tablet on the detailed wire, read through TPackedReceiver
 *
 * @note The leaves are observed through Pack(), the public metric values, which the node
 *       reports to the SysView Processor.
 */
TPackedBucketId Leaf(ui64 tabletId, ui32 followerId, const TString& tablePath = TABLE_PATH) {
    return TPackedBucketId::Leaf(tablePath, tabletId, followerId);
}

using TPackedTables = NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters>;

TPackedTables PackOnce(const TNodeDatabaseMetricsAggregatorPtr& aggregator) {
    TPackedTables out;
    aggregator->Pack(out);
    return out;
}

/**
 * @return The packed table entry of the given level (or nullptr if there is none),
 *         tagged with the tablet type, whose public metrics define the slots of its values
 */
const NKikimrSysView::TDetailedTableCounters* FindPackedTable(
    const TPackedTables& tables, EDetailedMetricsLevel level, const TString& tablePath = TABLE_PATH)
{
    for (const auto& table : tables) {
        if (table.GetTablePath() == tablePath && table.GetLevel() == level) {
            UNIT_ASSERT_VALUES_EQUAL(table.GetTabletType(), TABLET_TYPE);
            for (const auto& leaf : table.GetLeaves()) {
                UNIT_ASSERT(leaf.HasMetrics());
            }
            return &table;
        }
    }
    return nullptr;
}

const NKikimrSysView::TDetailedTableCounters::TLeaf* FindPackedLeaf(
    const NKikimrSysView::TDetailedTableCounters& table, ui64 tabletId, ui32 followerId)
{
    for (const auto& leaf : table.GetLeaves()) {
        if (leaf.GetTabletId() == tabletId && leaf.GetFollowerId() == followerId) {
            return &leaf;
        }
    }
    return nullptr;
}

/**
 * @return The packed public metric values of the only bucket of the given level:
 *         the TABLE bucket or the leaf of the leader 1000
 */
const NKikimrSysView::TDbCounters& GetSinglePackedCounters(
    const TPackedTables& tables, EDetailedMetricsLevel level)
{
    size_t matchingTables = 0;
    for (const auto& table : tables) {
        matchingTables += table.GetTablePath() == TABLE_PATH && table.GetLevel() == level;
    }
    UNIT_ASSERT_VALUES_EQUAL(matchingTables, 1);
    const auto* table = FindPackedTable(tables, level);
    if (level == TDetailedMetricsSettings::MetricsLevelTable) {
        UNIT_ASSERT(table->HasTableMetrics());
        UNIT_ASSERT_VALUES_EQUAL(table->LeavesSize(), 0);
        return table->GetTableMetrics();
    }
    UNIT_ASSERT(!table->HasTableMetrics());
    UNIT_ASSERT_VALUES_EQUAL(table->LeavesSize(), 1);
    const auto* leaf = FindPackedLeaf(*table, 1000, 0);
    UNIT_ASSERT(leaf);
    return leaf->GetMetrics();
}

ui64 GetPackedCumulativeDelta(const NKikimrSysView::TDbCounters& counters, ui32 index) {
    const auto& values = counters.GetCumulative();
    for (int i = 0; i + 1 < values.size(); i += 2) {
        if (values.Get(i) == index) {
            return values.Get(i + 1);
        }
    }
    return 0;
}

/**
 * @param[in] counters The packed counters of a bucket
 * @param[in] index The position of a non-derivative histogram
 *
 * @return The values of all the buckets of the histogram
 *
 * @note A non-derivative histogram is packed as its full current value: sparse
 *       (bucket, value) pairs, which replace whatever the receiver holds. It is present
 *       (possibly with no pairs at all) even when every bucket is empty.
 */
TVector<ui64> GetPackedNonDerivativeHistogram(const NKikimrSysView::TDbCounters& counters, ui32 index) {
    UNIT_ASSERT_C(index < (ui32)counters.HistogramSize(), "no histogram " << index);
    const auto& histogram = counters.GetHistogram(index);
    UNIT_ASSERT_C(histogram.GetNonDerivative(), "the histogram " << index << " is not marked NonDerivative");
    UNIT_ASSERT(histogram.HasBucketsCount());

    TVector<ui64> values(histogram.GetBucketsCount(), 0);
    const auto& encoded = histogram.GetBuckets();
    UNIT_ASSERT_VALUES_EQUAL(encoded.size() % 2, 0);
    for (int i = 0; i + 1 < encoded.size(); i += 2) {
        UNIT_ASSERT_C(encoded.Get(i) < values.size(), "bucket " << encoded.Get(i) << " is out of range");
        // A zero bucket is not encoded
        UNIT_ASSERT_VALUES_UNEQUAL(encoded.Get(i + 1), 0);
        values[encoded.Get(i)] = encoded.Get(i + 1);
    }
    return values;
}

ui64 GetPackedNonDerivativeHistogramTotal(const NKikimrSysView::TDbCounters& counters, ui32 index) {
    ui64 total = 0;
    for (ui64 value : GetPackedNonDerivativeHistogram(counters, index)) {
        total += value;
    }
    return total;
}

/**
 * The two aggregators of a node, one per role, as the two Tablet Counters Aggregator actors
 * build them; Settle() folds both into one report.
 */
struct TRoleTrees {
    TNodeDatabaseMetricsAggregatorPtr Leaders = CreateNodeDatabaseMetricsAggregator(
        DATABASE_PATH,
        false /* isFollowerRole */
    );

    TNodeDatabaseMetricsAggregatorPtr Followers = CreateNodeDatabaseMetricsAggregator(
        DATABASE_PATH,
        true /* isFollowerRole */
    );

    /**
     * Fold the reports of both roles, which the node sends to the SysView Processor as one,
     * until only the live buckets are left (see TPackedReceiver::Settle()).
     */
    const TPackedReceiver& Settle() {
        Packed.Settle({Leaders.Get(), Followers.Get()});
        return Packed;
    }

    TPackedReceiver Packed;
};

////////////////////////////////////////////////////////////////////////////////

/**
 * A reader thread, which runs the given check over and over off the main thread, the way
 * the SysView Service calls Pack(), until the writer on the main thread says it is done.
 *
 * @param[in] check Returns the description of the very first violation it finds, or an
 *                   empty string when the buckets look whole
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
     * @note The bucket belongs to the aggregator of the leaders alone.
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

        // The single bucket holds the 3 leader partitions and nothing else
        const auto& packed = trees.Settle();
        const auto table = TPackedBucketId::Table(TABLE_PATH);
        UNIT_ASSERT_VALUES_EQUAL(packed.LiveCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(table, ROW_COUNT), 1 + 2 + 4);
        UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(table, SIZE_BYTES), 10 + 20 + 40);
        UNIT_ASSERT_VALUES_EQUAL(packed.Rate(table, CONSUMED_CPU_MICROSECONDS), 100 + 200 + 400);
        UNIT_ASSERT_VALUES_EQUAL(packed.Rate(table, WRITE_ROWS), 1000 + 2000 + 4000);
        UNIT_ASSERT_VALUES_EQUAL(packed.HistTotal(table, USED_CORE_PERCENTS), 3);
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
        // them land in ONE report of the node, told apart by follower_id alone.
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

        // Every leaf holds exactly what its tablet has reported, all in the one report of the
        // node. A follower leaf carries no LeaderOnly metric (row_count, write.rows), so what
        // tells it apart is the work of the follower itself
        const auto& packed = trees.Settle();
        UNIT_ASSERT_VALUES_EQUAL(packed.LiveCount(), expectedValues.size());
        for (const auto& [tablet, expectedValue] : expectedValues) {
            const auto& [tabletId, followerId] = tablet;
            const auto leaf = Leaf(tabletId, followerId);
            UNIT_ASSERT_C(packed.Exists(leaf), "no leaf for " << tabletId << ":" << followerId);

            const bool isLeader = followerId == 0;
            UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(leaf, ROW_COUNT), isLeader ? expectedValue : 0);
            UNIT_ASSERT_VALUES_EQUAL(packed.Rate(leaf, CONSUMED_CPU_MICROSECONDS), expectedValue * 100);
            UNIT_ASSERT_VALUES_EQUAL(packed.Rate(leaf, WRITE_ROWS), isLeader ? expectedValue * 10 : 0);
            UNIT_ASSERT_VALUES_EQUAL(packed.HistTotal(leaf, USED_CORE_PERCENTS), 1);
        }

        // No table bucket and no on-node rollup at the partition level: the report holds
        // the leaves and nothing else
        UNIT_ASSERT(!packed.Exists(TPackedBucketId::Table(TABLE_PATH)));
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
        auto aggregator = CreateNodeDatabaseMetricsAggregator(
            DATABASE_PATH,
            false /* isFollowerRole */
        );

        TFakeTablet leader1(1000, 0);
        TFakeTablet leader2(2000, 0);
        TPackedReceiver packed;
        const auto table = TPackedBucketId::Table(TABLE_PATH);

        // TICK 1: both partitions report a non-zero delta
        TInstant now = TInstant::Seconds(100);

        leader1.AddCumulative(CONSUMED_CPU, 100);
        leader2.AddCumulative(CONSUMED_CPU, 200);

        for (auto* tablet : {&leader1, &leader2}) {
            tablet->Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);
        }

        packed.Settle(*aggregator);
        UNIT_ASSERT_VALUES_EQUAL(packed.Rate(table, CONSUMED_CPU_MICROSECONDS), 100 + 200);

        // TICK 2: 10 seconds later only the first partition does any work
        now += TDuration::Seconds(10);

        leader1.AddCumulative(CONSUMED_CPU, 300);

        for (auto* tablet : {&leader1, &leader2}) {
            tablet->Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);
        }

        // The accumulated value keeps growing and never goes backwards
        packed.Settle(*aggregator);
        UNIT_ASSERT_VALUES_EQUAL(packed.Rate(table, CONSUMED_CPU_MICROSECONDS), 100 + 200 + 300);

        // The CPU observation of a tablet is its own per second rate since its previous report:
        // 300 over 10 seconds (the <=100 bucket of the ranges {0, 10, 100}) and 0 of the idle one
        const auto histogram = packed.Hist(table, USED_CORE_PERCENTS);
        UNIT_ASSERT_VALUES_EQUAL(histogram[0], 1);
        UNIT_ASSERT_VALUES_EQUAL(histogram[2], 1);

        // TICK 3: nobody does any work at all
        now += TDuration::Seconds(10);

        for (auto* tablet : {&leader1, &leader2}) {
            tablet->Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);
        }

        packed.Settle(*aggregator);
        UNIT_ASSERT_VALUES_EQUAL(packed.Rate(table, CONSUMED_CPU_MICROSECONDS), 100 + 200 + 300);
        UNIT_ASSERT_VALUES_EQUAL(packed.Hist(table, USED_CORE_PERCENTS)[0], 2);
    }

    /**
     * Verify that the histogram aggregate named HIST(x) ends up in the TABLE bucket,
     * filled here from the counter named x. DataShard's allow-list has no ordinary
     * percentile counter, only this synthesized one.
     */
    Y_UNIT_TEST(PercentileCountersAreAggregated) {
        auto aggregator = CreateNodeDatabaseMetricsAggregator(
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

        // The histogram aggregate holds one observation per partition, taken from
        // the "ConsumedCPU" cumulative counter. The tablets do NOT fill it themselves,
        // so an empty histogram here would mean the aggregate is never fed.
        TPackedReceiver packed;
        packed.Settle(*aggregator);
        UNIT_ASSERT_VALUES_EQUAL(packed.HistTotal(TPackedBucketId::Table(TABLE_PATH), USED_CORE_PERCENTS), 2);
    }

    /**
     * Verify that forgetting a tablet drops its observations from the HIST(x) percentile
     * aggregate of the table bucket, while the accumulated cumulative counters keep
     * the work the tablet had already done.
     */
    Y_UNIT_TEST(ForgetTabletDropsPercentileObservations) {
        auto aggregator = CreateNodeDatabaseMetricsAggregator(
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

        // The very first report of a tablet contributes a 0 observation (there is no
        // previous report of it to derive a per second rate from), so both partitions'
        // observations land in the <=0 bucket of the ranges {0, 10, 100}
        TPackedReceiver packed;
        packed.Settle(*aggregator);
        const auto table = TPackedBucketId::Table(TABLE_PATH);
        UNIT_ASSERT_VALUES_EQUAL(packed.Hist(table, USED_CORE_PERCENTS)[0], 2);
        UNIT_ASSERT_VALUES_EQUAL(packed.HistTotal(table, USED_CORE_PERCENTS), 2);
        UNIT_ASSERT_VALUES_EQUAL(packed.Rate(table, CONSUMED_CPU_MICROSECONDS), 100 + 200);

        // The second partition is gone
        aggregator->ForgetTablet(leader2.TabletId, leader2.FollowerId);

        // The histogram aggregate is rebuilt from the surviving partitions only
        packed.Settle(*aggregator);
        UNIT_ASSERT_VALUES_EQUAL(packed.Hist(table, USED_CORE_PERCENTS)[0], 1);
        UNIT_ASSERT_VALUES_EQUAL(packed.HistTotal(table, USED_CORE_PERCENTS), 1);

        // The accumulated cumulative counter is NOT reduced: the CPU the forgotten
        // partition had burnt has still been burnt, and the series must not go backwards
        UNIT_ASSERT_VALUES_EQUAL(packed.Rate(table, CONSUMED_CPU_MICROSECONDS), 100 + 200);

        // The last partition is gone too, and the bucket is retired
        aggregator->ForgetTablet(leader1.TabletId, leader1.FollowerId);

        packed.Settle(*aggregator);
        UNIT_ASSERT(!packed.Exists(table));
        UNIT_ASSERT_VALUES_EQUAL(packed.LiveCount(), 0);
    }

    /**
     * Verify that forgetting a tablet of a table level table drops its contribution
     * from the table bucket and retires the bucket once empty.
     *
     * @note The table bucket belongs to the aggregator of the leaders alone, so the
     *       followers of a table level table contribute nothing at all.
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

        // The follower of a table level table is dropped on the floor: only the leaders
        // are in the bucket
        const auto table = TPackedBucketId::Table(TABLE_PATH);
        UNIT_ASSERT_VALUES_EQUAL(trees.Settle().LiveCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(trees.Packed.Gauge(table, ROW_COUNT), 1 + 2);

        // TEST 1: The table bucket is recomputed from the surviving partitions
        trees.Leaders->ForgetTablet(leader2.TabletId, leader2.FollowerId);

        UNIT_ASSERT_VALUES_EQUAL(trees.Settle().Gauge(table, ROW_COUNT), 1);

        // TEST 2: The bucket is retired once its last tablet is gone. Nothing else is
        //         left: the follower of a table level table never built anything of its own
        trees.Leaders->ForgetTablet(leader1.TabletId, leader1.FollowerId);

        UNIT_ASSERT(!trees.Settle().Exists(table));
        UNIT_ASSERT_VALUES_EQUAL(trees.Packed.LiveCount(), 0);

        // TEST 3: Forgetting the follower, which never contributed, is not an error
        trees.Followers->ForgetTablet(follower.TabletId, follower.FollowerId);

        // TEST 4: Forgetting an unknown tablet is not an error
        trees.Leaders->ForgetTablet(leader1.TabletId, leader1.FollowerId);

        // TEST 5: A tablet reported after the teardown creates the bucket afresh
        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 3);
        leader1.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelTable, now);

        UNIT_ASSERT_VALUES_EQUAL(trees.Settle().Gauge(table, ROW_COUNT), 3);
    }

    /**
     * Verify that forgetting a tablet of a partition level table removes its own leaf
     * and ONLY its own leaf.
     *
     * @note The leader of a partition and its followers may run on one node and are
     *       reported by the two different aggregators, while the followers of a partition
     *       are reported by one and the same aggregator under one tablet ID.
     */
    Y_UNIT_TEST(ForgetTabletAtPartitionLevel) {
        TRoleTrees trees;

        const TInstant now = TInstant::Seconds(100);

        // Two followers of the same partition plus one of another partition
        TFakeTablet follower1(1000, 1);
        TFakeTablet follower2(1000, 2);
        TFakeTablet follower3(2000, 1);

        // ... and the leader of the first partition, on this very node, reported by the
        // OTHER aggregator
        TFakeTablet leader1(1000, 0);

        for (auto* tablet : {&follower1, &follower2, &follower3}) {
            tablet->SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);
            tablet->Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelPartition, now);
        }

        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 7);
        leader1.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, now);

        // TEST 1: Only the leaf of the forgotten tablet is removed, its siblings survive
        trees.Followers->ForgetTablet(follower1.TabletId, follower1.FollowerId);

        UNIT_ASSERT(!trees.Settle().Exists(Leaf(follower1.TabletId, follower1.FollowerId)));
        UNIT_ASSERT(trees.Packed.Exists(Leaf(follower2.TabletId, follower2.FollowerId)));
        UNIT_ASSERT(trees.Packed.Exists(Leaf(leader1.TabletId, leader1.FollowerId)));

        // TEST 2: Emptying the followers of a partition must NOT take the leaf of its
        //         leader with it, reported by the other aggregator
        trees.Followers->ForgetTablet(follower2.TabletId, follower2.FollowerId);
        trees.Followers->ForgetTablet(follower3.TabletId, follower3.FollowerId);

        const auto& packed = trees.Settle();
        UNIT_ASSERT_VALUES_EQUAL(packed.LiveCount(), 1);
        UNIT_ASSERT(packed.Exists(Leaf(leader1.TabletId, leader1.FollowerId)));
        UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(Leaf(leader1.TabletId, leader1.FollowerId), ROW_COUNT), 7);

        // TEST 3: The last leaf goes too, and nothing of this database is reported any more
        trees.Leaders->ForgetTablet(leader1.TabletId, leader1.FollowerId);

        UNIT_ASSERT(!trees.Settle().Exists(Leaf(leader1.TabletId, leader1.FollowerId)));
        UNIT_ASSERT_VALUES_EQUAL(trees.Packed.LiveCount(), 0);

        // TEST 4: The two instances create the leaves from scratch afterwards. The follower
        //         leaf carries no LeaderOnly metric, so the work of the follower is what it reports
        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 5);
        leader1.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, now);

        follower1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 6).AddCumulative(CONSUMED_CPU, 6);
        follower1.Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelPartition, now);

        const auto& comeback = trees.Settle();
        UNIT_ASSERT_VALUES_EQUAL(comeback.Gauge(Leaf(leader1.TabletId, leader1.FollowerId), ROW_COUNT), 5);
        UNIT_ASSERT_VALUES_EQUAL(comeback.Gauge(Leaf(follower1.TabletId, follower1.FollowerId), ROW_COUNT), 0);
        UNIT_ASSERT_VALUES_EQUAL(
            comeback.Rate(Leaf(follower1.TabletId, follower1.FollowerId), CONSUMED_CPU_MICROSECONDS), 6);
    }

    /**
     * Verify that emptying one table of a database leaves its other tables reported.
     */
    Y_UNIT_TEST(ForgetTabletKeepsTheOtherTablesOfTheDatabase) {
        for (auto level : {TDetailedMetricsSettings::MetricsLevelTable, TDetailedMetricsSettings::MetricsLevelPartition}) {
            const bool isTableLevel = level == TDetailedMetricsSettings::MetricsLevelTable;
            auto bucketOf = [isTableLevel](const TFakeTablet& tablet, const TString& tablePath) {
                return isTableLevel
                    ? TPackedBucketId::Table(tablePath)
                    : Leaf(tablet.TabletId, tablet.FollowerId, tablePath);
            };

            TRoleTrees trees;

            const TInstant now = TInstant::Seconds(100);

            TFakeTablet leader(1000, 0);
            TFakeTablet otherLeader(2000, 0);

            leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);
            leader.Report(trees.Leaders, level, now);

            otherLeader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 2);
            otherLeader.Report(trees.Leaders, level, now, OTHER_TABLE_PATH);

            // The only tablet of the first table is gone: that table goes with it, and
            // the second table is still reported
            trees.Leaders->ForgetTablet(leader.TabletId, leader.FollowerId);

            const auto& packed = trees.Settle();
            UNIT_ASSERT(!packed.Exists(bucketOf(leader, TABLE_PATH)));

            const auto survivingBucket = bucketOf(otherLeader, OTHER_TABLE_PATH);
            UNIT_ASSERT(packed.Exists(survivingBucket));
            UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(survivingBucket, ROW_COUNT), 2);

            // The second table goes too, and now nothing of the database is reported
            trees.Leaders->ForgetTablet(otherLeader.TabletId, otherLeader.FollowerId);

            UNIT_ASSERT_VALUES_EQUAL(trees.Settle().LiveCount(), 0);
        }
    }

    /**
     * Verify that the tables, which do not collect detailed metrics, are ignored.
     */
    Y_UNIT_TEST(IgnoresTablesWithoutDetailedMetrics) {
        auto aggregator = CreateNodeDatabaseMetricsAggregator(
            DATABASE_PATH,
            false /* isFollowerRole */
        );

        const TInstant now = TInstant::Seconds(100);

        TFakeTablet tablet(1000, 0);
        tablet.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);

        tablet.Report(aggregator, TDetailedMetricsSettings::MetricsLevelUnspecified, now);
        tablet.Report(aggregator, TDetailedMetricsSettings::MetricsLevelDisabled, now);

        // Nothing is reported for such tables
        UNIT_ASSERT(PackOnce(aggregator).empty());
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
     * Leaving the TABLE bucket to the aggregator of the leaders is what keeps the follower
     * out of it, and it agrees with the rule that the high level table.datashard.* metrics
     * are computed from the leaders alone.
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

        // The single table bucket reads 100: the true row count, neither doubled
        // by the follower nor overwritten by it
        const auto& packed = trees.Settle();
        const auto table = TPackedBucketId::Table(TABLE_PATH);
        UNIT_ASSERT_VALUES_EQUAL(packed.LiveCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(table, ROW_COUNT), 100);

        // The follower contributed nothing at all, not even its cumulative counters:
        // at the table level its work is simply not collected on the node
        UNIT_ASSERT_VALUES_EQUAL(packed.Rate(table, CONSUMED_CPU_MICROSECONDS), 7);
        UNIT_ASSERT_VALUES_EQUAL(packed.HistTotal(table, USED_CORE_PERCENTS), 1);
    }

    /**
     * Verify that at the partition level, leaves of both roles are kept by their
     * respective aggregators and never merged, even on the same node.
     *
     * This is deliberate on the node: a leaf is single-owner and passes through
     * verbatim, and there is no on-node rollup at partition granularity. A follower leaf
     * carries no LeaderOnly metric, and the rollup takes those metrics from leaders only.
     *
     * This test documents the boundary: the two roles are reported as separate leaves
     * of one tablet, told apart by the follower ID, never merged.
     */
    Y_UNIT_TEST(PartitionLeavesCarryBothRolesByDesign) {
        TRoleTrees trees;

        const TInstant now = TInstant::Seconds(100);

        // The leader holds one value, the follower another, so we can verify each lands
        // in its own leaf of the ONE report of the node
        TFakeTablet leader(1000, 0);
        leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 42).AddCumulative(CONSUMED_CPU, 42);

        TFakeTablet follower(1000, 1);
        follower.SetSimple(DB_UNIQUE_ROWS_TOTAL, 99).AddCumulative(CONSUMED_CPU, 99);

        leader.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, now);
        follower.Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelPartition, now);

        const auto& packed = trees.Settle();
        UNIT_ASSERT_VALUES_EQUAL(packed.LiveCount(), 2);

        // follower_id=0 is the leader, carrying the leader's values
        UNIT_ASSERT(packed.Exists(Leaf(1000, 0)));
        UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(Leaf(1000, 0), ROW_COUNT), 42);
        UNIT_ASSERT_VALUES_EQUAL(packed.Rate(Leaf(1000, 0), CONSUMED_CPU_MICROSECONDS), 42);

        // follower_id=1 is the replica, carrying the replica's own work, and no LeaderOnly
        // metric (row_count) at all
        UNIT_ASSERT(packed.Exists(Leaf(1000, 1)));
        UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(Leaf(1000, 1), ROW_COUNT), 0);
        UNIT_ASSERT_VALUES_EQUAL(packed.Rate(Leaf(1000, 1), CONSUMED_CPU_MICROSECONDS), 99);
    }

    /**
     * Verify that a tablet, which is re-reported under another table, is MOVED rather than
     * copied: its contribution to the previous table is dropped.
     *
     * @note The reverse map holds one table per tablet, because the forget event carries no
     *       table identity. Overwriting the entry without cleaning up would leave the old
     *       table's contribution in the report with nothing left able to reach it: neither
     *       ForgetTablet, which now routes to the new table, nor the next report.
     */
    Y_UNIT_TEST(TabletReportedUnderAnotherTableIsMoved) {
        const TInstant now = TInstant::Seconds(100);

        // TEST 1: The table level, where the tablet contributes to a shared bucket
        {
            auto aggregator = CreateNodeDatabaseMetricsAggregator(
                DATABASE_PATH,
                false /* isFollowerRole */
            );

            TFakeTablet leader(1000, 0);
            TPackedReceiver packed;

            leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 5);
            leader.Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);

            packed.Settle(*aggregator);
            UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(TPackedBucketId::Table(TABLE_PATH), ROW_COUNT), 5);

            // The very same tablet now reports another table of the same database
            leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 7);
            leader.Report(
                aggregator,
                TDetailedMetricsSettings::MetricsLevelTable,
                now,
                OTHER_TABLE_PATH
            );

            // Nothing of the old table is left behind, and the new table holds the counters
            // instead
            packed.Settle(*aggregator);
            UNIT_ASSERT(!packed.Exists(TPackedBucketId::Table(TABLE_PATH)));

            const auto movedBucket = TPackedBucketId::Table(OTHER_TABLE_PATH);
            UNIT_ASSERT(packed.Exists(movedBucket));
            UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(movedBucket, ROW_COUNT), 7);

            // The reverse map points at the new table, so forgetting the tablet drops
            // the counters of the NEW table rather than of the one it no longer belongs to
            aggregator->ForgetTablet(leader.TabletId, leader.FollowerId);

            packed.Settle(*aggregator);
            UNIT_ASSERT(!packed.Exists(movedBucket));
            UNIT_ASSERT_VALUES_EQUAL(packed.LiveCount(), 0);
        }

        // TEST 2: The partition level, where the tablet owns a leaf of its own
        {
            auto aggregator = CreateNodeDatabaseMetricsAggregator(
                DATABASE_PATH,
                false /* isFollowerRole */
            );

            TFakeTablet leader(1000, 0);
            TPackedReceiver packed;

            leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 5);
            leader.Report(aggregator, TDetailedMetricsSettings::MetricsLevelPartition, now);

            packed.Settle(*aggregator);
            UNIT_ASSERT(packed.Exists(Leaf(leader.TabletId, leader.FollowerId)));

            leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 7);
            leader.Report(
                aggregator,
                TDetailedMetricsSettings::MetricsLevelPartition,
                now,
                OTHER_TABLE_PATH
            );

            // The old leaf is gone, and the new leaf holds the counters
            packed.Settle(*aggregator);
            UNIT_ASSERT(!packed.Exists(Leaf(leader.TabletId, leader.FollowerId)));

            const auto movedLeaf = Leaf(leader.TabletId, leader.FollowerId, OTHER_TABLE_PATH);
            UNIT_ASSERT(packed.Exists(movedLeaf));
            UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(movedLeaf, ROW_COUNT), 7);

            aggregator->ForgetTablet(leader.TabletId, leader.FollowerId);

            packed.Settle(*aggregator);
            UNIT_ASSERT(!packed.Exists(movedLeaf));
            UNIT_ASSERT_VALUES_EQUAL(packed.LiveCount(), 0);
        }
    }

    /**
     * Verify that two different tablets, which report the very same table path, share
     * ONE TABLE bucket rather than fragmenting the table between them.
     *
     * @note This is what keying the per-table state by PATH rather than by any secondary
     *       identity buys: whatever two tablets disagree about upstream (a table dropped
     *       and recreated at the same path, an ESchemeOpMoveTable rename, or simply two
     *       partitions of one live table), if they report the same path, they land in
     *       the same entry here. Telling a live table's tablets apart from a stale
     *       table's stragglers is the caller's job (the Tablet Counters Aggregator's
     *       LatestByPath), not this class's — see the class comment in the header.
     */
    Y_UNIT_TEST(TwoTabletsAtTheSamePathShareOneTable) {
        const TInstant now = TInstant::Seconds(100);

        // TEST 1: The table level, where both tablets contribute to a shared bucket
        {
            auto aggregator = CreateNodeDatabaseMetricsAggregator(
                DATABASE_PATH,
                false /* isFollowerRole */
            );

            TFakeTablet tabletA(1000, 0);
            TFakeTablet tabletB(1001, 0);

            tabletA.SetSimple(DB_UNIQUE_ROWS_TOTAL, 5);
            tabletA.Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);

            tabletB.SetSimple(DB_UNIQUE_ROWS_TOTAL, 7);
            tabletB.Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);

            // Exactly one bucket holds the sum of both tablets' contributions
            TPackedReceiver packed;
            packed.Settle(*aggregator);
            const auto table = TPackedBucketId::Table(TABLE_PATH);
            UNIT_ASSERT_VALUES_EQUAL(packed.LiveCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(table, ROW_COUNT), 5 + 7);

            // One tablet is forgotten: the bucket must stay, held up by the survivor
            aggregator->ForgetTablet(tabletA.TabletId, tabletA.FollowerId);

            packed.Settle(*aggregator);
            UNIT_ASSERT(packed.Exists(table));
            UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(table, ROW_COUNT), 7);
        }

        // TEST 2: The partition level, where both tablets own a leaf of their own
        {
            auto aggregator = CreateNodeDatabaseMetricsAggregator(
                DATABASE_PATH,
                false /* isFollowerRole */
            );

            TFakeTablet tabletA(1000, 0);
            TFakeTablet tabletB(1001, 0);

            tabletA.SetSimple(DB_UNIQUE_ROWS_TOTAL, 5);
            tabletA.Report(aggregator, TDetailedMetricsSettings::MetricsLevelPartition, now);

            tabletB.SetSimple(DB_UNIQUE_ROWS_TOTAL, 7);
            tabletB.Report(aggregator, TDetailedMetricsSettings::MetricsLevelPartition, now);

            // Both leaves are reported for the very same table
            TPackedReceiver packed;
            packed.Settle(*aggregator);
            UNIT_ASSERT_VALUES_EQUAL(packed.LiveCount(TABLE_PATH, TDetailedMetricsSettings::MetricsLevelPartition), 2);

            const auto leafA = Leaf(tabletA.TabletId, tabletA.FollowerId);
            UNIT_ASSERT(packed.Exists(leafA));
            UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(leafA, ROW_COUNT), 5);

            const auto leafB = Leaf(tabletB.TabletId, tabletB.FollowerId);
            UNIT_ASSERT(packed.Exists(leafB));
            UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(leafB, ROW_COUNT), 7);

            // Forgetting one tablet must not detach the surviving leaf
            aggregator->ForgetTablet(tabletA.TabletId, tabletA.FollowerId);

            packed.Settle(*aggregator);
            UNIT_ASSERT(!packed.Exists(leafA));
            UNIT_ASSERT(packed.Exists(leafB));
            UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(leafB, ROW_COUNT), 7);
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

        UNIT_ASSERT(trees.Settle().Exists(Leaf(leader1.TabletId, leader1.FollowerId)));
        UNIT_ASSERT(trees.Packed.Exists(Leaf(leader2.TabletId, leader2.FollowerId)));
        UNIT_ASSERT(trees.Packed.Exists(Leaf(follower.TabletId, follower.FollowerId)));

        // The database default drops to the table level: the very same schema version 1,
        // the very same tablets, only the level of the report changes
        for (auto* tablet : {&leader1, &leader2}) {
            tablet->Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelTable, now);
        }
        follower.Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelTable, now);

        // Not a single leaf is left, of either role
        UNIT_ASSERT_VALUES_EQUAL(trees.Settle().LiveCount(TABLE_PATH, TDetailedMetricsSettings::MetricsLevelPartition), 0);

        // ... and the collapse bucket holds the leaders alone
        UNIT_ASSERT_VALUES_EQUAL(trees.Packed.Gauge(TPackedBucketId::Table(TABLE_PATH), ROW_COUNT), 1 + 2);
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
        auto aggregator = CreateNodeDatabaseMetricsAggregator(
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

        TPackedReceiver packed;
        packed.Settle(*aggregator);
        UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(TPackedBucketId::Table(TABLE_PATH), ROW_COUNT), 1 + 2);

        // The level is raised to the partition one
        for (auto* tablet : {&leader1, &leader2}) {
            tablet->Report(aggregator, TDetailedMetricsSettings::MetricsLevelPartition, now);
        }

        // The bucket is gone, and every partition has a leaf now
        packed.Settle(*aggregator);
        UNIT_ASSERT(!packed.Exists(TPackedBucketId::Table(TABLE_PATH)));
        UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(Leaf(leader1.TabletId, leader1.FollowerId), ROW_COUNT), 1);
        UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(Leaf(leader2.TabletId, leader2.FollowerId), ROW_COUNT), 2);
    }

    /**
     * Verify that a table, which stops collecting detailed metrics, is dropped whole:
     * its buckets go, and its reports create nothing afterwards.
     */
    Y_UNIT_TEST(LevelChangeToDisabledDropsTheTableOnceEveryTabletConverges) {
        const TInstant now = TInstant::Seconds(100);

        // TEST 1: From the table level, where the collapse bucket has to go
        {
            auto aggregator = CreateNodeDatabaseMetricsAggregator(
                DATABASE_PATH,
                false /* isFollowerRole */
            );

            TFakeTablet leader(1000, 0);
            leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 5);

            leader.Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);

            TPackedReceiver packed;
            packed.Settle(*aggregator);
            UNIT_ASSERT(packed.Exists(TPackedBucketId::Table(TABLE_PATH)));

            leader.Report(aggregator, TDetailedMetricsSettings::MetricsLevelDisabled, now);

            packed.Settle(*aggregator);
            UNIT_ASSERT_VALUES_EQUAL(packed.LiveCount(), 0);

            // The reports of a disabled table keep creating nothing at all
            leader.Report(aggregator, TDetailedMetricsSettings::MetricsLevelDisabled, now);

            packed.Settle(*aggregator);
            UNIT_ASSERT_VALUES_EQUAL(packed.LiveCount(), 0);
        }

        // TEST 2: From the partition level, where the leaves have to go. The level is
        //         cleared rather than disabled: a table with no override of its own
        //         follows a database default, which collects nothing
        {
            auto aggregator = CreateNodeDatabaseMetricsAggregator(
                DATABASE_PATH,
                false /* isFollowerRole */
            );

            TFakeTablet leader1(1000, 0);
            TFakeTablet leader2(2000, 0);
            TPackedReceiver packed;

            for (auto* tablet : {&leader1, &leader2}) {
                tablet->SetSimple(DB_UNIQUE_ROWS_TOTAL, 5);
                tablet->Report(aggregator, TDetailedMetricsSettings::MetricsLevelPartition, now);
            }

            packed.Settle(*aggregator);
            UNIT_ASSERT(packed.Exists(Leaf(leader1.TabletId, leader1.FollowerId)));

            leader1.Report(aggregator, TDetailedMetricsSettings::MetricsLevelUnspecified, now);

            // leader1's leaf is gone, but leader2 has not noticed yet, so its own leaf
            // survives untouched
            packed.Settle(*aggregator);
            UNIT_ASSERT(!packed.Exists(Leaf(leader1.TabletId, leader1.FollowerId)));
            UNIT_ASSERT(packed.Exists(Leaf(leader2.TabletId, leader2.FollowerId)));
            UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(Leaf(leader2.TabletId, leader2.FollowerId), ROW_COUNT), 5);

            // The last partition converges too: NOW the table goes whole
            leader2.Report(aggregator, TDetailedMetricsSettings::MetricsLevelUnspecified, now);

            packed.Settle(*aggregator);
            UNIT_ASSERT(!packed.Exists(Leaf(leader1.TabletId, leader1.FollowerId)));
            UNIT_ASSERT(!packed.Exists(Leaf(leader2.TabletId, leader2.FollowerId)));
            UNIT_ASSERT_VALUES_EQUAL(packed.LiveCount(), 0);

            // The reports of a disabled table keep creating nothing at all
            leader1.Report(aggregator, TDetailedMetricsSettings::MetricsLevelUnspecified, now);

            packed.Settle(*aggregator);
            UNIT_ASSERT_VALUES_EQUAL(packed.LiveCount(), 0);
        }
    }

    /**
     * Verify that a per-table ALTER, which enables the detailed metrics of a table that
     * collected none, starts emitting the leaves.
     *
     * @note Whatever an ALTER TABLE bumps upstream (schema version, in production) is
     *       the Tablet Counters Aggregator's concern, not this class's — see the class
     *       comment in the header. All this layer ever reacts to is the level.
     */
    Y_UNIT_TEST(SchemaBumpEnablesPartitionLevel) {
        auto aggregator = CreateNodeDatabaseMetricsAggregator(
            DATABASE_PATH,
            false /* isFollowerRole */
        );

        const TInstant now = TInstant::Seconds(100);

        TFakeTablet leader(1000, 0);
        leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 5);

        // The table follows a database default, which collects nothing
        leader.Report(aggregator, TDetailedMetricsSettings::MetricsLevelUnspecified, now);

        TPackedReceiver packed;
        packed.Settle(*aggregator);
        UNIT_ASSERT_VALUES_EQUAL(packed.LiveCount(), 0);

        // ALTER TABLE ... SET (DETAILED_METRICS_LEVEL = PARTITION)
        leader.Report(aggregator, TDetailedMetricsSettings::MetricsLevelPartition, now);

        packed.Settle(*aggregator);
        UNIT_ASSERT(packed.Exists(Leaf(leader.TabletId, leader.FollowerId)));
        UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(Leaf(leader.TabletId, leader.FollowerId), ROW_COUNT), 5);
    }

    /**
     * Verify that a report, which does NOT change the effective level, keeps the
     * counters of the table exactly where they are.
     *
     * @note The level is the only thing, which decides the shape of the entry, so a
     *       plain ALTER (which does not, upstream, change the effective level) has
     *       nothing to reconcile here. Rebuilding the table on every such report instead
     *       would restart the accumulated cumulative counters from zero — a consumer
     *       reads that as a counter reset — and would blank the leaves of the
     *       partitions, which have not reported since.
     */
    Y_UNIT_TEST(SchemaBumpAtTheSameLevelKeepsTheCounters) {
        auto aggregator = CreateNodeDatabaseMetricsAggregator(
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

        TPackedReceiver packed;
        packed.Settle(*aggregator);
        UNIT_ASSERT_VALUES_EQUAL(packed.Rate(Leaf(leader1.TabletId, leader1.FollowerId), CONSUMED_CPU_MICROSECONDS), 100);

        // The ALTER reaches this class as a report at the very same level, and only the
        // first partition has noticed it so far
        leader1.AddCumulative(CONSUMED_CPU, 50);
        leader1.Report(aggregator, TDetailedMetricsSettings::MetricsLevelPartition, now + TDuration::Seconds(10));

        // The rate keeps adding up, and the leaf keeps the previous report of its tablet:
        // its CPU observation is the rate since that report (50 over 10 seconds, the <=10
        // bucket of the ranges {0, 10, 100}), not the zero of a leaf created afresh
        packed.Settle(*aggregator);
        UNIT_ASSERT_VALUES_EQUAL(
            packed.Rate(Leaf(leader1.TabletId, leader1.FollowerId), CONSUMED_CPU_MICROSECONDS), 100 + 50);
        const auto histogram = packed.Hist(Leaf(leader1.TabletId, leader1.FollowerId), USED_CORE_PERCENTS);
        UNIT_ASSERT_VALUES_EQUAL(histogram[0], 0);
        UNIT_ASSERT_VALUES_EQUAL(histogram[1], 1);

        // ... and the partition, which has not reported since, keeps its own leaf
        UNIT_ASSERT(packed.Exists(Leaf(leader2.TabletId, leader2.FollowerId)));
        UNIT_ASSERT_VALUES_EQUAL(packed.Rate(Leaf(leader2.TabletId, leader2.FollowerId), CONSUMED_CPU_MICROSECONDS), 200);
    }

    /**
     * Verify that the two instances of a node converge on a new level INDEPENDENTLY,
     * one report each, and that neither of them drops a leaf of the other while they
     * disagree.
     *
     * @note A level change reaches the instances through their own tablets' reports, so
     *       there is always a window where the leader instance has already switched and
     *       the follower one has not.
     */
    Y_UNIT_TEST(LevelChangeConvergesBothInstances) {
        TRoleTrees trees;

        const TInstant now = TInstant::Seconds(100);

        // The leader of a partition and its follower, on one node, under ONE tablet ID
        TFakeTablet leader(1000, 0);
        TFakeTablet follower(1000, 1);

        // The follower leaf carries no LeaderOnly metric (row_count), so its own work is what
        // its leaf is checked by
        leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 42);
        follower.SetSimple(DB_UNIQUE_ROWS_TOTAL, 99).AddCumulative(CONSUMED_CPU, 99);

        leader.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, now);
        follower.Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelPartition, now);

        UNIT_ASSERT(trees.Settle().Exists(Leaf(leader.TabletId, leader.FollowerId)));
        UNIT_ASSERT(trees.Packed.Exists(Leaf(follower.TabletId, follower.FollowerId)));

        // TEST 1: The leader instance notices the new level first
        leader.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelTable, now);

        UNIT_ASSERT(!trees.Settle().Exists(Leaf(leader.TabletId, leader.FollowerId)));

        // The follower's leaf is untouched: the leader instance removed its own leaf alone
        UNIT_ASSERT(trees.Packed.Exists(Leaf(follower.TabletId, follower.FollowerId)));
        UNIT_ASSERT_VALUES_EQUAL(
            trees.Packed.Rate(Leaf(follower.TabletId, follower.FollowerId), CONSUMED_CPU_MICROSECONDS), 99);

        UNIT_ASSERT_VALUES_EQUAL(trees.Packed.Gauge(TPackedBucketId::Table(TABLE_PATH), ROW_COUNT), 42);

        // TEST 2: The follower instance converges one report later, and the last leaf
        //         goes, while the collapse bucket of the other instance stays
        follower.Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelTable, now);

        UNIT_ASSERT_VALUES_EQUAL(trees.Settle().LiveCount(TABLE_PATH, TDetailedMetricsSettings::MetricsLevelPartition), 0);

        UNIT_ASSERT_VALUES_EQUAL(trees.Packed.LiveCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(trees.Packed.Gauge(TPackedBucketId::Table(TABLE_PATH), ROW_COUNT), 42);
    }

    Y_UNIT_TEST(PartialLevelConvergenceKeepsBothShapes) {
        auto aggregator = CreateNodeDatabaseMetricsAggregator(
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

        TPackedReceiver packed;
        packed.Settle(*aggregator);
        UNIT_ASSERT(packed.Exists(Leaf(leader1.TabletId, leader1.FollowerId)));
        UNIT_ASSERT(packed.Exists(Leaf(leader2.TabletId, leader2.FollowerId)));

        // Only the first partition converges on the new level
        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 3).AddCumulative(CONSUMED_CPU, 50);
        leader1.Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);

        // The switched partition has no leaf of its own any more, and its value is in
        // the table bucket instead
        packed.Settle(*aggregator);
        UNIT_ASSERT(!packed.Exists(Leaf(leader1.TabletId, leader1.FollowerId)));
        UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(TPackedBucketId::Table(TABLE_PATH), ROW_COUNT), 3);

        // The lagging partition keeps its own leaf, and — the whole point of this test
        // — its rate is exactly what it was, not reset to 0 by a table-wide teardown
        // that no longer happens
        const auto laggingLeaf = Leaf(leader2.TabletId, leader2.FollowerId);
        UNIT_ASSERT(packed.Exists(laggingLeaf));
        UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(laggingLeaf, ROW_COUNT), 2);
        UNIT_ASSERT_VALUES_EQUAL(packed.Rate(laggingLeaf, CONSUMED_CPU_MICROSECONDS), 200);

        // The lagging partition converges too
        leader2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 4).AddCumulative(CONSUMED_CPU, 20);
        leader2.Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);

        packed.Settle(*aggregator);
        UNIT_ASSERT_VALUES_EQUAL(packed.LiveCount(TABLE_PATH, TDetailedMetricsSettings::MetricsLevelPartition), 0);

        UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(TPackedBucketId::Table(TABLE_PATH), ROW_COUNT), 3 + 4);
    }

    Y_UNIT_TEST(StaleLevelReportDoesNotResetTheCumulativeCounters) {
        auto aggregator = CreateNodeDatabaseMetricsAggregator(
            DATABASE_PATH,
            false /* isFollowerRole */
        );

        const TInstant now = TInstant::Seconds(100);

        TFakeTablet flapping(1000, 0);
        TFakeTablet steady(2000, 0);

        flapping.AddCumulative(CONSUMED_CPU, 10);
        steady.AddCumulative(CONSUMED_CPU, 100);

        flapping.Report(aggregator, TDetailedMetricsSettings::MetricsLevelPartition, now);
        steady.Report(aggregator, TDetailedMetricsSettings::MetricsLevelPartition, now);

        TPackedReceiver packed;
        packed.Settle(*aggregator);
        UNIT_ASSERT_VALUES_EQUAL(packed.Rate(Leaf(steady.TabletId, steady.FollowerId), CONSUMED_CPU_MICROSECONDS), 100);

        // The flapping tablet jumps to the table level ...
        flapping.AddCumulative(CONSUMED_CPU, 1);
        flapping.Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);

        // ... straight back to the partition one ...
        flapping.AddCumulative(CONSUMED_CPU, 1);
        flapping.Report(aggregator, TDetailedMetricsSettings::MetricsLevelPartition, now);

        // ... and to the table level again, all without the steady tablet ever
        // reporting anything in between
        flapping.AddCumulative(CONSUMED_CPU, 1);
        flapping.Report(aggregator, TDetailedMetricsSettings::MetricsLevelTable, now);

        // The steady tablet's own leaf, and the rate accumulated over its reports, is
        // untouched by any of the flapping neighbour's moves: a leaf, which the flap
        // silently dropped, would be missing from the report here, while the flapping
        // tablet itself has no leaf any more
        packed.Settle(*aggregator);
        UNIT_ASSERT(!packed.Exists(Leaf(flapping.TabletId, flapping.FollowerId)));
        const auto steadyLeaf = Leaf(steady.TabletId, steady.FollowerId);
        UNIT_ASSERT(packed.Exists(steadyLeaf));
        UNIT_ASSERT_VALUES_EQUAL(packed.Rate(steadyLeaf, CONSUMED_CPU_MICROSECONDS), 100);
        UNIT_ASSERT_VALUES_EQUAL(packed.HistTotal(steadyLeaf, USED_CORE_PERCENTS), 1);
    }

    /**
     * Verify that a renamed table manifests as drop-old/create-new, with no stale
     * TABLE bucket left behind once the last of its tablets has reported the new path.
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
            auto aggregator = CreateNodeDatabaseMetricsAggregator(
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

            TPackedReceiver packed;
            packed.Settle(*aggregator);
            const auto oldTable = TPackedBucketId::Table(TABLE_PATH);
            const auto renamedTable = TPackedBucketId::Table(RENAMED_TABLE_PATH);
            UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(oldTable, ROW_COUNT), 1 + 2);

            // The first partition reports the new path: the old table keeps the second
            // one for as long as it has not moved over
            leader1.Report(
                aggregator,
                TDetailedMetricsSettings::MetricsLevelTable,
                now,
                RENAMED_TABLE_PATH
            );

            packed.Settle(*aggregator);
            UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(oldTable, ROW_COUNT), 2);
            UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(renamedTable, ROW_COUNT), 1);

            // The second one follows, and nothing of the old path is left
            leader2.Report(
                aggregator,
                TDetailedMetricsSettings::MetricsLevelTable,
                now,
                RENAMED_TABLE_PATH
            );

            packed.Settle(*aggregator);
            UNIT_ASSERT(!packed.Exists(oldTable));
            UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(renamedTable, ROW_COUNT), 1 + 2);

            // The reverse map points at the new path, so forgetting the tablets drops
            // the bucket of the renamed table
            for (auto* tablet : {&leader1, &leader2}) {
                aggregator->ForgetTablet(tablet->TabletId, tablet->FollowerId);
            }

            packed.Settle(*aggregator);
            UNIT_ASSERT_VALUES_EQUAL(packed.LiveCount(), 0);
        }

        // TEST 2: The partition level, where every tablet owns a leaf of its own
        {
            auto aggregator = CreateNodeDatabaseMetricsAggregator(
                DATABASE_PATH,
                false /* isFollowerRole */
            );

            TFakeTablet leader(1000, 0);
            leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 5);

            leader.Report(aggregator, TDetailedMetricsSettings::MetricsLevelPartition, now);

            TPackedReceiver packed;
            packed.Settle(*aggregator);
            UNIT_ASSERT(packed.Exists(Leaf(leader.TabletId, leader.FollowerId)));

            leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 7);
            leader.Report(
                aggregator,
                TDetailedMetricsSettings::MetricsLevelPartition,
                now,
                RENAMED_TABLE_PATH
            );

            packed.Settle(*aggregator);
            UNIT_ASSERT(!packed.Exists(Leaf(leader.TabletId, leader.FollowerId)));

            const auto renamedLeaf = Leaf(leader.TabletId, leader.FollowerId, RENAMED_TABLE_PATH);
            UNIT_ASSERT(packed.Exists(renamedLeaf));
            UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(renamedLeaf, ROW_COUNT), 7);

            aggregator->ForgetTablet(leader.TabletId, leader.FollowerId);

            packed.Settle(*aggregator);
            UNIT_ASSERT_VALUES_EQUAL(packed.LiveCount(), 0);
        }
    }

    /**
     * Verify that a tablet type with no detailed metrics allow-list publishes nothing.
     *
     * @note This is the production path: the aggregator falls back to
     *       GetDetailedMetricsDescriptor(tabletType), which returns nullptr for
     *       ColumnShard.
     */
    Y_UNIT_TEST(UnsupportedTabletTypePublishesNothing) {
        auto aggregator = CreateNodeDatabaseMetricsAggregator(
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
            TABLE_PATH,
            TTabletTypes::ColumnShard
        );

        UNIT_ASSERT(PackOnce(aggregator).empty());
    }

    /**
     * Verify that a counter absent from the DataShard allow-list (NotInTheAllowList,
     * a simple counter deliberately outside SourceCounters) is published nowhere: the
     * TABLE bucket and the PARTITION leaf each report the public metrics of DataShard
     * alone. An allow-listed counter right next to it is still published, so the absence
     * is the filter's doing, not an empty report.
     */
    Y_UNIT_TEST(NameFilterDropsUnlistedCounters) {
        auto aggregator = CreateNodeDatabaseMetricsAggregator(
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
            OTHER_TABLE_PATH
        );

        // Both levels: the TABLE bucket and the leaf report the public metrics of DataShard and
        // nothing else, one slot per public metric, and the unlisted counter is in none of them
        TPackedReceiver packed;
        packed.Settle(*aggregator);

        const auto* descriptor = GetDetailedMetricsDescriptor(TABLET_TYPE);
        UNIT_ASSERT(descriptor);
        for (const auto& [bucket, rowCount] : {
                std::pair{TPackedBucketId::Table(TABLE_PATH), 5u},
                std::pair{Leaf(partitionTablet.TabletId, partitionTablet.FollowerId, OTHER_TABLE_PATH), 7u}}) {
            UNIT_ASSERT_C(packed.Exists(bucket), bucket.ToString());

            const auto& values = packed.Get(bucket);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(values.SimpleSize()), descriptor->Gauges.size());
            UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(values.CumulativeSize()), descriptor->Rates.size());
            UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(values.HistogramSize()), descriptor->Histograms.size());
            UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(bucket, ROW_COUNT), rowCount);
            UNIT_ASSERT_VALUES_EQUAL(packed.Gauge(bucket, SIZE_BYTES), 0);
        }
    }

    /**
     * Verify that Pack(), which the SysView Service runs off its own thread, never observes
     * a half built or a half dropped bucket while the writer creates and drops the leaves and
     * the TABLE buckets of a table, and that not a single rate delta is lost or reported twice.
     *
     * @note Pack(), AddCounters() and ForgetTablet() take DetailedMetricsLock(), so a report
     *       holds whole buckets only: every public metric of the tablet type has its slot,
     *       a live bucket has its gauges and its level histogram observations, a retired one
     *       has neither, and every bucket is in the report once.
     *
     * @note The reader does NOT take the lock around Pack() itself, the way the SysView Service
     *       does not, so that a Pack(), which misses the lock, races the writer here as well.
     */
    Y_UNIT_TEST(ConcurrentPackSeesWholeBuckets) {
        constexpr ui32 PARTITION_COUNT = 16;
        constexpr ui32 WRITER_ITERATIONS = 200;

        auto aggregator = CreateNodeDatabaseMetricsAggregator(
            DATABASE_PATH,
            false /* isFollowerRole */
        );

        const auto* descriptor = GetDetailedMetricsDescriptor(TABLET_TYPE);
        UNIT_ASSERT(descriptor);

        TInstant now = TInstant::Seconds(100);

        TVector<THolder<TFakeTablet>> partitions;
        for (ui32 i = 0; i < PARTITION_COUNT; ++i) {
            auto& partition = partitions.emplace_back(MakeHolder<TFakeTablet>(1000 + i, 0));
            partition->SetSimple(DB_UNIQUE_ROWS_TOTAL, i + 1);
        }

        // The rate deltas of every report: the deltas of every packed bucket, live or retired,
        // add up to the work of the tablets, each of them exactly once
        ui64 packedCpu = 0;

        // @return The description of the first violation in the public values of a bucket, or
        //         an empty string if the bucket is whole
        auto checkBucket = [descriptor](const NKikimrSysView::TDbCounters& values, ui64 expectedRows) -> TString {
            if (static_cast<size_t>(values.SimpleSize()) != descriptor->Gauges.size()
                || static_cast<size_t>(values.GetCumulativeCount()) != descriptor->Rates.size()
                || static_cast<size_t>(values.HistogramSize()) != descriptor->Histograms.size())
            {
                return TStringBuilder() << "a bucket of " << values.SimpleSize() << " gauges, "
                    << values.GetCumulativeCount() << " rates and " << values.HistogramSize() << " histograms";
            }

            ui64 observations = 0;
            for (ui32 metric = 0; metric < descriptor->Histograms.size(); ++metric) {
                const auto& histogram = values.GetHistogram(metric);
                if (!histogram.GetNonDerivative()
                    || static_cast<size_t>(histogram.GetBucketsCount()) != descriptor->Histograms[metric].BucketCount()
                    || histogram.BucketsSize() % 2 != 0)
                {
                    return TStringBuilder() << "a torn histogram " << metric;
                }
                for (size_t i = 1; i < static_cast<size_t>(histogram.BucketsSize()); i += 2) {
                    observations += histogram.GetBuckets(i);
                }
            }

            // A live bucket has both its gauges and its observations, a retired one has neither
            const ui64 rows = values.GetSimple(ROW_COUNT);
            if ((rows == 0) != (observations == 0)) {
                return TStringBuilder() << "a bucket of " << rows << " rows and " << observations << " observations";
            }
            if (expectedRows && rows && rows != expectedRows) {
                return TStringBuilder() << "a leaf of " << rows << " rows instead of " << expectedRows;
            }
            if (expectedRows && observations > 1) {
                return TStringBuilder() << "a leaf of " << observations << " observations";
            }

            return {};
        };

        // @return The description of the first violation in the report, or an empty string
        //         if every bucket of it is whole
        auto checkReport = [&](const TPackedTables& tables) -> TString {
            THashSet<TPackedBucketId, TPackedBucketId::THash> buckets;
            for (const auto& table : tables) {
                if (table.GetTabletType() != TABLET_TYPE) {
                    return TStringBuilder() << "a table entry of the tablet type " << table.GetTabletType();
                }

                const bool isTableLevel = table.GetLevel() == TDetailedMetricsSettings::MetricsLevelTable;
                if (isTableLevel != table.HasTableMetrics() || isTableLevel == (table.LeavesSize() != 0)) {
                    return TStringBuilder() << "a table entry of the level " << static_cast<int>(table.GetLevel())
                        << " with the buckets of the other one";
                }

                auto addBucket = [&](const TPackedBucketId& id, const NKikimrSysView::TDbCounters& values, ui64 expectedRows) -> TString {
                    if (!buckets.insert(id).second) {
                        return TStringBuilder() << "the bucket " << id.ToString() << " is reported twice";
                    }
                    packedCpu += GetPackedCumulativeDelta(values, CONSUMED_CPU_MICROSECONDS);
                    if (auto failure = checkBucket(values, expectedRows)) {
                        return TStringBuilder() << id.ToString() << ": " << failure;
                    }
                    return {};
                };

                if (isTableLevel) {
                    if (auto failure = addBucket(TPackedBucketId::Table(table.GetTablePath()), table.GetTableMetrics(), 0)) {
                        return failure;
                    }
                }
                for (const auto& leaf : table.GetLeaves()) {
                    const auto id = TPackedBucketId::Leaf(table.GetTablePath(), leaf.GetTabletId(), leaf.GetFollowerId());
                    if (auto failure = addBucket(id, leaf.GetMetrics(), leaf.GetTabletId() - 1000 + 1)) {
                        return failure;
                    }
                }
            }
            return {};
        };

        TLockedReaderThread reader(
            [&]() -> TString {
                return checkReport(PackOnce(aggregator));
            }
        );

        ui64 reportedCpu = 0;
        for (ui32 iteration = 0; iteration < WRITER_ITERATIONS; ++iteration) {
            now += TDuration::Seconds(1);

            for (ui32 i = 0; i < PARTITION_COUNT; ++i) {
                // Now and then a partition moves to the table level and back, so that the TABLE
                // bucket comes and goes as well
                const auto level = (iteration + i) % 5 == 0
                    ? TDetailedMetricsSettings::MetricsLevelTable
                    : TDetailedMetricsSettings::MetricsLevelPartition;

                auto& partition = partitions[i];
                partition->AddCumulative(CONSUMED_CPU, 1 + i);
                reportedCpu += 1 + i;
                partition->Report(aggregator, level, now);

                // Drop the previous partition right after this one has reported, so that its
                // bucket is retired and created again under the reader
                const ui32 previous = (i + PARTITION_COUNT - 1) % PARTITION_COUNT;
                aggregator->ForgetTablet(partitions[previous]->TabletId, 0);
            }

            // Now and then drop every partition, so that every bucket is retired at once
            if (iteration % 8 == 0) {
                for (auto& partition : partitions) {
                    aggregator->ForgetTablet(partition->TabletId, 0);
                }
            }
        }

        UNIT_ASSERT(reader.Join() > 0);

        // The reports after the writer is done: the final values of the retired buckets
        // first, then the live buckets alone
        for (int report = 0; report < 2; ++report) {
            const auto failure = checkReport(PackOnce(aggregator));
            UNIT_ASSERT_C(failure.empty(), failure);
        }

        UNIT_ASSERT_VALUES_EQUAL(packedCpu, reportedCpu);
    }

    Y_UNIT_TEST(PackTableLevelUsesThePreviousSnapshotForCumulativeDeltas) {
        TRoleTrees trees;
        TFakeTablet leader(1000, 0);
        TInstant now = TInstant::Seconds(100);
        leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 10).AddCumulative(CONSUMED_CPU, 100);
        leader.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelTable, now);

        auto first = PackOnce(trees.Leaders);
        const auto* table = FindPackedTable(first, TDetailedMetricsSettings::MetricsLevelTable);
        UNIT_ASSERT(table);
        UNIT_ASSERT(table->HasTableMetrics());
        UNIT_ASSERT_VALUES_EQUAL(table->LeavesSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(table->GetTableMetrics().GetSimple(ROW_COUNT), 10);
        UNIT_ASSERT_VALUES_EQUAL(GetPackedCumulativeDelta(table->GetTableMetrics(), CONSUMED_CPU_MICROSECONDS), 100);

        // Several reports must not advance the Pack baseline.
        for (ui64 delta : {15, 25}) {
            now += TDuration::Seconds(5);
            leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 20).AddCumulative(CONSUMED_CPU, delta);
            leader.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelTable, now);
        }
        auto second = PackOnce(trees.Leaders);
        table = FindPackedTable(second, TDetailedMetricsSettings::MetricsLevelTable);
        UNIT_ASSERT(table);
        UNIT_ASSERT_VALUES_EQUAL(table->GetTableMetrics().GetSimple(ROW_COUNT), 20);
        UNIT_ASSERT_VALUES_EQUAL(GetPackedCumulativeDelta(table->GetTableMetrics(), CONSUMED_CPU_MICROSECONDS), 40);

        leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 0);
        leader.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelTable, now + TDuration::Seconds(5));
        auto third = PackOnce(trees.Leaders);
        table = FindPackedTable(third, TDetailedMetricsSettings::MetricsLevelTable);
        UNIT_ASSERT(table);
        UNIT_ASSERT_VALUES_EQUAL(table->GetTableMetrics().GetSimple(ROW_COUNT), 0);
        UNIT_ASSERT_VALUES_EQUAL(table->GetTableMetrics().CumulativeSize(), 0);
        TFakeTablet follower(1000, 1);
        follower.Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelTable, now);
        UNIT_ASSERT(PackOnce(trees.Followers).empty());
    }

    Y_UNIT_TEST(PackPartitionLevelKeepsBothRolesAndPerLeafBaselines) {
        TRoleTrees trees;
        const TInstant now = TInstant::Seconds(100);
        TFakeTablet leader(1000, 0);
        TFakeTablet follower1(1000, 1);
        TFakeTablet follower2(2000, 1);
        leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 3).AddCumulative(CONSUMED_CPU, 30);
        follower1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 9).AddCumulative(CONSUMED_CPU, 90);
        follower2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 5).AddCumulative(CONSUMED_CPU, 50);
        leader.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, now);
        follower1.Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelPartition, now);
        follower2.Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelPartition, now);

        auto leaders = PackOnce(trees.Leaders);
        auto followers = PackOnce(trees.Followers);
        const auto* leaderTable = FindPackedTable(leaders, TDetailedMetricsSettings::MetricsLevelPartition);
        const auto* followerTable = FindPackedTable(followers, TDetailedMetricsSettings::MetricsLevelPartition);
        UNIT_ASSERT(leaderTable && followerTable);
        UNIT_ASSERT(!leaderTable->HasTableMetrics() && !followerTable->HasTableMetrics());
        UNIT_ASSERT_VALUES_EQUAL(leaderTable->LeavesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(followerTable->LeavesSize(), 2);
        for (const auto& [table, tablet, value] : {
            std::tuple{leaderTable, &leader, 3u},
            std::tuple{followerTable, &follower1, 9u},
            std::tuple{followerTable, &follower2, 5u}})
        {
            const auto* leaf = FindPackedLeaf(*table, tablet->TabletId, tablet->FollowerId);
            UNIT_ASSERT(leaf);
            // row_count is LeaderOnly: a follower leaf leaves it zero
            UNIT_ASSERT_VALUES_EQUAL(leaf->GetMetrics().GetSimple(ROW_COUNT), tablet->FollowerId == 0 ? value : 0);
            UNIT_ASSERT_VALUES_EQUAL(GetPackedCumulativeDelta(leaf->GetMetrics(), CONSUMED_CPU_MICROSECONDS), value * 10);
            UNIT_ASSERT_VALUES_EQUAL(GetPackedNonDerivativeHistogramTotal(leaf->GetMetrics(), USED_CORE_PERCENTS), 1);
        }

        follower1.AddCumulative(CONSUMED_CPU, 7);
        follower1.Report(trees.Followers, TDetailedMetricsSettings::MetricsLevelPartition, now + TDuration::Seconds(5));
        auto next = PackOnce(trees.Followers);
        followerTable = FindPackedTable(next, TDetailedMetricsSettings::MetricsLevelPartition);
        UNIT_ASSERT(followerTable);
        const auto* changed = FindPackedLeaf(*followerTable, 1000, 1);
        const auto* unchanged = FindPackedLeaf(*followerTable, 2000, 1);
        UNIT_ASSERT(changed && unchanged);
        UNIT_ASSERT_VALUES_EQUAL(GetPackedCumulativeDelta(changed->GetMetrics(), CONSUMED_CPU_MICROSECONDS), 7);
        // The unchanged leaf still reports its whole level histogram, but no deltas
        UNIT_ASSERT_VALUES_EQUAL(GetPackedNonDerivativeHistogram(unchanged->GetMetrics(), USED_CORE_PERCENTS)[0], 1);
        UNIT_ASSERT_VALUES_EQUAL(GetPackedNonDerivativeHistogramTotal(unchanged->GetMetrics(), USED_CORE_PERCENTS), 1);
        UNIT_ASSERT_VALUES_EQUAL(unchanged->GetMetrics().CumulativeSize(), 0);
    }

    Y_UNIT_TEST(PackHistogramShrinksWhenATabletLeaves) {
        TRoleTrees trees;
        const TInstant now = TInstant::Seconds(100);
        TFakeTablet leader1(1000, 0);
        TFakeTablet leader2(2000, 0);
        for (auto* tablet : {&leader1, &leader2}) {
            tablet->AddCumulative(CONSUMED_CPU, 100);
            tablet->Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelTable, now);
        }
        auto first = PackOnce(trees.Leaders);
        const auto* firstTable = FindPackedTable(first, TDetailedMetricsSettings::MetricsLevelTable);
        UNIT_ASSERT(firstTable);
        UNIT_ASSERT_VALUES_EQUAL(
            GetPackedNonDerivativeHistogram(firstTable->GetTableMetrics(), USED_CORE_PERCENTS)[0], 2);

        trees.Leaders->ForgetTablet(leader2.TabletId, leader2.FollowerId);
        auto packed = PackOnce(trees.Leaders);
        const auto* table = FindPackedTable(packed, TDetailedMetricsSettings::MetricsLevelTable);
        UNIT_ASSERT(table);
        const auto& counters = table->GetTableMetrics();
        UNIT_ASSERT_VALUES_EQUAL(counters.HistogramSize(), 1);
        // The occupied bucket shrinks from 2 to 1: the report carries the new count itself,
        // not the decrease, so it does not depend on the receiver having seen the earlier one.
        UNIT_ASSERT_VALUES_EQUAL(counters.GetHistogram(USED_CORE_PERCENTS).BucketsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetHistogram(USED_CORE_PERCENTS).GetBuckets(0), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetHistogram(USED_CORE_PERCENTS).GetBuckets(1), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetPackedNonDerivativeHistogram(counters, USED_CORE_PERCENTS)[0], 1);
    }

    Y_UNIT_TEST(PackEmitsBothShapesWhileTheLevelConverges) {
        TRoleTrees trees;
        const TInstant now = TInstant::Seconds(100);
        TFakeTablet leader1(1000, 0);
        TFakeTablet leader2(2000, 0);
        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1).AddCumulative(CONSUMED_CPU, 100);
        leader2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 2).AddCumulative(CONSUMED_CPU, 200);
        for (auto* tablet : {&leader1, &leader2}) {
            tablet->Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelPartition, now);
        }
        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 3).AddCumulative(CONSUMED_CPU, 50);
        leader1.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelTable, now);

        auto packed = PackOnce(trees.Leaders);
        UNIT_ASSERT_VALUES_EQUAL(packed.size(), 2);
        const auto* table = FindPackedTable(packed, TDetailedMetricsSettings::MetricsLevelTable);
        const auto* partition = FindPackedTable(packed, TDetailedMetricsSettings::MetricsLevelPartition);
        UNIT_ASSERT(table && partition);
        UNIT_ASSERT(table->HasTableMetrics());
        UNIT_ASSERT_VALUES_EQUAL(table->LeavesSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(table->GetTableMetrics().GetSimple(ROW_COUNT), 3);
        UNIT_ASSERT(!partition->HasTableMetrics());
        UNIT_ASSERT_VALUES_EQUAL(partition->LeavesSize(), 2);
        const auto* retired = FindPackedLeaf(*partition, leader1.TabletId, leader1.FollowerId);
        UNIT_ASSERT(retired);
        UNIT_ASSERT_VALUES_EQUAL(retired->GetMetrics().GetSimple(ROW_COUNT), 0);
        UNIT_ASSERT_VALUES_EQUAL(GetPackedCumulativeDelta(retired->GetMetrics(), CONSUMED_CPU_MICROSECONDS), 100);
        const auto* leaf = FindPackedLeaf(*partition, leader2.TabletId, leader2.FollowerId);
        UNIT_ASSERT(leaf);
        UNIT_ASSERT_VALUES_EQUAL(leaf->GetMetrics().GetSimple(ROW_COUNT), 2);
        UNIT_ASSERT_VALUES_EQUAL(GetPackedCumulativeDelta(leaf->GetMetrics(), CONSUMED_CPU_MICROSECONDS), 200);
    }

    Y_UNIT_TEST(PackPreservesFinalDeltaWhenTheLastTabletChangesLevel) {
        for (auto oldLevel : {TDetailedMetricsSettings::MetricsLevelTable, TDetailedMetricsSettings::MetricsLevelPartition}) {
            const auto newLevel = oldLevel == TDetailedMetricsSettings::MetricsLevelTable
                ? TDetailedMetricsSettings::MetricsLevelPartition : TDetailedMetricsSettings::MetricsLevelTable;
            TRoleTrees trees;
            TFakeTablet leader(1000, 0);
            TInstant now = TInstant::Seconds(100);
            leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 10).AddCumulative(CONSUMED_CPU, 100);
            leader.Report(trees.Leaders, oldLevel, now);
            auto first = PackOnce(trees.Leaders);
            NKikimrSysView::TDbCounters oldState;
            NSysView::TAggregateCumulative<false>::Apply(&oldState, GetSinglePackedCounters(first, oldLevel));

            leader.AddCumulative(CONSUMED_CPU, 25);
            leader.Report(trees.Leaders, oldLevel, now += TDuration::Seconds(5));
            leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 20).AddCumulative(CONSUMED_CPU, 5);
            leader.Report(trees.Leaders, newLevel, now += TDuration::Seconds(5));
            auto changed = PackOnce(trees.Leaders);
            UNIT_ASSERT_VALUES_EQUAL(changed.size(), 2);
            const auto& retired = GetSinglePackedCounters(changed, oldLevel);
            const auto& active = GetSinglePackedCounters(changed, newLevel);
            UNIT_ASSERT_VALUES_EQUAL(GetPackedCumulativeDelta(retired, CONSUMED_CPU_MICROSECONDS), 25);
            UNIT_ASSERT_VALUES_EQUAL(GetPackedCumulativeDelta(active, CONSUMED_CPU_MICROSECONDS), 5);
            UNIT_ASSERT_VALUES_EQUAL(retired.GetSimple(ROW_COUNT), 0);
            UNIT_ASSERT_VALUES_EQUAL(active.GetSimple(ROW_COUNT), 20);
            NSysView::TAggregateCumulative<false>::Apply(&oldState, retired);
            UNIT_ASSERT_VALUES_EQUAL(oldState.GetCumulative(CONSUMED_CPU_MICROSECONDS), 125);

            auto next = PackOnce(trees.Leaders);
            UNIT_ASSERT_VALUES_EQUAL(next.size(), 1);
            UNIT_ASSERT(!FindPackedTable(next, oldLevel));
            UNIT_ASSERT_VALUES_EQUAL(GetPackedCumulativeDelta(GetSinglePackedCounters(next, newLevel), CONSUMED_CPU_MICROSECONDS), 0);
        }
    }

    Y_UNIT_TEST(PackRecreatedBucketReportsTheCurrentHistogramSnapshot) {
        for (auto level : {TDetailedMetricsSettings::MetricsLevelTable, TDetailedMetricsSettings::MetricsLevelPartition}) {
            TRoleTrees trees;
            TFakeTablet leader(1000, 0);
            TInstant now = TInstant::Seconds(100);
            leader.AddCumulative(CONSUMED_CPU, 100);
            leader.Report(trees.Leaders, level, now);
            auto first = PackOnce(trees.Leaders);
            NKikimrSysView::TDbCounters restored;
            NSysView::TAggregateCumulative<false>::Apply(&restored, GetSinglePackedCounters(first, level));
            UNIT_ASSERT_VALUES_EQUAL(
                GetPackedNonDerivativeHistogram(GetSinglePackedCounters(first, level), USED_CORE_PERCENTS)[0], 1);

            trees.Leaders->ForgetTablet(leader.TabletId, leader.FollowerId);
            leader.Report(trees.Leaders, level, now += TDuration::Seconds(5));
            for (int report = 0; report < 2; ++report) {
                auto packed = PackOnce(trees.Leaders);
                UNIT_ASSERT_VALUES_EQUAL(packed.size(), 1);
                const auto& counters = GetSinglePackedCounters(packed, level);
                UNIT_ASSERT_VALUES_EQUAL(GetPackedCumulativeDelta(counters, CONSUMED_CPU_MICROSECONDS), 0);
                NSysView::TAggregateCumulative<false>::Apply(&restored, counters);
                UNIT_ASSERT_VALUES_EQUAL(restored.GetCumulative(CONSUMED_CPU_MICROSECONDS), 100);
                // The cumulative history is not repeated, but the non-derivative histogram is reported
                // in full every time: the tablet is still there, and the retirement report is superseded
                UNIT_ASSERT_VALUES_EQUAL(GetPackedNonDerivativeHistogram(counters, USED_CORE_PERCENTS)[0], 1);
                UNIT_ASSERT_VALUES_EQUAL(GetPackedNonDerivativeHistogramTotal(counters, USED_CORE_PERCENTS), 1);
            }
        }
    }

    Y_UNIT_TEST(PackCoalescesMultipleRecreationsOfTheSameBucket) {
        for (auto level : {TDetailedMetricsSettings::MetricsLevelTable, TDetailedMetricsSettings::MetricsLevelPartition}) {
            TRoleTrees trees;
            TFakeTablet leader(1000, 0);
            TInstant now = TInstant::Seconds(100);
            leader.AddCumulative(CONSUMED_CPU, 100).AddAppCumulative(ENGINE_HOST_ROW_UPDATES, 1000);
            leader.Report(trees.Leaders, level, now);
            auto first = PackOnce(trees.Leaders);
            NKikimrSysView::TDbCounters restored;
            NSysView::TAggregateCumulative<false>::Apply(&restored, GetSinglePackedCounters(first, level));

            for (ui64 delta : {5, 7, 11}) {
                leader.AddCumulative(CONSUMED_CPU, delta).AddAppCumulative(ENGINE_HOST_ROW_UPDATES, delta * 10);
                leader.Report(trees.Leaders, level, now += TDuration::Seconds(5));
                trees.Leaders->ForgetTablet(leader.TabletId, leader.FollowerId);
                leader.Report(trees.Leaders, level, now += TDuration::Seconds(5));
            }
            leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 17).AddCumulative(CONSUMED_CPU, 13)
                .AddAppCumulative(ENGINE_HOST_ROW_UPDATES, 130);
            leader.Report(trees.Leaders, level, now += TDuration::Seconds(5));
            for (int report = 0; report < 2; ++report) {
                auto packed = PackOnce(trees.Leaders);
                UNIT_ASSERT_VALUES_EQUAL(packed.size(), 1);
                const auto& counters = GetSinglePackedCounters(packed, level);
                UNIT_ASSERT_VALUES_EQUAL(GetPackedCumulativeDelta(counters, CONSUMED_CPU_MICROSECONDS), report == 0 ? 36 : 0);
                UNIT_ASSERT_VALUES_EQUAL(GetPackedCumulativeDelta(counters, WRITE_ROWS), report == 0 ? 360 : 0);
                UNIT_ASSERT_VALUES_EQUAL(counters.GetSimple(ROW_COUNT), 17);
                NSysView::TAggregateCumulative<false>::Apply(&restored, counters);
                UNIT_ASSERT_VALUES_EQUAL(restored.GetCumulative(CONSUMED_CPU_MICROSECONDS), 136);
                UNIT_ASSERT_VALUES_EQUAL(restored.GetCumulative(WRITE_ROWS), 1360);
                // Only the state of the last incarnation of the bucket is reported
                const auto histogram = GetPackedNonDerivativeHistogram(counters, USED_CORE_PERCENTS);
                UNIT_ASSERT_VALUES_EQUAL(histogram[0], 0);
                UNIT_ASSERT_VALUES_EQUAL(histogram[1], 1);
                UNIT_ASSERT_VALUES_EQUAL(GetPackedNonDerivativeHistogramTotal(counters, USED_CORE_PERCENTS), 1);
            }
        }
    }

    Y_UNIT_TEST(PackFinalForgetEmitsOnceWithoutRetainingCounterGroups) {
        for (auto level : {TDetailedMetricsSettings::MetricsLevelTable, TDetailedMetricsSettings::MetricsLevelPartition}) {
            TRoleTrees trees;
            TFakeTablet leader(1000, 0);
            TInstant now = TInstant::Seconds(100);
            leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 10).AddCumulative(CONSUMED_CPU, 100);
            leader.Report(trees.Leaders, level, now);
            auto first = PackOnce(trees.Leaders);
            NKikimrSysView::TDbCounters restored;
            NSysView::TAggregateCumulative<false>::Apply(&restored, GetSinglePackedCounters(first, level));
            leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 20).AddCumulative(CONSUMED_CPU, 25);
            leader.Report(trees.Leaders, level, now += TDuration::Seconds(5));
            trees.Leaders->ForgetTablet(leader.TabletId, leader.FollowerId);

            auto final = PackOnce(trees.Leaders);
            UNIT_ASSERT_VALUES_EQUAL(final.size(), 1);
            const auto& counters = GetSinglePackedCounters(final, level);
            UNIT_ASSERT_VALUES_EQUAL(GetPackedCumulativeDelta(counters, CONSUMED_CPU_MICROSECONDS), 25);
            UNIT_ASSERT_VALUES_EQUAL(counters.GetSimple(ROW_COUNT), 0);
            NSysView::TAggregateCumulative<false>::Apply(&restored, counters);
            UNIT_ASSERT_VALUES_EQUAL(restored.GetCumulative(CONSUMED_CPU_MICROSECONDS), 125);
            // The retired bucket reports its non-derivative histogram empty
            UNIT_ASSERT_VALUES_EQUAL(GetPackedNonDerivativeHistogramTotal(counters, USED_CORE_PERCENTS), 0);
            UNIT_ASSERT(PackOnce(trees.Leaders).empty());
        }
    }

    Y_UNIT_TEST(PackAppendsRetiredDeltasWithoutChangingEarlierOutput) {
        TRoleTrees trees;
        TFakeTablet leader(1000, 0);
        TInstant now = TInstant::Seconds(100);
        leader.AddCumulative(CONSUMED_CPU, 100);
        leader.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelTable, now);
        auto packed = PackOnce(trees.Leaders);
        UNIT_ASSERT_VALUES_EQUAL(packed.size(), 1);
        const auto previous = packed.Get(0).SerializeAsString();

        leader.AddCumulative(CONSUMED_CPU, 25);
        leader.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelTable, now += TDuration::Seconds(5));
        trees.Leaders->ForgetTablet(leader.TabletId, leader.FollowerId);
        leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 17);
        leader.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelTable, now += TDuration::Seconds(5));
        trees.Leaders->Pack(packed);
        UNIT_ASSERT_VALUES_EQUAL(packed.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(packed.Get(0).SerializeAsString(), previous);
        UNIT_ASSERT_VALUES_EQUAL(packed.Get(1).GetTablePath(), TABLE_PATH);
        UNIT_ASSERT_VALUES_EQUAL(packed.Get(1).GetTabletType(), TABLET_TYPE);
        const auto& counters = packed.Get(1).GetTableMetrics();
        UNIT_ASSERT_VALUES_EQUAL(GetPackedCumulativeDelta(counters, CONSUMED_CPU_MICROSECONDS), 25);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetSimple(ROW_COUNT), 17);
    }

    Y_UNIT_TEST(PackMarksNonDerivativeHistogramsAndReemitsThemWhenUnchanged) {
        for (auto level : {TDetailedMetricsSettings::MetricsLevelTable, TDetailedMetricsSettings::MetricsLevelPartition}) {
            TRoleTrees trees;
            TFakeTablet leader(1000, 0);
            leader.AddCumulative(CONSUMED_CPU, 100).AddAppCumulative(ENGINE_HOST_ROW_UPDATES, 1000);
            leader.Report(trees.Leaders, level, TInstant::Seconds(100));

            auto first = PackOnce(trees.Leaders);
            const auto& firstCounters = GetSinglePackedCounters(first, level);
            UNIT_ASSERT_VALUES_EQUAL(firstCounters.HistogramSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetPackedCumulativeDelta(firstCounters, CONSUMED_CPU_MICROSECONDS), 100);
            const auto snapshot = GetPackedNonDerivativeHistogram(firstCounters, USED_CORE_PERCENTS);
            UNIT_ASSERT_VALUES_EQUAL(snapshot[0], 1);
            UNIT_ASSERT_VALUES_EQUAL(GetPackedNonDerivativeHistogramTotal(firstCounters, USED_CORE_PERCENTS), 1);

            // Nothing changed, yet the whole non-derivative histogram is reported again, so that a receiver,
            // which lost its copy in the meantime, is up to date after this very report
            for (int report = 0; report < 2; ++report) {
                auto next = PackOnce(trees.Leaders);
                const auto& counters = GetSinglePackedCounters(next, level);
                UNIT_ASSERT_VALUES_EQUAL(GetPackedCumulativeDelta(counters, CONSUMED_CPU_MICROSECONDS), 0);
                UNIT_ASSERT_VALUES_EQUAL(counters.CumulativeSize(), 0);
                UNIT_ASSERT_VALUES_EQUAL(counters.HistogramSize(), 1);
                UNIT_ASSERT(GetPackedNonDerivativeHistogram(counters, USED_CORE_PERCENTS) == snapshot);
            }
        }
    }

    Y_UNIT_TEST(PackRetirementReportsEmptyNonDerivativeHistogram) {
        for (auto level : {TDetailedMetricsSettings::MetricsLevelTable, TDetailedMetricsSettings::MetricsLevelPartition}) {
            TRoleTrees trees;
            TFakeTablet leader(1000, 0);
            leader.AddCumulative(CONSUMED_CPU, 100);
            leader.Report(trees.Leaders, level, TInstant::Seconds(100));
            UNIT_ASSERT_VALUES_EQUAL(
                GetPackedNonDerivativeHistogramTotal(GetSinglePackedCounters(PackOnce(trees.Leaders), level),
                                                USED_CORE_PERCENTS), 1);

            trees.Leaders->ForgetTablet(leader.TabletId, leader.FollowerId);
            auto final = PackOnce(trees.Leaders);
            UNIT_ASSERT_VALUES_EQUAL(final.size(), 1);
            const auto& histogram = GetSinglePackedCounters(final, level).GetHistogram(USED_CORE_PERCENTS);
            // The entry is there although it is empty: it is what tells the receiver to forget the tablet
            UNIT_ASSERT(histogram.GetNonDerivative());
            UNIT_ASSERT_VALUES_UNEQUAL(histogram.GetBucketsCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(histogram.BucketsSize(), 0);
            UNIT_ASSERT_VALUES_EQUAL(
                GetPackedNonDerivativeHistogramTotal(GetSinglePackedCounters(final, level), USED_CORE_PERCENTS), 0);
        }
    }

    Y_UNIT_TEST(PackRetiredAndRecreatedBucketReportsTheNewHistogramSnapshot) {
        for (auto level : {TDetailedMetricsSettings::MetricsLevelTable, TDetailedMetricsSettings::MetricsLevelPartition}) {
            TRoleTrees trees;
            TFakeTablet leader(1000, 0);
            TInstant now = TInstant::Seconds(100);
            leader.AddCumulative(CONSUMED_CPU, 100);
            leader.Report(trees.Leaders, level, now);

            // No Pack in between: the retirement report (empty) is still pending when
            // the very same bucket is created again, and must not win over the new one
            trees.Leaders->ForgetTablet(leader.TabletId, leader.FollowerId);
            leader.Report(trees.Leaders, level, now += TDuration::Seconds(5));

            auto packed = PackOnce(trees.Leaders);
            UNIT_ASSERT_VALUES_EQUAL(packed.size(), 1);
            const auto& counters = GetSinglePackedCounters(packed, level);
            UNIT_ASSERT_VALUES_EQUAL(counters.HistogramSize(), 1);
            // Neither 0 (the retirement) nor 2 (both added up)
            UNIT_ASSERT_VALUES_EQUAL(GetPackedNonDerivativeHistogram(counters, USED_CORE_PERCENTS)[0], 1);
            UNIT_ASSERT_VALUES_EQUAL(GetPackedNonDerivativeHistogramTotal(counters, USED_CORE_PERCENTS), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetPackedCumulativeDelta(counters, CONSUMED_CPU_MICROSECONDS), 100);
        }
    }

    /**
     * Verify that a tablet type is bound to the counter layout of its first report:
     * a report of another layout is skipped without an abort and registers nothing.
     */
    Y_UNIT_TEST(ReportOfAnotherCounterLayoutIsSkipped) {
        TRoleTrees trees;
        const TInstant now = TInstant::Seconds(100);
        TFakeTablet leader(1000, 0);
        leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 10);
        leader.Report(trees.Leaders, TDetailedMetricsSettings::MetricsLevelTable, now);

        constexpr const char* names[] = {"DbUniqueRowsTotal"};
        TTabletCountersBase executorCounters(Y_ARRAY_SIZE(names), 0, 0, names, nullptr, nullptr);
        TTabletCountersBase appCounters;
        executorCounters.Simple()[0].Set(30);
        for (const auto level : {TDetailedMetricsSettings::MetricsLevelTable, TDetailedMetricsSettings::MetricsLevelPartition}) {
            trees.Leaders->AddCounters(OTHER_TABLE_PATH, level, 2000, 0, TABLET_TYPE, executorCounters, appCounters, now);
        }
        trees.Leaders->AddCounters(TABLE_PATH, TDetailedMetricsSettings::MetricsLevelTable, 3000, 0, TABLET_TYPE,
            executorCounters, appCounters, now);

        auto packed = PackOnce(trees.Leaders);
        UNIT_ASSERT_VALUES_EQUAL(GetSinglePackedCounters(packed, TDetailedMetricsSettings::MetricsLevelTable).GetSimple(ROW_COUNT), 10);
        UNIT_ASSERT(!FindPackedTable(packed, TDetailedMetricsSettings::MetricsLevelTable, OTHER_TABLE_PATH));
        UNIT_ASSERT(!FindPackedTable(packed, TDetailedMetricsSettings::MetricsLevelPartition, OTHER_TABLE_PATH));

        trees.Leaders->ForgetTablet(1000, 0);

        // The final values of the retired TABLE bucket alone: the skipped reports registered nothing
        UNIT_ASSERT_VALUES_EQUAL(PackOnce(trees.Leaders).size(), 1);
        UNIT_ASSERT(PackOnce(trees.Leaders).empty());
    }

    /**
     * Verify the footprint of a PARTITION leaf on the node, which is the whole point of keeping
     * a leaf as its public metric values only (about 154 KB per leaf with a counter tree of its
     * own): with the production DataShard layouts, the hash map node of the leaf (the tablet key
     * and the accumulator), its slot of the bucket array and the heap of the accumulator stay
     * within a few hundred bytes, and the steady state reports allocate nothing.
     *
     * @note The leaves of a table are THashMap<TTabletKey, TDetailedValuesAccumulator> (see
     *       TTableEntry::Leaves of the aggregator), built here the very same way. Every
     *       allocation is rounded up to 16 bytes, an estimate of the size classes
     *       of the allocator.
     */
    Y_UNIT_TEST(LeafFootprintIsSmall) {
        constexpr ui64 LEAF_COUNT = 1000;
        constexpr size_t ALLOCATION_ALIGNMENT = 16;

        const auto* descriptor = GetDetailedMetricsDescriptor(TABLET_TYPE);
        UNIT_ASSERT(descriptor);

        NTabletFlatExecutor::TExecutorCounters executorCounters;
        const auto appCounters = CreateAppCountersByTabletType(TABLET_TYPE);
        const auto binding = BindDetailedMetrics(*descriptor, executorCounters, *appCounters);
        UNIT_ASSERT_C(binding->Problems.empty(), JoinSeq("\n", binding->Problems));

        using TLeaves = THashMap<NDetailedMetrics::TTabletKey, TDetailedValuesAccumulator>;
        TLeaves leaves;

        TInstant now = TInstant::Seconds(100);
        auto reportAll = [&]() {
            for (ui64 i = 0; i < LEAF_COUNT; ++i) {
                const NDetailedMetrics::TTabletKey tablet(1000 + i, 0);
                auto [leaf, inserted] = leaves.try_emplace(tablet, binding.Get(), false /* skipLeaderOnly */);
                leaf->second.Apply(tablet, executorCounters, *appCounters, now);
            }
            now += TDuration::Seconds(15);
        };

        auto measure = [&]() {
            auto roundUp = [](size_t bytes) {
                return AlignUp(bytes, ALLOCATION_ALIGNMENT);
            };

            // The bucket array of the hash map, shared by the leaves
            size_t bytes = roundUp(leaves.bucket_count() * sizeof(void*));
            for (const auto& [_, leaf] : leaves) {
                // The node of the hash map: the next pointer and the value (the key and the accumulator)
                bytes += roundUp(sizeof(void*) + sizeof(TLeaves::value_type));

                // The heap of the accumulator, two allocations (its source and its state), the second
                // of which is rounded up on its own
                bytes += roundUp(leaf.GetAllocatedBytes() - sizeof(TDetailedValuesAccumulator)) + ALLOCATION_ALIGNMENT;
            }
            return bytes;
        };

        reportAll();
        const size_t bytes = measure();
        const size_t bytesPerLeaf = bytes / LEAF_COUNT;

        Cerr << "TEST A PARTITION leaf takes " << bytesPerLeaf << " bytes on the node" << Endl;

        UNIT_ASSERT_LE(bytesPerLeaf, 512u);

        // The steady state reports and Pack() allocate nothing
        NKikimrSysView::TDbCounters packed;
        for (int round = 0; round < 3; ++round) {
            reportAll();
            for (auto& [_, leaf] : leaves) {
                leaf.Pack(packed);
            }
            UNIT_ASSERT_VALUES_EQUAL(measure(), bytes);
        }
    }

    Y_UNIT_TEST(NonDerivativeHistogramsOfDataShardAreConsumedCpuOnly) {
        // Of the published executor histograms only HIST(ConsumedCPU) is non-derivative
        // (the current state rather than increments), so it alone travels as its full value
        NTabletFlatExecutor::TExecutorCounters executorCounters;
        const auto* names = GetDetailedMetricsCounterNames(TTabletTypes::DataShard);
        UNIT_ASSERT(names);

        ::NKikimr::NPrivate::TAggregatedTabletCounters aggregated(MakeIntrusive<NMonitoring::TDynamicCounters>());
        aggregated.Initialize(&executorCounters, &names->ExecutorNames);
        const auto& indices = aggregated.GetNonDerivativeHistogramIndices();
        UNIT_ASSERT_VALUES_EQUAL(indices.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(indices[0], (ui32)NTabletFlatExecutor::TExecutorCounters::TX_PERCENTILE_CONSUMED_CPU);
    }

    Y_UNIT_TEST(NonDerivativeHistogramIndicesFollowTheDerivativeRule) {
        // A percentile counter is non-derivative when it is Integral or a HIST(x) aggregate,
        // whatever its Integral flag; an unpublished one is skipped and keeps no index
        constexpr const char* simpleNames[] = {"Gauge"};
        constexpr const char* percentileNames[] = {
            "Increments",
            "UnpublishedState",
            "State",
            "HIST(Gauge)",
            "Increments2",
        };
        TTabletCountersBase counters(
            Y_ARRAY_SIZE(simpleNames), 0, Y_ARRAY_SIZE(percentileNames),
            simpleNames, nullptr, percentileNames);
        counters.Percentile()[0].Initialize(PERCENTILE_RANGES, false /* integral */);
        counters.Percentile()[1].Initialize(PERCENTILE_RANGES, true /* integral */);
        counters.Percentile()[2].Initialize(PERCENTILE_RANGES, true /* integral */);
        counters.Percentile()[3].Initialize(PERCENTILE_RANGES, false /* integral */);
        counters.Percentile()[4].Initialize(PERCENTILE_RANGES, false /* integral */);

        const THashSet<TString> published = {"Gauge", "Increments", "State", "HIST(Gauge)", "Increments2"};
        ::NKikimr::NPrivate::TAggregatedTabletCounters aggregated(MakeIntrusive<NMonitoring::TDynamicCounters>());
        aggregated.Initialize(&counters, &published);

        // Indices into the full-size histogram list of ToProto ({2, 3}), not into
        // the published ones, where the unpublished counter leaves no gap ({1, 2})
        UNIT_ASSERT_VALUES_EQUAL(aggregated.GetNonDerivativeHistogramIndices(), TVector<ui32>({2, 3}));
    }
}
