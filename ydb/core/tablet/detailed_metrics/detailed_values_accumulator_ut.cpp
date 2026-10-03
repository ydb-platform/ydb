#include "detailed_metrics_binding.h"
#include "detailed_metrics_counter_set.h"
#include "detailed_values_accumulator.h"
#include "ut_helpers.h"

#include <ydb/core/protos/sys_view.pb.h>
#include <ydb/core/tablet/private/aggregated_tablet_counters.h>
#include <ydb/core/tablet/tablet_counters_app.h>
#include <ydb/core/tablet_flat/flat_executor_counters.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/generic/ylimits.h>
#include <util/random/fast.h>
#include <util/string/builder.h>
#include <util/string/join.h>

#include <type_traits>

using namespace NKikimr;
using namespace NKikimr::NDetailedMetricsTests;
using NTabletFlatExecutor::TExecutorCounters;
using TTabletKey = TDetailedValuesAccumulator::TTabletKey;

// The leaves of a table live by value in a rehashing hash map
static_assert(std::is_nothrow_move_constructible_v<TDetailedValuesAccumulator>);
static_assert(std::is_nothrow_move_assignable_v<TDetailedValuesAccumulator>);
static_assert(!std::is_copy_constructible_v<TDetailedValuesAccumulator>);

namespace {

////////////////////////////////////////////////////////////////////////////////
// The synthetic layout

// 1 bound + the implicit +Inf bucket = 2 buckets (fewer than the public ones)
constexpr TTabletPercentileCounter::TRangeDef RANGES_2[] = {
    {10, "10"},
};

// 3 bounds + the implicit +Inf bucket = 4 buckets (as many as the public ones)
constexpr TTabletPercentileCounter::TRangeDef RANGES_4[] = {
    {10, "10"},
    {20, "20"},
    {30, "30"},
};

// 5 bounds + the implicit +Inf bucket = 6 buckets (more than the public ones)
constexpr TTabletPercentileCounter::TRangeDef RANGES_6[] = {
    {10, "10"},
    {20, "20"},
    {30, "30"},
    {40, "40"},
    {50, "50"},
};

// The public metrics of the test descriptor (the index is the wire slot)
// Gauge 0 is SUM(ExecGauge) + SUM(AppGauge)
constexpr ui32 GAUGE_MAX = 1;               // MAX(ExecMax), LeaderOnly
constexpr ui32 RATE = 0;                    // ExecRate + AppRate
constexpr ui32 RATE_LEADER_ONLY = 1;        // AppLeaderRate, LeaderOnly
constexpr ui32 HIST_OF_CUMULATIVE = 0;      // HIST(ExecRate)
constexpr ui32 HIST_OF_SIMPLE = 1;          // HIST(ExecGauge)
constexpr ui32 PLAIN_LEVEL = 2;             // AppLevel, an integral percentile counter
constexpr ui32 PLAIN_INCREMENT = 3;         // AppIncrement, a derivative percentile counter
constexpr ui32 HIST_LEADER_ONLY = 4;        // HIST(AppLeaderRate), LeaderOnly

constexpr ui32 GAUGE_COUNT = 2;
constexpr ui32 RATE_COUNT = 2;
constexpr ui32 HISTOGRAM_COUNT = 5;
constexpr ui32 BUCKET_COUNT = 4;

// The slots of the synthetic layout
constexpr ui32 EXEC_GAUGE = 0;
constexpr ui32 EXEC_MAX = 1;
constexpr ui32 EXEC_UNUSED = 2;
constexpr ui32 EXEC_RATE = 0;
constexpr ui32 EXEC_UNUSED_RATE = 1;
constexpr ui32 APP_GAUGE = 0;
constexpr ui32 APP_RATE = 0;
constexpr ui32 APP_LEADER_RATE = 1;
constexpr ui32 APP_LEVEL = 0;
constexpr ui32 APP_INCREMENT = 1;

const TInstant T0 = TInstant::Seconds(1000000);

const TTabletKey TABLET_1(72075186224037888ull, 0);
const TTabletKey TABLET_2(72075186224037889ull, 0);
const TTabletKey TABLET_3(72075186224037890ull, 0);
const TTabletKey FOLLOWER_1(72075186224037888ull, 1);

TSourceRef ExecutorSource(ESourceWrapper wrapper, const TString& name) {
    return TSourceRef{SCC_EXECUTOR, wrapper, name};
}

TSourceRef AppSource(ESourceWrapper wrapper, const TString& name) {
    return TSourceRef{SCC_TABLET, wrapper, name};
}

TMetricSpec MakeSpec(EMetricKind kind, const TString& name, TVector<TSourceRef> sources, bool leaderOnly = false) {
    return MakeMetricSpec(kind, name, leaderOnly, std::move(sources));
}

/**
 * A histogram with BUCKET_COUNT public buckets.
 */
TMetricSpec MakeHistogramSpec(const TString& name, TVector<TSourceRef> sources, bool integral, bool leaderOnly = false) {
    return MakeMetricSpec(EMetricKind::Histogram, name, leaderOnly, std::move(sources), {1, 2, 3}, integral);
}

/**
 * The name of the aggregated counter of the source, as NPrivate::TAggregatedTabletCounters names it.
 */
TString GetAggregateName(const TSourceRef& source) {
    switch (source.Wrapper) {
    case ESourceWrapper::None:
        return source.Name;
    case ESourceWrapper::Sum:
        return TString::Join("SUM(", source.Name, ")");
    case ESourceWrapper::Max:
        return TString::Join("MAX(", source.Name, ")");
    case ESourceWrapper::Hist:
        return TString::Join("HIST(", source.Name, ")");
    }
    UNIT_FAIL("unexpected source wrapper");
    return {};
}

/**
 * A descriptor with every kind of source, including the LeaderOnly ones.
 */
TDetailedMetricsDescriptor MakeTestDescriptor() {
    TDetailedMetricsDescriptor descriptor;

    descriptor.Gauges.push_back(MakeSpec(EMetricKind::Gauge, "table.test.sum", {
        ExecutorSource(ESourceWrapper::Sum, "ExecGauge"),
        AppSource(ESourceWrapper::Sum, "AppGauge"),
    }));
    descriptor.Gauges.push_back(MakeSpec(EMetricKind::Gauge, "table.test.max", {
        ExecutorSource(ESourceWrapper::Max, "ExecMax"),
    }, true /* leaderOnly */));

    descriptor.Rates.push_back(MakeSpec(EMetricKind::Rate, "table.test.rate", {
        ExecutorSource(ESourceWrapper::None, "ExecRate"),
        AppSource(ESourceWrapper::None, "AppRate"),
    }));
    descriptor.Rates.push_back(MakeSpec(EMetricKind::Rate, "table.test.leader_rate", {
        AppSource(ESourceWrapper::None, "AppLeaderRate"),
    }, true /* leaderOnly */));

    descriptor.Histograms.push_back(MakeHistogramSpec("table.test.rate_level", {
        ExecutorSource(ESourceWrapper::Hist, "ExecRate"),
    }, false /* integral */));
    descriptor.Histograms.push_back(MakeHistogramSpec("table.test.gauge_level", {
        ExecutorSource(ESourceWrapper::Hist, "ExecGauge"),
    }, false /* integral */));
    descriptor.Histograms.push_back(MakeHistogramSpec("table.test.plain_level", {
        AppSource(ESourceWrapper::None, "AppLevel"),
    }, true /* integral */));
    descriptor.Histograms.push_back(MakeHistogramSpec("table.test.plain_increment", {
        AppSource(ESourceWrapper::None, "AppIncrement"),
    }, false /* integral */));
    descriptor.Histograms.push_back(MakeHistogramSpec("table.test.leader_level", {
        AppSource(ESourceWrapper::Hist, "AppLeaderRate"),
    }, false /* integral */, true /* leaderOnly */));

    return descriptor;
}

template <ui32 RangeCount>
TTestCounters MakeExecutorCounters(const TTabletPercentileCounter::TRangeDef (&ranges)[RangeCount]) {
    TTestCounters counters({
        .Simple = {"ExecGauge", "ExecMax", "ExecUnused"},
        .Cumulative = {"ExecRate", "ExecUnusedRate"},
        .Percentile = {"HIST(ExecRate)", "HIST(ExecGauge)"},
    });

    counters.InitPercentile(0, ranges, false /* integral */);
    counters.InitPercentile(1, ranges, false /* integral */);
    return counters;
}

TTestCounters MakeAppCountersWithoutPercentiles() {
    return TTestCounters({
        .Simple = {"AppGauge"},
        .Cumulative = {"AppRate", "AppLeaderRate"},
        .Percentile = {"AppLevel", "AppIncrement", "HIST(AppLeaderRate)"},
    });
}

template <ui32 RangeCount>
TTestCounters MakeAppCounters(const TTabletPercentileCounter::TRangeDef (&ranges)[RangeCount]) {
    TTestCounters counters = MakeAppCountersWithoutPercentiles();
    counters.InitPercentile(0, ranges, true /* integral */);
    counters.InitPercentile(1, ranges, false /* integral */);
    counters.InitPercentile(2, ranges, false /* integral */);
    return counters;
}

/**
 * The synthetic layout, whose source buckets match the public ones.
 */
TTestCounters MakeExecutorCounters() {
    return MakeExecutorCounters(RANGES_4);
}

TTestCounters MakeAppCounters() {
    return MakeAppCounters(RANGES_4);
}

/**
 * The values of one report of a source in the synthetic layout.
 */
struct TReport {
    ui64 ExecGauge = 0;
    ui64 ExecMax = 0;
    ui64 AppGauge = 0;

    // The deltas since the previous report
    ui64 ExecRate = 0;
    ui64 AppRate = 0;
    ui64 AppLeaderRate = 0;

    // The source buckets of the percentile counters (missing buckets are zero)
    TVector<ui64> AppLevel;
    TVector<ui64> AppIncrement;
};

/**
 * Set the bucket values of a percentile counter, whose values are zero.
 */
void SetBuckets(TTabletPercentileCounter& percentile, const TVector<ui64>& values) {
    UNIT_ASSERT_LE(values.size(), percentile.GetRangeCount());

    for (ui32 i = 0; i < values.size(); ++i) {
        // NOTE: The bound of a bucket falls into the bucket itself (right inclusive)
        percentile.AddFor(percentile.GetRangeBound(i), values[i]);
    }
}

/**
 * Fill the counters of the synthetic layout with one report. The unbound slots get
 * values, which must never show up in the public values.
 */
void FillCounters(TTestCounters& executor, TTestCounters& app, const TReport& report) {
    auto& executorCounters = executor.Get();
    auto& appCounters = app.Get();

    executorCounters.ResetCounters();
    appCounters.ResetCounters();

    executorCounters.Simple()[EXEC_GAUGE].Set(report.ExecGauge);
    executorCounters.Simple()[EXEC_MAX].Set(report.ExecMax);
    executorCounters.Simple()[EXEC_UNUSED].Set(1000000007);
    executorCounters.Cumulative()[EXEC_RATE].Increment(report.ExecRate);
    executorCounters.Cumulative()[EXEC_UNUSED_RATE].Increment(1000000009);

    appCounters.Simple()[APP_GAUGE].Set(report.AppGauge);
    appCounters.Cumulative()[APP_RATE].Increment(report.AppRate);
    appCounters.Cumulative()[APP_LEADER_RATE].Increment(report.AppLeaderRate);
    SetBuckets(appCounters.Percentile()[APP_LEVEL], report.AppLevel);
    SetBuckets(appCounters.Percentile()[APP_INCREMENT], report.AppIncrement);
}

/**
 * The descriptor, the synthetic layout and the binding of the descriptor to the layout.
 *
 * @note Not movable: the binding points to the descriptor.
 */
class TTestEnv : TNonCopyable {
public:
    /**
     * The synthetic layout, whose source buckets match the public ones: every source binds.
     */
    TTestEnv()
        : TTestEnv(MakeExecutorCounters(), MakeAppCounters())
    {
        UNIT_ASSERT_C(Binding->Problems.empty(), JoinSeq("\n", Binding->Problems));
    }

    TTestEnv(TTestCounters executor, TTestCounters app)
        : Descriptor(MakeTestDescriptor())
        , Executor(std::move(executor))
        , App(std::move(app))
        , Binding(BindDetailedMetrics(Descriptor, Executor.Get(), App.Get()))
    {
    }

    TDetailedValuesAccumulator MakeAccumulator(bool skipLeaderOnly = false) const {
        return TDetailedValuesAccumulator(Binding.Get(), skipLeaderOnly);
    }

    void Apply(TDetailedValuesAccumulator& accumulator, const TTabletKey& tablet, const TReport& report, TInstant now) {
        FillCounters(Executor, App, report);
        accumulator.Apply(tablet, Executor.Get(), App.Get(), now);
    }

    const TDetailedMetricsDescriptor Descriptor;
    TTestCounters Executor;
    TTestCounters App;
    const THolder<TDetailedMetricsBinding> Binding;
};

////////////////////////////////////////////////////////////////////////////////
// Reading the packed values

NKikimrSysView::TDbCounters Pack(TDetailedValuesAccumulator& accumulator) {
    NKikimrSysView::TDbCounters packed;

    // The previous contents of the message are cleared
    packed.AddSimple(12345);
    packed.AddCumulative(0);
    packed.AddCumulative(12345);
    packed.AddHistogram()->AddBuckets(0);

    accumulator.Pack(packed);
    return packed;
}

TVector<ui64> GetGauges(const NKikimrSysView::TDbCounters& packed) {
    return TVector<ui64>(packed.GetSimple().begin(), packed.GetSimple().end());
}

/**
 * @return The sparse (rate, delta) pairs, checked to be in order and non-zero
 */
TVector<ui64> GetRatePairs(const NKikimrSysView::TDbCounters& packed) {
    const auto& pairs = packed.GetCumulative();
    UNIT_ASSERT_VALUES_EQUAL(pairs.size() % 2, 0);

    for (int i = 0; i + 1 < pairs.size(); i += 2) {
        UNIT_ASSERT_LT(pairs[i], packed.GetCumulativeCount());
        UNIT_ASSERT_C(pairs[i + 1] != 0, "zero delta of rate " << pairs[i]);
        if (i > 0) {
            UNIT_ASSERT_LT(pairs[i - 2], pairs[i]);
        }
    }

    return TVector<ui64>(pairs.begin(), pairs.end());
}

/**
 * @return The dense buckets of the given histogram, the sparse pairs are checked
 *         to be in order, non-zero and within BucketsCount
 */
TVector<ui64> GetBuckets(const NKikimrSysView::TDbCounters& packed, ui32 metric) {
    UNIT_ASSERT_LT(metric, packed.HistogramSize());

    const auto& histogram = packed.GetHistogram(metric);
    const auto& pairs = histogram.GetBuckets();
    UNIT_ASSERT_VALUES_EQUAL(pairs.size() % 2, 0);

    TVector<ui64> buckets(histogram.GetBucketsCount(), 0);

    for (int i = 0; i + 1 < pairs.size(); i += 2) {
        UNIT_ASSERT_LT(pairs[i], buckets.size());
        UNIT_ASSERT_C(pairs[i + 1] != 0, "zero count of bucket " << pairs[i]);
        if (i > 0) {
            UNIT_ASSERT_LT(pairs[i - 2], pairs[i]);
        }

        buckets[pairs[i]] = pairs[i + 1];
    }

    return buckets;
}

/**
 * Check the shape of the packed values of the test descriptor: every gauge, every rate
 * and every histogram with its public bucket count and its NonDerivative mark.
 */
void AssertTestShape(const NKikimrSysView::TDbCounters& packed) {
    UNIT_ASSERT_VALUES_EQUAL(packed.SimpleSize(), GAUGE_COUNT);
    UNIT_ASSERT_VALUES_EQUAL(packed.GetCumulativeCount(), RATE_COUNT);
    UNIT_ASSERT_VALUES_EQUAL(packed.HistogramSize(), HISTOGRAM_COUNT);

    for (ui32 metric = 0; metric < HISTOGRAM_COUNT; ++metric) {
        const auto& histogram = packed.GetHistogram(metric);
        UNIT_ASSERT_VALUES_EQUAL_C(histogram.GetBucketsCount(), BUCKET_COUNT, metric);
        UNIT_ASSERT_VALUES_EQUAL_C(histogram.HasNonDerivative(), metric != PLAIN_INCREMENT, metric);
        UNIT_ASSERT_VALUES_EQUAL_C(histogram.GetNonDerivative(), metric != PLAIN_INCREMENT, metric);
    }
}

////////////////////////////////////////////////////////////////////////////////
// The oracle: the HEAD node path (NPrivate::TAggregatedTabletCounters configured
// the way the node bucket TCountersBucket configured them)

class TOracleBucket {
public:
    TOracleBucket(const TDetailedMetricsCounterNames& names, TTabletTypes::EType tabletType)
        : ExecutorGroup(MakeIntrusive<NMonitoring::TDynamicCounters>())
        , AppGroup(MakeIntrusive<NMonitoring::TDynamicCounters>())
        , ExecutorCounters(ExecutorGroup)
        , AppCounters(AppGroup)
        , Names(names)
        , TabletType(tabletType)
    {
    }

    void Apply(
        const TTabletKey& tablet,
        const TTabletCountersBase& executorCounters,
        const TTabletCountersBase& appCounters,
        TInstant now)
    {
        auto [it, inserted] = SourceIds.try_emplace(tablet, NextSourceId);
        if (inserted) {
            ++NextSourceId;
        }

        if (!ExecutorCounters.IsInitialized) {
            ExecutorCounters.Initialize(&executorCounters, &Names.ExecutorNames);
        }
        if (!AppCounters.IsInitialized) {
            AppCounters.Initialize(&appCounters, &Names.AppNames);
        }

        ExecutorCounters.Apply(it->second, &executorCounters, TabletType, now);
        AppCounters.Apply(it->second, &appCounters, TabletType, now);
    }

    void Forget(const TTabletKey& tablet) {
        auto it = SourceIds.find(tablet);
        if (it == SourceIds.end()) {
            return;
        }

        if (ExecutorCounters.IsInitialized) {
            ExecutorCounters.Forget(it->second);
        }
        if (AppCounters.IsInitialized) {
            AppCounters.Forget(it->second);
        }

        SourceIds.erase(it);
    }

    void RecalcAll() {
        if (ExecutorCounters.IsInitialized) {
            ExecutorCounters.RecalcAll();
        }
        if (AppCounters.IsInitialized) {
            AppCounters.RecalcAll();
        }
    }

    /**
     * @return The value of the aggregated counter (zero if there is none yet)
     */
    ui64 GetCounter(ESourceCounterCategory category, const TString& name) const {
        const auto counter = GetGroup(category)->FindCounter(name);
        return counter ? static_cast<ui64>(counter->Val()) : 0;
    }

    /**
     * @return The buckets of the aggregated histogram (empty if there is none yet)
     */
    TVector<ui64> GetHistogram(ESourceCounterCategory category, const TString& name) const {
        const auto histogram = GetGroup(category)->FindHistogram(name);
        if (!histogram) {
            return {};
        }

        const auto snapshot = histogram->Snapshot();

        TVector<ui64> buckets;
        for (ui32 i = 0; i < snapshot->Count(); ++i) {
            buckets.push_back(snapshot->Value(i));
        }

        return buckets;
    }

private:
    NMonitoring::TDynamicCounterPtr GetGroup(ESourceCounterCategory category) const {
        // The same rule as the binding: anything but SCC_TABLET is an Executor counter
        return category == ESourceCounterCategory::SCC_TABLET ? AppGroup : ExecutorGroup;
    }

    NMonitoring::TDynamicCounterPtr ExecutorGroup;
    NMonitoring::TDynamicCounterPtr AppGroup;
    NKikimr::NPrivate::TAggregatedTabletCounters ExecutorCounters;
    NKikimr::NPrivate::TAggregatedTabletCounters AppCounters;
    const TDetailedMetricsCounterNames& Names;
    const TTabletTypes::EType TabletType;

    THashMap<TTabletKey, ui64> SourceIds;
    ui64 NextSourceId = 0;
};

/**
 * Fill every simple and cumulative counter of the layout with random values.
 */
void FillRandom(TFastRng64& rng, TTabletCountersBase& counters) {
    counters.ResetCounters();

    for (ui32 i = 0; i < counters.Simple().Size(); ++i) {
        counters.Simple()[i].Set(rng.Uniform(1ull << 40));
    }

    for (ui32 i = 0; i < counters.Cumulative().Size(); ++i) {
        const ui64 kind = rng.Uniform(100);
        ui64 delta = 0;

        if (kind < 2) {
            // Overflows delta * 1000000 the same way on both sides
            delta = (1ull << 50) + rng.Uniform(1ull << 40);
        } else if (kind < 10) {
            delta = 0;
        } else {
            // Rates from zero to a few cores, so that HIST(ConsumedCPU) uses every bucket
            delta = rng.Uniform(3000000);
        }

        counters.Cumulative()[i].Increment(delta);
    }
}

/**
 * Fold the buckets beyond the given count into the last one.
 */
TVector<ui64> ClampBuckets(TVector<ui64> buckets, size_t bucketCount) {
    TVector<ui64> result(bucketCount, 0);

    for (size_t i = 0; i < buckets.size(); ++i) {
        result[Min(i, bucketCount - 1)] += buckets[i];
    }

    return result;
}

/**
 * Compare the packed values of the accumulator with the oracle, read by the source names
 * of the descriptor (the same counters, which the YDB metrics mapper used to read).
 *
 * @param[in,out] drainedRates The sum of the rate deltas packed so far
 */
void AssertMatchesOracle(
    const TDetailedMetricsDescriptor& descriptor,
    TOracleBucket& oracle,
    TDetailedValuesAccumulator& accumulator,
    bool skipLeaderOnly,
    TVector<ui64>& drainedRates,
    const TString& context)
{
    oracle.RecalcAll();
    const auto packed = Pack(accumulator);

    // Gauges: SUM(x) and MAX(x) of the oracle, summed over the sources of the metric
    UNIT_ASSERT_VALUES_EQUAL_C(packed.SimpleSize(), descriptor.Gauges.size(), context);

    for (size_t metric = 0; metric < descriptor.Gauges.size(); ++metric) {
        const auto& spec = descriptor.Gauges[metric];

        ui64 expected = 0;
        if (!(skipLeaderOnly && spec.LeaderOnly)) {
            for (const auto& source : spec.Sources) {
                expected += oracle.GetCounter(source.Category, GetAggregateName(source));
            }
        }

        UNIT_ASSERT_VALUES_EQUAL_C(packed.GetSimple(metric), expected, context << ", gauge " << spec.Name);
    }

    // Rates: the drained deltas add up to the accumulated counters x of the oracle
    UNIT_ASSERT_VALUES_EQUAL_C(packed.GetCumulativeCount(), descriptor.Rates.size(), context);

    const auto pairs = GetRatePairs(packed);
    for (size_t i = 0; i < pairs.size(); i += 2) {
        drainedRates[pairs[i]] += pairs[i + 1];
    }

    for (size_t metric = 0; metric < descriptor.Rates.size(); ++metric) {
        const auto& spec = descriptor.Rates[metric];

        ui64 expected = 0;
        if (!(skipLeaderOnly && spec.LeaderOnly)) {
            for (const auto& source : spec.Sources) {
                expected += oracle.GetCounter(source.Category, source.Name);
            }
        }

        UNIT_ASSERT_VALUES_EQUAL_C(drainedRates[metric], expected, context << ", rate " << spec.Name);
    }

    // Level histograms: the buckets of HIST(x) of the oracle
    UNIT_ASSERT_VALUES_EQUAL_C(packed.HistogramSize(), descriptor.Histograms.size(), context);

    for (ui32 metric = 0; metric < descriptor.Histograms.size(); ++metric) {
        const auto& spec = descriptor.Histograms[metric];
        UNIT_ASSERT_C(spec.StaticLevel, "the differential test covers HIST(x) only: " << spec.Name);
        UNIT_ASSERT_C(packed.GetHistogram(metric).GetNonDerivative(), context << ", histogram " << spec.Name);
        UNIT_ASSERT_VALUES_EQUAL_C(packed.GetHistogram(metric).GetBucketsCount(), spec.BucketCount(), context);

        TVector<ui64> expected(spec.BucketCount(), 0);
        if (!(skipLeaderOnly && spec.LeaderOnly)) {
            for (const auto& source : spec.Sources) {
                const auto buckets = ClampBuckets(
                    oracle.GetHistogram(source.Category, GetAggregateName(source)),
                    spec.BucketCount());

                for (size_t bucket = 0; bucket < expected.size(); ++bucket) {
                    expected[bucket] += buckets[bucket];
                }
            }
        }

        UNIT_ASSERT_VALUES_EQUAL_C(GetBuckets(packed, metric), expected, context << ", histogram " << spec.Name);
    }
}

/**
 * Run a random stream of reports, forgets and packs of up to 20 DataShard tablets
 * through the oracle and the accumulator, comparing them at every pack.
 */
void RunDifferential(ui64 seed, bool skipLeaderOnly) {
    const auto* descriptor = GetDetailedMetricsDescriptor(TTabletTypes::DataShard);
    UNIT_ASSERT(descriptor);

    TExecutorCounters executorCounters;
    const auto appCounters = CreateAppCountersByTabletType(TTabletTypes::DataShard);
    UNIT_ASSERT(appCounters);

    const auto binding = BindDetailedMetrics(*descriptor, executorCounters, *appCounters);
    UNIT_ASSERT_C(binding->Problems.empty(), JoinSeq("\n", binding->Problems));

    TFastRng64 rng(seed);

    const size_t sourceCount = 1 + rng.Uniform(20);
    TVector<TTabletKey> keys;
    for (size_t i = 0; i < sourceCount; ++i) {
        keys.emplace_back(72075186224037888ull + i, skipLeaderOnly ? 1 + rng.Uniform(2) : 0);
    }

    const auto* names = GetDetailedMetricsCounterNames(TTabletTypes::DataShard);
    UNIT_ASSERT(names);
    TOracleBucket oracle(*names, TTabletTypes::DataShard);
    TDetailedValuesAccumulator accumulator(binding.Get(), skipLeaderOnly);
    TVector<ui64> drainedRates(descriptor->Rates.size(), 0);

    TInstant now = T0;

    for (ui32 step = 0; step < 1500; ++step) {
        // The time mostly goes forward, but also stays the same or goes back
        const ui64 timeStep = rng.Uniform(100);
        if (timeStep < 15) {
            // The same time
        } else if (timeStep < 25) {
            now -= TDuration::MilliSeconds(rng.Uniform(3000));
        } else {
            now += TDuration::MilliSeconds(1 + rng.Uniform(5000));
        }

        const auto& key = keys[rng.Uniform(keys.size())];
        const ui64 operation = rng.Uniform(100);

        if (operation < 8) {
            oracle.Forget(key);
            accumulator.Forget(key);
        } else if (operation < 14) {
            AssertMatchesOracle(*descriptor, oracle, accumulator, skipLeaderOnly, drainedRates,
                TStringBuilder() << "seed " << seed << ", step " << step);
        } else {
            FillRandom(rng, executorCounters);
            FillRandom(rng, *appCounters);
            oracle.Apply(key, executorCounters, *appCounters, now);
            accumulator.Apply(key, executorCounters, *appCounters, now);
        }
    }

    // Forget everything: the final deltas, then nothing
    for (const auto& key : keys) {
        oracle.Forget(key);
        accumulator.Forget(key);
    }

    UNIT_ASSERT(accumulator.IsEmpty());
    AssertMatchesOracle(*descriptor, oracle, accumulator, skipLeaderOnly, drainedRates,
        TStringBuilder() << "seed " << seed << ", final");
    AssertMatchesOracle(*descriptor, oracle, accumulator, skipLeaderOnly, drainedRates,
        TStringBuilder() << "seed " << seed << ", after final");
}

} // namespace

Y_UNIT_TEST_SUITE(TDetailedValuesAccumulatorTest) {

    Y_UNIT_TEST(LeafPacksEveryKindOfMetric) {
        TTestEnv env;
        auto accumulator = env.MakeAccumulator();
        UNIT_ASSERT(accumulator.IsEmpty());

        env.Apply(accumulator, TABLET_1, {
            .ExecGauge = 15,
            .ExecMax = 7,
            .AppGauge = 100,
            .ExecRate = 25,
            .AppRate = 3,
            .AppLeaderRate = 4,
            .AppLevel = {1, 0, 2, 5},
            .AppIncrement = {0, 3, 0, 1},
        }, T0);

        UNIT_ASSERT(!accumulator.IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(accumulator.GetSourceCount(), 1);

        const auto packed = Pack(accumulator);
        AssertTestShape(packed);

        UNIT_ASSERT_VALUES_EQUAL(GetGauges(packed), TVector<ui64>({115, 7}));
        UNIT_ASSERT_VALUES_EQUAL(GetRatePairs(packed), TVector<ui64>({RATE, 28, RATE_LEADER_ONLY, 4}));

        // The first report of a source has no rate yet: the observation is zero
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, HIST_OF_CUMULATIVE), TVector<ui64>({1, 0, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, HIST_LEADER_ONLY), TVector<ui64>({1, 0, 0, 0}));

        // 15 falls into the source bucket (10, 20]
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, HIST_OF_SIMPLE), TVector<ui64>({0, 1, 0, 0}));

        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, PLAIN_LEVEL), TVector<ui64>({1, 0, 2, 5}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, PLAIN_INCREMENT), TVector<ui64>({0, 3, 0, 1}));

        // The exact sparse encoding
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>(packed.GetHistogram(PLAIN_LEVEL).GetBuckets().begin(), packed.GetHistogram(PLAIN_LEVEL).GetBuckets().end()),
            TVector<ui64>({0, 1, 2, 2, 3, 5}));
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>(packed.GetHistogram(PLAIN_INCREMENT).GetBuckets().begin(), packed.GetHistogram(PLAIN_INCREMENT).GetBuckets().end()),
            TVector<ui64>({1, 3, 3, 1}));
    }

    Y_UNIT_TEST(GaugesSumAndMaxTheLatestValuesOfTheLiveSources) {
        TTestEnv env;
        auto accumulator = env.MakeAccumulator();

        env.Apply(accumulator, TABLET_1, {.ExecGauge = 1, .ExecMax = 30, .AppGauge = 10}, T0);
        env.Apply(accumulator, TABLET_2, {.ExecGauge = 2, .ExecMax = 50, .AppGauge = 20}, T0);
        env.Apply(accumulator, TABLET_3, {.ExecGauge = 4, .ExecMax = 40, .AppGauge = 40}, T0);

        // SUM(ExecGauge) + SUM(AppGauge), MAX(ExecMax)
        UNIT_ASSERT_VALUES_EQUAL(GetGauges(Pack(accumulator)), TVector<ui64>({77, 50}));

        // The latest values replace the previous ones of the source
        env.Apply(accumulator, TABLET_2, {.ExecGauge = 8, .ExecMax = 5, .AppGauge = 80}, T0 + TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(GetGauges(Pack(accumulator)), TVector<ui64>({143, 40}));

        // A forgotten source is gone
        accumulator.Forget(TABLET_3);
        UNIT_ASSERT_VALUES_EQUAL(GetGauges(Pack(accumulator)), TVector<ui64>({99, 30}));

        // The gauges are absolute, so packing them again gives the same values
        UNIT_ASSERT_VALUES_EQUAL(GetGauges(Pack(accumulator)), TVector<ui64>({99, 30}));
    }

    Y_UNIT_TEST(RatesAddEveryReportUntilPacked) {
        TTestEnv env;
        auto accumulator = env.MakeAccumulator();

        env.Apply(accumulator, TABLET_1, {.ExecRate = 5, .AppRate = 1}, T0);
        env.Apply(accumulator, TABLET_1, {.ExecRate = 7}, T0 + TDuration::Seconds(1));
        env.Apply(accumulator, TABLET_2, {.ExecRate = 10, .AppLeaderRate = 3}, T0);

        auto packed = Pack(accumulator);
        UNIT_ASSERT_VALUES_EQUAL(packed.GetCumulativeCount(), RATE_COUNT);
        UNIT_ASSERT_VALUES_EQUAL(GetRatePairs(packed), TVector<ui64>({RATE, 23, RATE_LEADER_ONLY, 3}));

        // Drained: no deltas until the next report
        packed = Pack(accumulator);
        UNIT_ASSERT_VALUES_EQUAL(packed.GetCumulativeCount(), RATE_COUNT);
        UNIT_ASSERT_VALUES_EQUAL(GetRatePairs(packed), TVector<ui64>());

        // A zero delta makes no pair
        env.Apply(accumulator, TABLET_2, {.AppLeaderRate = 2}, T0 + TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(GetRatePairs(Pack(accumulator)), TVector<ui64>({RATE_LEADER_ONLY, 2}));

        // The deltas add up modulo 2^64
        env.Apply(accumulator, TABLET_1, {.ExecRate = Max<ui64>()}, T0 + TDuration::Seconds(2));
        env.Apply(accumulator, TABLET_2, {.ExecRate = 3}, T0 + TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(GetRatePairs(Pack(accumulator)), TVector<ui64>({RATE, 2}));
    }

    Y_UNIT_TEST(ForgetKeepsThePendingDeltasForTheFinalPack) {
        TTestEnv env;
        auto accumulator = env.MakeAccumulator();

        env.Apply(accumulator, TABLET_1, {
            .ExecGauge = 15,
            .ExecMax = 7,
            .AppGauge = 100,
            .ExecRate = 25,
            .AppLevel = {1, 0, 0, 0},
            .AppIncrement = {0, 3, 0, 0},
        }, T0);
        env.Apply(accumulator, TABLET_2, {
            .ExecGauge = 1,
            .ExecMax = 1,
            .AppRate = 5,
            .AppLeaderRate = 6,
            .AppLevel = {0, 0, 0, 2},
            .AppIncrement = {1, 0, 0, 0},
        }, T0);

        // Forgetting an unknown source changes nothing
        accumulator.Forget(TABLET_3);
        UNIT_ASSERT_VALUES_EQUAL(accumulator.GetSourceCount(), 2);

        accumulator.Forget(TABLET_1);
        accumulator.Forget(TABLET_2);
        UNIT_ASSERT(accumulator.IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(accumulator.GetSourceCount(), 0);

        // The final deltas of the retired bucket: zero gauges, empty level histograms,
        // the pending rates and increments
        auto packed = Pack(accumulator);
        AssertTestShape(packed);

        UNIT_ASSERT_VALUES_EQUAL(GetGauges(packed), TVector<ui64>({0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetRatePairs(packed), TVector<ui64>({RATE, 30, RATE_LEADER_ONLY, 6}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, HIST_OF_CUMULATIVE), TVector<ui64>({0, 0, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, HIST_OF_SIMPLE), TVector<ui64>({0, 0, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, PLAIN_LEVEL), TVector<ui64>({0, 0, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, PLAIN_INCREMENT), TVector<ui64>({1, 3, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, HIST_LEADER_ONLY), TVector<ui64>({0, 0, 0, 0}));

        // Nothing is left after the final deltas
        packed = Pack(accumulator);
        AssertTestShape(packed);

        UNIT_ASSERT_VALUES_EQUAL(GetGauges(packed), TVector<ui64>({0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetRatePairs(packed), TVector<ui64>());
        for (ui32 metric = 0; metric < HISTOGRAM_COUNT; ++metric) {
            UNIT_ASSERT_VALUES_EQUAL_C(packed.GetHistogram(metric).BucketsSize(), 0, metric);
        }
    }

    Y_UNIT_TEST(DestructivePackDrainsOnlyTheIncrements) {
        TTestEnv env;
        auto accumulator = env.MakeAccumulator();

        env.Apply(accumulator, TABLET_1, {
            .ExecGauge = 25,
            .ExecMax = 3,
            .AppGauge = 5,
            .ExecRate = 11,
            .AppLevel = {0, 4, 0, 1},
            .AppIncrement = {2, 0, 0, 0},
        }, T0);
        env.Apply(accumulator, TABLET_1, {
            .ExecGauge = 25,
            .ExecMax = 3,
            .AppGauge = 5,
            .ExecRate = 15,
            .AppLevel = {0, 4, 0, 1},
            .AppIncrement = {0, 0, 0, 7},
        }, T0 + TDuration::Seconds(1));

        const auto first = Pack(accumulator);
        AssertTestShape(first);
        UNIT_ASSERT_VALUES_EQUAL(GetGauges(first), TVector<ui64>({30, 3}));
        UNIT_ASSERT_VALUES_EQUAL(GetRatePairs(first), TVector<ui64>({RATE, 26}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(first, HIST_OF_CUMULATIVE), TVector<ui64>({0, 1, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(first, HIST_OF_SIMPLE), TVector<ui64>({0, 0, 1, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(first, PLAIN_LEVEL), TVector<ui64>({0, 4, 0, 1}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(first, PLAIN_INCREMENT), TVector<ui64>({2, 0, 0, 7}));

        // The same gauges and levels, no rates, no increments
        const auto second = Pack(accumulator);
        AssertTestShape(second);
        UNIT_ASSERT_VALUES_EQUAL(GetGauges(second), GetGauges(first));
        UNIT_ASSERT_VALUES_EQUAL(GetRatePairs(second), TVector<ui64>());
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(second, HIST_OF_CUMULATIVE), GetBuckets(first, HIST_OF_CUMULATIVE));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(second, HIST_OF_SIMPLE), GetBuckets(first, HIST_OF_SIMPLE));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(second, PLAIN_LEVEL), GetBuckets(first, PLAIN_LEVEL));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(second, PLAIN_INCREMENT), TVector<ui64>({0, 0, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(second, HIST_LEADER_ONLY), GetBuckets(first, HIST_LEADER_ONLY));
    }

    Y_UNIT_TEST(HistogramOfCumulativeObservesTheRateSinceThePreviousReport) {
        TTestEnv env;
        auto accumulator = env.MakeAccumulator();

        // The rate counts every delta regardless of the time
        ui64 rateTotal = 0;

        const auto observe = [&](ui64 delta, TInstant now) {
            env.Apply(accumulator, TABLET_1, {.ExecRate = delta}, now);

            const auto packed = Pack(accumulator);
            const auto pairs = GetRatePairs(packed);
            for (size_t i = 0; i < pairs.size(); i += 2) {
                rateTotal += pairs[i + 1];
            }

            return GetBuckets(packed, HIST_OF_CUMULATIVE);
        };

        // The first report has no previous one: the rate is zero
        UNIT_ASSERT_VALUES_EQUAL(observe(1000, T0), TVector<ui64>({1, 0, 0, 0}));

        // 15 per second, (10, 20]
        UNIT_ASSERT_VALUES_EQUAL(observe(15, T0 + TDuration::Seconds(1)), TVector<ui64>({0, 1, 0, 0}));

        // 50 in 2 seconds, (20, 30]
        UNIT_ASSERT_VALUES_EQUAL(observe(50, T0 + TDuration::Seconds(3)), TVector<ui64>({0, 0, 1, 0}));

        // 10 in half a second, (10, 20]
        UNIT_ASSERT_VALUES_EQUAL(observe(10, T0 + TDuration::MilliSeconds(3500)), TVector<ui64>({0, 1, 0, 0}));

        // The same time: no duration, the rate is zero
        UNIT_ASSERT_VALUES_EQUAL(observe(50, T0 + TDuration::MilliSeconds(3500)), TVector<ui64>({1, 0, 0, 0}));

        // The time goes back: no duration either, but the time is kept
        UNIT_ASSERT_VALUES_EQUAL(observe(50, T0 + TDuration::Seconds(1)), TVector<ui64>({1, 0, 0, 0}));

        // So the next duration counts from the time, which went back: 35 per second, (30, +Inf)
        UNIT_ASSERT_VALUES_EQUAL(observe(35, T0 + TDuration::Seconds(2)), TVector<ui64>({0, 0, 0, 1}));

        // The bounds are right inclusive: exactly 10 per second is [0, 10]
        UNIT_ASSERT_VALUES_EQUAL(observe(100, T0 + TDuration::Seconds(12)), TVector<ui64>({1, 0, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(observe(11, T0 + TDuration::Seconds(13)), TVector<ui64>({0, 1, 0, 0}));

        UNIT_ASSERT_VALUES_EQUAL(rateTotal, 1000 + 15 + 50 + 10 + 50 + 50 + 35 + 100 + 11);

        // A source added again has no previous report
        accumulator.Forget(TABLET_1);
        UNIT_ASSERT_VALUES_EQUAL(observe(1000, T0 + TDuration::Seconds(14)), TVector<ui64>({1, 0, 0, 0}));
    }

    Y_UNIT_TEST(HistogramOfSimpleObservesTheLatestValueOfEverySource) {
        TTestEnv env;
        auto accumulator = env.MakeAccumulator();

        const TVector<ui64> values = {0, 10, 11, 30, 31, Max<ui64>()};
        for (ui64 i = 0; i < values.size(); ++i) {
            env.Apply(accumulator, TTabletKey(100 + i, 0), {.ExecGauge = values[i]}, T0);
        }

        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(Pack(accumulator), HIST_OF_SIMPLE), TVector<ui64>({2, 1, 1, 2}));

        // One observation per source: the latest value replaces the previous one
        env.Apply(accumulator, TTabletKey(100, 0), {.ExecGauge = 20}, T0 + TDuration::Seconds(1));
        accumulator.Forget(TTabletKey(105, 0));

        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(Pack(accumulator), HIST_OF_SIMPLE), TVector<ui64>({1, 2, 1, 1}));
    }

    Y_UNIT_TEST(LevelPercentileSumsTheLatestBucketsOfTheLiveSources) {
        TTestEnv env;
        auto accumulator = env.MakeAccumulator();

        env.Apply(accumulator, TABLET_1, {.AppLevel = {1, 2, 0, 0}}, T0);
        env.Apply(accumulator, TABLET_2, {.AppLevel = {0, 1, 1, 1}}, T0);
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(Pack(accumulator), PLAIN_LEVEL), TVector<ui64>({1, 3, 1, 1}));

        // The latest buckets replace the previous ones of the source
        env.Apply(accumulator, TABLET_1, {.AppLevel = {0, 0, 0, 4}}, T0 + TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(Pack(accumulator), PLAIN_LEVEL), TVector<ui64>({0, 1, 1, 5}));

        accumulator.Forget(TABLET_2);
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(Pack(accumulator), PLAIN_LEVEL), TVector<ui64>({0, 0, 0, 4}));
    }

    Y_UNIT_TEST(IncrementPercentileAddsEveryReportUntilPacked) {
        TTestEnv env;
        auto accumulator = env.MakeAccumulator();

        env.Apply(accumulator, TABLET_1, {.AppIncrement = {1, 0, 0, 0}}, T0);
        env.Apply(accumulator, TABLET_1, {.AppIncrement = {0, 2, 0, 0}}, T0 + TDuration::Seconds(1));
        env.Apply(accumulator, TABLET_2, {.AppIncrement = {0, 1, 3, 0}}, T0);

        auto packed = Pack(accumulator);
        UNIT_ASSERT(!packed.GetHistogram(PLAIN_INCREMENT).HasNonDerivative());
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, PLAIN_INCREMENT), TVector<ui64>({1, 3, 3, 0}));

        // Drained
        packed = Pack(accumulator);
        UNIT_ASSERT_VALUES_EQUAL(packed.GetHistogram(PLAIN_INCREMENT).GetBucketsCount(), BUCKET_COUNT);
        UNIT_ASSERT_VALUES_EQUAL(packed.GetHistogram(PLAIN_INCREMENT).BucketsSize(), 0);

        // A forgotten source keeps its increments until the next pack
        env.Apply(accumulator, TABLET_1, {.AppIncrement = {0, 0, 0, 9}}, T0 + TDuration::Seconds(2));
        accumulator.Forget(TABLET_1);
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(Pack(accumulator), PLAIN_INCREMENT), TVector<ui64>({0, 0, 0, 9}));
    }

    Y_UNIT_TEST(SourceBucketsBeyondThePublicOnesAreCountedInTheLastOne) {
        // 6 source buckets for 4 public ones: the binding reports it, but keeps the sources
        TTestEnv env(MakeExecutorCounters(RANGES_6), MakeAppCounters(RANGES_6));
        UNIT_ASSERT_VALUES_EQUAL_C(env.Binding->Problems.size(), HISTOGRAM_COUNT, JoinSeq("\n", env.Binding->Problems));
        for (const auto& problem : env.Binding->Problems) {
            UNIT_ASSERT_STRING_CONTAINS(problem, "has 6 buckets, but the histogram has 4");
        }

        auto accumulator = env.MakeAccumulator();

        // (20, 30], (40, 50], (50, +Inf) and (30, 40]
        const TVector<ui64> values = {25, 45, 1000, 31};
        for (ui64 i = 0; i < values.size(); ++i) {
            env.Apply(accumulator, TTabletKey(100 + i, 0), {
                .ExecGauge = values[i],
                .AppLevel = {1, 1, 1, 1, 1, 1},
                .AppIncrement = {0, 0, 1, 1, 1, 1},
            }, T0);
        }

        const auto packed = Pack(accumulator);
        AssertTestShape(packed);

        // Every observation is kept
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, HIST_OF_SIMPLE), TVector<ui64>({0, 0, 1, 3}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, PLAIN_LEVEL), TVector<ui64>({4, 4, 4, 12}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, PLAIN_INCREMENT), TVector<ui64>({0, 0, 4, 12}));
    }

    Y_UNIT_TEST(ShortAndUninitializedPercentilesAreReadSafely) {
        TTestEnv env;
        auto accumulator = env.MakeAccumulator();

        env.Apply(accumulator, TABLET_1, {.AppLevel = {1, 2, 3, 4}, .AppIncrement = {1, 1, 1, 1}}, T0);

        // The same layout (the same sizes and names), but the percentile counters
        // were never initialized: nothing to read, the level of the source stays
        auto executor = MakeExecutorCounters();
        auto uninitialized = MakeAppCountersWithoutPercentiles();

        FillCounters(executor, uninitialized, {});
        accumulator.Apply(TABLET_1, executor.Get(), uninitialized.Get(), T0 + TDuration::Seconds(1));

        auto packed = Pack(accumulator);
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, PLAIN_LEVEL), TVector<ui64>({1, 2, 3, 4}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, PLAIN_INCREMENT), TVector<ui64>({1, 1, 1, 1}));

        // Fewer buckets than bound: only those are read
        auto shortCounters = MakeAppCounters(RANGES_2);

        FillCounters(executor, shortCounters, {.AppLevel = {5, 6}, .AppIncrement = {7, 8}});
        accumulator.Apply(TABLET_1, executor.Get(), shortCounters.Get(), T0 + TDuration::Seconds(2));

        packed = Pack(accumulator);
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, PLAIN_LEVEL), TVector<ui64>({5, 6, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, PLAIN_INCREMENT), TVector<ui64>({7, 8, 0, 0}));

        // Another layout breaks the precondition, the bound slots are missing:
        // nothing crashes, the missing values are zero
        TTestCounters emptyExecutor;
        TTestCounters emptyApp;

        accumulator.Apply(TABLET_1, emptyExecutor.Get(), emptyApp.Get(), T0 + TDuration::Seconds(3));
        accumulator.Apply(TABLET_2, emptyExecutor.Get(), emptyApp.Get(), T0 + TDuration::Seconds(3));

        packed = Pack(accumulator);
        AssertTestShape(packed);
        UNIT_ASSERT_VALUES_EQUAL(GetGauges(packed), TVector<ui64>({0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetRatePairs(packed), TVector<ui64>());
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, HIST_OF_SIMPLE), TVector<ui64>({2, 0, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, PLAIN_LEVEL), TVector<ui64>({5, 6, 0, 0}));
    }

    Y_UNIT_TEST(FollowerSkipsTheLeaderOnlyMetrics) {
        TTestEnv env;
        auto leader = env.MakeAccumulator(false /* skipLeaderOnly */);
        auto follower = env.MakeAccumulator(true /* skipLeaderOnly */);

        const TVector<TReport> reports = {
            {
                .ExecGauge = 15,
                .ExecMax = 7,
                .AppGauge = 100,
                .ExecRate = 25,
                .AppRate = 3,
                .AppLeaderRate = 40,
                .AppLevel = {1, 0, 0, 0},
                .AppIncrement = {0, 1, 0, 0},
            },
            {
                .ExecGauge = 15,
                .ExecMax = 9,
                .AppGauge = 100,
                .ExecRate = 25,
                .AppRate = 3,
                .AppLeaderRate = 40,
                .AppLevel = {1, 0, 0, 0},
                .AppIncrement = {0, 1, 0, 0},
            },
        };

        for (size_t i = 0; i < reports.size(); ++i) {
            env.Apply(leader, TABLET_1, reports[i], T0 + TDuration::Seconds(i));
            env.Apply(follower, FOLLOWER_1, reports[i], T0 + TDuration::Seconds(i));
        }

        const auto leaderPacked = Pack(leader);
        const auto followerPacked = Pack(follower);
        AssertTestShape(leaderPacked);
        AssertTestShape(followerPacked);

        UNIT_ASSERT_VALUES_EQUAL(GetGauges(leaderPacked), TVector<ui64>({115, 9}));
        UNIT_ASSERT_VALUES_EQUAL(GetRatePairs(leaderPacked), TVector<ui64>({RATE, 56, RATE_LEADER_ONLY, 80}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(leaderPacked, HIST_LEADER_ONLY), TVector<ui64>({0, 0, 0, 1}));

        // The LeaderOnly gauge is zero, the LeaderOnly rate has no deltas,
        // the LeaderOnly histogram is present, but empty
        UNIT_ASSERT_VALUES_EQUAL(GetGauges(followerPacked), TVector<ui64>({115, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetGauges(followerPacked)[GAUGE_MAX], 0);
        UNIT_ASSERT_VALUES_EQUAL(GetRatePairs(followerPacked), TVector<ui64>({RATE, 56}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(followerPacked, HIST_LEADER_ONLY), TVector<ui64>({0, 0, 0, 0}));

        // Everything else is the same
        for (ui32 metric : {HIST_OF_CUMULATIVE, HIST_OF_SIMPLE, PLAIN_LEVEL, PLAIN_INCREMENT}) {
            UNIT_ASSERT_VALUES_EQUAL_C(GetBuckets(followerPacked, metric), GetBuckets(leaderPacked, metric), metric);
        }
    }

    Y_UNIT_TEST(DifferentialAgainstAggregatedTabletCountersOfLeaders) {
        for (ui64 seed = 1; seed <= 16; ++seed) {
            RunDifferential(seed, false /* skipLeaderOnly */);
        }
    }

    Y_UNIT_TEST(DifferentialAgainstAggregatedTabletCountersOfFollowers) {
        for (ui64 seed = 101; seed <= 116; ++seed) {
            RunDifferential(seed, true /* skipLeaderOnly */);
        }
    }

}
