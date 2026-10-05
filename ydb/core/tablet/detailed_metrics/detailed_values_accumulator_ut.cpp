#include "detailed_metrics_binding.h"
#include "detailed_values_accumulator.h"
#include "ut_helpers.h"

#include <ydb/core/protos/sys_view.pb.h>
#include <ydb/core/tablet/private/aggregated_tablet_counters.h>
#include <ydb/core/tablet/tablet_counters_app.h>
#include <ydb/core/tablet_flat/flat_executor_counters.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
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

static_assert(std::is_nothrow_move_constructible_v<TDetailedValuesAccumulator>);
static_assert(std::is_nothrow_move_assignable_v<TDetailedValuesAccumulator>);
static_assert(!std::is_copy_constructible_v<TDetailedValuesAccumulator>);

namespace {

////////////////////////////////////////////////////////////////////////////////
// The synthetic layout

// RANGES_<n> has n buckets, the implicit +Inf one included; the public histograms have 4
constexpr TTabletPercentileCounter::TRangeDef RANGES_2[] = {
    {10, "10"},
};

constexpr TTabletPercentileCounter::TRangeDef RANGES_4[] = {
    {10, "10"},
    {20, "20"},
    {30, "30"},
};

constexpr TTabletPercentileCounter::TRangeDef RANGES_6[] = {
    {10, "10"},
    {20, "20"},
    {30, "30"},
    {40, "40"},
    {50, "50"},
};

// Public metrics of the test descriptor by wire slot; gauge 0 is SUM(ExecGauge) + SUM(AppGauge)
constexpr ui32 GAUGE_MAX = 1;               // MAX(ExecMax), LeaderOnly
constexpr ui32 RATE = 0;                    // ExecRate + AppRate
constexpr ui32 RATE_LEADER_ONLY = 1;        // AppLeaderRate, LeaderOnly
constexpr ui32 HIST_OF_CUMULATIVE = 0;      // HIST(ExecRate)
constexpr ui32 HIST_OF_SIMPLE = 1;          // HIST(ExecGauge)
constexpr ui32 PLAIN_NON_DERIVATIVE = 2;    // AppIntegral, integral
constexpr ui32 PLAIN_DERIVATIVE = 3;        // AppDerivative
constexpr ui32 HIST_LEADER_ONLY = 4;        // HIST(AppLeaderRate), LeaderOnly

constexpr ui32 GAUGE_COUNT = 2;
constexpr ui32 RATE_COUNT = 2;
constexpr ui32 HISTOGRAM_COUNT = 5;
constexpr ui32 BUCKET_COUNT = 4;

// Slots of the synthetic layout
constexpr ui32 EXEC_GAUGE = 0;
constexpr ui32 EXEC_MAX = 1;
constexpr ui32 EXEC_UNUSED = 2;
constexpr ui32 EXEC_RATE = 0;
constexpr ui32 EXEC_UNUSED_RATE = 1;
constexpr ui32 APP_GAUGE = 0;
constexpr ui32 APP_RATE = 0;
constexpr ui32 APP_LEADER_RATE = 1;
constexpr ui32 APP_INTEGRAL = 0;
constexpr ui32 APP_DERIVATIVE = 1;

const TInstant T0 = TInstant::Seconds(1000000);

const TTabletKey TABLET_1(72075186224037888ull, 0);
const TTabletKey TABLET_2(72075186224037889ull, 0);
const TTabletKey TABLET_3(72075186224037890ull, 0);
const TTabletKey FOLLOWER_1(72075186224037888ull, 1);

TSourceRef ExecutorSource(TStringBuf text) {
    return ParseSourceRef(text, SCC_EXECUTOR);
}

TSourceRef AppSource(TStringBuf text) {
    return ParseSourceRef(text, SCC_TABLET);
}

TMetricSpec MakeHistogramSpec(
    const TString& name,
    TVector<TSourceRef> sources,
    bool nonDerivative,
    bool leaderOnly = false)
{
    return TMetricSpec{
        .Name = name,
        .LeaderOnly = leaderOnly,
        .Sources = std::move(sources),
        .Bounds = {1, 2, 3},
        .NonDerivative = nonDerivative,
    };
}

TDetailedMetricsDescriptor MakeTestDescriptor() {
    TDetailedMetricsDescriptor descriptor;

    descriptor.Gauges.push_back(TMetricSpec{.Name = "table.test.sum", .Sources = {
        ExecutorSource("SUM(ExecGauge)"),
        AppSource("SUM(AppGauge)"),
    }});
    descriptor.Gauges.push_back(TMetricSpec{.Name = "table.test.max", .LeaderOnly = true, .Sources = {
        ExecutorSource("MAX(ExecMax)"),
    }});

    descriptor.Rates.push_back(TMetricSpec{.Name = "table.test.rate", .Sources = {
        ExecutorSource("ExecRate"),
        AppSource("AppRate"),
    }});
    descriptor.Rates.push_back(TMetricSpec{.Name = "table.test.leader_rate", .LeaderOnly = true, .Sources = {
        AppSource("AppLeaderRate"),
    }});

    descriptor.Histograms.push_back(MakeHistogramSpec("table.test.rate_hist", {
        ExecutorSource("HIST(ExecRate)"),
    }, true /* nonDerivative */));
    descriptor.Histograms.push_back(MakeHistogramSpec("table.test.gauge_hist", {
        ExecutorSource("HIST(ExecGauge)"),
    }, true /* nonDerivative */));
    descriptor.Histograms.push_back(MakeHistogramSpec("table.test.plain_non_derivative", {
        AppSource("AppIntegral"),
    }, true /* nonDerivative */));
    descriptor.Histograms.push_back(MakeHistogramSpec("table.test.plain_derivative", {
        AppSource("AppDerivative"),
    }, false /* nonDerivative */));
    descriptor.Histograms.push_back(MakeHistogramSpec("table.test.leader_hist", {
        AppSource("HIST(AppLeaderRate)"),
    }, true /* nonDerivative */, true /* leaderOnly */));

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
        .Percentile = {"AppIntegral", "AppDerivative", "HIST(AppLeaderRate)"},
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

TTestCounters MakeExecutorCounters() {
    return MakeExecutorCounters(RANGES_4);
}

TTestCounters MakeAppCounters() {
    return MakeAppCounters(RANGES_4);
}

struct TReport {
    ui64 ExecGauge = 0;
    ui64 ExecMax = 0;
    ui64 AppGauge = 0;

    ui64 ExecRate = 0;
    ui64 AppRate = 0;
    ui64 AppLeaderRate = 0;

    // Source buckets; missing ones are zero
    TVector<ui64> AppIntegral;
    TVector<ui64> AppDerivative;
};

// Expects a zeroed percentile counter
void SetBuckets(TTabletPercentileCounter& percentile, const TVector<ui64>& values) {
    UNIT_ASSERT_LE(values.size(), percentile.GetRangeCount());

    for (ui32 i = 0; i < values.size(); ++i) {
        // A bound falls into its own bucket (right inclusive)
        percentile.AddFor(percentile.GetRangeBound(i), values[i]);
    }
}

// Unbound slots get values that must never show up in the public values
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
    SetBuckets(appCounters.Percentile()[APP_INTEGRAL], report.AppIntegral);
    SetBuckets(appCounters.Percentile()[APP_DERIVATIVE], report.AppDerivative);
}

// Not movable: the binding points to the descriptor
class TTestEnv : TNonCopyable {
public:
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

    // Garbage that Pack() must clear
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

void AssertTestShape(const NKikimrSysView::TDbCounters& packed) {
    UNIT_ASSERT_VALUES_EQUAL(packed.SimpleSize(), GAUGE_COUNT);
    UNIT_ASSERT_VALUES_EQUAL(packed.GetCumulativeCount(), RATE_COUNT);
    UNIT_ASSERT_VALUES_EQUAL(packed.HistogramSize(), HISTOGRAM_COUNT);

    for (ui32 metric = 0; metric < HISTOGRAM_COUNT; ++metric) {
        const auto& histogram = packed.GetHistogram(metric);
        UNIT_ASSERT_VALUES_EQUAL_C(histogram.GetBucketsCount(), BUCKET_COUNT, metric);
        UNIT_ASSERT_VALUES_EQUAL_C(histogram.HasNonDerivative(), metric != PLAIN_DERIVATIVE, metric);
        UNIT_ASSERT_VALUES_EQUAL_C(histogram.GetNonDerivative(), metric != PLAIN_DERIVATIVE, metric);
    }
}

////////////////////////////////////////////////////////////////////////////////
// The oracle: NPrivate::TAggregatedTabletCounters configured like the node bucket TCountersBucket

class TOracleBucket {
public:
    TOracleBucket(const TDetailedMetricsDescriptor& descriptor, TTabletTypes::EType tabletType)
        : ExecutorGroup(MakeIntrusive<NMonitoring::TDynamicCounters>())
        , AppGroup(MakeIntrusive<NMonitoring::TDynamicCounters>())
        , ExecutorCounters(ExecutorGroup)
        , AppCounters(AppGroup)
        , Descriptor(descriptor)
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
            ExecutorCounters.Initialize(&executorCounters, &Descriptor.ExecutorCounterNames);
        }
        if (!AppCounters.IsInitialized) {
            AppCounters.Initialize(&appCounters, &Descriptor.AppCounterNames);
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

    ui64 GetCounter(ESourceCounterCategory category, const TString& name) const {
        const auto counter = GetGroup(category)->FindCounter(name);
        return counter ? static_cast<ui64>(counter->Val()) : 0;
    }

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
        // The same rule as the binding
        return category == ESourceCounterCategory::SCC_TABLET ? AppGroup : ExecutorGroup;
    }

    NMonitoring::TDynamicCounterPtr ExecutorGroup;
    NMonitoring::TDynamicCounterPtr AppGroup;
    NKikimr::NPrivate::TAggregatedTabletCounters ExecutorCounters;
    NKikimr::NPrivate::TAggregatedTabletCounters AppCounters;
    const TDetailedMetricsDescriptor& Descriptor;
    const TTabletTypes::EType TabletType;

    THashMap<TTabletKey, ui64> SourceIds;
    ui64 NextSourceId = 0;
};

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

TVector<ui64> ClampBuckets(TVector<ui64> buckets, size_t bucketCount) {
    TVector<ui64> result(bucketCount, 0);

    for (size_t i = 0; i < buckets.size(); ++i) {
        result[Min(i, bucketCount - 1)] += buckets[i];
    }

    return result;
}

// Reads the oracle by the source names of the descriptor, like the YDB metrics mapper
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

    UNIT_ASSERT_VALUES_EQUAL_C(packed.SimpleSize(), descriptor.Gauges.size(), context);

    for (size_t metric = 0; metric < descriptor.Gauges.size(); ++metric) {
        const auto& spec = descriptor.Gauges[metric];

        ui64 expected = 0;
        if (!(skipLeaderOnly && spec.LeaderOnly)) {
            for (const auto& source : spec.Sources) {
                expected += oracle.GetCounter(source.Category, source.Text);
            }
        }

        UNIT_ASSERT_VALUES_EQUAL_C(packed.GetSimple(metric), expected, context << ", gauge " << spec.Name);
    }

    // The drained deltas add up to the oracle's accumulated x
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

    UNIT_ASSERT_VALUES_EQUAL_C(packed.HistogramSize(), descriptor.Histograms.size(), context);

    for (ui32 metric = 0; metric < descriptor.Histograms.size(); ++metric) {
        const auto& spec = descriptor.Histograms[metric];
        const bool histOnly = !spec.Sources.empty() && AllOf(spec.Sources, [](const TSourceRef& source) {
            return source.Wrapper == ESourceWrapper::Hist;
        });
        UNIT_ASSERT_C(histOnly, "the differential test covers HIST(x) only: " << spec.Name);
        UNIT_ASSERT_C(packed.GetHistogram(metric).GetNonDerivative(), context << ", histogram " << spec.Name);
        UNIT_ASSERT_VALUES_EQUAL_C(packed.GetHistogram(metric).GetBucketsCount(), spec.BucketCount(), context);

        TVector<ui64> expected(spec.BucketCount(), 0);
        if (!(skipLeaderOnly && spec.LeaderOnly)) {
            for (const auto& source : spec.Sources) {
                const auto buckets = ClampBuckets(
                    oracle.GetHistogram(source.Category, source.Text),
                    spec.BucketCount());

                for (size_t bucket = 0; bucket < expected.size(); ++bucket) {
                    expected[bucket] += buckets[bucket];
                }
            }
        }

        UNIT_ASSERT_VALUES_EQUAL_C(GetBuckets(packed, metric), expected, context << ", histogram " << spec.Name);
    }
}

// Random reports, forgets and packs, checked against the oracle at every pack
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

    TOracleBucket oracle(*descriptor, TTabletTypes::DataShard);
    TDetailedValuesAccumulator accumulator(binding.Get(), skipLeaderOnly);
    TVector<ui64> drainedRates(descriptor->Rates.size(), 0);

    TInstant now = T0;

    for (ui32 step = 0; step < 1500; ++step) {
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
            .AppIntegral = {1, 0, 2, 5},
            .AppDerivative = {0, 3, 0, 1},
        }, T0);

        UNIT_ASSERT(!accumulator.IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(accumulator.GetSourceCount(), 1);

        const auto packed = Pack(accumulator);
        AssertTestShape(packed);

        UNIT_ASSERT_VALUES_EQUAL(GetGauges(packed), TVector<ui64>({115, 7}));
        UNIT_ASSERT_VALUES_EQUAL(GetRatePairs(packed), TVector<ui64>({RATE, 28, RATE_LEADER_ONLY, 4}));

        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, HIST_OF_CUMULATIVE), TVector<ui64>({1, 0, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, HIST_LEADER_ONLY), TVector<ui64>({1, 0, 0, 0}));

        // 15 is in the source bucket (10, 20]
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, HIST_OF_SIMPLE), TVector<ui64>({0, 1, 0, 0}));

        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, PLAIN_NON_DERIVATIVE), TVector<ui64>({1, 0, 2, 5}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, PLAIN_DERIVATIVE), TVector<ui64>({0, 3, 0, 1}));

        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>(packed.GetHistogram(PLAIN_NON_DERIVATIVE).GetBuckets().begin(), packed.GetHistogram(PLAIN_NON_DERIVATIVE).GetBuckets().end()),
            TVector<ui64>({0, 1, 2, 2, 3, 5}));
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui64>(packed.GetHistogram(PLAIN_DERIVATIVE).GetBuckets().begin(), packed.GetHistogram(PLAIN_DERIVATIVE).GetBuckets().end()),
            TVector<ui64>({1, 3, 3, 1}));
    }

    Y_UNIT_TEST(GaugesSumAndMaxTheLatestValuesOfTheLiveSources) {
        TTestEnv env;
        auto accumulator = env.MakeAccumulator();

        env.Apply(accumulator, TABLET_1, {.ExecGauge = 1, .ExecMax = 30, .AppGauge = 10}, T0);
        env.Apply(accumulator, TABLET_2, {.ExecGauge = 2, .ExecMax = 50, .AppGauge = 20}, T0);
        env.Apply(accumulator, TABLET_3, {.ExecGauge = 4, .ExecMax = 40, .AppGauge = 40}, T0);

        UNIT_ASSERT_VALUES_EQUAL(GetGauges(Pack(accumulator)), TVector<ui64>({77, 50}));

        env.Apply(accumulator, TABLET_2, {.ExecGauge = 8, .ExecMax = 5, .AppGauge = 80}, T0 + TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(GetGauges(Pack(accumulator)), TVector<ui64>({143, 40}));

        accumulator.Forget(TABLET_3);
        UNIT_ASSERT_VALUES_EQUAL(GetGauges(Pack(accumulator)), TVector<ui64>({99, 30}));

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

        packed = Pack(accumulator);
        UNIT_ASSERT_VALUES_EQUAL(packed.GetCumulativeCount(), RATE_COUNT);
        UNIT_ASSERT_VALUES_EQUAL(GetRatePairs(packed), TVector<ui64>());

        env.Apply(accumulator, TABLET_2, {.AppLeaderRate = 2}, T0 + TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(GetRatePairs(Pack(accumulator)), TVector<ui64>({RATE_LEADER_ONLY, 2}));

        // Deltas add up modulo 2^64
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
            .AppIntegral = {1, 0, 0, 0},
            .AppDerivative = {0, 3, 0, 0},
        }, T0);
        env.Apply(accumulator, TABLET_2, {
            .ExecGauge = 1,
            .ExecMax = 1,
            .AppRate = 5,
            .AppLeaderRate = 6,
            .AppIntegral = {0, 0, 0, 2},
            .AppDerivative = {1, 0, 0, 0},
        }, T0);

        accumulator.Forget(TABLET_3);
        UNIT_ASSERT_VALUES_EQUAL(accumulator.GetSourceCount(), 2);

        accumulator.Forget(TABLET_1);
        accumulator.Forget(TABLET_2);
        UNIT_ASSERT(accumulator.IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(accumulator.GetSourceCount(), 0);

        auto packed = Pack(accumulator);
        AssertTestShape(packed);

        UNIT_ASSERT_VALUES_EQUAL(GetGauges(packed), TVector<ui64>({0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetRatePairs(packed), TVector<ui64>({RATE, 30, RATE_LEADER_ONLY, 6}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, HIST_OF_CUMULATIVE), TVector<ui64>({0, 0, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, HIST_OF_SIMPLE), TVector<ui64>({0, 0, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, PLAIN_NON_DERIVATIVE), TVector<ui64>({0, 0, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, PLAIN_DERIVATIVE), TVector<ui64>({1, 3, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, HIST_LEADER_ONLY), TVector<ui64>({0, 0, 0, 0}));

        packed = Pack(accumulator);
        AssertTestShape(packed);

        UNIT_ASSERT_VALUES_EQUAL(GetGauges(packed), TVector<ui64>({0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetRatePairs(packed), TVector<ui64>());
        for (ui32 metric = 0; metric < HISTOGRAM_COUNT; ++metric) {
            UNIT_ASSERT_VALUES_EQUAL_C(packed.GetHistogram(metric).BucketsSize(), 0, metric);
        }
    }

    Y_UNIT_TEST(DestructivePackDrainsOnlyTheDeltas) {
        TTestEnv env;
        auto accumulator = env.MakeAccumulator();

        env.Apply(accumulator, TABLET_1, {
            .ExecGauge = 25,
            .ExecMax = 3,
            .AppGauge = 5,
            .ExecRate = 11,
            .AppIntegral = {0, 4, 0, 1},
            .AppDerivative = {2, 0, 0, 0},
        }, T0);
        env.Apply(accumulator, TABLET_1, {
            .ExecGauge = 25,
            .ExecMax = 3,
            .AppGauge = 5,
            .ExecRate = 15,
            .AppIntegral = {0, 4, 0, 1},
            .AppDerivative = {0, 0, 0, 7},
        }, T0 + TDuration::Seconds(1));

        const auto first = Pack(accumulator);
        AssertTestShape(first);
        UNIT_ASSERT_VALUES_EQUAL(GetGauges(first), TVector<ui64>({30, 3}));
        UNIT_ASSERT_VALUES_EQUAL(GetRatePairs(first), TVector<ui64>({RATE, 26}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(first, HIST_OF_CUMULATIVE), TVector<ui64>({0, 1, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(first, HIST_OF_SIMPLE), TVector<ui64>({0, 0, 1, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(first, PLAIN_NON_DERIVATIVE), TVector<ui64>({0, 4, 0, 1}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(first, PLAIN_DERIVATIVE), TVector<ui64>({2, 0, 0, 7}));

        const auto second = Pack(accumulator);
        AssertTestShape(second);
        UNIT_ASSERT_VALUES_EQUAL(GetGauges(second), GetGauges(first));
        UNIT_ASSERT_VALUES_EQUAL(GetRatePairs(second), TVector<ui64>());
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(second, HIST_OF_CUMULATIVE), GetBuckets(first, HIST_OF_CUMULATIVE));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(second, HIST_OF_SIMPLE), GetBuckets(first, HIST_OF_SIMPLE));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(second, PLAIN_NON_DERIVATIVE), GetBuckets(first, PLAIN_NON_DERIVATIVE));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(second, PLAIN_DERIVATIVE), TVector<ui64>({0, 0, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(second, HIST_LEADER_ONLY), GetBuckets(first, HIST_LEADER_ONLY));
    }

    Y_UNIT_TEST(HistogramOfCumulativeObservesTheRateSinceThePreviousReport) {
        TTestEnv env;
        auto accumulator = env.MakeAccumulator();

        // Rate deltas count regardless of the time
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

        // No previous report: zero rate
        UNIT_ASSERT_VALUES_EQUAL(observe(1000, T0), TVector<ui64>({1, 0, 0, 0}));

        // 15 per second, (10, 20]
        UNIT_ASSERT_VALUES_EQUAL(observe(15, T0 + TDuration::Seconds(1)), TVector<ui64>({0, 1, 0, 0}));

        // 50 in 2 seconds, (20, 30]
        UNIT_ASSERT_VALUES_EQUAL(observe(50, T0 + TDuration::Seconds(3)), TVector<ui64>({0, 0, 1, 0}));

        // 10 in half a second, (10, 20]
        UNIT_ASSERT_VALUES_EQUAL(observe(10, T0 + TDuration::MilliSeconds(3500)), TVector<ui64>({0, 1, 0, 0}));

        // The same time: zero rate
        UNIT_ASSERT_VALUES_EQUAL(observe(50, T0 + TDuration::MilliSeconds(3500)), TVector<ui64>({1, 0, 0, 0}));

        // The time goes back: zero rate
        UNIT_ASSERT_VALUES_EQUAL(observe(50, T0 + TDuration::Seconds(1)), TVector<ui64>({1, 0, 0, 0}));

        // The duration counts from the time that went back: 35 per second, (30, +Inf)
        UNIT_ASSERT_VALUES_EQUAL(observe(35, T0 + TDuration::Seconds(2)), TVector<ui64>({0, 0, 0, 1}));

        // Right inclusive: exactly 10 per second is [0, 10]
        UNIT_ASSERT_VALUES_EQUAL(observe(100, T0 + TDuration::Seconds(12)), TVector<ui64>({1, 0, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(observe(11, T0 + TDuration::Seconds(13)), TVector<ui64>({0, 1, 0, 0}));

        UNIT_ASSERT_VALUES_EQUAL(rateTotal, 1000 + 15 + 50 + 10 + 50 + 50 + 35 + 100 + 11);

        // A re-added source has no previous report
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

        env.Apply(accumulator, TTabletKey(100, 0), {.ExecGauge = 20}, T0 + TDuration::Seconds(1));
        accumulator.Forget(TTabletKey(105, 0));

        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(Pack(accumulator), HIST_OF_SIMPLE), TVector<ui64>({1, 2, 1, 1}));
    }

    Y_UNIT_TEST(NonDerivativePercentileSumsTheLatestBucketsOfTheLiveSources) {
        TTestEnv env;
        auto accumulator = env.MakeAccumulator();

        env.Apply(accumulator, TABLET_1, {.AppIntegral = {1, 2, 0, 0}}, T0);
        env.Apply(accumulator, TABLET_2, {.AppIntegral = {0, 1, 1, 1}}, T0);
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(Pack(accumulator), PLAIN_NON_DERIVATIVE), TVector<ui64>({1, 3, 1, 1}));

        env.Apply(accumulator, TABLET_1, {.AppIntegral = {0, 0, 0, 4}}, T0 + TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(Pack(accumulator), PLAIN_NON_DERIVATIVE), TVector<ui64>({0, 1, 1, 5}));

        accumulator.Forget(TABLET_2);
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(Pack(accumulator), PLAIN_NON_DERIVATIVE), TVector<ui64>({0, 0, 0, 4}));
    }

    Y_UNIT_TEST(DerivativePercentileAddsEveryReportUntilPacked) {
        TTestEnv env;
        auto accumulator = env.MakeAccumulator();

        env.Apply(accumulator, TABLET_1, {.AppDerivative = {1, 0, 0, 0}}, T0);
        env.Apply(accumulator, TABLET_1, {.AppDerivative = {0, 2, 0, 0}}, T0 + TDuration::Seconds(1));
        env.Apply(accumulator, TABLET_2, {.AppDerivative = {0, 1, 3, 0}}, T0);

        auto packed = Pack(accumulator);
        UNIT_ASSERT(!packed.GetHistogram(PLAIN_DERIVATIVE).HasNonDerivative());
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, PLAIN_DERIVATIVE), TVector<ui64>({1, 3, 3, 0}));

        packed = Pack(accumulator);
        UNIT_ASSERT_VALUES_EQUAL(packed.GetHistogram(PLAIN_DERIVATIVE).GetBucketsCount(), BUCKET_COUNT);
        UNIT_ASSERT_VALUES_EQUAL(packed.GetHistogram(PLAIN_DERIVATIVE).BucketsSize(), 0);

        env.Apply(accumulator, TABLET_1, {.AppDerivative = {0, 0, 0, 9}}, T0 + TDuration::Seconds(2));
        accumulator.Forget(TABLET_1);
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(Pack(accumulator), PLAIN_DERIVATIVE), TVector<ui64>({0, 0, 0, 9}));
    }

    Y_UNIT_TEST(SourceBucketsBeyondThePublicOnesAreCountedInTheLastOne) {
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
                .AppIntegral = {1, 1, 1, 1, 1, 1},
                .AppDerivative = {0, 0, 1, 1, 1, 1},
            }, T0);
        }

        const auto packed = Pack(accumulator);
        AssertTestShape(packed);

        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, HIST_OF_SIMPLE), TVector<ui64>({0, 0, 1, 3}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, PLAIN_NON_DERIVATIVE), TVector<ui64>({4, 4, 4, 12}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, PLAIN_DERIVATIVE), TVector<ui64>({0, 0, 4, 12}));
    }

    Y_UNIT_TEST(ShortAndUninitializedPercentilesAreReadSafely) {
        TTestEnv env;
        auto accumulator = env.MakeAccumulator();

        env.Apply(accumulator, TABLET_1, {.AppIntegral = {1, 2, 3, 4}, .AppDerivative = {1, 1, 1, 1}}, T0);

        // The same layout, but the percentile counters were never initialized
        auto executor = MakeExecutorCounters();
        auto uninitialized = MakeAppCountersWithoutPercentiles();

        FillCounters(executor, uninitialized, {});
        accumulator.Apply(TABLET_1, executor.Get(), uninitialized.Get(), T0 + TDuration::Seconds(1));

        auto packed = Pack(accumulator);
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, PLAIN_NON_DERIVATIVE), TVector<ui64>({1, 2, 3, 4}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, PLAIN_DERIVATIVE), TVector<ui64>({1, 1, 1, 1}));

        // Fewer source buckets than bound
        auto shortCounters = MakeAppCounters(RANGES_2);

        FillCounters(executor, shortCounters, {.AppIntegral = {5, 6}, .AppDerivative = {7, 8}});
        accumulator.Apply(TABLET_1, executor.Get(), shortCounters.Get(), T0 + TDuration::Seconds(2));

        packed = Pack(accumulator);
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, PLAIN_NON_DERIVATIVE), TVector<ui64>({5, 6, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, PLAIN_DERIVATIVE), TVector<ui64>({7, 8, 0, 0}));

        // Breaks the precondition: the bound slots are missing
        TTestCounters emptyExecutor;
        TTestCounters emptyApp;

        accumulator.Apply(TABLET_1, emptyExecutor.Get(), emptyApp.Get(), T0 + TDuration::Seconds(3));
        accumulator.Apply(TABLET_2, emptyExecutor.Get(), emptyApp.Get(), T0 + TDuration::Seconds(3));

        packed = Pack(accumulator);
        AssertTestShape(packed);
        UNIT_ASSERT_VALUES_EQUAL(GetGauges(packed), TVector<ui64>({0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetRatePairs(packed), TVector<ui64>());
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, HIST_OF_SIMPLE), TVector<ui64>({2, 0, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(packed, PLAIN_NON_DERIVATIVE), TVector<ui64>({5, 6, 0, 0}));
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
                .AppIntegral = {1, 0, 0, 0},
                .AppDerivative = {0, 1, 0, 0},
            },
            {
                .ExecGauge = 15,
                .ExecMax = 9,
                .AppGauge = 100,
                .ExecRate = 25,
                .AppRate = 3,
                .AppLeaderRate = 40,
                .AppIntegral = {1, 0, 0, 0},
                .AppDerivative = {0, 1, 0, 0},
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

        UNIT_ASSERT_VALUES_EQUAL(GetGauges(followerPacked), TVector<ui64>({115, 0}));
        UNIT_ASSERT_VALUES_EQUAL(GetGauges(followerPacked)[GAUGE_MAX], 0);
        UNIT_ASSERT_VALUES_EQUAL(GetRatePairs(followerPacked), TVector<ui64>({RATE, 56}));
        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(followerPacked, HIST_LEADER_ONLY), TVector<ui64>({0, 0, 0, 0}));

        for (ui32 metric : {HIST_OF_CUMULATIVE, HIST_OF_SIMPLE, PLAIN_NON_DERIVATIVE, PLAIN_DERIVATIVE}) {
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
