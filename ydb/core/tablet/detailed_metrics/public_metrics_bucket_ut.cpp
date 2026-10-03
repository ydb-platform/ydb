#include "public_metrics_bucket.h"
#include "ut_helpers.h"
#include "ydb_metrics_mapper.h"

#include <ydb/core/protos/counters_detailed_datashard.pb.h>

#include <library/cpp/monlib/dynamic_counters/encode.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ylimits.h>
#include <util/string/join.h>

#include <initializer_list>
#include <utility>

using namespace NKikimr;
using namespace NKikimr::NDetailedMetricsTests;

namespace {

/**
 * The indices of the metrics of the test descriptor (see MakeTestDescriptor()).
 */
constexpr size_t GAUGE_SUM = 0;
constexpr size_t GAUGE_MAX = 1;
constexpr size_t GAUGE_LEADER_ONLY = 2;

constexpr size_t RATE = 0;
constexpr size_t RATE_LEADER_ONLY = 1;

constexpr size_t LEVEL = 0;
constexpr size_t INTEGRAL = 1;
constexpr size_t INCREMENTS = 2;

const TString GAUGE_SUM_NAME = "table.test.gauge_sum";
const TString GAUGE_MAX_NAME = "table.test.gauge_max";
const TString GAUGE_LEADER_ONLY_NAME = "table.test.gauge_leader_only";
const TString RATE_NAME = "table.test.rate";
const TString RATE_LEADER_ONLY_NAME = "table.test.rate_leader_only";
const TString LEVEL_NAME = "table.test.level";
const TString INTEGRAL_NAME = "table.test.integral";
const TString INCREMENTS_NAME = "table.test.increments";

using TBuckets = TVector<std::pair<ui64, ui64>>;

TSourceRef MakeSource(ESourceCounterCategory category, ESourceWrapper wrapper, const TString& name) {
    return TSourceRef{category, wrapper, name};
}

TMetricSpec MakeSpec(
    const TString& name,
    TVector<TSourceRef> sources,
    bool leaderOnly = false,
    TVector<ui64> bounds = {},
    bool integral = false)
{
    TMetricSpec spec;
    spec.Name = name;
    spec.Sources = std::move(sources);
    spec.LeaderOnly = leaderOnly;
    spec.Bounds = std::move(bounds);
    spec.Integral = integral;
    return spec;
}

/**
 * Build a small finalized descriptor with every kind of public metric:
 * - gauges: SUM(x), MAX(x) (CombineByMax) and a LeaderOnly SUM(x);
 * - rates: a plain one and a LeaderOnly one;
 * - histograms: HIST(x) (a StaticLevel level, 4 buckets), an integral percentile
 *   (a level, but not StaticLevel, 3 buckets) and a derivative percentile
 *   (increments, 3 buckets).
 */
TDetailedMetricsDescriptor MakeTestDescriptor() {
    TDetailedMetricsDescriptor descriptor;
    descriptor.Type = TTabletTypes::DataShard;

    descriptor.Gauges.push_back(MakeSpec(GAUGE_SUM_NAME, {
        MakeSource(SCC_EXECUTOR, ESourceWrapper::Sum, "ExecGauge"),
        MakeSource(SCC_TABLET, ESourceWrapper::Sum, "AppGauge"),
    }));
    descriptor.Gauges.push_back(MakeSpec(GAUGE_MAX_NAME, {
        MakeSource(SCC_EXECUTOR, ESourceWrapper::Max, "ExecMaxGauge"),
    }));
    descriptor.Gauges.push_back(MakeSpec(GAUGE_LEADER_ONLY_NAME, {
        MakeSource(SCC_TABLET, ESourceWrapper::Sum, "AppLeaderGauge"),
    }, true /* leaderOnly */));

    descriptor.Rates.push_back(MakeSpec(RATE_NAME, {
        MakeSource(SCC_TABLET, ESourceWrapper::None, "AppRate"),
    }));
    descriptor.Rates.push_back(MakeSpec(RATE_LEADER_ONLY_NAME, {
        MakeSource(SCC_TABLET, ESourceWrapper::None, "AppLeaderRate"),
    }, true /* leaderOnly */));

    descriptor.Histograms.push_back(MakeSpec(LEVEL_NAME, {
        MakeSource(SCC_EXECUTOR, ESourceWrapper::Hist, "ExecGauge"),
    }, false /* leaderOnly */, {10, 20, 30}));
    descriptor.Histograms.push_back(MakeSpec(INTEGRAL_NAME, {
        MakeSource(SCC_TABLET, ESourceWrapper::None, "AppIntegral"),
    }, false /* leaderOnly */, {1, 2}, true /* integral */));
    descriptor.Histograms.push_back(MakeSpec(INCREMENTS_NAME, {
        MakeSource(SCC_TABLET, ESourceWrapper::None, "AppIncrements"),
    }, false /* leaderOnly */, {1, 2}));

    TString error;
    UNIT_ASSERT_C(FinalizeDescriptor(descriptor, &error), error);

    UNIT_ASSERT(descriptor.Gauges[GAUGE_MAX].CombineByMax);
    UNIT_ASSERT(!descriptor.Gauges[GAUGE_SUM].CombineByMax);
    UNIT_ASSERT(descriptor.Histograms[LEVEL].StaticLevel && descriptor.Histograms[LEVEL].IsLevel);
    UNIT_ASSERT(!descriptor.Histograms[INTEGRAL].StaticLevel && descriptor.Histograms[INTEGRAL].IsLevel);
    UNIT_ASSERT(!descriptor.Histograms[INCREMENTS].StaticLevel && !descriptor.Histograms[INCREMENTS].IsLevel);

    return descriptor;
}

/**
 * The builder of the public values of a bucket reported by a node.
 *
 * Starts with the shape a node always reports: one histogram entry per metric,
 * a level histogram marked NonDerivative with the full bucket count.
 */
class TPayload {
public:
    explicit TPayload(const TDetailedMetricsDescriptor& descriptor) {
        for (const auto& spec : descriptor.Histograms) {
            auto* histogram = Values.AddHistogram();
            histogram->SetBucketsCount(spec.BucketCount());

            if (spec.IsLevel) {
                histogram->SetNonDerivative(true);
            }
        }
    }

    TPayload& Gauges(std::initializer_list<ui64> values) {
        Values.ClearSimple();

        for (ui64 value : values) {
            Values.AddSimple(value);
        }

        return *this;
    }

    TPayload& Rate(ui64 index, ui64 delta) {
        Values.AddCumulative(index);
        Values.AddCumulative(delta);
        Values.SetCumulativeCount(Max<ui64>(Values.GetCumulativeCount(), index + 1));
        return *this;
    }

    TPayload& Buckets(size_t index, const TBuckets& buckets) {
        auto* histogram = Values.MutableHistogram(index);

        for (const auto& [bucket, count] : buckets) {
            histogram->AddBuckets(bucket);
            histogram->AddBuckets(count);
        }

        return *this;
    }

    TPayload& Marked(size_t index, bool marked) {
        Values.MutableHistogram(index)->SetNonDerivative(marked);
        return *this;
    }

    NKikimrSysView::TDbCounters& Mutable() {
        return Values;
    }

    const NKikimrSysView::TDbCounters& Get() const {
        return Values;
    }

private:
    NKikimrSysView::TDbCounters Values;
};

ui64 GetGauge(const NMonitoring::TDynamicCounterPtr& group, const TString& name) {
    auto counter = group->FindNamedCounter("name", name);
    UNIT_ASSERT_C(counter, "no gauge " << name);
    UNIT_ASSERT_C(!counter->ForDerivative(), "the gauge " << name << " is derivative");
    return counter->Val();
}

ui64 GetRate(const NMonitoring::TDynamicCounterPtr& group, const TString& name) {
    auto counter = group->FindNamedCounter("name", name);
    UNIT_ASSERT_C(counter, "no rate " << name);
    UNIT_ASSERT_C(counter->ForDerivative(), "the rate " << name << " is not derivative");
    return counter->Val();
}

TVector<ui64> GetBuckets(const NMonitoring::TDynamicCounterPtr& group, const TString& name) {
    auto histogram = group->FindNamedHistogram("name", name);
    UNIT_ASSERT_C(histogram, "no histogram " << name);

    auto snapshot = histogram->Snapshot();
    TVector<ui64> buckets;

    for (ui32 i = 0; i < snapshot->Count(); ++i) {
        buckets.push_back(snapshot->Value(i));
    }

    return buckets;
}

/**
 * @return The number of the series in the group (a bucket group has no subgroups)
 */
size_t CountSensors(const NMonitoring::TDynamicCounterPtr& group) {
    return group->ReadSnapshot().size();
}

/**
 * The bucket with its own counter group, over the test descriptor.
 */
struct TTestBucket {
    const bool IsPartitionBucket;
    TDetailedMetricsDescriptor Descriptor = MakeTestDescriptor();
    NMonitoring::TDynamicCounterPtr Group = MakeIntrusive<NMonitoring::TDynamicCounters>();
    TPublicBucket Bucket;

    explicit TTestBucket(bool isPartitionBucket, bool skipLeaderOnly = false)
        : IsPartitionBucket(isPartitionBucket)
        , Bucket(Descriptor, Group, isPartitionBucket, skipLeaderOnly)
    {
    }

    TPayload Payload() const {
        return TPayload(Descriptor);
    }

    void Apply(ui32 nodeId, const TPayload& payload) {
        Bucket.Apply(nodeId, payload.Get());
    }

    ui64 Gauge(const TString& name) {
        Bucket.Publish();
        return GetGauge(Group, Name(name));
    }

    ui64 Rate(const TString& name) {
        Bucket.Publish();
        return GetRate(Group, Name(name));
    }

    TVector<ui64> Histogram(const TString& name) {
        Bucket.Publish();
        return GetBuckets(Group, Name(name));
    }

    /**
     * @return The published name of the given test metric in the scope of the bucket
     */
    TString Name(const TString& name) const {
        return IsPartitionBucket ? MakeYdbMetricName(name, EYdbMetricNameScope::Partition) : name;
    }
};

/**
 * Dump the targets created by the classic YDB metrics mapper for DataShard
 * and by TPublicTargets over the DataShard descriptor, for the given scope and role.
 */
std::pair<TString, TString> DumpClassicAndPublicTargets(EYdbMetricNameScope scope, bool isFollower) {
    auto classicGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();
    auto sourceGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();
    auto mapper = CreateYdbMetricsMapperByTabletType(
        TTabletTypes::DataShard, classicGroup, sourceGroup, scope, isFollower);
    UNIT_ASSERT(mapper);

    const auto* descriptor = GetDetailedMetricsDescriptor(TTabletTypes::DataShard);
    UNIT_ASSERT(descriptor);

    auto publicGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();
    TPublicTargets targets(*descriptor, publicGroup, scope, isFollower);

    return {
        NormalizeJson(NMonitoring::ToJson(*classicGroup)),
        NormalizeJson(NMonitoring::ToJson(*publicGroup)),
    };
}

} // namespace

Y_UNIT_TEST_SUITE(TPublicMetricsBucketTest) {

    Y_UNIT_TEST(PublicTargetsMatchTheClassicMapper) {
        for (auto scope : {EYdbMetricNameScope::Aggregate, EYdbMetricNameScope::Partition}) {
            for (bool isFollower : {false, true}) {
                const auto [classic, published] = DumpClassicAndPublicTargets(scope, isFollower);

                UNIT_ASSERT_VALUES_EQUAL_C(published, classic,
                    "scope " << static_cast<int>(scope) << ", follower " << isFollower);

                // The dump has every kind of series: gauges (unless all of them are skipped),
                // rates and histograms with their bounds
                const TString usedCorePercents = MakeYdbMetricName("table.datashard.used_core_percents", scope);
                UNIT_ASSERT_STRING_CONTAINS(published, usedCorePercents);
                UNIT_ASSERT_STRING_CONTAINS(published, "\"bounds\"");
                UNIT_ASSERT_STRING_CONTAINS(published, "\"RATE\"");
                if (!isFollower) {
                    UNIT_ASSERT_STRING_CONTAINS(published, "\"GAUGE\"");
                }
            }
        }
    }

    Y_UNIT_TEST(PublicTargetsOfADataShardLeaf) {
        const auto* descriptor = GetDetailedMetricsDescriptor(TTabletTypes::DataShard);
        UNIT_ASSERT(descriptor);

        auto leaderGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TPublicTargets leader(*descriptor, leaderGroup, EYdbMetricNameScope::Partition, false);
        UNIT_ASSERT_VALUES_EQUAL(CountSensors(leaderGroup), 16u);

        // A follower leaf publishes 7 rates, which are not LeaderOnly, and used_core_percents
        auto followerGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TPublicTargets follower(*descriptor, followerGroup, EYdbMetricNameScope::Partition, true);
        UNIT_ASSERT_VALUES_EQUAL(CountSensors(followerGroup), 8u);

        UNIT_ASSERT(!follower.Gauges[NDataShard::COUNTER_DATASHARD_ROW_COUNT]);
        UNIT_ASSERT(!follower.Rates[NDataShard::COUNTER_DATASHARD_WRITE_ROWS]);
        UNIT_ASSERT(follower.Rates[NDataShard::COUNTER_DATASHARD_CONSUMED_CPU_MICROSECONDS]);
        UNIT_ASSERT(follower.Histograms[NDataShard::COUNTER_DATASHARD_USED_CORE_PERCENTS]);

        UNIT_ASSERT(followerGroup->FindNamedCounter("name", "table.datashard.partition.consumed_cpu_us"));
        UNIT_ASSERT(followerGroup->FindNamedHistogram("name", "table.datashard.partition.used_core_percents"));
        UNIT_ASSERT(!followerGroup->FindNamedCounter("name", "table.datashard.partition.row_count"));
        UNIT_ASSERT(!followerGroup->FindNamedCounter("name", "table.datashard.consumed_cpu_us"));
    }

    Y_UNIT_TEST(PublicTargetsKeepAnInvalidMetric) {
        // A metric, which failed the validation, has no sources, but keeps its target:
        // the rollup looks up every series by name
        auto descriptor = MakeTestDescriptor();
        descriptor.Rates.push_back(MakeSpec("table.test.broken", {
            MakeSource(SCC_TABLET, ESourceWrapper::Sum, "AppRate"),
        }));

        // The bounds of these histograms cannot make an explicit histogram:
        // no bounds (Ranges), descending bounds, and too many bounds
        TVector<ui64> tooManyBounds;
        for (ui64 bound = 0; bound <= NMonitoring::HISTOGRAM_MAX_BUCKETS_COUNT; ++bound) {
            tooManyBounds.push_back(bound);
        }

        const TVector<TString> placeholderNames = {
            "table.test.no_bounds",
            "table.test.descending_bounds",
            "table.test.too_many_bounds",
        };
        descriptor.Histograms.push_back(MakeSpec(placeholderNames[0], {
            MakeSource(SCC_TABLET, ESourceWrapper::None, "AppNoBounds"),
        }));
        descriptor.Histograms.push_back(MakeSpec(placeholderNames[1], {
            MakeSource(SCC_TABLET, ESourceWrapper::None, "AppDescendingBounds"),
        }, false /* leaderOnly */, {20, 10}));
        descriptor.Histograms.push_back(MakeSpec(placeholderNames[2], {
            MakeSource(SCC_TABLET, ESourceWrapper::None, "AppTooManyBounds"),
        }, false /* leaderOnly */, tooManyBounds));

        UNIT_ASSERT(!FinalizeDescriptor(descriptor, nullptr));
        UNIT_ASSERT(descriptor.Rates.back().Sources.empty());
        UNIT_ASSERT(descriptor.Histograms[INCREMENTS + 1].Sources.empty());
        UNIT_ASSERT(descriptor.Histograms[INCREMENTS + 2].Sources.empty());
        UNIT_ASSERT(descriptor.Histograms[INCREMENTS + 3].Sources.empty());

        auto group = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TPublicTargets targets(descriptor, group, EYdbMetricNameScope::Aggregate, false);
        UNIT_ASSERT(targets.Rates.back());
        UNIT_ASSERT(group->FindNamedCounter("name", "table.test.broken"));

        // Such a histogram gets the single placeholder bound 0 (and +Inf)
        for (size_t i = 0; i < placeholderNames.size(); ++i) {
            const auto& name = placeholderNames[i];
            UNIT_ASSERT_C(targets.Histograms[INCREMENTS + 1 + i], name);

            auto histogram = group->FindNamedHistogram("name", name);
            UNIT_ASSERT_C(histogram, name);

            auto snapshot = histogram->Snapshot();
            UNIT_ASSERT_VALUES_EQUAL_C(snapshot->Count(), 2u, name);
            UNIT_ASSERT_VALUES_EQUAL_C(snapshot->UpperBound(0), 0.0, name);
        }

        // The valid histograms keep their bounds
        UNIT_ASSERT_VALUES_EQUAL(group->FindNamedHistogram("name", LEVEL_NAME)->Snapshot()->Count(), 4u);

        // A bucket over such a descriptor publishes as usual
        auto bucketGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TPublicBucket bucket(descriptor, bucketGroup, false /* isPartitionBucket */, false /* skipLeaderOnly */);
        bucket.Apply(1, TPayload(descriptor).Get());
        bucket.Publish();

        for (const auto& name : placeholderNames) {
            UNIT_ASSERT_VALUES_EQUAL_C(GetBuckets(bucketGroup, name), TVector<ui64>(2, 0), name);
        }
    }

    Y_UNIT_TEST(BucketTargetsFollowTheScopeAndTheRole) {
        TTestBucket partial(false /* isPartitionBucket */);
        UNIT_ASSERT(partial.Group->FindNamedCounter("name", GAUGE_SUM_NAME));
        UNIT_ASSERT(partial.Group->FindNamedCounter("name", RATE_LEADER_ONLY_NAME));
        UNIT_ASSERT(partial.Group->FindNamedHistogram("name", LEVEL_NAME));
        UNIT_ASSERT_VALUES_EQUAL(CountSensors(partial.Group), 8u);

        TTestBucket leaf(true /* isPartitionBucket */);
        UNIT_ASSERT(leaf.Group->FindNamedCounter("name", "table.test.partition.gauge_sum"));
        UNIT_ASSERT(leaf.Group->FindNamedHistogram("name", "table.test.partition.level"));
        UNIT_ASSERT(!leaf.Group->FindNamedCounter("name", GAUGE_SUM_NAME));
        UNIT_ASSERT_VALUES_EQUAL(CountSensors(leaf.Group), 8u);

        TTestBucket follower(true /* isPartitionBucket */, true /* skipLeaderOnly */);
        UNIT_ASSERT(!follower.Group->FindNamedCounter("name", "table.test.partition.gauge_leader_only"));
        UNIT_ASSERT(!follower.Group->FindNamedCounter("name", "table.test.partition.rate_leader_only"));
        UNIT_ASSERT_VALUES_EQUAL(CountSensors(follower.Group), 6u);
        UNIT_ASSERT(!follower.Bucket.GetTargets().Gauges[GAUGE_LEADER_ONLY]);
        UNIT_ASSERT(!follower.Bucket.GetTargets().Rates[RATE_LEADER_ONLY]);

        // A follower payload, which still carries LeaderOnly values, publishes the rest
        follower.Apply(1, follower.Payload().Gauges({1, 2, 3}).Rate(RATE, 4).Rate(RATE_LEADER_ONLY, 5));
        UNIT_ASSERT_VALUES_EQUAL(follower.Gauge(GAUGE_SUM_NAME), 1u);
        UNIT_ASSERT_VALUES_EQUAL(follower.Rate(RATE_NAME), 4u);
        UNIT_ASSERT_VALUES_EQUAL(CountSensors(follower.Group), 6u);
    }

    Y_UNIT_TEST(PartialGaugesAreSummedOverNodesUnlessCombinedByMax) {
        TTestBucket partial(false /* isPartitionBucket */);
        partial.Apply(1, partial.Payload().Gauges({10, 7, 100}));
        partial.Apply(2, partial.Payload().Gauges({20, 9, 200}));

        UNIT_ASSERT_VALUES_EQUAL(partial.Gauge(GAUGE_SUM_NAME), 30u);
        UNIT_ASSERT_VALUES_EQUAL(partial.Gauge(GAUGE_MAX_NAME), 9u);
        UNIT_ASSERT_VALUES_EQUAL(partial.Gauge(GAUGE_LEADER_ONLY_NAME), 300u);

        // Every report replaces the previous values of the node
        partial.Apply(1, partial.Payload().Gauges({4, 11, 0}));
        UNIT_ASSERT_VALUES_EQUAL(partial.Gauge(GAUGE_SUM_NAME), 24u);
        UNIT_ASSERT_VALUES_EQUAL(partial.Gauge(GAUGE_MAX_NAME), 11u);
        UNIT_ASSERT_VALUES_EQUAL(partial.Gauge(GAUGE_LEADER_ONLY_NAME), 200u);

        UNIT_ASSERT(!partial.Bucket.DropNode(2));
        UNIT_ASSERT_VALUES_EQUAL(partial.Gauge(GAUGE_SUM_NAME), 4u);
        UNIT_ASSERT_VALUES_EQUAL(partial.Gauge(GAUGE_MAX_NAME), 11u);
        UNIT_ASSERT_VALUES_EQUAL(partial.Gauge(GAUGE_LEADER_ONLY_NAME), 0u);
    }

    Y_UNIT_TEST(LeafGaugesTakeTheMaximumOverNodes) {
        // Two nodes report the same leaf while the partition moves: the gauge is not doubled
        TTestBucket leaf(true /* isPartitionBucket */);
        leaf.Apply(1, leaf.Payload().Gauges({10, 7, 100}));
        leaf.Apply(2, leaf.Payload().Gauges({20, 3, 90}));

        UNIT_ASSERT_VALUES_EQUAL(leaf.Gauge(GAUGE_SUM_NAME), 20u);
        UNIT_ASSERT_VALUES_EQUAL(leaf.Gauge(GAUGE_MAX_NAME), 7u);
        UNIT_ASSERT_VALUES_EQUAL(leaf.Gauge(GAUGE_LEADER_ONLY_NAME), 100u);

        UNIT_ASSERT(!leaf.Bucket.DropNode(2));
        UNIT_ASSERT_VALUES_EQUAL(leaf.Gauge(GAUGE_SUM_NAME), 10u);

        UNIT_ASSERT(leaf.Bucket.DropNode(1));
        UNIT_ASSERT_VALUES_EQUAL(leaf.Gauge(GAUGE_SUM_NAME), 0u);
        UNIT_ASSERT_VALUES_EQUAL(leaf.Gauge(GAUGE_MAX_NAME), 0u);
    }

    Y_UNIT_TEST(RatesAccumulateOverNodesAndSurviveDropNode) {
        for (bool isPartitionBucket : {false, true}) {
            TTestBucket bucket(isPartitionBucket);
            bucket.Apply(1, bucket.Payload().Rate(RATE, 5).Rate(RATE_LEADER_ONLY, 1));
            bucket.Apply(2, bucket.Payload().Rate(RATE, 7));
            bucket.Apply(1, bucket.Payload().Rate(RATE, 3));

            UNIT_ASSERT_VALUES_EQUAL(bucket.Rate(RATE_NAME), 15u);
            UNIT_ASSERT_VALUES_EQUAL(bucket.Rate(RATE_LEADER_ONLY_NAME), 1u);

            // An idle report adds nothing
            bucket.Apply(2, bucket.Payload());
            UNIT_ASSERT_VALUES_EQUAL(bucket.Rate(RATE_NAME), 15u);

            // A removed node keeps its contribution
            UNIT_ASSERT(!bucket.Bucket.DropNode(1));
            UNIT_ASSERT_VALUES_EQUAL(bucket.Rate(RATE_NAME), 15u);

            bucket.Apply(2, bucket.Payload().Rate(RATE, 1));
            UNIT_ASSERT_VALUES_EQUAL(bucket.Rate(RATE_NAME), 16u);

            // The last node is gone, the final publish still has every delta
            UNIT_ASSERT(bucket.Bucket.DropNode(2));
            UNIT_ASSERT_VALUES_EQUAL(bucket.Rate(RATE_NAME), 16u);
            UNIT_ASSERT_VALUES_EQUAL(bucket.Rate(RATE_LEADER_ONLY_NAME), 1u);
        }
    }

    Y_UNIT_TEST(RateTotalsWrapModulo2To64) {
        TTestBucket bucket(true /* isPartitionBucket */);
        bucket.Apply(1, bucket.Payload().Rate(RATE, Max<ui64>()));
        bucket.Apply(1, bucket.Payload().Rate(RATE, 3));
        UNIT_ASSERT_VALUES_EQUAL(bucket.Rate(RATE_NAME), 2u);
    }

    Y_UNIT_TEST(LevelHistogramIsReplacedPerNodeAndSummedOverNodes) {
        for (size_t index : {LEVEL, INTEGRAL}) {
            const TString name = index == LEVEL ? LEVEL_NAME : INTEGRAL_NAME;
            const size_t bucketCount = index == LEVEL ? 4 : 3;

            TTestBucket bucket(false /* isPartitionBucket */);
            bucket.Apply(1, bucket.Payload().Buckets(index, {{0, 2}, {2, 1}}));
            bucket.Apply(2, bucket.Payload().Buckets(index, {{1, 4}}));

            TVector<ui64> expected(bucketCount, 0);
            expected[0] = 2;
            expected[1] = 4;
            expected[2] = 1;
            UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(name), expected);

            // A report replaces the whole level of the node, the last bucket is +Inf
            bucket.Apply(1, bucket.Payload().Buckets(index, {{bucketCount - 1, 5}}));
            expected.assign(bucketCount, 0);
            expected[1] = 4;
            expected[bucketCount - 1] = 5;
            UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(name), expected);

            // Publishing again does not double the level
            UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(name), expected);

            // A present, but empty level clears the node
            bucket.Apply(2, bucket.Payload());
            expected[1] = 0;
            UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(name), expected);

            UNIT_ASSERT(!bucket.Bucket.DropNode(2));
            UNIT_ASSERT(bucket.Bucket.DropNode(1));
            UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(name), TVector<ui64>(bucketCount, 0));
            UNIT_ASSERT(bucket.Bucket.TakeWarnings().empty());
        }
    }

    Y_UNIT_TEST(MissingLevelHistogramClearsTheNode) {
        TTestBucket bucket(true /* isPartitionBucket */);
        bucket.Apply(1, bucket.Payload().Buckets(LEVEL, {{1, 3}}).Buckets(INTEGRAL, {{0, 2}}));
        UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(LEVEL_NAME), TVector<ui64>({0, 3, 0, 0}));

        auto payload = bucket.Payload();
        payload.Mutable().ClearHistogram();
        bucket.Apply(1, payload);
        UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(LEVEL_NAME), TVector<ui64>({0, 0, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(INTEGRAL_NAME), TVector<ui64>({0, 0, 0}));
        UNIT_ASSERT(bucket.Bucket.TakeWarnings().empty());
    }

    Y_UNIT_TEST(IncrementHistogramAccumulatesOverNodesAndSurvivesDropNode) {
        TTestBucket bucket(false /* isPartitionBucket */);
        bucket.Apply(1, bucket.Payload().Buckets(INCREMENTS, {{0, 1}}));
        bucket.Apply(2, bucket.Payload().Buckets(INCREMENTS, {{0, 2}, {2, 3}}));
        bucket.Apply(1, bucket.Payload().Buckets(INCREMENTS, {{1, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(INCREMENTS_NAME), TVector<ui64>({3, 1, 3}));

        // Publishing again does not double the increments
        UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(INCREMENTS_NAME), TVector<ui64>({3, 1, 3}));

        UNIT_ASSERT(!bucket.Bucket.DropNode(1));
        UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(INCREMENTS_NAME), TVector<ui64>({3, 1, 3}));

        UNIT_ASSERT(bucket.Bucket.DropNode(2));
        UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(INCREMENTS_NAME), TVector<ui64>({3, 1, 3}));
        UNIT_ASSERT(bucket.Bucket.TakeWarnings().empty());
    }

    Y_UNIT_TEST(UnmarkedStaticLevelHistogramClearsTheNodeAndWarnsOnce) {
        TTestBucket bucket(false /* isPartitionBucket */);
        bucket.Apply(1, bucket.Payload().Buckets(LEVEL, {{1, 3}}).Gauges({10, 0, 0}));
        bucket.Apply(2, bucket.Payload().Buckets(LEVEL, {{0, 1}}).Gauges({20, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(LEVEL_NAME), TVector<ui64>({1, 3, 0, 0}));

        // The marked level of node 1 is followed by an unmarked payload (a delta):
        // the node contributes nothing to the histogram, the rest of the report is applied
        bucket.Apply(1, bucket.Payload().Marked(LEVEL, false).Buckets(LEVEL, {{2, 5}}).Gauges({11, 0, 0}).Rate(RATE, 2));
        UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(LEVEL_NAME), TVector<ui64>({1, 0, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(bucket.Gauge(GAUGE_SUM_NAME), 31u);
        UNIT_ASSERT_VALUES_EQUAL(bucket.Rate(RATE_NAME), 2u);

        auto warnings = bucket.Bucket.TakeWarnings();
        UNIT_ASSERT_VALUES_EQUAL_C(warnings.size(), 1u, JoinSeq("; ", warnings));
        UNIT_ASSERT_STRING_CONTAINS(warnings[0], LEVEL_NAME);
        UNIT_ASSERT_STRING_CONTAINS(warnings[0], "node 1");

        // The warning is given once per bucket
        bucket.Apply(2, bucket.Payload().Marked(LEVEL, false).Buckets(LEVEL, {{2, 5}}));
        UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(LEVEL_NAME), TVector<ui64>({0, 0, 0, 0}));
        UNIT_ASSERT(bucket.Bucket.TakeWarnings().empty());

        // The next marked report restores the level of the node
        bucket.Apply(1, bucket.Payload().Buckets(LEVEL, {{3, 2}}));
        UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(LEVEL_NAME), TVector<ui64>({0, 0, 0, 2}));
    }

    Y_UNIT_TEST(MismatchedHistogramMarkIsIgnoredAndWarnsOnce) {
        TTestBucket bucket(true /* isPartitionBucket */);
        bucket.Apply(1, bucket.Payload().Buckets(INTEGRAL, {{0, 2}}).Buckets(INCREMENTS, {{1, 1}}));
        UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(INTEGRAL_NAME), TVector<ui64>({2, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(INCREMENTS_NAME), TVector<ui64>({0, 1, 0}));
        UNIT_ASSERT(bucket.Bucket.TakeWarnings().empty());

        // An unmarked payload of an integral percentile: the previous level is kept
        bucket.Apply(1, bucket.Payload().Marked(INTEGRAL, false).Buckets(INTEGRAL, {{1, 9}}));
        UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(INTEGRAL_NAME), TVector<ui64>({2, 0, 0}));

        auto warnings = bucket.Bucket.TakeWarnings();
        UNIT_ASSERT_VALUES_EQUAL_C(warnings.size(), 1u, JoinSeq("; ", warnings));
        UNIT_ASSERT_STRING_CONTAINS(warnings[0], INTEGRAL_NAME);

        // A marked payload of a derivative percentile: the increments are not added
        bucket.Apply(1, bucket.Payload().Marked(INCREMENTS, true).Buckets(INCREMENTS, {{1, 7}}));
        UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(INCREMENTS_NAME), TVector<ui64>({0, 1, 0}));
        UNIT_ASSERT(bucket.Bucket.TakeWarnings().empty());
    }

    Y_UNIT_TEST(RemotePayloadIsClamped) {
        TTestBucket bucket(false /* isPartitionBucket */);

        // A shorter Simple: the missing gauges are zero
        bucket.Apply(1, bucket.Payload().Gauges({10, 20, 30}));
        bucket.Apply(1, bucket.Payload().Gauges({5}));
        UNIT_ASSERT_VALUES_EQUAL(bucket.Gauge(GAUGE_SUM_NAME), 5u);
        UNIT_ASSERT_VALUES_EQUAL(bucket.Gauge(GAUGE_MAX_NAME), 0u);
        UNIT_ASSERT_VALUES_EQUAL(bucket.Gauge(GAUGE_LEADER_ONLY_NAME), 0u);

        // A longer Simple: the extra gauges are ignored
        bucket.Apply(1, bucket.Payload().Gauges({1, 2, 3, 4, 5, 6}));
        UNIT_ASSERT_VALUES_EQUAL(bucket.Gauge(GAUGE_SUM_NAME), 1u);
        UNIT_ASSERT_VALUES_EQUAL(bucket.Gauge(GAUGE_LEADER_ONLY_NAME), 3u);

        // Unknown rates, a huge CumulativeCount and an odd tail are ignored
        {
            auto payload = bucket.Payload().Rate(RATE, 4).Rate(2, 100).Rate(Max<ui64>(), 100);
            payload.Mutable().SetCumulativeCount(Max<ui64>());
            payload.Mutable().AddCumulative(RATE);
            bucket.Apply(1, payload);
        }
        UNIT_ASSERT_VALUES_EQUAL(bucket.Rate(RATE_NAME), 4u);
        UNIT_ASSERT_VALUES_EQUAL(bucket.Rate(RATE_LEADER_ONLY_NAME), 0u);

        // A missing CumulativeCount does not drop the rates
        {
            auto payload = bucket.Payload().Rate(RATE, 1);
            payload.Mutable().ClearCumulativeCount();
            bucket.Apply(1, payload);
        }
        UNIT_ASSERT_VALUES_EQUAL(bucket.Rate(RATE_NAME), 5u);

        // Buckets beyond the public ones (a huge BucketsCount) and an odd tail are ignored
        {
            auto payload = bucket.Payload().Buckets(LEVEL, {{1, 3}, {4, 100}, {Max<ui64>(), 100}});
            payload.Mutable().MutableHistogram(LEVEL)->SetBucketsCount(Max<ui64>());
            payload.Mutable().MutableHistogram(LEVEL)->AddBuckets(0);
            payload.Buckets(INCREMENTS, {{2, 1}, {3, 100}});
            payload.Mutable().MutableHistogram(INCREMENTS)->SetBucketsCount(1000);
            payload.Mutable().MutableHistogram(INCREMENTS)->AddBuckets(1);
            bucket.Apply(1, payload);
        }
        UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(LEVEL_NAME), TVector<ui64>({0, 3, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(INCREMENTS_NAME), TVector<ui64>({0, 0, 1}));

        // Buckets beyond the BucketsCount of the payload are ignored
        {
            auto payload = bucket.Payload().Buckets(LEVEL, {{0, 1}, {2, 7}}).Buckets(INCREMENTS, {{0, 1}, {1, 8}});
            payload.Mutable().MutableHistogram(LEVEL)->SetBucketsCount(2);
            payload.Mutable().MutableHistogram(INCREMENTS)->SetBucketsCount(1);
            bucket.Apply(1, payload);
        }
        UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(LEVEL_NAME), TVector<ui64>({1, 0, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(INCREMENTS_NAME), TVector<ui64>({1, 0, 1}));

        // Histogram entries beyond the public ones are ignored
        {
            auto payload = bucket.Payload().Buckets(LEVEL, {{3, 1}});
            for (int i = 0; i < 3; ++i) {
                auto* extra = payload.Mutable().AddHistogram();
                extra->SetNonDerivative(i % 2 == 0);
                extra->SetBucketsCount(Max<ui64>());
                extra->AddBuckets(Max<ui64>());
                extra->AddBuckets(Max<ui64>());
            }
            bucket.Apply(1, payload);
        }
        UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(LEVEL_NAME), TVector<ui64>({0, 0, 0, 1}));

        // A completely empty payload is a node without values
        bucket.Bucket.Apply(1, NKikimrSysView::TDbCounters());
        UNIT_ASSERT_VALUES_EQUAL(bucket.Gauge(GAUGE_SUM_NAME), 0u);
        UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(LEVEL_NAME), TVector<ui64>({0, 0, 0, 0}));
        UNIT_ASSERT_VALUES_EQUAL(bucket.Rate(RATE_NAME), 5u);
        UNIT_ASSERT_VALUES_EQUAL(bucket.Histogram(INCREMENTS_NAME), TVector<ui64>({1, 0, 1}));
        UNIT_ASSERT(bucket.Bucket.TakeWarnings().empty());
    }

    Y_UNIT_TEST(DropNodeReturnsTrueWhenNoNodeIsLeft) {
        TTestBucket bucket(true /* isPartitionBucket */);
        UNIT_ASSERT(bucket.Bucket.DropNode(1));

        bucket.Apply(1, bucket.Payload());
        bucket.Apply(2, bucket.Payload());
        bucket.Apply(3, bucket.Payload());
        UNIT_ASSERT(!bucket.Bucket.DropNode(4));
        UNIT_ASSERT(!bucket.Bucket.DropNode(2));
        UNIT_ASSERT(!bucket.Bucket.DropNode(2));
        UNIT_ASSERT(!bucket.Bucket.DropNode(1));
        UNIT_ASSERT(bucket.Bucket.DropNode(3));
        UNIT_ASSERT(bucket.Bucket.DropNode(3));
    }

    Y_UNIT_TEST(DataShardUsedCorePercentsFillsEveryPublicBucket) {
        // The public bucket i is the bucket i of the target, the last one is +Inf
        const auto* descriptor = GetDetailedMetricsDescriptor(TTabletTypes::DataShard);
        UNIT_ASSERT(descriptor);

        const auto& spec = descriptor->Histograms[NDataShard::COUNTER_DATASHARD_USED_CORE_PERCENTS];
        UNIT_ASSERT_VALUES_EQUAL(spec.BucketCount(), 12u);

        auto group = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TPublicBucket bucket(*descriptor, group, true /* isPartitionBucket */, true /* skipLeaderOnly */);

        TPayload payload(*descriptor);
        TBuckets buckets;
        TVector<ui64> expected;
        for (ui64 i = 0; i < spec.BucketCount(); ++i) {
            buckets.emplace_back(i, i + 1);
            expected.push_back(i + 1);
        }
        payload.Buckets(NDataShard::COUNTER_DATASHARD_USED_CORE_PERCENTS, buckets);
        payload.Rate(NDataShard::COUNTER_DATASHARD_CONSUMED_CPU_MICROSECONDS, 42);
        bucket.Apply(1, payload.Get());
        bucket.Publish();

        UNIT_ASSERT_VALUES_EQUAL(GetBuckets(group, "table.datashard.partition.used_core_percents"), expected);
        UNIT_ASSERT_VALUES_EQUAL(GetRate(group, "table.datashard.partition.consumed_cpu_us"), 42u);
        UNIT_ASSERT_VALUES_EQUAL(CountSensors(group), 8u);
    }

    Y_UNIT_TEST(BucketStateDoesNotGrowInSteadyState) {
        const auto* descriptor = GetDetailedMetricsDescriptor(TTabletTypes::DataShard);
        UNIT_ASSERT(descriptor);

        auto group = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TPublicBucket bucket(*descriptor, group, true /* isPartitionBucket */, false /* skipLeaderOnly */);

        const auto makePayload = [descriptor](ui64 step) {
            TPayload payload(*descriptor);
            payload.Gauges({step, 2 * step});
            for (ui64 i = 0; i < descriptor->Rates.size(); ++i) {
                payload.Rate(i, step + i);
            }
            payload.Buckets(NDataShard::COUNTER_DATASHARD_USED_CORE_PERCENTS, {{step % 12, 1}});
            return payload;
        };

        bucket.Apply(1, makePayload(1).Get());
        bucket.Publish();
        const size_t initialBytes = bucket.GetAllocatedBytes();
        Cerr << "TEST sizeof(TPublicBucket) = " << sizeof(TPublicBucket)
             << ", allocated bytes of a DataShard leaf = " << initialBytes << Endl;

        for (ui64 step = 2; step < 100; ++step) {
            bucket.Apply(1, makePayload(step).Get());
            bucket.Publish();
        }

        UNIT_ASSERT_VALUES_EQUAL(bucket.GetAllocatedBytes(), initialBytes);

        // The state of a leaf reported by one node stays small (the targets excluded)
        UNIT_ASSERT_LE(sizeof(TPublicBucket), 256u);
        UNIT_ASSERT_LE(initialBytes, 768u);
    }
}
