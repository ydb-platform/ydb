#pragma once

#include <ydb/core/base/tablet_types.h>
#include <ydb/core/protos/counters.pb.h>
#include <ydb/core/tablet/tablet_counters.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/ylimits.h>

#include <array>

namespace NKikimr {

/**
 * The wrapper around the name of a source counter in the SourceCounters definition
 * of a public detailed metric: "SUM(x)", "MAX(x)", "HIST(x)" or a plain "x".
 */
enum class ESourceWrapper {
    None,
    Sum,
    Max,
    Hist,
};

struct TSourceRef {
    ESourceCounterCategory Category = ESourceCounterCategory::SCC_TABLET;
    ESourceWrapper Wrapper = ESourceWrapper::None;
    // The name x with the wrapper stripped
    TString Name;

    bool operator==(const TSourceRef& other) const = default;
};

enum class EMetricKind {
    Gauge,     // ESimpleDetailedCounters
    Rate,      // ECumulativeDetailedCounters
    Histogram, // EPercentileDetailedCounters
};

struct TMetricSpec {
    TString Name;
    // MakeYdbMetricName(Name, EYdbMetricNameScope::Partition), shared by every leaf target
    TString PartitionName;
    bool LeaderOnly = false;
    TVector<TSourceRef> Sources;
    // Gauges: every source is MAX(x), so partial values are combined by MAX instead of SUM
    bool CombineByMax = false;
    // Histograms: the public bucket bounds, +Inf is implicit
    TVector<ui64> Bounds;
    // Histograms: every source is HIST(x)
    bool StaticLevel = false;
    // Histograms: a level (any source is HIST(x), or the metric is Integral), which goes up and down,
    // rather than increments, which only accumulate
    bool IsLevel = false;

    size_t BucketCount() const {
        return Bounds.size() + 1;
    }
};

/**
 * Make the specification of a public metric, deriving PartitionName, CombineByMax,
 * StaticLevel and IsLevel from the rest.
 *
 * @param[in] integral Histograms only: the Integral option of the enum entry of the metric
 */
TMetricSpec MakeMetricSpec(
    EMetricKind kind,
    const TString& name,
    bool leaderOnly,
    TVector<TSourceRef> sources,
    TVector<ui64> bounds = {},
    bool integral = false);

/**
 * The public detailed metrics of one tablet type, as defined in its counters_detailed_<type>.proto.
 *
 * @note The index of a metric in Gauges, Rates and Histograms is the value of its enum entry.
 */
struct TDetailedMetricsDescriptor {
    TTabletTypes::EType Type = TTabletTypes::TypeInvalid;
    TVector<TMetricSpec> Gauges;
    TVector<TMetricSpec> Rates;
    TVector<TMetricSpec> Histograms;
};

/**
 * @return The static descriptor of the tablet type, or nullptr if it has no detailed metrics
 */
const TDetailedMetricsDescriptor* GetDetailedMetricsDescriptor(TTabletTypes::EType tabletType);

/**
 * How the value of one source counter becomes (a part of) the value of a public metric.
 */
enum class ESourceOp {
    SimpleSum,           // gauge SUM(x), simple x: the latest x of every source, summed
    SimpleMax,           // gauge MAX(x), simple x: the latest x of every source, the maximum
    CumulativeDelta,     // rate x, cumulative x: the deltas of x, summed over the sources and the reports
    HistOfSimple,        // level HIST(x), simple x: one observation per source, the latest x
    HistOfCumulative,    // level HIST(x), cumulative x: one observation per source, the per second rate of x
    PercentileLevel,     // level, integral percentile p: the latest buckets of p of every source, summed
    PercentileIncrement, // increments, derivative percentile p: the bucket deltas, summed over the sources and the reports
};

enum class EBank {
    Executor, // SCC_EXECUTOR
    App,      // SCC_TABLET
};

/**
 * One source counter of a public metric, resolved to its slot in the counter layout.
 */
struct TBoundTerm {
    static constexpr ui32 NoSlot = Max<ui32>();

    EMetricKind Kind = EMetricKind::Gauge;
    // The index of the metric in Gauges, Rates or Histograms
    ui32 Metric = 0;
    ESourceOp Op = ESourceOp::SimpleSum;
    EBank Bank = EBank::Executor;
    // The slot of x in Simple() or Cumulative(), or of p in Percentile()
    ui32 Slot = NoSlot;
    // The offset of the per-source state of the term, NoSlot if it keeps none
    ui32 StateOffset = NoSlot;
    // PercentileIncrement: the offset of the pending buckets of the metric, shared by its terms
    ui32 PendingOffset = NoSlot;
    // HistOfSimple, HistOfCumulative: the bucket bounds of HIST(x) and Max<double>() for +Inf.
    // An observation falls into the first bucket, whose bound is not less than it; a source
    // bucket past the last public one is counted in the last public bucket.
    TVector<double> SourceBounds;
    bool LeaderOnly = false;
};

/**
 * The public metrics of a tablet type bound to its counter layout: every source counter
 * is resolved to its slot once, by name. A source counter, which cannot be bound, is dropped
 * and reported in Problems: the binding never aborts, like the YDB metrics mapper.
 *
 * The state of an accumulator over N sources is one array of ui64:
 *
 *     [rates: R][pending increment buckets: PendingHistSize][N x per-source state: PerSourceStateSize]
 *
 * The per-source state is 1 slot for SimpleSum, SimpleMax, HistOfSimple and HistOfCumulative
 * (the latest value or bucket) and BucketCount() slots for PercentileLevel.
 */
struct TDetailedMetricsBinding {
    // Executor simple, cumulative, percentile, then app simple, cumulative, percentile
    using TLayoutSizes = std::array<ui32, 6>;

    const TDetailedMetricsDescriptor* Descriptor = nullptr;
    // Ordered by the metric kind, then by the metric, then by the source
    TVector<TBoundTerm> Terms;
    ui32 PerSourceStateSize = 0;
    ui32 PendingHistSize = 0;
    TLayoutSizes LayoutSizes = {};
    TVector<TString> Problems;

    static TLayoutSizes GetLayoutSizes(const TTabletCountersBase& executorCounters, const TTabletCountersBase& appCounters);
};

/**
 * Bind the descriptor to the counter layout. A simple x takes HIST(x) before a cumulative one,
 * like NPrivate::TAggregatedTabletCounters::Initialize().
 *
 * @return The binding, never nullptr; the descriptor must outlive it
 */
THolder<TDetailedMetricsBinding> BindDetailedMetrics(
    const TDetailedMetricsDescriptor& descriptor,
    const TTabletCountersBase& executorCounters,
    const TTabletCountersBase& appCounters);

} // namespace NKikimr
