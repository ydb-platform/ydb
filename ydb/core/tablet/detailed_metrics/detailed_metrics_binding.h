#pragma once

#include <ydb/core/base/tablet_types.h>
#include <ydb/core/protos/counters.pb.h>
#include <ydb/core/tablet/tablet_counters.h>

#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/ylimits.h>

#include <array>

namespace NKikimr {

// The wrapper of a SourceCounters name: "SUM(x)", "MAX(x)", "HIST(x)" or a plain "x"
enum class ESourceWrapper {
    None,
    Sum,
    Max,
    Hist,
};

struct TSourceRef {
    ESourceCounterCategory Category = ESourceCounterCategory::SCC_TABLET;
    ESourceWrapper Wrapper = ESourceWrapper::None;
    // x, without the wrapper
    TString Name;
    // As written in SourceCounters, e.g. "SUM(x)"
    TString Text;

    bool operator==(const TSourceRef& other) const = default;
};

TSourceRef ParseSourceRef(TStringBuf text, ESourceCounterCategory category);

enum class EMetricKind {
    Gauge,
    Rate,
    Histogram,
};

struct TMetricSpec {
    TString Name;
    bool LeaderOnly = false;
    TVector<TSourceRef> Sources;
    // Histograms: the public bucket bounds, +Inf implicit
    TVector<ui64> Bounds;
    // Histograms: holds the full current value rather than deltas: any source is HIST(x), or the Integral option
    bool NonDerivative = false;

    // Gauges: every source is MAX(x), so partial values combine by MAX, not SUM
    bool CombineByMax() const;

    size_t BucketCount() const {
        return Bounds.size() + 1;
    }
};

/**
 * The public detailed metrics of a tablet type from its counters_detailed_<type>.proto:
 * the index of a metric in Gauges, Rates and Histograms is its enum value.
 */
struct TDetailedMetricsDescriptor {
    TTabletTypes::EType Type = TTabletTypes::TypeInvalid;
    TVector<TMetricSpec> Gauges;
    TVector<TMetricSpec> Rates;
    TVector<TMetricSpec> Histograms;
    // The low level counters the debug tree aggregates: every source as written, plus x of SUM(x) and MAX(x)
    THashSet<TString> ExecutorCounterNames;
    THashSet<TString> AppCounterNames;
};

// A static descriptor, nullptr if the tablet type has no detailed metrics
const TDetailedMetricsDescriptor* GetDetailedMetricsDescriptor(TTabletTypes::EType tabletType);

// How a source counter value contributes to a public metric value
enum class ESourceOp {
    SimpleSum,               // gauge SUM(x): the latest x of every source, summed
    SimpleMax,               // gauge MAX(x): the maximum of the latest x of every source
    CumulativeDelta,         // rate x: the deltas of x, summed over the sources and reports
    HistOfSimple,            // HIST(x): one observation per source, its latest x
    HistOfCumulative,        // HIST(x): one observation per source, the per second rate of x
    PercentileNonDerivative, // integral p: the latest buckets of every source, summed
    PercentileDerivative,    // derivative p: the bucket deltas, summed over the sources and reports
};

// A source counter of a public metric, resolved to its slot
struct TBoundTerm {
    static constexpr ui32 NoSlot = Max<ui32>();

    EMetricKind Kind = EMetricKind::Gauge;
    // The index in Gauges, Rates or Histograms
    ui32 Metric = 0;
    ESourceOp Op = ESourceOp::SimpleSum;
    // Of the source: SCC_TABLET selects the app counters, any other value the executor ones
    ESourceCounterCategory Category = ESourceCounterCategory::SCC_TABLET;
    // The slot of x in Simple() or Cumulative(), or of p in Percentile()
    ui32 Slot = NoSlot;
    // In the per-source state, NoSlot if the term keeps none
    ui32 StateOffset = NoSlot;
    // PercentileDerivative: the pending buckets of the metric, shared by its terms
    ui32 PendingOffset = NoSlot;
    // HistOfSimple, HistOfCumulative: the bounds of HIST(x), Max<double>() for +Inf.
    // An observation falls into the first bucket whose bound is not less than it.
    TVector<double> SourceBounds;
    bool LeaderOnly = false;
};

/**
 * The public metrics of a tablet type bound to its counter layout once, by name. A source counter
 * that cannot be bound is dropped and reported in Problems: like the YDB metrics mapper, binding never aborts.
 *
 * The state of an accumulator over N sources is one ui64 array:
 *
 *     [rates: R][pending derivative buckets: PendingHistSize][N x per-source state: PerSourceStateSize]
 */
struct TDetailedMetricsBinding {
    // Executor, then app: simple, cumulative, percentile
    using TLayoutSizes = std::array<ui32, 6>;

    const TDetailedMetricsDescriptor* Descriptor = nullptr;
    // Ordered by kind, metric, then source
    TVector<TBoundTerm> Terms;
    ui32 PerSourceStateSize = 0;
    ui32 PendingHistSize = 0;
    TLayoutSizes LayoutSizes = {};
    TVector<TString> Problems;

    static TLayoutSizes GetLayoutSizes(const TTabletCountersBase& executorCounters, const TTabletCountersBase& appCounters);
};

/**
 * HIST(x) takes a simple x before a cumulative one, like NPrivate::TAggregatedTabletCounters::Initialize().
 * Never returns nullptr; the descriptor must outlive the binding.
 */
THolder<TDetailedMetricsBinding> BindDetailedMetrics(
    const TDetailedMetricsDescriptor& descriptor,
    const TTabletCountersBase& executorCounters,
    const TTabletCountersBase& appCounters);

} // namespace NKikimr
