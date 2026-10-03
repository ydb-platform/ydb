#pragma once

#include "detailed_metrics_descriptor.h"

#include <ydb/core/tablet/tablet_counters.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/str_stl.h>

#include <array>
#include <limits>

namespace NKikimr {

/**
 * How the value of one source counter becomes (a part of) the value
 * of a public detailed metric.
 */
enum class ESourceOp {
    /**
     * Gauge SUM(x), x is a simple counter: the latest value of x of every source,
     * summed over the sources.
     */
    SimpleSum,
    /**
     * Gauge MAX(x), x is a simple counter: the latest value of x of every source,
     * the maximum over the sources.
     */
    SimpleMax,
    /**
     * Rate x, x is a cumulative counter: the deltas of x reported by every source,
     * summed over the sources and over the reports.
     */
    CumulativeDelta,
    /**
     * Level histogram HIST(x), x is a simple counter: one observation per source,
     * the latest value of x.
     */
    HistOfSimple,
    /**
     * Level histogram HIST(x), x is a cumulative counter: one observation per source,
     * the per second rate of x since the previous report of the source.
     */
    HistOfCumulative,
    /**
     * Level histogram over the integral percentile counter p: the latest buckets of p
     * of every source, summed over the sources.
     */
    PercentileLevel,
    /**
     * Increment histogram over the derivative percentile counter p: the bucket deltas
     * of p reported by every source, summed over the sources and over the reports.
     */
    PercentileIncrement,
};

/**
 * The counter set, which holds a source counter.
 */
enum class EBank {
    /**
     * The Executor counters (ESourceCounterCategory::SCC_EXECUTOR).
     */
    Executor,
    /**
     * The application counters of the tablet (ESourceCounterCategory::SCC_TABLET).
     */
    App,
};

/**
 * One source counter of a public detailed metric, resolved to its slot
 * in the counter layout of the tablet type.
 */
struct TBoundTerm {
    static constexpr ui32 NoSlot = std::numeric_limits<ui32>::max();

    /**
     * The kind of the public metric.
     */
    EMetricKind Kind = EMetricKind::Gauge;

    /**
     * The public metric: the index in Gauges, Rates or Histograms of the descriptor
     * (the enum value and the wire slot).
     */
    ui32 Metric = 0;

    /**
     * The index of the source counter in TMetricSpec::Sources of the metric.
     */
    ui32 Source = 0;

    ESourceOp Op = ESourceOp::SimpleSum;
    EBank Bank = EBank::Executor;

    /**
     * The slot of the source counter in its bank: the base counter x in Simple()
     * (SimpleSum, SimpleMax, HistOfSimple) or in Cumulative() (CumulativeDelta,
     * HistOfCumulative), or the percentile counter p in Percentile() (PercentileLevel,
     * PercentileIncrement).
     */
    ui32 Slot = NoSlot;

    /**
     * HistOfSimple and HistOfCumulative only: the slot of the percentile counter
     * HIST(x) in Percentile() of the bank, which defines the source bucket bounds
     * (the tablets never fill it themselves).
     */
    ui32 HistSlot = NoSlot;

    /**
     * The offset of the per-source state of the term within the state of one source
     * (see TDetailedMetricsBinding), or NoSlot if the term keeps no per-source state.
     */
    ui32 StateOffset = NoSlot;

    /**
     * PercentileIncrement only: the offset of the pending increment buckets
     * of the metric within the pending histogram area (see TDetailedMetricsBinding),
     * shared by every term of the metric; NoSlot otherwise.
     */
    ui32 PendingOffset = NoSlot;

    /**
     * HistOfSimple and HistOfCumulative only: the upper bounds of the source buckets,
     * GetRangeBound(0 .. rangeCount - 2) of HIST(x) followed by Max<double>()
     * for the implicit +Inf bucket. An observation v falls into the first bucket,
     * whose bound is not less than v (lower_bound, the same way as an explicit
     * monlib histogram does it). Source bucket i is public bucket i, a source bucket
     * beyond the last public one is counted in the last public bucket.
     */
    TVector<double> SourceBounds;

    /**
     * The metric is meaningful only on leaders (TMetricSpec::LeaderOnly).
     */
    bool LeaderOnly = false;
};

/**
 * A cheap signature of a counter layout as seen by one binding: the sizes
 * of the six counter arrays and a hash of the names at the slots the binding reads.
 *
 * @note Two layouts with different signatures differ in a way, which matters
 *       for the binding. Equal signatures are a hint, not a proof (a hash may collide):
 *       TDetailedMetricsBinding::Matches() is the exact check.
 */
struct TDetailedMetricsLayoutSignature {
    /**
     * The sizes of the counter arrays, see TDetailedMetricsBinding::ELayoutArray.
     */
    std::array<ui32, 6> Sizes = {};

    /**
     * The hash of the names at the bound slots, in the order of the bound terms.
     */
    ui64 NamesHash = 0;

    bool operator==(const TDetailedMetricsLayoutSignature& other) const = default;

    size_t Hash() const noexcept;
};

/**
 * The public detailed metrics of one tablet type, bound to the counter layout
 * (the Executor and the application counters) of that tablet type: every source
 * counter of the descriptor is resolved to its slot once, by name, so the values
 * of the public metrics are computed from the slots directly, without looking at
 * any other counter of the layout.
 *
 * A source counter, which cannot be bound, is dropped (its metric publishes zero,
 * or the sum of the other sources) and the reason is added to Problems: the binding
 * never aborts, the same way as the YDB metrics mapper ignores missing source counters.
 *
 * The state of an accumulator of the public values over N sources (tablets) is laid out
 * as one array of ui64:
 *
 *     [rates: R][pending histogram buckets: PendingHistSize][N x per-source state: PerSourceStateSize]
 *
 * - rates: the pending (not yet packed) delta of rate metric i is at i, R is the number
 *   of rates in the descriptor, every CumulativeDelta term adds to its metric;
 * - pending histogram buckets: BucketCount() of the metric for every increment histogram,
 *   which has at least one bound term, starting at the PendingOffset of its terms,
 *   every PercentileIncrement term adds its bucket deltas there;
 * - per-source state, at the StateOffset of a term:
 *     * SimpleSum, SimpleMax: 1 slot, the latest value of x reported by the source;
 *     * HistOfSimple, HistOfCumulative: 1 slot, the public bucket of the latest
 *       observation of the source;
 *     * PercentileLevel: BucketCount() of the metric, the latest buckets of p reported
 *       by the source (source buckets beyond the last public one are added to it);
 *     * CumulativeDelta and PercentileIncrement keep no per-source state.
 *
 * The per-source state is the same for leaders and followers: a follower leaves the state
 * of the LeaderOnly terms zero.
 *
 * For example, DataShard binds 18 terms: 2 SimpleSum gauges (state 0 and 1), 15
 * CumulativeDelta rates, 1 HistOfCumulative (HIST(ConsumedCPU), state 2), so
 * PerSourceStateSize is 3 and PendingHistSize is 0.
 */
struct TDetailedMetricsBinding {
    /**
     * The positions of the counter arrays in LayoutSizes and in the layout signature.
     */
    enum ELayoutArray : ui32 {
        ExecutorSimple = 0,
        ExecutorCumulative = 1,
        ExecutorPercentile = 2,
        AppSimple = 3,
        AppCumulative = 4,
        AppPercentile = 5,
    };

    using TLayoutSizes = std::array<ui32, 6>;

    /**
     * The descriptor, which is bound (it must outlive the binding).
     */
    const TDetailedMetricsDescriptor* Descriptor = nullptr;

    /**
     * The bound source counters, ordered by the metric kind (gauges, rates, histograms),
     * then by the metric, then by the source.
     */
    TVector<TBoundTerm> Terms;

    /**
     * Whether histogram i holds a level (TMetricSpec::IsLevel), the index is the metric.
     */
    TVector<bool> HistIsLevel;

    /**
     * The number of ui64 slots of the state of one source.
     */
    ui32 PerSourceStateSize = 0;

    /**
     * The number of ui64 slots of the pending increment buckets of all increment histograms.
     */
    ui32 PendingHistSize = 0;

    /**
     * The sizes of the counter arrays of the bound layout, see ELayoutArray.
     */
    TLayoutSizes LayoutSizes = {};

    /**
     * The signature of the bound layout: GetLayoutSignature() of the counters,
     * which were bound.
     */
    TDetailedMetricsLayoutSignature LayoutSignature;

    /**
     * The source counters, which were dropped (or bound with a caveat), and why.
     * The binding is built without an actor context, so the users log these themselves.
     */
    TVector<TString> Problems;

    /**
     * @return The sizes of the counter arrays of the given layout, see ELayoutArray
     */
    static TLayoutSizes GetLayoutSizes(const TTabletCountersBase& executorCounters, const TTabletCountersBase& appCounters);

    /**
     * @return The source counter of the given bound term
     */
    const TSourceRef& GetSource(const TBoundTerm& term) const;

    /**
     * Check that the given layout is the bound one: the sizes of all six counter arrays
     * are the same and every bound slot holds the same counter name.
     *
     * @note The bucket bounds of the percentile counters are not compared: they are
     *       a property of the counter name, and the values are clamped into the public
     *       buckets anyway.
     *
     * @return Whether the values of the given counters can be read by this binding
     */
    bool Matches(const TTabletCountersBase& executorCounters, const TTabletCountersBase& appCounters) const;

    /**
     * Compute the signature of the given layout as seen by this binding: the sizes
     * of its counter arrays and the hash of its names at the slots this binding reads.
     * It equals LayoutSignature for the bound layout. Cheap: no allocation,
     * one hash per bound slot.
     *
     * @note The intended use is keying an extra binding of a tablet type by the layout,
     *       when another layout of the same type does not match the first binding.
     */
    TDetailedMetricsLayoutSignature GetLayoutSignature(
        const TTabletCountersBase& executorCounters,
        const TTabletCountersBase& appCounters) const;
};

/**
 * Bind the public detailed metrics of the descriptor to the given counter layout.
 *
 * Every source counter is looked up by name (SimpleCounterName(), CumulativeCounterName()
 * and PercentileCounterName(), slots without a name are skipped) in the bank
 * of its category:
 * - SUM(x), MAX(x): x is a simple counter;
 * - a rate source x: x is a cumulative counter;
 * - HIST(x): the percentile counter named HIST(x) (its buckets) and the base counter x,
 *   a simple counter or else a cumulative one (the same order as
 *   NPrivate::TAggregatedTabletCounters::Initialize() uses);
 * - a plain histogram source p: the percentile counter p, which is a level (PercentileLevel)
 *   if it is integral, increments (PercentileIncrement) otherwise.
 *
 * A source counter is dropped (and a problem is reported) if it is missing, if it is
 * of a wrong kind (SUM(x) or MAX(x) of a cumulative x, a rate of a simple x), if HIST(x)
 * has no base counter x, if its percentile counter is not initialized (fewer than
 * two buckets), or if it is a level in an increment histogram or vice versa
 * (see TMetricSpec::IsLevel). A source bucket count, which differs from the public one,
 * is reported, but the source is kept (the values are clamped into the public buckets).
 *
 * @param[in] descriptor The descriptor to bind, it must outlive the binding
 * @param[in] executorCounters The Executor counters of the layout
 * @param[in] appCounters The application counters of the layout
 *
 * @return The binding, never nullptr
 */
THolder<TDetailedMetricsBinding> BindDetailedMetrics(
    const TDetailedMetricsDescriptor& descriptor,
    const TTabletCountersBase& executorCounters,
    const TTabletCountersBase& appCounters);

} // namespace NKikimr

template <>
struct THash<NKikimr::TDetailedMetricsLayoutSignature> {
    size_t operator()(const NKikimr::TDetailedMetricsLayoutSignature& signature) const noexcept {
        return signature.Hash();
    }
};
