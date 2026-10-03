#pragma once

#include "detailed_metrics_binding.h"
#include "detailed_metrics_tree.h"

#include <ydb/core/tablet/tablet_counters.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/generic/ylimits.h>

namespace NKikimrSysView {
class TDbCounters;
}

namespace NKikimr {

/**
 * The public detailed metric values of one bucket (a TABLE bucket of N tablets
 * or a PARTITION leaf of one tablet), accumulated over the sources (tablets) of the bucket
 * from their low level counters, through a binding of the descriptor of the tablet type.
 *
 * A PARTITION leaf is exactly a TABLE bucket of one source, so both are computed
 * by the very same code. The values are the same, which the YDB metrics mapper used
 * to publish from the low level counters aggregated by NPrivate::TAggregatedTabletCounters:
 * - a gauge SUM(x) (MAX(x)) is the sum (the maximum) of the latest x of every live source,
 *   the metric value is the sum of its terms;
 * - a rate x adds the reported x (a delta since the previous report, as the tablets
 *   report their cumulative counters) of every report to the pending delta of the metric,
 *   Pack() drains it, Forget() keeps it;
 * - a level histogram HIST(x) gets one observation per live source: the latest x
 *   (a simple x), or the per second rate of the latest delta of x since the previous report
 *   of the source (a cumulative x), which is zero on the first report and on a report,
 *   whose time is not after the previous one; the observation falls into the source bucket
 *   of the percentile counter HIST(x), whose bound is not less than it, a source bucket
 *   beyond the last public one is counted in the last public bucket;
 * - a level histogram over an integral percentile counter p sums the latest buckets of p
 *   of every live source;
 * - an increment histogram over a derivative percentile counter p adds the reported
 *   buckets of p of every report to the pending buckets of the metric, Pack() drains them,
 *   Forget() keeps them.
 *
 * A follower bucket (skipLeaderOnly) ignores the LeaderOnly metrics: their gauges are zero,
 * their rates have no deltas and their histograms are empty.
 *
 * Apply() reads only the bound slots of the counters (about twenty loads for DataShard)
 * and allocates nothing for a source, which is already known. There are no monlib counters:
 * Pack() writes the values for the wire (see NKikimrSysView::TDbCounters).
 *
 * The state is one array of ui64, laid out by the binding (see TDetailedMetricsBinding):
 *
 *     [pending rates: R][pending increment buckets][N x per-source state]
 *
 * The sources are found by a linear scan, or by a hash index once there are more than
 * IndexThreshold sources. A forgotten source is replaced by the last one (swap-remove).
 *
 * @note The accumulator is movable, so it can live by value in a rehashing hash map.
 *       The binding (and its descriptor) must outlive it.
 */
class TDetailedValuesAccumulator {
public:
    using TTabletKey = NDetailedMetrics::TTabletKey;

    /**
     * The number of sources, up to which they are found by a linear scan.
     */
    static constexpr size_t IndexThreshold = 8;

    /**
     * Create an empty accumulator.
     *
     * @param[in] binding The binding of the descriptor of the tablet type to its counter layout,
     *            not null, it must outlive the accumulator
     * @param[in] skipLeaderOnly True for a follower bucket: the LeaderOnly metrics are ignored
     */
    TDetailedValuesAccumulator(const TDetailedMetricsBinding* binding, bool skipLeaderOnly);

    TDetailedValuesAccumulator(TDetailedValuesAccumulator&&) = default;
    TDetailedValuesAccumulator& operator=(TDetailedValuesAccumulator&&) = default;

    /**
     * Apply a report of one source: its current low level counters, the cumulative ones
     * being the deltas since its previous report. An unknown source is added.
     *
     * The time of the report replaces the time of the previous report of the source,
     * even if it is not after it (the same way as NPrivate::TAggregatedTabletCounters does it).
     *
     * @warning The counters must have the bound layout: Binding->Matches(executorCounters,
     *          appCounters) must be true. The accumulator never reads beyond the counter
     *          arrays (a missing slot reads as zero) and never writes beyond its state,
     *          but the values of the counters of another layout are meaningless.
     *
     * @param[in] tablet The source
     * @param[in] executorCounters The Executor counters of the source
     * @param[in] appCounters The application counters of the source
     * @param[in] now The time of the report
     */
    void Apply(
        const TTabletKey& tablet,
        const TTabletCountersBase& executorCounters,
        const TTabletCountersBase& appCounters,
        TInstant now);

    /**
     * Remove a source: its gauges and its level histogram observations are gone,
     * the pending rate deltas and increment buckets of the bucket are kept until
     * the next Pack(). An unknown source is ignored.
     *
     * @param[in] tablet The source to remove
     */
    void Forget(const TTabletKey& tablet);

    /**
     * @return True if the bucket has no sources (there still may be pending deltas)
     */
    bool IsEmpty() const {
        return Sources.empty();
    }

    /**
     * Write the public values of the bucket (the previous contents of the message are cleared):
     * - Simple: the value of every gauge, dense;
     * - Cumulative: sparse (rate, delta since the previous Pack()) pairs with non-zero deltas,
     *   CumulativeCount is the number of the rates;
     * - Histogram: one entry per histogram metric, BucketsCount is the public bucket count;
     *   a level histogram is marked NonDerivative and holds sparse (bucket, count) pairs
     *   of its full current value, the entry is present even if empty; an increment
     *   histogram is not marked and holds sparse (bucket, delta since the previous Pack()) pairs.
     *
     * @note Destructive: the pending rate deltas and increment buckets are drained,
     *       so the next Pack() reports only what comes after this one. After the last source
     *       is forgotten, Pack() reports the final deltas: zero gauges, empty level
     *       histograms, the pending deltas.
     *
     * @param[out] out The public values of the bucket
     */
    void Pack(NKikimrSysView::TDbCounters& out);

    /**
     * @return The binding, which every report applied to the accumulator must match
     */
    const TDetailedMetricsBinding* GetBinding() const {
        return Binding;
    }

    /**
     * @return The number of live sources
     */
    size_t GetSourceCount() const {
        return Sources.size();
    }

    /**
     * @return The number of ui64 slots of the state: the pending deltas plus
     *         the per-source state of every live source
     */
    size_t GetDataSize() const {
        return Data.size();
    }

    /**
     * @return The number of bytes held by the accumulator itself, its heap allocations included
     *         (the hash index is estimated)
     */
    size_t GetAllocatedBytes() const;

private:
    struct TSourceHeader {
        TTabletKey Key;

        /**
         * The time of the previous report of the source, valid if HasUpdate is set.
         */
        TInstant LastUpdate;
        bool HasUpdate = false;
    };

    static constexpr ui32 NoSource = Max<ui32>();

    ui32 FindSource(const TTabletKey& tablet) const;
    ui32 AddSource(const TTabletKey& tablet);

    /**
     * @return The offset of the per-source state of the first source in Data
     */
    size_t GetStateBegin() const;

    bool IsSkipped(const TBoundTerm& term) const {
        return SkipLeaderOnly && term.LeaderOnly;
    }

    void ApplyPercentile(const TBoundTerm& term, const TTabletPercentileCounter& percentile, ui64* state);

    const TDetailedMetricsBinding* Binding;
    TVector<TSourceHeader> Sources;
    THolder<THashMap<TTabletKey, ui32>> Index;
    TVector<ui64> Data;
    bool SkipLeaderOnly;
};

} // namespace NKikimr
