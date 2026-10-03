#pragma once

#include "detailed_metrics_binding.h"
#include "detailed_metrics_tree.h"

#include <ydb/core/tablet/tablet_counters.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

namespace NKikimrSysView {
class TDbCounters;
}

namespace NKikimr {

/**
 * The public detailed metric values of one bucket (a TABLE bucket of N tablets or a PARTITION
 * leaf of one tablet), accumulated over its sources (tablets) through the binding of the tablet
 * type (see ESourceOp for how every kind of source counter is combined). The values are the ones
 * the YDB metrics mapper publishes from the counters aggregated by NPrivate::TAggregatedTabletCounters.
 *
 * Apply() reads only the bound slots and allocates nothing for a known source. The state is laid
 * out by the binding (see TDetailedMetricsBinding).
 *
 * @note Movable, so it can live by value in a rehashing hash map. The binding must outlive it.
 */
class TDetailedValuesAccumulator {
public:
    using TTabletKey = NDetailedMetrics::TTabletKey;

    /**
     * @param[in] skipLeaderOnly True for a follower bucket: the LeaderOnly metrics are ignored
     *            (zero gauges, no rate deltas, empty histograms)
     */
    TDetailedValuesAccumulator(const TDetailedMetricsBinding* binding, bool skipLeaderOnly);

    TDetailedValuesAccumulator(TDetailedValuesAccumulator&&) = default;
    TDetailedValuesAccumulator& operator=(TDetailedValuesAccumulator&&) = default;

    /**
     * Apply a report of a source (an unknown source is added): its current counters,
     * the cumulative ones being the deltas since its previous report.
     *
     * @warning The counters must have the bound layout. A missing slot reads as zero.
     */
    void Apply(
        const TTabletKey& tablet,
        const TTabletCountersBase& executorCounters,
        const TTabletCountersBase& appCounters,
        TInstant now);

    /**
     * Remove a source (an unknown one is ignored): its gauges and level histogram observations
     * are gone, the pending rate deltas and increment buckets stay until the next Pack().
     */
    void Forget(const TTabletKey& tablet);

    /**
     * @return True if there are no sources (there still may be pending deltas)
     */
    bool IsEmpty() const {
        return Sources.empty();
    }

    /**
     * Write the public values (see NKikimrSysView::TDbCounters), the message is cleared first.
     *
     * @note Destructive: the pending deltas are drained.
     */
    void Pack(NKikimrSysView::TDbCounters& out);

    size_t GetSourceCount() const {
        return Sources.size();
    }

    /**
     * @return The bytes held by the accumulator, its heap allocations included
     */
    size_t GetAllocatedBytes() const;

private:
    struct TSourceHeader {
        TTabletKey Key;
        TInstant LastUpdate;
        bool HasUpdate = false;
    };

    // The position of the source, or the one to insert it at
    ui32 FindSource(const TTabletKey& tablet) const;

    size_t GetStateBegin() const;

    bool IsSkipped(const TBoundTerm& term) const {
        return SkipLeaderOnly && term.LeaderOnly;
    }

    void ApplyPercentile(const TBoundTerm& term, const TTabletPercentileCounter& percentile, ui64* state);

    const TDetailedMetricsBinding* Binding;
    TVector<TSourceHeader> Sources;
    TVector<ui64> Data;
    bool SkipLeaderOnly;
};

} // namespace NKikimr
