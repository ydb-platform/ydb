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
 * Public detailed metric values of one bucket, accumulated over its sources (tablets) through the binding
 * of the tablet type (see ESourceOp). They match what the YDB metrics mapper publishes from
 * NPrivate::TAggregatedTabletCounters.
 *
 * Apply() reads only the bound slots and allocates nothing for a known source. Movable, so it can
 * live by value in a rehashing hash map.
 */
class TDetailedValuesAccumulator {
public:
    using TTabletKey = NDetailedMetrics::TTabletKey;

    /**
     * @param[in] binding Not null, must outlive the accumulator
     * @param[in] skipLeaderOnly True for a follower bucket: LeaderOnly metrics are ignored
     *            (zero gauges, no rate deltas, empty histograms)
     */
    TDetailedValuesAccumulator(const TDetailedMetricsBinding* binding, bool skipLeaderOnly);

    TDetailedValuesAccumulator(TDetailedValuesAccumulator&&) = default;
    TDetailedValuesAccumulator& operator=(TDetailedValuesAccumulator&&) = default;

    /**
     * Apply a report of a source, adding an unknown one. Cumulative counters are the deltas since its
     * previous report. The report time replaces the previous one even if earlier.
     *
     * @warning The counters must have the bound layout. A missing slot reads as zero.
     */
    void Apply(
        const TTabletKey& tablet,
        const TTabletCountersBase& executorCounters,
        const TTabletCountersBase& appCounters,
        TInstant now);

    /**
     * Remove a source (an unknown one is ignored). Its gauges and non-derivative histogram observations go,
     * the pending rate deltas and derivative buckets stay until the next Pack().
     */
    void Forget(const TTabletKey& tablet);

    bool IsEmpty() const {
        return Sources.empty();
    }

    // Clear the message and write the public values (see NKikimrSysView::TDbCounters), draining the pending deltas
    void Pack(NKikimrSysView::TDbCounters& out);

    size_t GetSourceCount() const {
        return Sources.size();
    }

    // The object itself plus its heap allocations
    size_t GetAllocatedBytes() const;

private:
    struct TSourceHeader {
        TTabletKey Key;
        TInstant LastUpdate;
        bool HasUpdate = false;
    };

    // The position of the source, or the one to insert it at
    size_t FindSource(const TTabletKey& tablet) const;

    size_t GetStateBegin() const;

    bool IsSkipped(const TBoundTerm& term) const {
        return SkipLeaderOnly && term.LeaderOnly;
    }

    void ApplyPercentile(const TBoundTerm& term, const TTabletPercentileCounter& percentile, ui64* state);

    const TDetailedMetricsBinding* Binding;
    TVector<TSourceHeader> Sources;
    // Laid out by TDetailedMetricsBinding
    TVector<ui64> Data;
    bool SkipLeaderOnly;
};

} // namespace NKikimr
