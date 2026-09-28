#pragma once

#include "aggregated_counters.h"

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NKikimrSysView {
class TDbCounters;
}

namespace NKikimr::NPrivate {

/**
 * The aggregate of a single category (the Executor counters or the application counters)
 * of the low level counters of one or more tablets of the same type.
 *
 * @note The class knows nothing about where in the counter tree it lives: it just fills
 *       the counter group it is given. Both the "tablets" counter group and
 *       the detailed metrics counter tree are built out of these aggregates.
 *
 * @note The aggregated counter names are derived from the low level counter names:
 *
 *           * a simple counter x     -> SUM(x) and MAX(x)
 *           * a cumulative counter x -> x (accumulated) and MAX(x) (a per second rate)
 *           * a percentile counter x -> x (a histogram)
 *
 *       A percentile counter named HIST(x) is a histogram aggregate: the tablet does NOT
 *       fill it, instead one observation per tablet is collected here from the simple
 *       or the cumulative counter named x.
 */
class TAggregatedTabletCounters {
public:
    /**
     * Whether the counter set reported by the tablets is already known.
     */
    bool IsInitialized;

    explicit TAggregatedTabletCounters(
        ::NMonitoring::TDynamicCounterPtr counterGroup,
        ::NMonitoring::TCountableBase::EVisibility visibility
            = ::NMonitoring::TCountableBase::EVisibility::Public);

    /**
     * Create the aggregated counters for the counter set reported by the tablets.
     *
     * @note The layout of the counter set is a property of the tablet type,
     *       so it is defined once and for all by the very first reporting tablet.
     */
    void Initialize(const TTabletCountersBase* counters, const THashSet<TString>* nameFilter = nullptr);

    /**
     * Add the counters of a single tablet to the aggregate.
     *
     * @note The cumulative counters are expected to be the delta since the previous call
     *       for this tablet (this is what the Executor sends).
     *
     * @warning The counters are NOT recalculated by this function.
     *          RecalcAll() must be called explicitly afterwards.
     *
     * @param[in] now Used to differentiate the cumulative counters into per second rates
     */
    void Apply(
        ui64 tabletId,
        const TTabletCountersBase* counters,
        TTabletTypes::EType tabletType,
        TInstant now);

    /**
     * Drop the contribution of a single tablet from the aggregate.
     */
    void Forget(ui64 tabletId);

    void RecalcAll();

    void ToProto(NKikimrSysView::TDbCounters& sumCounters, NKikimrSysView::TDbCounters& maxCounters);
    void FromProto(NKikimrSysView::TDbCounters& sumCounters, NKikimrSysView::TDbCounters& maxCounters);

    bool Find(const TString& name, TVector<TTabletCounterValue>& results) const;

private:
    template <bool IsSaving>
    void Convert(NKikimrSysView::TDbCounters& sumCounters, NKikimrSysView::TDbCounters& maxCounters);

private:
    ui32 FullSizeSimple = 0;
    THashSet<ui32> DeprecatedSimple;
    ui32 FullSizeCumulative = 0;
    THashSet<ui32> DeprecatedCumulative;
    ui32 FullSizePercentile = 0;
    THashSet<ui32> DeprecatedPercentile;
    //
    TAggregatedSimpleCounters AggregatedSimpleCounters;
    TCountersVector CumulativeCounters;
    TAggregatedCumulativeCounters AggregatedCumulativeCounters;
    TAggregatedHistogramCounters AggregatedHistogramCounters;

    THashMap<ui64, TInstant> LastAggregateUpdateTime;

    ::NMonitoring::TDynamicCounterPtr CounterGroup;
    ::NMonitoring::TCountableBase::EVisibility Visibility;
};

} // namespace NKikimr::NPrivate
