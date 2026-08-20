#include "aggregated_tablet_counters.h"

#include <ydb/core/protos/sys_view.pb.h>

namespace NKikimr::NPrivate {

TAggregatedTabletCounters::TAggregatedTabletCounters(
    ::NMonitoring::TDynamicCounterPtr counterGroup,
    ::NMonitoring::TCountableBase::EVisibility visibility)
    : IsInitialized(false)
    , AggregatedSimpleCounters(counterGroup, visibility)
    , AggregatedCumulativeCounters(counterGroup, visibility)
    , AggregatedHistogramCounters(counterGroup, visibility)
    , CounterGroup(counterGroup)
    , Visibility(visibility)
{}

void TAggregatedTabletCounters::Initialize(const TTabletCountersBase* counters, const THashSet<TString>* nameFilter) {
    Y_ABORT_UNLESS(!IsInitialized);

    const auto isPublished = [nameFilter](const char* name) {
        return name && (!nameFilter || nameFilter->empty() || nameFilter->contains(name));
    };

    if (counters) {
        THashMap<TString, THolder<THistogramCounter>> histogramAggregates;

        // percentile counters
        FullSizePercentile = counters->Percentile().Size();
        AggregatedHistogramCounters.Reserve(FullSizePercentile);
        for (ui32 i = 0; i < FullSizePercentile; ++i) {
            if (!isPublished(counters->PercentileCounterName(i))) {
                DeprecatedPercentile.insert(i);
                continue;
            }

            auto& percentileCounter = counters->Percentile()[i];
            const char* percentileCounterName = counters->PercentileCounterName(i);
            AggregatedHistogramCounters.AddCounter(
                percentileCounterName,
                percentileCounter,
                histogramAggregates);
        }

        // simple counters
        FullSizeSimple = counters->Simple().Size();
        AggregatedSimpleCounters.Reserve(FullSizeSimple);
        for (ui32 i = 0; i < FullSizeSimple; ++i) {
            const char* name = counters->SimpleCounterName(i);
            if (!isPublished(name)) {
                DeprecatedSimple.insert(i);
                continue;
            }
            auto itHistogramAggregate = histogramAggregates.find(name);
            if (itHistogramAggregate != histogramAggregates.end()) {
                AggregatedSimpleCounters.AddSimpleCounter(name, std::move(itHistogramAggregate->second));
            } else {
                AggregatedSimpleCounters.AddSimpleCounter(name);
            }
        }

        // cumulative counters
        FullSizeCumulative = counters->Cumulative().Size();
        AggregatedCumulativeCounters.Reserve(FullSizeCumulative);
        for (ui32 i = 0; i < FullSizeCumulative; ++i) {
            const char* name = counters->CumulativeCounterName(i);
            if (!isPublished(name)) {
                DeprecatedCumulative.insert(i);
                continue;
            }
            auto itHistogramAggregate = histogramAggregates.find(name);
            if (itHistogramAggregate != histogramAggregates.end()) {
                AggregatedCumulativeCounters.AddCumulativeCounter(name, std::move(itHistogramAggregate->second));
            } else {
                AggregatedCumulativeCounters.AddCumulativeCounter(name);
            }
            auto counter = CounterGroup->GetCounter(name, true, Visibility);
            CumulativeCounters.push_back(counter);
        }
    }

    //
    IsInitialized = true;
}

void TAggregatedTabletCounters::Apply(
    ui64 tabletId,
    const TTabletCountersBase* counters,
    TTabletTypes::EType tabletType,
    TInstant now)
{
    Y_ABORT_UNLESS(counters);

    auto it = LastAggregateUpdateTime.find(tabletId);
    TDuration diff;
    if (it != LastAggregateUpdateTime.end()) {
        diff = now - it->second;
        it->second = now;
    } else {
        LastAggregateUpdateTime.emplace(tabletId, now);
    }

    // simple counters
    ui32 nextSimpleOffset = 0;
    TVector<ui64> simpleValues;
    simpleValues.resize(FullSizeSimple); // more than needed
    for (ui32 i = 0; i < FullSizeSimple; ++i) {
        if (DeprecatedSimple.contains(i)) {
            continue;
        }
        const ui32 offset = nextSimpleOffset++;
        const ui64 value = counters->Simple()[i].Get();
        simpleValues[offset] = value;
    }
    AggregatedSimpleCounters.SetValues(tabletId, simpleValues, tabletType);

    // cumulative counters
    ui32 nextCumulativeOffset = 0;
    TVector<ui64> cumulativeValues;
    cumulativeValues.resize(FullSizeCumulative, 0);
    for (ui32 i = 0; i < FullSizeCumulative; ++i) {
        if (DeprecatedCumulative.contains(i)) {
            continue;
        }
        const ui32 offset = nextCumulativeOffset++;
        const ui64 valueDiff = counters->Cumulative()[i].Get();
        if (diff) {
            cumulativeValues[offset] = valueDiff * 1000000 / diff.MicroSeconds(); // differentiate value to per second rate
        }
        Y_ABORT_UNLESS(offset < CumulativeCounters.size(), "inconsistent counters for tablet type %s", TTabletTypes::TypeToStr(tabletType));
        *CumulativeCounters[offset] += valueDiff;
    }
    AggregatedCumulativeCounters.SetValues(tabletId, cumulativeValues, tabletType);

    // percentile counters
    ui32 nextPercentileOffset = 0;
    for (ui32 i = 0; i < FullSizePercentile; ++i) {
        if (DeprecatedPercentile.contains(i)) {
            continue;
        }

        const ui32 offset = nextPercentileOffset++;
        AggregatedHistogramCounters.SetValue(
            tabletId,
            offset,
            counters->Percentile()[i],
            counters->PercentileCounterName(i),
            tabletType);
    }
}

void TAggregatedTabletCounters::Forget(ui64 tabletId) {
    Y_ABORT_UNLESS(IsInitialized);

    AggregatedSimpleCounters.ForgetTablet(tabletId);
    AggregatedCumulativeCounters.ForgetTablet(tabletId);
    AggregatedHistogramCounters.ForgetTablet(tabletId);
    LastAggregateUpdateTime.erase(tabletId);
}

void TAggregatedTabletCounters::RecalcAll() {
    AggregatedSimpleCounters.RecalcAll();
    AggregatedCumulativeCounters.RecalcAll();
}

template <bool IsSaving>
void TAggregatedTabletCounters::Convert(
    NKikimrSysView::TDbCounters& sumCounters,
    NKikimrSysView::TDbCounters& maxCounters)
{
    // simple counters
    auto* simpleSum = sumCounters.MutableSimple();
    auto* simpleMax = maxCounters.MutableSimple();
    simpleSum->Resize(FullSizeSimple, 0);
    simpleMax->Resize(FullSizeSimple, 0);
    ui32 nextSimpleOffset = 0;
    for (ui32 i = 0; i < FullSizeSimple; ++i) {
        if (DeprecatedSimple.find(i) != DeprecatedSimple.end()) {
            if constexpr (IsSaving) {
                (*simpleSum)[i] = 0;
                (*simpleMax)[i] = 0;
            }
            continue;
        }
        const ui32 offset = nextSimpleOffset++;
        if constexpr (IsSaving) {
            (*simpleSum)[i] = AggregatedSimpleCounters.GetSum(offset);
            (*simpleMax)[i] = AggregatedSimpleCounters.GetMax(offset);
        } else {
            AggregatedSimpleCounters.SetSum(offset, (*simpleSum)[i]);
            AggregatedSimpleCounters.SetMax(offset, (*simpleMax)[i]);
        }
    }
    // cumulative counters
    auto* cumulativeSum = sumCounters.MutableCumulative();
    auto* cumulativeMax = maxCounters.MutableCumulative();
    cumulativeSum->Resize(FullSizeCumulative, 0);
    cumulativeMax->Resize(FullSizeCumulative, 0);
    ui32 nextCumulativeOffset = 0;
    for (ui32 i = 0; i < FullSizeCumulative; ++i) {
        if (DeprecatedCumulative.find(i) != DeprecatedCumulative.end()) {
            if constexpr (IsSaving) {
                (*cumulativeSum)[i] = 0;
                (*cumulativeMax)[i] = 0;
            }
            continue;
        }
        const ui32 offset = nextCumulativeOffset++;
        Y_ABORT_UNLESS(offset < CumulativeCounters.size(),
            "inconsistent cumulative counters %u >= %lu", offset, CumulativeCounters.size());
        if constexpr (IsSaving) {
            (*cumulativeSum)[i] = *CumulativeCounters[offset];
            (*cumulativeMax)[i] = AggregatedCumulativeCounters.GetMax(offset);
        } else {
            *CumulativeCounters[offset] = (*cumulativeSum)[i];
            AggregatedCumulativeCounters.SetMax(offset, (*cumulativeMax)[i]);
        }
    }
    // percentile counters
    auto* histogramSum = sumCounters.MutableHistogram();
    if (sumCounters.HistogramSize() < FullSizePercentile) {
        auto missing = FullSizePercentile - sumCounters.HistogramSize();
        for (; missing > 0; --missing) {
            sumCounters.AddHistogram();
        }
    }
    ui32 nextPercentileOffset = 0;
    for (ui32 i = 0; i < FullSizePercentile; ++i) {
        if (DeprecatedPercentile.find(i) != DeprecatedPercentile.end()) {
            continue;
        }
        auto* buckets = (*histogramSum)[i].MutableBuckets();
        const ui32 offset = nextPercentileOffset++;
        auto histogram = AggregatedHistogramCounters.GetHistogram(offset);
        auto snapshot = histogram->Snapshot();
        auto count = snapshot->Count();
        buckets->Resize(count, 0);
        if constexpr (!IsSaving) {
            histogram->Reset();
        }
        for (ui32 r = 0; r < count; ++r) {
            if constexpr (IsSaving) {
                (*buckets)[r] = snapshot->Value(r);
            } else {
                histogram->Collect(snapshot->UpperBound(r), (*buckets)[r]);
            }
        }
    }
}

void TAggregatedTabletCounters::ToProto(
    NKikimrSysView::TDbCounters& sumCounters,
    NKikimrSysView::TDbCounters& maxCounters)
{
    Convert<true>(sumCounters, maxCounters);
}

void TAggregatedTabletCounters::FromProto(
    NKikimrSysView::TDbCounters& sumCounters,
    NKikimrSysView::TDbCounters& maxCounters)
{
    Convert<false>(sumCounters, maxCounters);
}

bool TAggregatedTabletCounters::Find(const TString& name, TVector<TTabletCounterValue>& results) const {
    if (!IsInitialized) {
        return false;
    }

    return AggregatedSimpleCounters.Find(name, results)
        || AggregatedCumulativeCounters.Find(name, results);
}

} // namespace NKikimr::NPrivate
