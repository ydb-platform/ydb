#pragma once

#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/core/base/tablet_types.h>
// #include <ydb/core/protos/tablet_counters_aggregator.pb.h>

namespace NKikimrLabeledCounters {
class TTabletLabeledCounters;
}

namespace NKikimr {
struct TTabletLabeledCountersResponseContext;
}

namespace NKikimr::NPrivate {

////////////////////////////////////////////
using TCountersVector = TVector<::NMonitoring::TDynamicCounters::TCounterPtr>;

struct THistogramCounter {
    TVector<TTabletPercentileCounter::TRangeDef> Ranges;
    TCountersVector Values;
    NMonitoring::THistogramPtr Histogram;

    THistogramCounter(
        const TVector<TTabletPercentileCounter::TRangeDef>& ranges,
        TCountersVector&& values,
        NMonitoring::THistogramPtr histogram);

    void Clear();
    void IncrementFor(ui64 value);
};

using THistogramVector = TVector<THolder<THistogramCounter>>;

class TAggregatedSimpleCounters {
public:
    //
    TAggregatedSimpleCounters(::NMonitoring::TDynamicCounterPtr counterGroup);

    void Reserve(size_t hint);

    void AddSimpleCounter(const char* name, THolder<THistogramCounter> percentileAggregate = {});

    ui64 GetSum(ui32 counterIndex) const;
    void SetSum(ui32 counterIndex, ui64 value);

    ui64 GetMax(ui32 counterIndex) const;
    void SetMax(ui32 counterIndex, ui64 value);

    void SetValues(ui64 tabletId, const TVector<ui64>& values, NKikimrTabletBase::TTabletTypes::EType tabletType);
    void ForgetTablet(ui64 tabletId);
    void RecalcAll();

private:
    ::NMonitoring::TDynamicCounterPtr CounterGroup;

    TCountersVector MaxSimpleCounters;
    TCountersVector SumSimpleCounters;
    THistogramVector HistSimpleCounters;

    using TCountersByTabletIdMap = THashMap<ui64, TVector<ui64>>;
    TCountersByTabletIdMap CountersByTabletId;

    TVector<bool> ChangedCounters;
};

class TAggregatedCumulativeCounters {
public:
    //
    TAggregatedCumulativeCounters(::NMonitoring::TDynamicCounterPtr counterGroup);

    void Reserve(size_t hint);

    void AddCumulativeCounter(const char* name, THolder<THistogramCounter> percentileAggregate = {});

    ui64 GetMax(ui32 counterIndex) const;
    void SetMax(ui32 counterIndex, ui64 value);

    void SetValues(ui64 tabletId, const TVector<ui64>& values, NKikimrTabletBase::TTabletTypes::EType tabletType);
    void ForgetTablet(ui64 tabletId);
    void RecalcAll();

private:
    ::NMonitoring::TDynamicCounterPtr CounterGroup;

    TCountersVector MaxCumulativeCounters;
    THistogramVector HistCumulativeCounters;

    using TCountersByTabletIdMap = THashMap<ui64, TVector<ui64>>;
    TCountersByTabletIdMap CountersByTabletId;

    TVector<bool> ChangedCounters;
};

class TAggregatedHistogramCounters {
public:
    TAggregatedHistogramCounters(::NMonitoring::TDynamicCounterPtr counterGroup);

    void Reserve(size_t hint);

    void AddCounter(
        const char* name,
        const NKikimr::TTabletPercentileCounter& percentileCounter,
        THashMap<TString, THolder<THistogramCounter>>& histogramAggregates);

    void SetValue(
        ui64 tabletId,
        ui32 counterIndex,
        const NKikimr::TTabletPercentileCounter& percentileCounter,
        const char* name,
        NKikimrTabletBase::TTabletTypes::EType tabletType);

    void ForgetTablet(ui64 tabletId);

    NMonitoring::THistogramPtr GetHistogram(size_t i);

private:
    using TValuesVec = TVector<ui64>;

    void SubValues(size_t counterIndex, const TValuesVec& values);
    void AddValues(size_t counterIndex, const TValuesVec& values);
    void AddValues(size_t counterIndex, const NKikimr::TTabletPercentileCounter& percentileCounter);

private:
    ::NMonitoring::TDynamicCounterPtr CounterGroup;

    // monitoring counters holders, updated only during recalculation
    TVector<TCountersVector> PercentileCounters;    // old style (ranges);
    TVector<NMonitoring::THistogramPtr> Histograms; // new style (bins);
    TVector<bool> IsDerivative;

    // per percentile counter bounds.
    TVector<NMonitoring::TBucketBounds> BucketBounds;

    // tabletId -> values
    using TCountersByTabletIdMap = THashMap<ui64, TValuesVec>;

    // counter values (not "real" monitoring counters);
    TVector<TCountersByTabletIdMap> CountersByTabletId; // each index is map from tablet to counter value
};

class TAggregatedLabeledCounters : public TThrRefBase {
public:
    //
    TAggregatedLabeledCounters(ui32 count, const ui8* aggrFunc, const char * const * names, const ui8* types, const TString& groupNames);

    void SetValue(ui64 tabletID, ui32 counterIndex, ui64 value, ui64 id);
    bool ForgetTablet(ui64 tabletId);
    ui32 Size() const;
    ui64 GetValue(ui32 index) const;
    ui64 GetId(ui32 index) const;

    void FillGetRequestV1(NKikimrLabeledCounters::TTabletLabeledCounters* labeledCounters,
                          const TString& group, ui32 start, ui32 end) const;

    void FillGetRequestV2(NKikimr::TTabletLabeledCountersResponseContext* context, const TString& group) const;

    void ToProto(NKikimrLabeledCounters::TTabletLabeledCounters& labeledCounters) const;
    void FromProto(NMonitoring::TDynamicCounterPtr group,
                   const NKikimrLabeledCounters::TTabletLabeledCounters& labeledCounters) const;

private:
    //
    ::NMonitoring::TDynamicCounterPtr CounterGroup;
    const ui8* AggrFunc;
    const char* const * Names;
    TString GroupNames;
    const ui8* Types;

    mutable TVector<ui64> AggrCounters;
    mutable TVector<ui64> Ids;
    mutable bool Changed;

    using TCountersByTabletIdMap = THashMap<ui64, std::pair<ui64, ui64>>; //second pair is for counter and id
    TVector<TCountersByTabletIdMap> CountersByTabletId;

private:
    void Recalc(ui32 idx) const;
};

} // namespace NKikimr::NPrivate
