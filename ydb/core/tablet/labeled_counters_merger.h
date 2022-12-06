#pragma once

#include "tablet_counters_aggregator.h"


namespace NKikimr {

class TMerger {
public:
    TMerger(THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse>& response,
            TTabletLabeledCountersResponseContext& record);

    static void MergeOne(const NKikimrLabeledCounters::TTabletLabeledCounter& source,
                         NKikimrLabeledCounters::TTabletLabeledCounter& target) {
        ui64 value(source.GetValue());
        TLabeledCounterOptions::ECounterType type(source.GetType());
        NKikimr::TLabeledCounterOptions::EAggregateFunc func(source.GetAggregateFunc());
        if (type == TLabeledCounterOptions::CT_TIMELAG) {
            type = TLabeledCounterOptions::CT_SIMPLE;
            auto now = TInstant::Now().MilliSeconds();
            value =  now > value ? now - value : 0;
            switch (func) {
            case NKikimr::TLabeledCounterOptions::EAF_MIN:
                func = NKikimr::TLabeledCounterOptions::EAF_MAX;
                break;
            case NKikimr::TLabeledCounterOptions::EAF_MAX:
                func = NKikimr::TLabeledCounterOptions::EAF_MIN;
                break;
            default:
                break;
            }
        }
        if (target.HasValue()) {
            switch (func) {
            case NKikimr::TLabeledCounterOptions::EAF_MIN:
                target.SetValue(std::min(target.GetValue(), value));
                break;
            case NKikimr::TLabeledCounterOptions::EAF_MAX:
                target.SetValue(std::max(target.GetValue(), value));
                break;
            case NKikimr::TLabeledCounterOptions::EAF_SUM:
                target.SetValue(target.GetValue() + value);
                break;
            }
        } else {
            target.SetValue(value);
            target.SetType(type);
            target.SetAggregateFunc(func);
            target.SetNameId(source.GetNameId());
        }
    }

    void Merge(const NKikimrLabeledCounters::TEvTabletLabeledCountersResponse& source);

    NKikimrLabeledCounters::TTabletLabeledCounters* GetCounters(const TString& group);
    NKikimrLabeledCounters::TTabletLabeledCounter* GetCounter(NKikimrLabeledCounters::TTabletLabeledCounters* counters, ui32 nameId);

private:
    THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse>& Response;
    TTabletLabeledCountersResponseContext& ResponseContext;
    THashMap<TString, NKikimrLabeledCounters::TTabletLabeledCounters*> IndexTabletLabeledCounters;
    THashMap<std::pair<NKikimrLabeledCounters::TTabletLabeledCounters*, ui32>, NKikimrLabeledCounters::TTabletLabeledCounter*> IndexTabletLabeledCounter;
};

} // namespace NKikimr
