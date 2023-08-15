#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/library/persqueue/topic_parser/type_definitions.h>

namespace NKikimr {
namespace NPQ {

class TMultiCounter {
public:
    TMultiCounter() = default;

    TMultiCounter(::NMonitoring::TDynamicCounterPtr counters,
                  const TVector<NPersQueue::TPQLabelsInfo>& labels,
                  const TVector<std::pair<TString, TString>>& subgroups,
                  const TVector<TString>& counter_names,
                  bool deriv,
                  const TString& name = "sensor",
                  bool expiring = true);

    void Inc(ui64 val = 1);
    void Dec(ui64 val = 1);
    void Set(ui64 value);

    operator bool();

private:
    ui64 Value = 0;
    TVector<::NMonitoring::TDynamicCounters::TCounterPtr> Counters;
};

class TPercentileCounter {
public:
    TPercentileCounter() = default;
    TPercentileCounter(TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
                       const TVector<NPersQueue::TPQLabelsInfo>& labels,
                       const TVector<std::pair<TString, TString>>& subgroups,
                       const TString& sensor,
                       const TVector<std::pair<ui64, TString>>& intervals,
                       const bool deriv,
                       bool expiring = true);

    void IncFor(ui64 key, ui64 value = 1);
    void DecFor(ui64 key, ui64 value = 1);

    operator bool();

private:
    TVector<TMultiCounter> Counters;
    TVector<ui64> Ranges;
};

NKikimr::NPQ::TPercentileCounter CreateSLIDurationCounter(
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, TVector<NPersQueue::TPQLabelsInfo> aggr,
        const TString name, ui32 border, TVector<ui32> durations);

}// NPQ
}// NKikimr
