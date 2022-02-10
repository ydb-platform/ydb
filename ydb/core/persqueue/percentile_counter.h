#pragma once 
 
#include <library/cpp/monlib/dynamic_counters/counters.h>
 
namespace NKikimr { 
namespace NPQ { 
 
NMonitoring::TDynamicCounterPtr GetCounters(NMonitoring::TDynamicCounterPtr counters,
                                            const TString& subsystem,
                                            const TString& topic);
NMonitoring::TDynamicCounterPtr GetCountersForStream(NMonitoring::TDynamicCounterPtr counters,
                                                     const TString& subsystem);
 
struct TLabelsInfo { 
    TVector<std::pair<TString,TString>> Labels; 
    TVector<TString> AggrNames; 
}; 
 
TVector<TLabelsInfo> GetLabels(const TString& topic); 
TVector<TLabelsInfo> GetLabels(const TString& cluster, const TString& oldTopic); 
TVector<TLabelsInfo> GetLabelsForStream(const TString& topic, const TString& cloudId,
                                        const TString& dbId, const TString& folderId);
 
class TMultiCounter { 
public: 
    TMultiCounter() = default;
 
    TMultiCounter(NMonitoring::TDynamicCounterPtr counters,
                  const TVector<TLabelsInfo>& labels,
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
    TVector<NMonitoring::TDynamicCounters::TCounterPtr> Counters; 
}; 
 
class TPercentileCounter { 
public: 
    TPercentileCounter() = default;
    TPercentileCounter(TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
                       const TVector<TLabelsInfo>& labels,
                       const TVector<std::pair<TString, TString>>& subgroups,
                       const TString& sensor,
                       const TVector<std::pair<ui64, TString>>& intervals,
                       const bool deriv,
                       bool expiring = true);
 
    void IncFor(ui64 key, ui64 value = 1);
    void DecFor(ui64 key, ui64 value = 1);

private: 
    TVector<TMultiCounter> Counters; 
    TVector<ui64> Ranges; 
}; 
 
NKikimr::NPQ::TPercentileCounter CreateSLIDurationCounter(TIntrusivePtr<NMonitoring::TDynamicCounters> counters, TVector<NPQ::TLabelsInfo> aggr, const TString name, ui32 border, TVector<ui32> durations); 
 
}// NPQ 
}// NKikimr 
