#pragma once
#include <ydb/library/signals/owner.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NPrioritiesQueue {

class TCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;

public:
    const ::NMonitoring::TDynamicCounters::TCounterPtr UsedCount;
    const ::NMonitoring::TDynamicCounters::TCounterPtr Using;
    const ::NMonitoring::TDynamicCounters::TCounterPtr Ask;
    const ::NMonitoring::TDynamicCounters::TCounterPtr AskMax;
    const ::NMonitoring::TDynamicCounters::TCounterPtr Free;
    const ::NMonitoring::TDynamicCounters::TCounterPtr FreeNoClient;
    const ::NMonitoring::TDynamicCounters::TCounterPtr Register;
    const ::NMonitoring::TDynamicCounters::TCounterPtr Unregister;
    const ::NMonitoring::TDynamicCounters::TCounterPtr QueueSize;
    const ::NMonitoring::TDynamicCounters::TCounterPtr Clients;
    const ::NMonitoring::TDynamicCounters::TCounterPtr Limit;

    TCounters(const TString& queueName, TIntrusivePtr<::NMonitoring::TDynamicCounters> baseSignals);
};

}   // namespace NKikimr::NPrioritiesQueue
