#pragma once
#include "agent.h"
#include "client.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/system/mutex.h>
#include <deque>

namespace NKikimr::NColumnShard {

class TCommonCountersOwner {
private:
    ::NMonitoring::TDynamicCounterPtr SubGroup;
    const TString ModuleId;
    TString NormalizeSignalName(const TString& name) const;
protected:
    std::shared_ptr<TValueAggregationAgent> GetValueAutoAggregations(const TString& name) const;
    std::shared_ptr<TValueAggregationClient> GetValueAutoAggregationsClient(const TString& name) const;
public:
    NMonitoring::TDynamicCounters::TCounterPtr GetValue(const TString& name) const;
    NMonitoring::TDynamicCounters::TCounterPtr GetDeriviative(const TString& name) const;
    NMonitoring::THistogramPtr GetHistogram(const TString& name, NMonitoring::IHistogramCollectorPtr&& hCollector) const;

    TCommonCountersOwner(const TString& module, TIntrusivePtr<::NMonitoring::TDynamicCounters> baseSignals = nullptr);
};

}
