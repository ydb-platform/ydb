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
    std::map<TString, TString> SignalKeys;
    TString NormalizeSignalName(const TString& name) const;
protected:
    std::shared_ptr<TValueAggregationAgent> GetValueAutoAggregations(const TString& name) const;
    std::shared_ptr<TValueAggregationClient> GetValueAutoAggregationsClient(const TString& name) const;

    TCommonCountersOwner(const TCommonCountersOwner& sameAs)
        : SubGroup(sameAs.SubGroup)
        , ModuleId(sameAs.ModuleId)
        , SignalKeys(sameAs.SignalKeys) {

    }

    TCommonCountersOwner(const TCommonCountersOwner& sameAs, const TString& componentName)
        : SubGroup(sameAs.SubGroup)
        , ModuleId(sameAs.ModuleId)
        , SignalKeys(sameAs.SignalKeys) {
        DeepSubGroup("component", componentName);
    }

    TCommonCountersOwner(const TCommonCountersOwner& sameAs, const TString& deepParamName, const TString& deepParamValue)
        : SubGroup(sameAs.SubGroup)
        , ModuleId(sameAs.ModuleId)
        , SignalKeys(sameAs.SignalKeys) {
        DeepSubGroup(deepParamName, deepParamValue);
    }
public:
    const TString& GetModuleId() const {
        return ModuleId;
    }

    TString GetAggregationPathInfo() const;

    NMonitoring::TDynamicCounters::TCounterPtr GetAggregationValue(const TString& name) const;
    NMonitoring::TDynamicCounters::TCounterPtr GetValue(const TString& name) const;
    NMonitoring::TDynamicCounters::TCounterPtr GetDeriviative(const TString& name) const;
    void DeepSubGroup(const TString& id, const TString& value);
    void DeepSubGroup(const TString& componentName);
    TCommonCountersOwner CreateSubGroup(const TString& param, const TString& value) const {
        return TCommonCountersOwner(*this, param, value);
    }
    NMonitoring::THistogramPtr GetHistogram(const TString& name, NMonitoring::IHistogramCollectorPtr&& hCollector) const;

    TCommonCountersOwner(const TString& module, TIntrusivePtr<::NMonitoring::TDynamicCounters> baseSignals = nullptr);

    TCommonCountersOwner(TCommonCountersOwner&& other)
        : TCommonCountersOwner(other) {
    }
};

class TValueGuard {
private:
    NMonitoring::TDynamicCounters::TCounterPtr Value;
    TAtomicCounter Counter;
public:
    TValueGuard(NMonitoring::TDynamicCounters::TCounterPtr value)
        : Value(value) {

    }

    ~TValueGuard() {
        Sub(Counter.Val());
    }

    void Add(const i64 value) {
        Counter.Add(value);
        Value->Add(value);
    }
    void Sub(const i64 value) {
        Counter.Sub(value);
        Value->Sub(value);
    }
};

}
