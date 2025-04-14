#include "owner.h"
#include "private.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NColumnShard {

NMonitoring::TDynamicCounters::TCounterPtr TCommonCountersOwner::GetDeriviative(const TString& name) const {
    return SubGroup->GetCounter(NormalizeSignalName("Deriviative/" + name), true);
}

NMonitoring::TDynamicCounters::TCounterPtr TCommonCountersOwner::GetValue(const TString& name) const {
    return SubGroup->GetCounter(NormalizeSignalName("Value/" + name), false);
}

NMonitoring::TDynamicCounters::TCounterPtr TCommonCountersOwner::GetAggregationValue(const TString& name) const {
    return SubGroup->GetCounter(NormalizeSignalName("Aggregation/" + name), false);
}

TString TCommonCountersOwner::NormalizeSignalName(const TString& name) const {
    return TFsPath(name).Fix().GetPath();
}

TCommonCountersOwner::TCommonCountersOwner(const TString& module, TIntrusivePtr<::NMonitoring::TDynamicCounters> baseSignals)
    : ModuleId(module)
{
    if (baseSignals) {
        SubGroup = baseSignals->GetSubgroup("module_id", module);
    } else if (NActors::TlsActivationContext) {
        SubGroup = GetServiceCounters(AppData()->Counters, "tablets")->GetSubgroup("subsystem", "columnshard")->GetSubgroup("module_id", module);
    } else {
        SubGroup = new NMonitoring::TDynamicCounters();
    }
}

NMonitoring::THistogramPtr TCommonCountersOwner::GetHistogram(const TString& name, NMonitoring::IHistogramCollectorPtr&& hCollector) const {
    return SubGroup->GetHistogram(NormalizeSignalName("Histogram/" + name), std::move(hCollector));
}

std::shared_ptr<TValueAggregationAgent> TCommonCountersOwner::GetValueAutoAggregations(const TString& name) const {
    return NPrivate::TAggregationsController::GetAggregation(name, *this);
}

std::shared_ptr<TValueAggregationClient> TCommonCountersOwner::GetValueAutoAggregationsClient(const TString& name) const {
    std::shared_ptr<TValueAggregationAgent> agent = NPrivate::TAggregationsController::GetAggregation(name, *this);
    return agent->GetClient();
}

void TCommonCountersOwner::DeepSubGroup(const TString& id, const TString& value) {
    AFL_VERIFY(SignalKeys.emplace(id, value).second)(id, value);
    SubGroup = SubGroup->GetSubgroup(id, value);
}

void TCommonCountersOwner::DeepSubGroup(const TString& componentName) {
    AFL_VERIFY(SignalKeys.emplace("component", componentName).second)("component", componentName);
    SubGroup = SubGroup->GetSubgroup("component", componentName);
}

TString TCommonCountersOwner::GetAggregationPathInfo() const {
    TStringBuilder result;

    result << ModuleId << "/";
    for (auto&& i : SignalKeys) {
        result << i.first << "/" << i.second << "/";
    }
    return result;
}

}
