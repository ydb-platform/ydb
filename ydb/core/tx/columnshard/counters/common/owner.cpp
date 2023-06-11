#include "owner.h"
#include "private.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>

namespace NKikimr::NColumnShard {

NMonitoring::TDynamicCounters::TCounterPtr TCommonCountersOwner::GetDeriviative(const TString& name) const {
    return SubGroup->GetCounter(NormalizeSignalName(ModuleId + "/Deriviative/" + name), true);
}

NMonitoring::TDynamicCounters::TCounterPtr TCommonCountersOwner::GetValue(const TString& name) const {
    return SubGroup->GetCounter(NormalizeSignalName(ModuleId + "/Value/" + name), false);
}

TString TCommonCountersOwner::NormalizeSignalName(const TString& name) const {
    return TFsPath(name).Fix().GetPath();
}

TCommonCountersOwner::TCommonCountersOwner(const TString& module)
    : ModuleId(module)
{
    if (NActors::TlsActivationContext) {
        SubGroup = GetServiceCounters(AppData()->Counters, "tablets")->GetSubgroup("subsystem", "columnshard");
    } else {
        SubGroup = new NMonitoring::TDynamicCounters();
    }
}

NMonitoring::THistogramPtr TCommonCountersOwner::GetHistogram(const TString& name, NMonitoring::IHistogramCollectorPtr&& hCollector) const {
    return SubGroup->GetHistogram(NormalizeSignalName(ModuleId + "/Histogram/" + name), std::move(hCollector));
}

std::shared_ptr<TValueAggregationAgent> TCommonCountersOwner::GetValueAutoAggregations(const TString& name) const {
    return NPrivate::TAggregationsController::GetAggregation(name, *this);
}

std::shared_ptr<TValueAggregationClient> TCommonCountersOwner::GetValueAutoAggregationsClient(const TString& name) const {
    std::shared_ptr<TValueAggregationAgent> agent = NPrivate::TAggregationsController::GetAggregation(name, *this);
    return agent->GetClient(agent);
}

}
