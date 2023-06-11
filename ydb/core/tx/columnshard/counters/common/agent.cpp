#include "agent.h"
#include "owner.h"

namespace NKikimr::NColumnShard {

TValueAggregationAgent::TValueAggregationAgent(const TString& signalName, const TCommonCountersOwner& signalsOwner)
    : ValueSignalSum(signalsOwner.GetValue("SUM/" + signalName))
    , ValueSignalMin(signalsOwner.GetValue("MIN/" + signalName))
    , ValueSignalMax(signalsOwner.GetValue("MAX/" + signalName))
{

}

bool TValueAggregationAgent::CalcAggregations(i64& sum, i64& minValue, i64& maxValue) const {
    if (Values.empty()) {
        return false;
    }
    sum = 0;
    minValue = Values.front()->GetValue();
    maxValue = Values.front()->GetValue();
    for (auto&& i : Values) {
        const i64 v = i->GetValue();
        sum += v;
        if (minValue > v) {
            minValue = v;
        }
        if (maxValue < v) {
            maxValue = v;
        }
    }
    return true;
}

std::optional<NKikimr::NColumnShard::TSignalAggregations> TValueAggregationAgent::GetAggregations() const {
    i64 sum;
    i64 min;
    i64 max;
    if (!CalcAggregations(sum, min, max)) {
        return {};
    }
    return TSignalAggregations(sum, min, max);
}

void TValueAggregationAgent::ResendStatus() const {
    TGuard<TMutex> g(Mutex);
    std::optional<TSignalAggregations> aggr = GetAggregations();
    if (!!aggr) {
        ValueSignalMin->Set(aggr->Min);
        ValueSignalMax->Set(aggr->Max);
        ValueSignalSum->Set(aggr->Sum);
    }
}

std::shared_ptr<NKikimr::NColumnShard::TValueAggregationClient> TValueAggregationAgent::GetClient(std::shared_ptr<TValueAggregationAgent> selfPtr) {
    TGuard<TMutex> g(Mutex);
    auto it = Values.emplace(Values.end(), nullptr);
    auto result = std::make_shared<TValueAggregationClient>(selfPtr, it);
    *it = result.get();
    return result;
}

void TValueAggregationAgent::UnregisterClient(std::list<TValueAggregationClient*>::iterator it) {
    TGuard<TMutex> g(Mutex);
    Values.erase(it);
}

}
