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
    const ui32 count = Values.size();
    if (!count) {
        return false;
    }
    sum = 0;
    minValue = Values.front();
    maxValue = Values.front();
    for (ui32 i = 0; i < count; ++i) {
        sum += Values[i];
        if (minValue > Values[i]) {
            minValue = Values[i];
        }
        if (maxValue < Values[i]) {
            maxValue = Values[i];
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
    return std::make_shared<TValueAggregationClient>(selfPtr);
}

i64* TValueAggregationAgent::RegisterValue(const i64 zeroValue /*= 0*/) {
    TGuard<TMutex> g(Mutex);
    Values.emplace_back(zeroValue);
    return &Values.back();
}

}
