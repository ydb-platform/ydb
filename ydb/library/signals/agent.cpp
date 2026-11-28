#include "agent.h"
#include "owner.h"

namespace NKikimr::NColumnShard {

TValueAggregationAgent::TValueAggregationAgent(const TString& signalName, const TCommonCountersOwner& signalsOwner)
    : ValueSignalSum(signalsOwner.GetLazyAggregationValue("SUM/" + signalName))
    , ValueSignalMin(signalsOwner.GetLazyAggregationValue("MIN/" + signalName))
    , ValueSignalMax(signalsOwner.GetLazyAggregationValue("MAX/" + signalName))
{

}

bool TValueAggregationAgent::HasWrittenClientAndClean() const {
    bool written = false;
    for (auto it = Values.begin(); it != Values.end();) {
        written |= (*it)->IsWritten();
        if (it->use_count() == 1) {
            it = Values.erase(it);
        } else {
            ++it;
        }
    }
    return written;
}

bool TValueAggregationAgent::CalcAggregationsAndClean(i64& sum, i64& minValue, i64& maxValue) const {
    if (Values.empty()) {
        return false;
    }
    sum = 0;
    const TInstant now = TInstant::Now();
    std::optional<i64> minValueLocal;
    std::optional<i64> maxValueLocal;
    for (auto it = Values.begin(); it != Values.end();) {
        if (it->use_count() == 1) {
            it = Values.erase(it);
        } else {
            const std::optional<i64> v = (*it)->GetValue(now);
            if (!v) {
                ++it;
                continue;
            }
            sum += *v;
            if (!minValueLocal || *minValueLocal > *v) {
                minValueLocal = *v;
            }
            if (!maxValueLocal || *maxValueLocal < *v) {
                maxValueLocal = *v;
            }
            ++it;
        }
    }
    if (!maxValueLocal) {
        return false;
    }
    minValue = *minValueLocal;
    maxValue = *maxValueLocal;
    return true;
}

std::optional<NKikimr::NColumnShard::TSignalAggregations> TValueAggregationAgent::GetAggregations() const {
    i64 sum;
    i64 min;
    i64 max;
    if (!CalcAggregationsAndClean(sum, min, max)) {
        return {};
    }
    return TSignalAggregations(sum, min, max);
}

void TValueAggregationAgent::ResendStatus() const {
    TGuard<TMutex> g(Mutex);
    // A client written and released within one period still counts: Written is read before the drop
    if (!Exported) {
        if (!HasWrittenClientAndClean()) {
            return;
        }
        Exported = true;
    }
    std::optional<TSignalAggregations> aggr = GetAggregations();
    if (!!aggr) {
        ValueSignalMin->Set(aggr->Min);
        ValueSignalMax->Set(aggr->Max);
        ValueSignalSum->Set(aggr->Sum);
    } else {
        ValueSignalMin->Set(0);
        ValueSignalMax->Set(0);
        ValueSignalSum->Set(0);
    }
}

std::shared_ptr<NKikimr::NColumnShard::TValueAggregationClient> TValueAggregationAgent::GetClient() {
    TGuard<TMutex> g(Mutex);
    return *Values.emplace(Values.end(), std::make_shared<TValueAggregationClient>());
}

}
