#pragma once
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include "owner.h"
#include "agent.h"

namespace NKikimr::NColumnShard::NPrivate {
class TAggregationsController {
public:
    static std::shared_ptr<TValueAggregationAgent> GetAggregation(const TString& signalName, const TCommonCountersOwner& signalsOwner);
};

}
