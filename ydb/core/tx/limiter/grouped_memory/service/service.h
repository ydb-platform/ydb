#pragma once
#include "actor.h"

#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

template <class TMemoryLimiterPolicy>
NActors::IActor* CreateService(const TConfig& config, TIntrusivePtr<::NMonitoring::TDynamicCounters> signals) {
    using TOperator = TServiceOperatorImpl<TMemoryLimiterPolicy>;
    TOperator::Register(config);
    return new TMemoryLimiterActor(
        config, TOperator::GetMemoryLimiterName(), signals, TOperator::GetConsumerKind(), TOperator::GetHardLimitMultiplier());
}

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
