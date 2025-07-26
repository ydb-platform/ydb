#pragma once

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimrConfig {
    class TQueryServiceConfig;
}

namespace NKikimr::NKqp {

NActors::IActor* CreateScriptExecutionLeaseCheckActor(const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TIntrusivePtr<TKqpCounters> counters);

}  // namespace NKikimr::NKqp
