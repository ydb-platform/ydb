#pragma once

#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h> 

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NYql {
namespace NDq {

NActors::IActor* MakeDqExecuter(
    const NActors::TActorId& gwmActorId,
    const NActors::TActorId& printerId,
    const TString& traceId, const TString& username,
    const TDqConfiguration::TPtr& settings,
    const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters,
    TInstant requestStartTime = TInstant::Now(),
    bool createTaskSuspended = false
);

} // namespace NDq
} // namespace NYql
