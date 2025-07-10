#pragma once

#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>

#include <ydb/library/actors/core/actor.h>

#include <yql/essentials/providers/common/metrics/service_counters.h>

namespace NYql {

THolder<NActors::IActor> MakeTaskController(
    const TString& traceId,
    const NActors::TActorId& executerId,
    const NActors::TActorId& resultId,
    const NActors::TActorId& checkpointCoordinatorId,
    const TDqConfiguration::TPtr& settings,
    const ::NYql::NCommon::TServiceCounters& serviceCounters,
    const TDuration& pingPeriod = TDuration::Zero(),
    const TDuration& aggrPeriod = TDuration::Seconds(1));

} // namespace NYql
