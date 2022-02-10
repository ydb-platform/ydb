#pragma once

#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>

#include <library/cpp/actors/core/actor.h>

#include <ydb/core/yq/libs/common/service_counters.h>

namespace NYql {

THolder<NActors::IActor> MakeTaskController(
    const TString& traceId,
    const NActors::TActorId& executerId,
    const NActors::TActorId& resultId,
    const TDqConfiguration::TPtr& settings,
    const ::NYq::NCommon::TServiceCounters& serviceCounters,
    const TDuration& pingPeriod = TDuration::Zero(),
    const TDuration& aggrPeriod = TDuration::Seconds(1));

} // namespace NYql
