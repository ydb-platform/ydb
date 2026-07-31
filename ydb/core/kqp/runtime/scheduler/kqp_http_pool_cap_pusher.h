#pragma once

#include "kqp_compute_scheduler_service.h"

#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NKqp::NScheduler {

NActors::IActor* CreateHttpPoolCapPusher(
    TComputeSchedulerPtr scheduler,
    NYql::IHTTPGateway::TWeakPtr gateway,
    TDuration period,
    size_t maxHandlers,
    double minDefaultFraction);

// Registers puller if both scheduler (via AppData) and gateway are available. No-op otherwise.
void RegisterHttpPoolCapPusherIfNeeded(
    NActors::TActorSystem* actorSystem,
    NYql::IHTTPGateway::TPtr gateway);

} // namespace NKikimr::NKqp::NScheduler
