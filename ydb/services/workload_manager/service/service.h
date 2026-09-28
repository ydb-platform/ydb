#pragma once

#include <ydb/services/workload_manager/gateway_internal.h>

#include <ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NWorkloadManager {

NActors::TActorId MakeServiceId(ui32 nodeId);

NMonitoring::TDynamicCounterPtr GetWorkloadManagerCounters(NMonitoring::TDynamicCounterPtr rootCounters);

NActors::IActor* CreateService(
    NMonitoring::TDynamicCounterPtr counters,
    std::shared_ptr<NPrivate::TWorkloadManagerGateway> gateway);

}  // namespace NKikimr::NWorkloadManager
