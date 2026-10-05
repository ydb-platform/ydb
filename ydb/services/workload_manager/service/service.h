#pragma once

#include <ydb/core/resource_pools/resource_pool_settings.h>

#include <ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <memory>

namespace NKikimr::NWorkloadManager {

namespace NPrivate {
    class TWorkloadManagerGateway;
}

NActors::TActorId MakeServiceId(ui32 nodeId);

NMonitoring::TDynamicCounterPtr GetWorkloadManagerCounters(NMonitoring::TDynamicCounterPtr rootCounters);

NActors::IActor* CreateService(
    NMonitoring::TDynamicCounterPtr counters,
    std::shared_ptr<NPrivate::TWorkloadManagerGateway> gateway);

}  // namespace NKikimr::NWorkloadManager
