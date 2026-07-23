#pragma once

#include <ydb/core/resource_pools/resource_pool_settings.h>

#include <ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NWorkloadManager {

NActors::TActorId MakeServiceId(ui32 nodeId);

NMonitoring::TDynamicCounterPtr GetWorkloadManagerCounters(NMonitoring::TDynamicCounterPtr rootCounters);

NActors::IActor* CreateService(NMonitoring::TDynamicCounterPtr counters);

}  // namespace NKikimr::NWorkloadManager
