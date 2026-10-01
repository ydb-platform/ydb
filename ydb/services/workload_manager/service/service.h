#pragma once

#include <ydb/core/resource_pools/resource_pool_settings.h>

#include <ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NWorkloadManager {

inline NActors::TActorId MakeServiceId(ui32 nodeId) {
    const char name[12] = "kqp_workld";
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

NMonitoring::TDynamicCounterPtr GetWorkloadManagerCounters(NMonitoring::TDynamicCounterPtr rootCounters);

NActors::IActor* CreateService(NMonitoring::TDynamicCounterPtr counters);

}  // namespace NKikimr::NWorkloadManager
