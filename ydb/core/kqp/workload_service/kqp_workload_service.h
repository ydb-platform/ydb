#pragma once

#include <ydb/core/resource_pools/resource_pool_settings.h>

#include <ydb/library/actors/core/actor.h>


namespace NKikimr::NKqp {

NActors::IActor* CreateKqpWorkloadService(NMonitoring::TDynamicCounterPtr counters);

}  // namespace NKikimr::NKqp
