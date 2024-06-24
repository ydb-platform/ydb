#pragma once

#include <ydb/core/resource_pools/resource_pool_settings.h>
#include <ydb/library/actors/core/actor.h>


namespace NKikimr::NKqp {

namespace NWorkload {

struct TWorkloadManagerConfig {
    std::unordered_map<TString, NResourcePool::TPoolSettings> Pools;
};

void SetWorkloadManagerConfig(const TWorkloadManagerConfig& workloadManagerConfig);

};

NActors::IActor* CreateKqpWorkloadService(NMonitoring::TDynamicCounterPtr counters);

}  // namespace NKikimr::NKqp
