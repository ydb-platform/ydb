#pragma once

#include <ydb/library/actors/core/actor.h>


namespace NKikimr::NKqp {

namespace NWorkload {

struct TWorkloadManagerConfig {
    struct TPoolConfig {
        ui64 ConcurrentQueryLimit = 0;  // 0 = infinity
        ui64 QueryCountLimit = 0;  // 0 = infinity
        TDuration QueryCancelAfter = TDuration::Days(1);

        TString ACL = "";  // empty = full access for all users
    };

    std::unordered_map<TString, TPoolConfig> Pools;
};

void SetWorkloadManagerConfig(const TWorkloadManagerConfig& workloadManagerConfig);

};

NActors::IActor* CreateKqpWorkloadService(NMonitoring::TDynamicCounterPtr counters);

}  // namespace NKikimr::NKqp
