#pragma once

#include "dq_solomon_actors_util.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/yql/providers/solomon/common/util.h>

namespace NYql::NSo {
class ISolomonAccessorClient;
}

namespace NYql::NDq {

NActors::IActor* CreateSolomonMetricsQueueActor(
    ui64 consumersCount,
    TDqSolomonReadParams readParams,
    std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider,
    const NSo::TSolomonReadActorConfig& cfg,
    std::shared_ptr<NSo::ISolomonAccessorClient> solomonClient = nullptr);

} // namespace NYql::NDq
