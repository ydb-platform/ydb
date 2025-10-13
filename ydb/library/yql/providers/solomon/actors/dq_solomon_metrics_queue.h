#pragma once

#include "dq_solomon_actors_util.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb-cpp-sdk/client/types/credentials/credentials.h>

namespace NYql::NDq {

NActors::IActor* CreateSolomonMetricsQueueActor(
    ui64 consumersCount,
    TDqSolomonReadParams readParams,
    std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider);

} // namespace NYql::NDq
