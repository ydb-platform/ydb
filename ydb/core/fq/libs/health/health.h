#pragma once

#include <ydb/core/fq/libs/config/protos/health_config.pb.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NFq {

NActors::TActorId HealthActorId();

NActors::IActor* CreateHealthActor(
        const NConfig::THealthConfig& config,
        const NFq::TYqSharedResources::TPtr& yqSharedResources,
        const ::NMonitoring::TDynamicCounterPtr& counters);

} // namespace NFq
