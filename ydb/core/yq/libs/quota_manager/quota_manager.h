#pragma once

#include <ydb/core/yq/libs/quota_manager/events/events.h>

#include <ydb/core/yq/libs/config/protos/quotas_manager.pb.h>
#include <ydb/core/yq/libs/shared_resources/shared_resources.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NYq {

NActors::TActorId MakeQuotaServiceActorId();

NActors::IActor* CreateQuotaServiceActor(
    const NConfig::TQuotasManagerConfig& config,
    const NYq::TYqSharedResources::TPtr& yqSharedResources,
    const NMonitoring::TDynamicCounterPtr& counters,
    std::vector<TQuotaDescription> quotaDesc);

} /* NYq */
