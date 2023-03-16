#pragma once

#include <ydb/core/fq/libs/quota_manager/events/events.h>
#include <ydb/core/fq/libs/config/protos/quotas_manager.pb.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NFq {

NActors::TActorId MakeQuotaProxyActorId();

NActors::IActor* CreateQuotaProxyActor(
    const NConfig::TQuotasManagerConfig& config,
    const ::NMonitoring::TDynamicCounterPtr& counters);

} /* NFq */
