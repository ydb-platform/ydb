#pragma once

#include <ydb/core/fq/libs/config/protos/audit.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/system/types.h>


namespace NFq {

NActors::IActor* CreateYqCloudAuditServiceActor(const NConfig::TAuditConfig& config, const NMonitoring::TDynamicCounterPtr& counters);

} // namespace NFq
