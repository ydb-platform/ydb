#pragma once

#include <ydb/library/actors/core/actor.h>

#include "service_node_resolver.h"
#include "coordination_helper.h"

namespace NYql {

struct TWorkerRuntimeData;
struct TResourceManagerOptions;

NActors::IActor* CreateServiceNodePinger(
    ui32 nodeId,
    const TString& address,
    ui16 port,
    const TString& role,
    const THashMap<TString, TString>& attributes,
    const IServiceNodeResolver::TPtr& ptr,
    const ICoordinationHelper::TPtr& coordinator,
    const TResourceManagerOptions& rmOptions);

} // namespace NYql
