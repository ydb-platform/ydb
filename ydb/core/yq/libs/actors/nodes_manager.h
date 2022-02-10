#pragma once

#include <ydb/core/yq/libs/common/service_counters.h>
#include <ydb/core/yq/libs/events/events.h>
#include <ydb/core/yq/libs/shared_resources/shared_resources.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h> 
#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h> 
#include <ydb/library/yql/providers/dq/worker_manager/interface/counters.h> 
#include <ydb/library/yql/providers/dq/actors/proto_builder.h> 
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h> 

#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/time_provider/time_provider.h>
#include <library/cpp/random_provider/random_provider.h>

namespace NYq {

using namespace NActors;

TActorId MakeYqlNodesManagerId();
TActorId MakeYqlNodesManagerHttpId();

IActor* CreateYqlNodesManager(
    const NYql::NDqs::TWorkerManagerCounters& workerManagerCounters,
    TIntrusivePtr<ITimeProvider> timeProvider,
    TIntrusivePtr<IRandomProvider> randomProvider,
    const ::NYq::NCommon::TServiceCounters& serviceCounters,
    const NConfig::TPrivateApiConfig& privateApiConfig,
    const NYq::TYqSharedResources::TPtr& yqSharedResources,
    const ui32& icPort,
    const TString& address,
    const TString& tenant = "",
    ui64 mkqlInitialMemoryLimit = 0,
    const NMonitoring::TDynamicCounterPtr& clientCounters = MakeIntrusive<NMonitoring::TDynamicCounters>());

}
