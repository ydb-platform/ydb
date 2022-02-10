#pragma once
#include <ydb/core/yq/libs/config/protos/pinger.pb.h>
#include "run_actor_params.h" 
#include <util/datetime/base.h>

#include <ydb/core/yq/libs/events/events.h>
#include <ydb/core/yq/libs/private_client/private_client.h>
#include <ydb/core/yq/libs/shared_resources/db_pool.h>
#include <ydb/core/yq/libs/shared_resources/shared_resources.h>
#include <ydb/core/yq/libs/signer/signer.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <ydb/library/yql/providers/dq/worker_manager/interface/counters.h>
#include <ydb/library/yql/providers/dq/actors/proto_builder.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/providers/pq/cm_client/interface/client.h>

#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/time_provider/time_provider.h>
#include <library/cpp/random_provider/random_provider.h>

#include <ydb/core/yq/libs/common/service_counters.h>

namespace NKikimr  {
    namespace NMiniKQL {
        class IFunctionRegistry;
    }
}

namespace NYq { 

NActors::TActorId MakeYqlAnalyticsHttpProxyId();
NActors::TActorId MakeYqlAnalyticsFetcherId(ui32 nodeId);

NActors::IActor* CreatePendingFetcher(
    const NYq::TYqSharedResources::TPtr& yqSharedResources,
    const ::NYq::NConfig::TCommonConfig& commonConfig,
    const ::NYq::NConfig::TCheckpointCoordinatorConfig& checkpointCoordinatorConfig,
    const ::NYq::NConfig::TPrivateApiConfig& privateApiConfig,
    const ::NYq::NConfig::TGatewaysConfig& gatewaysConfig,
    const ::NYq::NConfig::TPingerConfig& pingerConfig,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    TIntrusivePtr<ITimeProvider> timeProvider,
    TIntrusivePtr<IRandomProvider> randomProvider,
    NKikimr::NMiniKQL::TComputationNodeFactory dqCompFactory,
    const ::NYq::NCommon::TServiceCounters& serviceCounters,
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory, 
    NYql::IHTTPGateway::TPtr s3Gateway,
    ::NPq::NConfigurationManager::IConnections::TPtr pqCmConnections, 
    const NMonitoring::TDynamicCounterPtr& clientCounters 
    );

NActors::IActor* CreateRunActor(
    const ::NYq::NCommon::TServiceCounters& serviceCounters,
    TRunActorParams&& params
    );

struct TResultId {
    TString Id;
    int SetId;
    TString HistoryId; 
    TString Owner; 
    TString CloudId;
};

NActors::IActor* CreateResultWriter(
    const NYdb::TDriver& driver,
    const NActors::TActorId& executerId,
    const TString& resultType,
    const NConfig::TPrivateApiConfig& privateApiConfig,
    const TResultId& resultId,
    const TVector<TString>& columns,
    const TString& traceId,
    const TInstant& deadline, 
    const NMonitoring::TDynamicCounterPtr& clientCounters 
    ); 

NActors::IActor* CreatePingerActor( 
    const TScope& scope,
    const TString& userId,
    const TString& id,
    const TString& owner,
    const NYq::TPrivateClient& client, 
    const NActors::TActorId parent,
    const NConfig::TPingerConfig& config,
    const TInstant& deadline);

TString MakeInternalError(const TString& text);

} /* NYq */ 
