#pragma once
#include <ydb/core/yq/libs/config/protos/pinger.pb.h>
#include "run_actor_params.h"
#include <util/datetime/base.h>

#include <ydb/core/mon/mon.h>

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
#include <ydb/library/yql/providers/pq/cm_client/client.h>

#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/time_provider/time_provider.h>
#include <library/cpp/random_provider/random_provider.h>

#include <ydb/library/yql/providers/common/metrics/service_counters.h>

namespace NKikimr  {
    namespace NMiniKQL {
        class IFunctionRegistry;
    }
}

namespace NYq {

NActors::TActorId MakeYqlAnalyticsHttpProxyId();
NActors::TActorId MakePendingFetcherId(ui32 nodeId);

NActors::IActor* CreatePendingFetcher(
    const NYq::TYqSharedResources::TPtr& yqSharedResources,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const ::NYq::NConfig::TCommonConfig& commonConfig,
    const ::NYq::NConfig::TCheckpointCoordinatorConfig& checkpointCoordinatorConfig,
    const ::NYq::NConfig::TPrivateApiConfig& privateApiConfig,
    const ::NYq::NConfig::TGatewaysConfig& gatewaysConfig,
    const ::NYq::NConfig::TPingerConfig& pingerConfig,
    const ::NYq::NConfig::TRateLimiterConfig& rateLimiterConfig,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    TIntrusivePtr<ITimeProvider> timeProvider,
    TIntrusivePtr<IRandomProvider> randomProvider,
    NKikimr::NMiniKQL::TComputationNodeFactory dqCompFactory,
    const ::NYql::NCommon::TServiceCounters& serviceCounters,
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    NYql::IHTTPGateway::TPtr s3Gateway,
    ::NPq::NConfigurationManager::IConnections::TPtr pqCmConnections,
    const ::NMonitoring::TDynamicCounterPtr& clientCounters,
    const TString& tenantName,
    NActors::TMon* monitoring
    );

NActors::IActor* CreateRunActor(
    const NActors::TActorId& fetcherId,
    const ::NYql::NCommon::TServiceCounters& serviceCounters,
    TRunActorParams&& params);

struct TResultId {
    TString Id;
    int SetId;
    TString HistoryId;
    TString Owner;
    TString CloudId;
};

NActors::IActor* CreateResultWriter(
    const NActors::TActorId& executerId,
    const TString& resultType,
    const TResultId& resultId,
    const TVector<TString>& columns,
    const TString& traceId,
    const TInstant& deadline,
    ui64 resultBytesLimit);

NActors::IActor* CreatePingerActor(
    const TString& tenantName,
    const TScope& scope,
    const TString& userId,
    const TString& id,
    const TString& owner,
    const NActors::TActorId parent,
    const NConfig::TPingerConfig& config,
    TInstant deadline,
    const ::NYql::NCommon::TServiceCounters& queryCounters,
    TInstant createdAt);

NActors::IActor* CreateRateLimiterResourceCreator(
    const NActors::TActorId& parent,
    const TString& ownerId,
    const TString& queryId,
    const TScope& scope,
    const TString& tenant);

NActors::IActor* CreateRateLimiterResourceDeleter(
    const NActors::TActorId& parent,
    const TString& ownerId,
    const TString& queryId,
    const TScope& scope,
    const TString& tenant);

TString MakeInternalError(const TString& text);

} /* NYq */
