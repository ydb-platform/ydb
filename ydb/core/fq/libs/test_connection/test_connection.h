#pragma once

#include "counters.h"

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/config/protos/test_connection.pb.h>
#include <ydb/core/fq/libs/db_id_async_resolver_impl/db_async_resolver_impl.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>
#include <ydb/core/fq/libs/signer/signer.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/pq/cm_client/client.h>
#include <ydb/public/api/protos/draft/fq.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>


#define TC_LOG_D(s) \
    LOG_YQ_TEST_CONNECTION_DEBUG(s)
#define TC_LOG_I(s) \
    LOG_YQ_TEST_CONNECTION_INFO(s)
#define TC_LOG_W(s) \
    LOG_YQ_TEST_CONNECTION_WARN(s)
#define TC_LOG_E(s) \
    LOG_YQ_TEST_CONNECTION_ERROR(s)
#define TC_LOG_T(s) \
    LOG_YQ_TEST_CONNECTION_TRACE(s)

#define TC_LOG_AS_D(a, s) \
    LOG_YQ_TEST_CONNECTION_AS_DEBUG(a, s)
#define TC_LOG_AS_I(a, s) \
    LOG_YQ_TEST_CONNECTION_AS_INFO(a, s)
#define TC_LOG_AS_W(a, s) \
    LOG_YQ_TEST_CONNECTION_AS_WARN(a, s)
#define TC_LOG_AS_E(a, s) \
    LOG_YQ_TEST_CONNECTION_AS_ERROR(a, s)
#define TC_LOG_AS_T(a, s) \
    LOG_YQ_TEST_CONNECTION_AS_TRACE(a, s)

namespace NFq {

NActors::TActorId TestConnectionActorId();

NActors::IActor* CreateTestConnectionActor(
        const NConfig::TTestConnectionConfig& config,
        const NConfig::TControlPlaneStorageConfig& controlPlaneStorageConfig,
        const NYql::TS3GatewayConfig& s3Config,
        const NConfig::TCommonConfig& commonConfig,
        const ::NFq::TSigner::TPtr& signer,
        const NFq::TYqSharedResources::TPtr& sharedResources,
        const NYql::ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory,
        const NPq::NConfigurationManager::IConnections::TPtr& cmConnections,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        const NYql::IHTTPGateway::TPtr& httpGateway,
        const ::NMonitoring::TDynamicCounterPtr& counters);

NActors::IActor* CreateTestDataStreamsConnectionActor(
        const FederatedQuery::DataStreams& ds,
        const NFq::NConfig::TCommonConfig& commonConfig,
        const std::shared_ptr<NYql::IDatabaseAsyncResolver>& dbResolver,
        const NActors::TActorId& sender,
        ui64 cookie,
        const NFq::TYqSharedResources::TPtr& sharedResources,
        const NYql::ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory,
        const ::NPq::NConfigurationManager::IConnections::TPtr& cmConnections,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        const TString& scope,
        const TString& user,
        const TString& token,
        const NFq::TSigner::TPtr& signer,
        const TTestConnectionRequestCountersPtr& counters);

NActors::IActor* CreateTestObjectStorageConnectionActor(
        const FederatedQuery::ObjectStorageConnection& os,
        const NFq::NConfig::TCommonConfig& commonConfig,
        const NActors::TActorId& sender,
        ui64 cookie,
        const NYql::ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory,
        NYql::IHTTPGateway::TPtr gateway,
        const TString& scope,
        const TString& user,
        const TString& token,
        const NFq::TSigner::TPtr& signer,
        const TTestConnectionRequestCountersPtr& counters);

NActors::IActor* CreateTestMonitoringConnectionActor(
        const FederatedQuery::Monitoring& monitoring,
        const NActors::TActorId& sender,
        ui64 cookie,
        const TString& endpoint,
        const NYql::ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory,
        const TString& scope,
        const TString& user,
        const TString& token,
        const NFq::TSigner::TPtr& signer,
        const TTestConnectionRequestCountersPtr& counters);

} // namespace NFq
