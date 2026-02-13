#pragma once

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/pq/cm_client/client.h>
#include <ydb/library/yql/providers/pq/gateway/clients/local/yql_pq_local_topic_client_factory.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <yql/essentials/providers/common/metrics/metrics_registry.h>

namespace NKikimr::NMiniKQL {

class IFunctionRegistry;

} // namespace NKikimr::NMiniKQL

namespace NYql {

class TPqGatewayConfig;
using TPqGatewayConfigPtr = std::shared_ptr<TPqGatewayConfig>;

struct TPqGatewayServices {
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry = nullptr;
    TPqGatewayConfigPtr Config;
    IMetricsRegistryPtr Metrics;
    ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    ::NPq::NConfigurationManager::IConnections::TPtr CmConnections;
    NYdb::TDriver YdbDriver;
    TMaybe<NYdb::NTopic::TTopicClientSettings> CommonTopicClientSettings;
    IPqLocalClientFactory::TPtr LocalTopicClientFactory;

    TPqGatewayServices(
        NYdb::TDriver driver,
        ::NPq::NConfigurationManager::IConnections::TPtr cmConnections,
        ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        TPqGatewayConfigPtr config,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        IMetricsRegistryPtr metrics = nullptr,
        TMaybe<NYdb::NTopic::TTopicClientSettings> commonTopicClientSettings = Nothing(),
        IPqLocalClientFactory::TPtr localTopicClientFactory = nullptr);
};

} // namespace NYql
