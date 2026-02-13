#include "yql_pq_gateway_services.h"

namespace NYql {

TPqGatewayServices::TPqGatewayServices(
    NYdb::TDriver driver,
    ::NPq::NConfigurationManager::IConnections::TPtr cmConnections,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    TPqGatewayConfigPtr config,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    IMetricsRegistryPtr metrics,
    TMaybe<NYdb::NTopic::TTopicClientSettings> commonTopicClientSettings,
    IPqLocalClientFactory::TPtr localTopicClientFactory)
    : FunctionRegistry(functionRegistry)
    , Config(std::move(config))
    , Metrics(std::move(metrics))
    , CredentialsFactory(std::move(credentialsFactory))
    , CmConnections(std::move(cmConnections))
    , YdbDriver(std::move(driver))
    , CommonTopicClientSettings(std::move(commonTopicClientSettings))
    , LocalTopicClientFactory(std::move(localTopicClientFactory))
{}

} // namespace NYql
