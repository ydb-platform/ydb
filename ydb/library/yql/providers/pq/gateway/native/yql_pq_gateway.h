#pragma once

#include <ydb/library/yql/providers/common/metrics/metrics_registry.h>
#include <ydb/library/yql/providers/pq/cm_client/client.h>
#include <ydb/library/yql/providers/pq/provider/yql_pq_gateway.h>

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <util/generic/ptr.h>

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

    TPqGatewayServices(
        NYdb::TDriver driver,
        ::NPq::NConfigurationManager::IConnections::TPtr cmConnections,
        ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        TPqGatewayConfigPtr config,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        IMetricsRegistryPtr metrics = nullptr)
        : FunctionRegistry(functionRegistry)
        , Config(std::move(config))
        , Metrics(std::move(metrics))
        , CredentialsFactory(std::move(credentialsFactory))
        , CmConnections(std::move(cmConnections))
        , YdbDriver(std::move(driver))
    {
    }
};

IPqGateway::TPtr CreatePqNativeGateway(const TPqGatewayServices& services);

} // namespace NYql
