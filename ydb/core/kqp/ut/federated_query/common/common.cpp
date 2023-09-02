#include "common.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp::NFederatedQueryTest {

    NYdb::NQuery::TScriptExecutionOperation WaitScriptExecutionOperation(const NYdb::TOperation::TOperationId& operationId, const NYdb::TDriver& ydbDriver) {
        NYdb::NOperation::TOperationClient client(ydbDriver);
        NThreading::TFuture<NYdb::NQuery::TScriptExecutionOperation> op;
        do {
            if (op.Initialized()) {
                Sleep(TDuration::MilliSeconds(10));
            }
            op = client.Get<NYdb::NQuery::TScriptExecutionOperation>(operationId);
            UNIT_ASSERT_C(op.GetValueSync().Status().IsSuccess(), TStringBuilder() << op.GetValueSync().Status().GetStatus() << ":" << op.GetValueSync().Status().GetIssues().ToString());
        } while (!op.GetValueSync().Ready());
        return op.GetValueSync();
    }

    std::shared_ptr<TKikimrRunner> MakeKikimrRunner(
        NYql::IHTTPGateway::TPtr httpGateway,
        NYql::NConnector::IClient::TPtr connectorClient,
        std::optional<NKikimrConfig::TAppConfig> appConfig)
    {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableExternalDataSources(true);
        featureFlags.SetEnableScriptExecutionOperations(true);

        auto federatedQuerySetupFactory = std::make_shared<TKqpFederatedQuerySetupFactoryMock>(
            httpGateway, connectorClient, nullptr, nullptr, appConfig ? appConfig->GetQueryServiceConfig().GetS3() : NYql::TS3GatewayConfig()
        );

        auto settings = TKikimrSettings()
                            .SetFeatureFlags(featureFlags)
                            .SetFederatedQuerySetupFactory(federatedQuerySetupFactory)
                            .SetKqpSettings({})
                            .SetEnableScriptExecutionOperations(true);
            
        settings = appConfig ? settings.SetAppConfig(appConfig.value()) : settings.SetAppConfig({});

        return std::make_shared<TKikimrRunner>(settings);
    }

}
