#include "common.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp::NFederatedQueryTest {

    NYdb::NQuery::TScriptExecutionOperation WaitScriptExecutionOperation(const NYdb::TOperation::TOperationId& operationId, const NYdb::TDriver& ydbDriver) {
        NYdb::NOperation::TOperationClient client(ydbDriver);
        while (1) {
            auto op = client.Get<NYdb::NQuery::TScriptExecutionOperation>(operationId).GetValueSync();
            if (op.Ready()) {
                return op;
            }
            UNIT_ASSERT_C(op.Status().IsSuccess(), TStringBuilder() << op.Status().GetStatus() << ":" << op.Status().GetIssues().ToString());
            Sleep(TDuration::MilliSeconds(10));
        }
    }

    std::shared_ptr<TKikimrRunner> MakeKikimrRunner(
        NYql::IHTTPGateway::TPtr httpGateway,
        NYql::NConnector::IClient::TPtr connectorClient,
        NYql::IDatabaseAsyncResolver::TPtr databaseAsyncResolver,
        std::optional<NKikimrConfig::TAppConfig> appConfig)
    {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableExternalDataSources(true);
        featureFlags.SetEnableScriptExecutionOperations(true);
        if (!appConfig) {
            appConfig.emplace();
        }
        appConfig->MutableTableServiceConfig()->SetEnablePreparedDdl(true);

        auto federatedQuerySetupFactory = std::make_shared<TKqpFederatedQuerySetupFactoryMock>(
            httpGateway,
            connectorClient,
            nullptr,
            databaseAsyncResolver,
            appConfig->GetQueryServiceConfig().GetS3(),
            appConfig->GetQueryServiceConfig().GetGeneric(),
            appConfig->GetQueryServiceConfig().GetYt(),
            nullptr,
            nullptr);

        auto settings = TKikimrSettings()
                            .SetFeatureFlags(featureFlags)
                            .SetFederatedQuerySetupFactory(federatedQuerySetupFactory)
                            .SetKqpSettings({});

        settings = settings.SetAppConfig(appConfig.value());

        return std::make_shared<TKikimrRunner>(settings);
    }

}
