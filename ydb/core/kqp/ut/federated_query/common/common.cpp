#include "common.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>

namespace NKikimr::NKqp::NFederatedQueryTest {
    TString GetSymbolsString(char start, char end, const TString& skip) {
        TStringBuilder result;
        for (char symbol = start; symbol <= end; ++symbol) {
            if (skip.Contains(symbol)) {
                continue;
            }
            result << symbol;
        }
        return result;
    }

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

    void WaitResourcesPublish(ui32 nodeId, ui32 expectedNodeCount) {
        std::shared_ptr<NKikimr::NKqp::NRm::IKqpResourceManager> resourceManager;
        while (true) {
            if (!resourceManager) {
                resourceManager = NKikimr::NKqp::TryGetKqpResourceManager(nodeId);
            }
            if (resourceManager && resourceManager->GetClusterResources().size() == expectedNodeCount) {
                return;
            }
            Sleep(TDuration::MilliSeconds(10));
        }
    }

    void WaitResourcesPublish(const TKikimrRunner& kikimrRunner) {
        const auto& testServer = kikimrRunner.GetTestServer();
        const auto nodeCount = testServer.StaticNodes();
        for (ui32 nodeId = 0; nodeId < nodeCount; ++nodeId) {
            WaitResourcesPublish(testServer.GetRuntime()->GetNodeId(nodeId), nodeCount);
        }
    }

    std::shared_ptr<TKikimrRunner> MakeKikimrRunner(
        bool initializeHttpGateway,
        NYql::NConnector::IClient::TPtr connectorClient,
        NYql::IDatabaseAsyncResolver::TPtr databaseAsyncResolver,
        std::optional<NKikimrConfig::TAppConfig> appConfig,
        std::shared_ptr<NYql::NDq::IS3ActorsFactory> s3ActorsFactory,
        const TKikimrRunnerOptions& options)
    {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableExternalDataSources(true);
        featureFlags.SetEnableScriptExecutionOperations(true);
        featureFlags.SetEnableExternalSourceSchemaInference(true);
        featureFlags.SetEnableMoveColumnTable(true);
        if (!appConfig) {
            appConfig.emplace();
            appConfig->MutableQueryServiceConfig()->SetAllExternalDataSourcesAreAvailable(true);
        }

        auto settings = TKikimrSettings();

        NYql::IHTTPGateway::TPtr httpGateway;
        if (initializeHttpGateway) {
            httpGateway = MakeHttpGateway(appConfig->GetQueryServiceConfig().GetHttpGateway(), settings.CountersRoot);
        }
        auto driver = std::make_shared<NYdb::TDriver>(NYdb::TDriverConfig());

        auto federatedQuerySetupFactory = std::make_shared<TKqpFederatedQuerySetupFactoryMock>(
            httpGateway,
            connectorClient,
            options.CredentialsFactory,
            databaseAsyncResolver,
            appConfig->GetQueryServiceConfig().GetS3(),
            appConfig->GetQueryServiceConfig().GetGeneric(),
            appConfig->GetQueryServiceConfig().GetYt(),
            nullptr,
            appConfig->GetQueryServiceConfig().GetSolomon(),
            nullptr,
            nullptr,
            NYql::NDq::CreateReadActorFactoryConfig(appConfig->GetQueryServiceConfig().GetS3()),
            nullptr,
            NYql::TPqGatewayConfig{},
            NKqp::MakePqGateway(driver, NYql::TPqGatewayConfig{}),
            nullptr,
            driver);

        settings
            .SetFeatureFlags(featureFlags)
            .SetFederatedQuerySetupFactory(federatedQuerySetupFactory)
            .SetKqpSettings({})
            .SetS3ActorsFactory(std::move(s3ActorsFactory))
            .SetWithSampleTables(false)
            .SetDomainRoot(options.DomainRoot)
            .SetNodeCount(options.NodeCount);

        settings = settings.SetAppConfig(appConfig.value());

        return std::make_shared<TKikimrRunner>(settings);
    }

} // namespace NKikimr::NKqp::NFederatedQueryTest
