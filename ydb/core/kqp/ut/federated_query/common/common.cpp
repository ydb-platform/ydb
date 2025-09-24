#include "common.h"

#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/tls_backend.h>

#include <library/cpp/testing/unittest/registar.h>

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
        const auto timeout = TInstant::Now() + TDuration::Seconds(10);
        std::shared_ptr<NKikimr::NKqp::NRm::IKqpResourceManager> resourceManager;
        while (true) {
            if (!resourceManager) {
                resourceManager = NKikimr::NKqp::TryGetKqpResourceManager(nodeId);
            }

            if (resourceManager && resourceManager->GetClusterResources().size() == expectedNodeCount) {
                return;
            }

            if (TInstant::Now() > timeout) {
                UNIT_FAIL("Timeout waiting for resources to publish on node [" << nodeId << "], expectedNodeCount: " << expectedNodeCount << " has resourceManager: " << (resourceManager ? "true" : "false") << ", node count: " << (resourceManager ? resourceManager->GetClusterResources().size() : 0));
            }

            Sleep(TDuration::MilliSeconds(100));
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
        // Logger should be initialized before http gateway creation
        NYql::NLog::InitLogger(new NYql::NLog::TTlsLogBackend(NActors::CreateNullBackend()));

        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableExternalDataSources(true);
        featureFlags.SetEnableScriptExecutionOperations(true);
        featureFlags.SetEnableExternalSourceSchemaInference(true);
        featureFlags.SetEnableMoveColumnTable(true);
        if (!appConfig) {
            appConfig.emplace();
            appConfig->MutableQueryServiceConfig()->SetAllExternalDataSourcesAreAvailable(true);
        }

        auto settings = TKikimrSettings(*appConfig);

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
            options.PqGateway ? options.PqGateway : NKqp::MakePqGateway(driver, NYql::TPqGatewayConfig{}),
            nullptr,
            driver);

        settings
            .SetFeatureFlags(featureFlags)
            .SetFederatedQuerySetupFactory(federatedQuerySetupFactory)
            .SetKqpSettings({})
            .SetS3ActorsFactory(std::move(s3ActorsFactory))
            .SetWithSampleTables(false)
            .SetDomainRoot(options.DomainRoot)
            .SetNodeCount(options.NodeCount)
            .SetEnableStorageProxy(true)
            .SetCheckpointPeriod(options.CheckpointPeriod);

        settings.EnableScriptExecutionBackgroundChecks = options.EnableScriptExecutionBackgroundChecks;

        auto kikimr = std::make_shared<TKikimrRunner>(settings);

        if (GetTestParam("DEFAULT_LOG", "enabled") == "enabled") {
            auto& runtime = *kikimr->GetTestServer().GetRuntime();

            const auto descriptor = NKikimrServices::EServiceKikimr_descriptor();
            for (i64 i = 0; i < descriptor->value_count(); ++i) {
                runtime.SetLogPriority(static_cast<NKikimrServices::EServiceKikimr>(descriptor->value(i)->number()), NLog::PRI_NOTICE);
            }

            runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_INFO);
            runtime.SetLogPriority(NKikimrServices::KQP_PROXY, NLog::PRI_DEBUG);
            runtime.SetLogPriority(NKikimrServices::KQP_COMPUTE, NLog::PRI_INFO);
        }

        return kikimr;
    }

} // namespace NKikimr::NKqp::NFederatedQueryTest
