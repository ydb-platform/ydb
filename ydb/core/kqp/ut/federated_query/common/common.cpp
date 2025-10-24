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

    NYdb::NQuery::TScriptExecutionOperation WaitScriptExecutionOperation(const NYdb::TOperation::TOperationId& operationId, const NYdb::TDriver& ydbDriver, const TString& userSID) {
        NYdb::TCommonClientSettings settings;

        if (userSID) {
            settings.AuthToken(userSID);
        }

        NYdb::NOperation::TOperationClient client(ydbDriver, settings);
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
        const auto& queryServiceConfig = appConfig->GetQueryServiceConfig();
        if (initializeHttpGateway) {
            httpGateway = MakeHttpGateway(queryServiceConfig.GetHttpGateway(), settings.CountersRoot);
        }
        auto driver = std::make_shared<NYdb::TDriver>(NYdb::TDriverConfig());

        const auto& s3Config = queryServiceConfig.GetS3();
        const auto& solomonConfig = queryServiceConfig.GetSolomon();
        auto federatedQuerySetupFactory = std::make_shared<TKqpFederatedQuerySetupFactoryMock>(
            httpGateway,
            connectorClient,
            options.CredentialsFactory,
            databaseAsyncResolver,
            s3Config,
            queryServiceConfig.GetGeneric(),
            queryServiceConfig.GetYt(),
            nullptr,
            solomonConfig,
            NYql::CreateSolomonGateway(solomonConfig),
            nullptr,
            NYql::NDq::CreateReadActorFactoryConfig(s3Config),
            nullptr,
            NYql::TPqGatewayConfig{},
            options.PqGateway ? options.PqGateway : NKqp::MakePqGateway(driver),
            nullptr,
            driver);

        auto logSettings = options.LogSettings;
        logSettings.DefaultLogPriority = std::max(NLog::PRI_NOTICE, logSettings.DefaultLogPriority);
        logSettings
            .AddLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_INFO)
            .AddLogPriority(NKikimrServices::KQP_PROXY, NLog::PRI_DEBUG)
            .AddLogPriority(NKikimrServices::KQP_COMPUTE, NLog::PRI_INFO);

        const auto& kqpSettings = appConfig->GetKQPConfig().GetSettings();
        settings
            .SetFeatureFlags(featureFlags)
            .SetFederatedQuerySetupFactory(federatedQuerySetupFactory)
            .SetKqpSettings({kqpSettings.begin(), kqpSettings.end()})
            .SetS3ActorsFactory(std::move(s3ActorsFactory))
            .SetWithSampleTables(false)
            .SetDomainRoot(options.DomainRoot)
            .SetNodeCount(options.NodeCount)
            .SetEnableStorageProxy(true)
            .SetCheckpointPeriod(options.CheckpointPeriod)
            .SetUseLocalCheckpointsInStreamingQueries(options.UseLocalCheckpointsInStreamingQueries)
            .SetLogSettings(std::move(logSettings));

        settings.EnableScriptExecutionBackgroundChecks = options.EnableScriptExecutionBackgroundChecks;

        return std::make_shared<TKikimrRunner>(settings);
    }

    class TStaticCredentialsProvider: public NYdb::ICredentialsProvider {
    public:
        TStaticCredentialsProvider(const TString& yqlToken)
            : YqlToken_(yqlToken)
        {
        }

        std::string GetAuthInfo() const override {
            return YqlToken_;
        }

        bool IsValid() const override {
            return true;
        }

    private:
        std::string YqlToken_;
    };

    class TStaticCredentialsProviderFactory: public NYdb::ICredentialsProviderFactory {
    public:
        TStaticCredentialsProviderFactory(const TString& yqlToken)
            : YqlToken_(yqlToken)
        {
        }

        std::shared_ptr<NYdb::ICredentialsProvider> CreateProvider() const override {
            return std::make_shared<TStaticCredentialsProvider>(YqlToken_);
        }

    private:
        TString YqlToken_;
    };

    class TStaticSecuredCredentialsFactory: public NYql::ISecuredServiceAccountCredentialsFactory {
    public:
        TStaticSecuredCredentialsFactory(const TString& yqlToken)
            : YqlToken_(yqlToken)
        {
        }

        std::shared_ptr<NYdb::ICredentialsProviderFactory> Create(const TString&, const TString&) override {
            return std::make_shared<TStaticCredentialsProviderFactory>(YqlToken_);
        }

    private:
        TString YqlToken_;
    };

    std::shared_ptr<NYql::ISecuredServiceAccountCredentialsFactory> CreateCredentialsFactory(const TString& token) {
        return std::make_shared<TStaticSecuredCredentialsFactory>(token);
    }

} // namespace NKikimr::NKqp::NFederatedQueryTest
