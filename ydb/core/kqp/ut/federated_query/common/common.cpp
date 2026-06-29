#include "common.h"

#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/library/yql/providers/pq/gateway/dummy/yql_pq_dummy_gateway_factory.h>
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

        const auto& status = op.Status();
        UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), NYdb::EStatus::SUCCESS, status.GetIssues().ToString());
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
    featureFlags.SetEnableSchemaSecrets(true);

    if (appConfig && appConfig->HasFeatureFlags()) {
        const auto& appFlags = appConfig->GetFeatureFlags();
        if (appFlags.GetEnableColumnshardBool()) {
            featureFlags.SetEnableColumnshardBool(true);
        }

        if (appFlags.GetEnableColumnStore()) {
            featureFlags.SetEnableColumnStore(true);
        }
    }

    if (!appConfig) {
        appConfig.emplace();
        appConfig->MutableQueryServiceConfig()->SetAllExternalDataSourcesAreAvailable(true);
    }

    appConfig->MutableQueryServiceConfig()->MutableS3()->SetAllowLocalFiles(true);

    auto settings = TKikimrSettings(*appConfig);

    NYql::IHTTPGateway::TPtr httpGateway;
    const auto& queryServiceConfig = appConfig->GetQueryServiceConfig();
    if (initializeHttpGateway) {
        httpGateway = MakeHttpGateway(queryServiceConfig.GetHttpGateway(), settings.CountersRoot);
    }

    NYdb::TDriverConfig cfg;
    cfg.SetDiscoveryMode(NYdb::EDiscoveryMode::Async);
    cfg.SetMaxQueuedRequests(std::numeric_limits<i64>::max());
    auto driver = std::make_shared<NYdb::TDriver>(cfg);

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
        nullptr,
        NYql::NDq::CreateReadActorFactoryConfig(s3Config),
        nullptr,
        NYql::TPqGatewayConfig{},
        options.PqGateway ? NYql::CreatePqFileGatewayFactory(options.PqGateway) : NKqp::MakePqGatewayFactory(driver, options.CredentialsFactory),
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
        .SetDynamicNodeCount(options.DynamicNodeCount)
        .SetEnableStorageProxy(true)
        .SetStoragePoolTypes(options.StoragePoolTypes)
        .SetCheckpointPeriod(options.CheckpointPeriod)
        .SetUseLocalCheckpointsInStreamingQueries(options.UseLocalCheckpointsInStreamingQueries)
        .SetLogSettings(std::move(logSettings))
        .SetNeedsStatsCollectors(options.NeedsStatsCollectors)
        .SetInitFederatedQuerySetupFactory(options.InternalInitFederatedQuerySetupFactory);

    settings.EnableScriptExecutionBackgroundChecks = options.EnableScriptExecutionBackgroundChecks;
    federatedQuerySetupFactory->SetScriptExecutionSettings({
        .EnableBackgroundLeaseChecks = options.EnableScriptExecutionBackgroundChecks,
        .LeaseCheckStartupTimeout = TDuration::Zero(),
    });

    return std::make_shared<TKikimrRunner>(settings);
}

class TStaticCredentialsProvider: public NYdb::ICredentialsProvider {
public:
    TStaticCredentialsProvider(const TString& token)
        : Token_(token)
    {
    }

    std::string GetAuthInfo() const override {
        return Token_;
    }

    bool IsValid() const override {
        return true;
    }

private:
    std::string Token_;
};

class TStaticCredentialsProviderFactory: public NYdb::ICredentialsProviderFactory {
public:
    explicit TStaticCredentialsProviderFactory(const TString& token)
        : Token_(token)
    {
    }

    std::shared_ptr<NYdb::ICredentialsProvider> CreateProvider() const override {
        return std::make_shared<TStaticCredentialsProvider>(Token_);
    }

private:
    const TString Token_;
};

class TStaticSecuredCredentialsFactory: public NYql::IStructuredTokenCredentialsFactory {
public:
    explicit TStaticSecuredCredentialsFactory(const TString& token)
        : Token_(token)
    {
    }

    std::shared_ptr<NYdb::ICredentialsProviderFactory> Create(const TString& structuredTokenJson, bool) override {
        if (NYql::IsStructuredTokenJson(structuredTokenJson)) {
            NYql::TStructuredTokenParser parser = NYql::CreateStructuredTokenParser(structuredTokenJson);
            if (parser.HasIamAuth()) {
                // the same validation as in KikimrIamAuthCredentialsProviderFactory
                if (!NKikimr::AppData()->FeatureFlags.GetEnableExternalDataSourceAuthMethodIam()) {
                    throw yexception() << "AUTH_METHOD=IAM is disabled. Please contact your system administrator to enable it";
                }
            }
        }

        return std::make_shared<TStaticCredentialsProviderFactory>(Token_);
    }

private:
    const TString Token_;
};

std::shared_ptr<NYql::IStructuredTokenCredentialsFactory> CreateCredentialsFactory(const TString& token) {
    return std::make_shared<TStaticSecuredCredentialsFactory>(token);
}

std::function<void(const std::string&)> AstChecker(ui64 txCount, ui64 stagesCount) {
    const auto stringCounter = [](const std::string& str, const std::string& subStr) {
        ui64 count = 0;
        for (size_t i = str.find(subStr); i != std::string::npos; i = str.find(subStr, i + subStr.size())) {
            ++count;
        }
        return count;
    };

    return [txCount, stagesCount, stringCounter](const std::string& ast) {
        UNIT_ASSERT_VALUES_EQUAL(stringCounter(ast, "KqpPhysicalTx"), txCount);
        UNIT_ASSERT_VALUES_EQUAL(stringCounter(ast, "DqPhyStage"), stagesCount);
    };
}

} // namespace NKikimr::NKqp::NFederatedQueryTest
