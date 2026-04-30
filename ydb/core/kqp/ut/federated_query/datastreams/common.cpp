#include "common.h"

#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/proxy_service/kqp_script_executions.h>
#include <ydb/core/kqp/ut/federated_query/generic_ut/iceberg_ut_data.h>
#include <ydb/core/kqp/ut/federated_query/s3/s3_recipe_ut_helpers.h>
#include <ydb/library/testlib/solomon_helpers/solomon_emulator_helpers.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>

#include <fmt/format.h>

#include <library/cpp/protobuf/interop/cast.h>


namespace NKikimr::NKqp {

using namespace fmt::literals;
using namespace NFederatedQueryTest;
using namespace NTestUtils;
using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NYql::NConnector::NApi;
using namespace NYql::NConnector::NTest;

TStreamingTestFixture::~TStreamingTestFixture () {
    if (InternalDriver) {
        InternalDriver->Stop(true);
    }
    if (ExternalDriver) {
        ExternalDriver->Stop(true);
    }
}

// Local kikimr settings

NKikimrConfig::TAppConfig& TStreamingTestFixture::SetupAppConfig() {
    UNIT_ASSERT_C(!AppConfig, "AppConfig is already initialized");
    EnsureNotInitialized("AppConfig");

    auto& result = AppConfig.emplace();
    result.MutableTableServiceConfig()->SetDqChannelVersion(1u);
    return result;
}

void TStreamingTestFixture::UpdateConfig(NKikimrConfig::TAppConfig& appConfig) {
    auto& runtime = GetRuntime();
    const auto edgeActor = runtime.AllocateEdgeActor();
    auto evProxy = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationRequest>();
    *evProxy->Record.MutableConfig() = appConfig;

    runtime.Send(MakeKqpProxyID(runtime.GetNodeId()), edgeActor, evProxy.release());
    auto response = runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(edgeActor, TEST_OPERATION_TIMEOUT);
    UNIT_ASSERT(response);

    auto evStorage = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationRequest>();
    *evStorage->Record.MutableConfig() = appConfig;

    runtime.Send(NYql::NDq::MakeCheckpointStorageID(), edgeActor, evStorage.release());
    response = runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(edgeActor, TEST_OPERATION_TIMEOUT);
    UNIT_ASSERT(response);
}

TIntrusivePtr<IMockPqGateway> TStreamingTestFixture::SetupMockPqGateway() {
    UNIT_ASSERT_C(!PqGateway, "PqGateway is already initialized");
    EnsureNotInitialized("MockPqGateway");

    const auto mockPqGateway = CreateMockPqGateway({.OperationTimeout = TEST_OPERATION_TIMEOUT});
    PqGateway = mockPqGateway;

    return mockPqGateway;
}

std::shared_ptr<TConnectorClientMock> TStreamingTestFixture::SetupMockConnectorClient() {
    UNIT_ASSERT_C(!ConnectorClient, "ConnectorClient is already initialized");
    EnsureNotInitialized("ConnectorClient");

    auto mockConnectorClient = std::make_shared<TConnectorClientMock>();
    ConnectorClient = mockConnectorClient;

    return mockConnectorClient;
}

// Local kikimr test cluster

std::shared_ptr<TKikimrRunner> TStreamingTestFixture::GetKikimrRunner() {
    if (!Kikimr) {
        if (!AppConfig) {
            AppConfig.emplace();
        }

        auto& featureFlags = *AppConfig->MutableFeatureFlags();
        featureFlags.SetEnableStreamingQueries(true);
        featureFlags.SetEnableSchemaSecrets(true);

        auto& queryServiceConfig = *AppConfig->MutableQueryServiceConfig();
        queryServiceConfig.SetEnableMatchRecognize(true);

        auto& tableServiceConfig = *AppConfig->MutableTableServiceConfig();
        tableServiceConfig.SetDqChannelVersion(1u);

        LogSettings
            .AddLogPriority(NKikimrServices::STREAMS_STORAGE_SERVICE, NLog::PRI_DEBUG)
            .AddLogPriority(NKikimrServices::STREAMS_CHECKPOINT_COORDINATOR, NLog::PRI_DEBUG)
            .AddLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG)
            .AddLogPriority(NKikimrServices::KQP_COMPUTE, NLog::PRI_INFO);

        Kikimr = MakeKikimrRunner(true, ConnectorClient, nullptr, AppConfig, NYql::NDq::CreateS3ActorsFactory(), {
            .NodeCount = NodeCount,
            .DynamicNodeCount = DynamicNodeCount,
            .CredentialsFactory = CreateCredentialsFactory(),
            .PqGateway = PqGateway,
            .CheckpointPeriod = CheckpointPeriod,
            .LogSettings = LogSettings,
            .UseLocalCheckpointsInStreamingQueries = true,
            .InternalInitFederatedQuerySetupFactory = InternalInitFederatedQuerySetupFactory,
            .StoragePoolTypes = StoragePoolTypes,
        });

        Kikimr->GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        Kikimr->GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableStreamingQueries(true);
    }

    return Kikimr;
}

TTestActorRuntime& TStreamingTestFixture::GetRuntime() {
    return *GetKikimrRunner()->GetTestServer().GetRuntime();
}

std::shared_ptr<TDriver> TStreamingTestFixture::GetInternalDriver() {
    if (!InternalDriver) {
        InternalDriver = std::make_shared<TDriver>(GetKikimrRunner()->GetDriver());
    }

    return InternalDriver;
}

std::shared_ptr<TQueryClient> TStreamingTestFixture::GetQueryClient() {
    if (!QueryClient) {
        QueryClient = std::make_shared<TQueryClient>(
            GetKikimrRunner()->GetQueryClient(QueryClientSettings)
        );
    }

    return QueryClient;
}

std::shared_ptr<NYdb::NTable::TTableClient> TStreamingTestFixture::GetTableClient() {
    if (!TableClient) {
        TableClient = std::make_shared<NYdb::NTable::TTableClient>(GetKikimrRunner()->GetTableClient());
    }

    return TableClient;
}

std::shared_ptr<NYdb::NOperation::TOperationClient> TStreamingTestFixture::GetOperationClient() {
    if (!OperationClient) {
        OperationClient = std::make_shared<NYdb::NOperation::TOperationClient>(*GetInternalDriver());
    }

    return OperationClient;
}

std::shared_ptr<NYdb::NTable::TSession> TStreamingTestFixture::GetTableClientSession() {
    if (!TableClientSession) {
        TableClientSession = std::make_shared<NYdb::NTable::TSession>(
            GetTableClient()->CreateSession().ExtractValueSync().GetSession()
        );
    }

    return TableClientSession;
}

std::shared_ptr<NKikimr::NPersQueueTests::TFlatMsgBusPQClient> TStreamingTestFixture::GetLocalFlatMsgBusPQClient() {
    if (!LocalFlatMsgBusPQClient) {
        const auto& server = GetKikimrRunner()->GetTestServer();
        LocalFlatMsgBusPQClient = std::make_shared<NKikimr::NPersQueueTests::TFlatMsgBusPQClient>(
            server.GetSettings(),
            server.GetGRpcServer().GetPort(),
            "/Root"
        );
    }

    return LocalFlatMsgBusPQClient;
}

void TStreamingTestFixture::KillTopicPqrbTablet(const std::string& topicPath) {
    const auto tabletClient = GetLocalFlatMsgBusPQClient();
    const auto& describeResult = tabletClient->Ls(JoinPath(TVector{TString{"/Root"}, TString{topicPath}}));
    UNIT_ASSERT_C(describeResult->Record.GetPathDescription().HasPersQueueGroup(), describeResult->Record);

    const auto& persQueueGroup = describeResult->Record.GetPathDescription().GetPersQueueGroup();
    tabletClient->KillTablet(GetKikimrRunner()->GetTestServer(), persQueueGroup.GetBalancerTabletID());
}

// External YDB recipe

std::shared_ptr<TDriver> TStreamingTestFixture::GetExternalDriver() {
    if (!ExternalDriver) {
        ExternalDriver = std::make_shared<TDriver>(TDriverConfig()
            .SetDiscoveryMode(NYdb::EDiscoveryMode::Async)
            .SetEndpoint(YDB_ENDPOINT)
            .SetDatabase(YDB_DATABASE));
    }

    return ExternalDriver;
}

std::shared_ptr<NYdb::NTopic::TTopicClient> TStreamingTestFixture::GetTopicClient(bool local) {
    if (local && !LocalTopicClient) {
        LocalTopicClient = std::make_shared<NYdb::NTopic::TTopicClient>(*GetInternalDriver(), TopicClientSettings);
    }

    if (!TopicClient) {
        TopicClient = std::make_shared<NYdb::NTopic::TTopicClient>(*GetExternalDriver(), NYdb::NTopic::TTopicClientSettings()
            .DiscoveryEndpoint(YDB_ENDPOINT)
            .Database(YDB_DATABASE));
    }

    return local ? LocalTopicClient : TopicClient;
}

std::shared_ptr<TQueryClient> TStreamingTestFixture::GetExternalQueryClient() {
    if (!ExternalQueryClient) {
        ExternalQueryClient = std::make_shared<TQueryClient>(*GetExternalDriver(), TClientSettings()
            .DiscoveryEndpoint(YDB_ENDPOINT)
            .Database(YDB_DATABASE));
    }

    return ExternalQueryClient;
}

std::vector<TResultSet> TStreamingTestFixture::ExecExternalQuery(const std::string& query, NYdb::EStatus expectedStatus) {
    auto result = GetExternalQueryClient()->ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), expectedStatus, result.GetIssues().ToOneLineString());
    return result.GetResultSets();
}

// Topic client SDK (external YDB recipe)

void TStreamingTestFixture::CreateTopic(const std::string& topicName, std::optional<NYdb::NTopic::TCreateTopicSettings> settings, bool local) {
    if (!settings) {
        settings.emplace()
            .PartitioningSettings(1, 1)
            .BeginAddConsumer("test_consumer");
    }

    const auto result = GetTopicClient(local)->CreateTopic(topicName, *settings).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToOneLineString());
}

void TStreamingTestFixture::WriteTopicMessage(const std::string& topicName, const std::string& message, ui64 partition, bool local) {
    auto writeSession = GetTopicClient(local)->CreateSimpleBlockingWriteSession(NYdb::NTopic::TWriteSessionSettings()
        .Path(topicName)
        .PartitionId(partition));

    writeSession->Write(NYdb::NTopic::TWriteMessage(message));
    writeSession->Close();
}

void TStreamingTestFixture::WriteTopicMessages(const std::string& topicName, const std::vector<std::string>& messages, ui64 partition) {
    for (const auto& message : messages) {
        WriteTopicMessage(topicName, message, partition);
    }
}

void TStreamingTestFixture::ReadTopicMessage(const std::string& topicName, const std::string& expectedMessage, TInstant disposition, bool local) {
    ReadTopicMessages(topicName, {expectedMessage}, disposition, /* sort */ false, local);
}

void TStreamingTestFixture::ReadTopicMessages(const std::string& topicName, std::vector<std::string> expectedMessages, TInstant disposition, bool sort, bool local) {
    NYdb::NTopic::TReadSessionSettings readSettings;
    readSettings
        .WithoutConsumer()
        .AppendTopics(
            NYdb::NTopic::TTopicReadSettings(topicName).ReadFromTimestamp(disposition)
                .AppendPartitionIds(0)
        );

    readSettings.EventHandlers_.StartPartitionSessionHandler(
        [](NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent& event) {
            event.Confirm(0);
        }
    );

    auto readSession = GetTopicClient(local)->CreateReadSession(readSettings);
    std::vector<std::string> received;
    WaitFor(TEST_OPERATION_TIMEOUT, "topic output messages", [&](TString& error) {
        if (!readSession->WaitEvent().HasValue()) {
            error = TStringBuilder() << "no event set, received #" << received.size() << " / " << expectedMessages.size() << " messages";
            return false;
        }

        auto event = readSession->GetEvent(/* block */ true);
        if (const auto dataEvent = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
            for (const auto& message : dataEvent->GetMessages()) {
                received.push_back(message.GetData());
            }

            if (received.size() == expectedMessages.size()) {
                return true;
            }
        }

        UNIT_ASSERT_C(expectedMessages.size() >= received.size(), TStringBuilder()
            << "expected #" << expectedMessages.size() << " messages ("
            << JoinSeq(", ", expectedMessages) << "), got #" << received.size() << " messages ("
            << JoinSeq(", ", received) << ")");

        error = TStringBuilder() << "got new event, received #" << received.size() << " / " << expectedMessages.size() << " messages";
        return false;
    });

    if (sort) {
        Sort(expectedMessages);
        Sort(received);
    }

    UNIT_ASSERT_VALUES_EQUAL(received.size(), expectedMessages.size());
    for (size_t i = 0; i < received.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(received[i], expectedMessages[i]);
    }
}

void TStreamingTestFixture::TestReadTopicBasic(const std::string& testSuffix) {
    const std::string sourceName = "sourceName" + testSuffix;
    const std::string topicName = "topicName" + testSuffix;
    CreateTopic(topicName);

    CreatePqSourceBasicAuth(sourceName, UseSchemaSecrets());

    const auto scriptExecutionOperation = ExecScript(fmt::format(
        R"sql(
            SELECT key || "{id}", value FROM `{source}`.`{topic}` WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA = (
                    key String NOT NULL,
                    value String NOT NULL
                )
            )
            LIMIT 1;
        )sql",
        "source"_a=sourceName,
        "topic"_a=topicName,
        "id"_a=testSuffix
    ));

    WriteTopicMessage(topicName, R"({"key": "key1", "value": "value1"})");

    CheckScriptResult(scriptExecutionOperation, 2, 1, [testSuffix](TResultSetParser& result) {
        UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser(0).GetString(), "key1" + testSuffix);
        UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser(1).GetString(), "value1");
    });

    const auto metadata = GetScriptExecutionOperation(scriptExecutionOperation).Metadata();
    const auto& plan = metadata.ExecStats.GetPlan();
    UNIT_ASSERT(plan);
    UNIT_ASSERT_STRING_CONTAINS(*plan, "Mkql_TotalNodes");
}

// Table client SDK

void TStreamingTestFixture::ExecSchemeQuery(const std::string& query, NYdb::EStatus expectedStatus) {
    const auto result = GetTableClientSession()->ExecuteSchemeQuery(query).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), expectedStatus, result.GetIssues().ToOneLineString());
}

// Query client SDK

std::vector<TResultSet> TStreamingTestFixture::ExecQuery(const std::string& query, NYdb::EStatus expectedStatus, const std::string& expectedError, std::function<void(const std::string&)> astValidator) {
    auto settings = TExecuteQuerySettings();
    if (astValidator) {
        settings.StatsMode(EStatsMode::Full);
    }

    auto result = GetQueryClient()->ExecuteQuery(query, TTxControl::NoTx(), settings).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), expectedStatus, result.GetIssues().ToOneLineString());

    if (astValidator) {
        const auto& stats = result.GetStats();
        UNIT_ASSERT(stats);
        const auto& ast = stats->GetAst();
        UNIT_ASSERT(ast);
        astValidator(*ast);
    }

    if (!expectedError.empty()) {
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), expectedError);
    }

    return result.GetResultSets();
}

void TStreamingTestFixture::CreatePqSource(const std::string& pqSourceName) {
    ExecQuery(fmt::format(
        R"sql(
            CREATE EXTERNAL DATA SOURCE `{pq_source}` WITH (
                SOURCE_TYPE = "Ydb",
                LOCATION = "{pq_location}",
                DATABASE_NAME = "{pq_database_name}",
                AUTH_METHOD = "NONE"
            );
        )sql",
        "pq_source"_a = pqSourceName,
        "pq_location"_a = YDB_ENDPOINT,
        "pq_database_name"_a = YDB_DATABASE
    ));
}

void TStreamingTestFixture::CreatePqSourceBasicAuth(const std::string& pqSourceName, const bool useSchemaSecrets) {
    const std::string secretName = useSchemaSecrets ? "/Root/secret_local_password" : "secret_local_password";
    if (useSchemaSecrets) {
        ExecQuery(fmt::format(
            R"sql(
                CREATE SECRET `{secret_name}` WITH (value = "1234");
            )sql",
            "secret_name"_a = secretName
        ));
    } else {
        ExecQuery(fmt::format(
            R"sql(
                CREATE OBJECT `{secret_name}` (TYPE SECRET) WITH (value = "1234");
            )sql",
            "secret_name"_a = secretName
        ));
    }

    ExecQuery(fmt::format(
        R"sql(
            CREATE EXTERNAL DATA SOURCE `{pq_source}` WITH (
                SOURCE_TYPE = "Ydb",
                LOCATION = "{pq_location}",
                DATABASE_NAME = "{pq_database_name}",
                AUTH_METHOD = "BASIC",
                LOGIN = "root",
                PASSWORD_SECRET_NAME = "{secret_name}"
            );
        )sql",
        "pq_source"_a = pqSourceName,
        "pq_location"_a = YDB_ENDPOINT,
        "pq_database_name"_a = YDB_DATABASE,
        "secret_name"_a = secretName
    ));
}

void TStreamingTestFixture::CreateS3Source(const std::string& bucket, const std::string& s3SourceName) {
    ExecQuery(fmt::format(
        R"sql(
            CREATE EXTERNAL DATA SOURCE `{s3_source}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{s3_location}",
                AUTH_METHOD="NONE"
            );
        )sql",
        "s3_source"_a = s3SourceName,
        "s3_location"_a = GetBucketLocation(bucket)
    ));
}

void TStreamingTestFixture::CreateYdbSource(const std::string& ydbSourceName) {
    ExecQuery(fmt::format(
        R"sql(
            CREATE SECRET ydb_source_secret WITH (value = "{token}");
            CREATE EXTERNAL DATA SOURCE `{ydb_source}` WITH (
                SOURCE_TYPE = "Ydb",
                LOCATION = "{ydb_location}",
                DATABASE_NAME = "{ydb_database_name}",
                AUTH_METHOD = "TOKEN",
                TOKEN_SECRET_PATH = "ydb_source_secret",
                USE_TLS = "FALSE"
            );
        )sql",
        "ydb_source"_a = ydbSourceName,
        "ydb_location"_a = YDB_ENDPOINT,
        "ydb_database_name"_a = YDB_DATABASE,
        "token"_a = BUILTIN_ACL_ROOT
    ));
}

void TStreamingTestFixture::CreateSolomonSource(const std::string& solomonSourceName) {
    ExecQuery(fmt::format(
        R"sql(
            CREATE EXTERNAL DATA SOURCE `{solomon_source}` WITH (
                SOURCE_TYPE = "Solomon",
                LOCATION = "localhost:{solomon_port}",
                AUTH_METHOD = "NONE",
                USE_TLS = "false"
            );
        )sql",
        "solomon_source"_a = solomonSourceName,
        "solomon_port"_a = getenv("SOLOMON_HTTP_PORT")
    ));
}

// Script executions (using query client SDK)

TOperation::TOperationId TStreamingTestFixture::ExecScript(const std::string& query, std::optional<TExecuteScriptSettings> settings, bool waitRunning) {
    if (!settings) {
        settings.emplace()
            .StatsMode(EStatsMode::Profile);
    }

    const auto operation = GetQueryClient()->ExecuteScript(query, *settings).ExtractValueSync();
    const auto& status = operation.Status();
    UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), NYdb::EStatus::SUCCESS, status.GetIssues().ToOneLineString());

    if (waitRunning) {
        WaitScriptExecution(operation.Id(), EExecStatus::Running);
        Sleep(TDuration::Seconds(1));
    }

    return operation.Id();
}

TScriptExecutionOperation TStreamingTestFixture::GetScriptExecutionOperation(const TOperation::TOperationId& operationId, bool checkStatus) {
    const auto operation = GetOperationClient()->Get<TScriptExecutionOperation>(operationId).GetValueSync();

    if (checkStatus) {
        const auto& status = operation.Status();
        UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), NYdb::EStatus::SUCCESS, status.GetIssues().ToOneLineString());
    }

    return operation;
}

TScriptExecutionOperation TStreamingTestFixture::WaitScriptExecution(const TOperation::TOperationId& operationId, EExecStatus finalStatus, bool waitRetry) {
    const bool waitForFinalStatus = !waitRetry && IsIn({EExecStatus::Completed, EExecStatus::Failed}, finalStatus);

    std::optional<TScriptExecutionOperation> operation;
    WaitFor(TEST_OPERATION_TIMEOUT, TStringBuilder() << "script execution status" << finalStatus, [&](TString& error) {
        operation = GetScriptExecutionOperation(operationId, /* checkStatus */ false);

        const auto execStatus = operation->Metadata().ExecStatus;
        if (execStatus == finalStatus && (!waitForFinalStatus || operation->Ready())) {
            return true;
        }

        const auto& status = operation->Status();
        UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), NYdb::EStatus::SUCCESS, TStringBuilder() << "Status: " << execStatus << ", Issues: " << status.GetIssues().ToOneLineString());
        UNIT_ASSERT_C(!operation->Ready(), "Operation unexpectedly ready in status " << execStatus << " (expected status " << finalStatus << ")");

        error = TStringBuilder() << "operation status: " << execStatus << ", ready: " << operation->Ready();
        return false;
    });

    const auto& status = operation->Status();
    const auto execStatus = operation->Metadata().ExecStatus;
    UNIT_ASSERT_VALUES_EQUAL_C(execStatus, finalStatus, status.GetIssues().ToOneLineString());

    if (finalStatus != EExecStatus::Failed) {
        UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), NYdb::EStatus::SUCCESS, status.GetIssues().ToOneLineString());
    }

    UNIT_ASSERT_VALUES_EQUAL(operation->Ready(), waitForFinalStatus);
    return *operation;
}

TScriptExecutionOperation TStreamingTestFixture::ExecAndWaitScript(const std::string& query, EExecStatus finalStatus, std::optional<TExecuteScriptSettings> settings) {
    return WaitScriptExecution(ExecScript(query, settings, /* waitRunning */ false), finalStatus, false);
}

TResultSet TStreamingTestFixture::FetchScriptResult(const TOperation::TOperationId& operationId, ui64 resultSetId) {
    WaitScriptExecution(operationId);

    auto result = GetQueryClient()->FetchScriptResults(operationId, resultSetId).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToOneLineString());

    return result.ExtractResultSet();
}

void TStreamingTestFixture::CheckScriptResult(const TResultSet& result, ui64 columnsCount, ui64 rowsCount, std::function<void(TResultSetParser&)> validator) {
    TResultSetParser resultSet(result);
    UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), columnsCount);
    UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), rowsCount);

    for (ui64 i = 0; i < rowsCount; ++i) {
        UNIT_ASSERT_C(resultSet.TryNextRow(), i);
        validator(resultSet);
    }
}

void TStreamingTestFixture::CheckScriptResult(const TOperation::TOperationId& operationId, ui64 columnsCount, ui64 rowsCount, std::function<void(TResultSetParser&)> validator, ui64 resultSetId) {
    CheckScriptResult(FetchScriptResult(operationId, resultSetId), columnsCount, rowsCount, validator);
}

void TStreamingTestFixture::CancelScriptExecution(const TOperation::TOperationId& operationId) {
    const auto& result = GetOperationClient()->Cancel(operationId).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToOneLineString());
}

// Script executions (native events API)

std::pair<std::string, TOperation::TOperationId> TStreamingTestFixture::ExecScriptNative(const std::string& query, const TScriptQuerySettings& settings, bool waitRunning) {
    auto& runtime = GetRuntime();

    NKikimrKqp::TEvQueryRequest queryProto;
    queryProto.SetUserToken(NACLib::TUserToken(BUILTIN_ACL_ROOT, TVector<NACLib::TSID>{runtime.GetAppData().AllAuthenticatedUsers}).SerializeAsString());

    auto& req = *queryProto.MutableRequest();
    req.SetDatabase(TEST_DATABASE);
    req.SetQuery(TString{query});
    req.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
    req.SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT);
    req.SetCollectStats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_PROFILE);

    auto ev = std::make_unique<TEvKqp::TEvScriptRequest>();
    ev->Record = queryProto;
    ev->ForgetAfter = settings.Timeout;
    ev->ResultsTtl = settings.Timeout;
    ev->RetryMapping = {settings.RetryMapping};
    ev->SaveQueryPhysicalGraph = settings.SaveState;
    ev->QueryPhysicalGraph = settings.PhysicalGraph;
    ev->CheckpointId = settings.CheckpointId;
    ev->ProgressStatsPeriod = TDuration::Seconds(1);

    const auto edgeActor = runtime.AllocateEdgeActor();
    runtime.Send(MakeKqpProxyID(runtime.GetNodeId()), edgeActor, ev.release());

    const auto reply = runtime.GrabEdgeEvent<TEvKqp::TEvScriptResponse>(edgeActor, settings.Timeout);
    UNIT_ASSERT_C(reply, "CreateScript response is empty");
    UNIT_ASSERT_VALUES_EQUAL_C(reply->Get()->Status, Ydb::StatusIds::SUCCESS, reply->Get()->Issues.ToString());

    const auto& executionId = reply->Get()->ExecutionId;
    UNIT_ASSERT(executionId);
    const auto& operationId = TOperation::TOperationId(ScriptExecutionOperationFromExecutionId(executionId));

    if (waitRunning) {
        WaitScriptExecution(operationId, EExecStatus::Running);
        Sleep(TDuration::Seconds(1));
    }

    return {executionId, operationId};
}

NKikimrKqp::TScriptExecutionRetryState::TMapping TStreamingTestFixture::CreateRetryMapping(const std::vector<Ydb::StatusIds::StatusCode>& statuses, TDuration backoffDuration) {
    NKikimrKqp::TScriptExecutionRetryState::TMapping retryMapping;
    for (const auto status : statuses) {
        retryMapping.AddStatusCode(status);
    }

    auto& policy = *retryMapping.MutableExponentialDelayPolicy();
    policy.SetBackoffMultiplier(1.5);
    *policy.MutableInitialBackoff() = NProtoInterop::CastToProto(backoffDuration);
    *policy.MutableMaxBackoff() = NProtoInterop::CastToProto(backoffDuration);

    return retryMapping;
}

NKikimrKqp::TQueryPhysicalGraph TStreamingTestFixture::LoadPhysicalGraph(const std::string& executionId) {
    auto& runtime = GetRuntime();
    const auto edgeActor = runtime.AllocateEdgeActor();
    runtime.Register(CreateGetScriptExecutionPhysicalGraphActor(edgeActor, TEST_DATABASE, TString{executionId}));

    const auto graph = runtime.GrabEdgeEvent<TEvGetScriptPhysicalGraphResponse>(edgeActor, TEST_OPERATION_TIMEOUT);
    UNIT_ASSERT_C(graph, "Empty graph response");
    UNIT_ASSERT_VALUES_EQUAL_C(graph->Get()->Status, Ydb::StatusIds::SUCCESS, graph->Get()->Issues.ToOneLineString());

    const auto& graphProto = graph->Get()->PhysicalGraph;
    UNIT_ASSERT(graphProto);

    return *graphProto;
}

void TStreamingTestFixture::CheckScriptExecutionsCount(ui64 expectedExecutionsCount, ui64 expectedLeasesCount) {
    const auto& result = ExecQuery(
        R"sql(
            SELECT COUNT(*) FROM `.metadata/script_executions`;
            SELECT COUNT(*) FROM `.metadata/script_execution_leases`;
        )sql"
    );

    UNIT_ASSERT_VALUES_EQUAL(result.size(), 2);

    CheckScriptResult(result[0], 1, 1, [&](TResultSetParser& resultSet) {
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUint64(), expectedExecutionsCount);
    });

    CheckScriptResult(result[1], 1, 1, [&](TResultSetParser& resultSet) {
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUint64(), expectedLeasesCount);
    });
}

void TStreamingTestFixture::WaitCheckpointUpdate(const std::string& checkpointId) {
    std::optional<uint64_t> minSeqNo;
    WaitFor(TEST_OPERATION_TIMEOUT, "checkpoint update", [&](TString& error) {
        const auto& result = ExecQuery(fmt::format(
            R"sql(
                SELECT MIN(seq_no) AS seq_no FROM `.metadata/streaming/checkpoints/checkpoints_metadata`
                WHERE graph_id = "{checkpoint_id}";
            )sql",
            "checkpoint_id"_a = checkpointId
        ));
        UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);

        std::optional<uint64_t> seqNo;
        CheckScriptResult(result[0], 1, 1, [&seqNo](TResultSetParser& resultSet) {
            seqNo = resultSet.ColumnParser(0).GetOptionalUint64();
        });

        if (!seqNo) {
            error = "seq_no is null";
            return false;
        }

        if (!minSeqNo) {
            minSeqNo = *seqNo;
            error = TStringBuilder() << "found initial seq_no: " << *minSeqNo;
            return false;
        }

        if (*minSeqNo != *seqNo) {
            return true;
        }

        error = TStringBuilder() << "seq_no is not changed from: " << *minSeqNo;
        return false;
    });
}

// Mock Connector utils

NYql::TGenericDataSourceInstance TStreamingTestFixture::GetMockConnectorSourceInstance() {
    NYql::TGenericDataSourceInstance dataSourceInstance;
    dataSourceInstance.set_kind(NYql::YDB);
    dataSourceInstance.set_database(YDB_DATABASE);
    dataSourceInstance.set_use_tls(false);
    dataSourceInstance.set_protocol(NYql::NATIVE);

    auto& endpoint = *dataSourceInstance.mutable_endpoint();
    TIpPort port;
    NHttp::CrackAddress(TString{YDB_ENDPOINT}, *endpoint.mutable_host(), port);
    endpoint.set_port(port);

    auto& iamToken = *dataSourceInstance.mutable_credentials()->mutable_token();
    iamToken.set_type("IAM");
    iamToken.set_value(BUILTIN_ACL_ROOT);

    return dataSourceInstance;
}

// Should be called at most once
void TStreamingTestFixture::SetupMockConnectorTableDescription(std::shared_ptr<TConnectorClientMock> mockClient, const TMockConnectorTableDescriptionSettings& settings) {
    TTypeMappingSettings typeMappingSettings;
    typeMappingSettings.set_date_time_format(STRING_FORMAT);

    if (settings.DescribeCount) {
        auto describeTableBuilder = mockClient->ExpectDescribeTable();
        describeTableBuilder
            .Table(settings.TableName)
            .DataSourceInstance(GetMockConnectorSourceInstance())
            .TypeMappingSettings(typeMappingSettings);

        for (ui64 i = 0; i < settings.DescribeCount; ++i) {
            auto responseBuilder = describeTableBuilder.Response();
            FillMockConnectorRequestColumns(responseBuilder, settings.Columns);
        }
    }

    if (settings.ListSplitsCount) {
        auto listSplitsBuilder = mockClient->ExpectListSplits();
        auto fillListSplitExpectation = listSplitsBuilder
            .ValidateArgs(settings.ValidateListSplitsArgs ? TConnectorClientMock::EArgsValidation::Strict : TConnectorClientMock::EArgsValidation::DataSourceInstance)
            .Select()
                .DataSourceInstance(GetMockConnectorSourceInstance())
                .Table(settings.TableName)
                .What();

        FillMockConnectorRequestColumns(fillListSplitExpectation, settings.Columns);

        for (ui64 i = 0; i < settings.ListSplitsCount; ++i) {
            auto responseBuilder = listSplitsBuilder.Result()
                .AddResponse(NYql::NConnector::NewSuccess())
                    .Description("some binary description")
                    .Select()
                        .DataSourceInstance(GetMockConnectorSourceInstance())
                        .What();
            FillMockConnectorRequestColumns(responseBuilder, settings.Columns);
        }
    }
}

// Should be called at most once
void TStreamingTestFixture::SetupMockConnectorTableData(std::shared_ptr<TConnectorClientMock> mockClient, const TMockConnectorReadSplitsSettings& settings) {
    auto readSplitsBuilder = mockClient->ExpectReadSplits();

    {
        auto columnsBuilder = readSplitsBuilder
            .Filtering(TReadSplitsRequest::FILTERING_OPTIONAL)
            .ValidateArgs(settings.ValidateReadSplitsArgs ? TConnectorClientMock::EArgsValidation::Strict : TConnectorClientMock::EArgsValidation::DataSourceInstance)
            .Split()
                .Description("some binary description")
                .Select()
                    .Table(settings.TableName)
                    .DataSourceInstance(GetMockConnectorSourceInstance())
                    .What();
        FillMockConnectorRequestColumns(columnsBuilder, settings.Columns);
    }

    for (ui64 i = 0; i < settings.NumberReadSplits; ++i) {
        readSplitsBuilder.Result()
            .AddResponse(settings.ResultFactory(), NYql::NConnector::NewSuccess());
    }
}

// Other helpers

std::function<void(const std::string&)> TStreamingTestFixture::AstChecker(ui64 txCount, ui64 stagesCount) {
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

void TStreamingTestFixture::EnsureNotInitialized(const std::string& info) {
    UNIT_ASSERT_C(!Kikimr, "Kikimr runner is already initialized, can not setup " << info);
}

void TStreamingTestFixture::TearDown(NUnitTest::TTestContext& context)  {
    PqGateway.Reset();
    TBase::TearDown(context);
}

bool TStreamingTestFixture::UseSchemaSecrets() {
    return false;
}

bool TStreamingWithSchemaSecretsTestFixture::UseSchemaSecrets() {
    return true;
}

void TStreamingSysViewTestFixture::Setup() {
    LogSettings.AddLogPriority(NKikimrServices::SYSTEM_VIEWS, NLog::PRI_DEBUG);
    ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

    UNIT_ASSERT_C(InputTopic.empty(), "Setup called twice");
    InputTopic = TStringBuilder() << INPUT_TOPIC_NAME << Name_;
    OutputTopic = TStringBuilder() << OUTPUT_TOPIC_NAME << Name_;
    CreateTopic(InputTopic);
    CreateTopic(OutputTopic);
    CreatePqSource(PQ_SOURCE);
}

void TStreamingSysViewTestFixture::StartQuery(const std::string& name) {
    ExecQuery(fmt::format(
        R"sql(
            CREATE STREAMING QUERY `{query_name}` AS DO BEGIN{text}END DO
        )sql",
        "query_name"_a = name,
        "text"_a = GetQueryText(name)
    ));
}

std::vector<TStreamingSysViewTestFixture::TSysViewResult> TStreamingSysViewTestFixture::CheckSysView(std::vector<TSysViewRow> rows, const std::string& filter, const std::string& order) {
    const auto& result = ExecQuery(TStringBuilder()
        << "SELECT * FROM `.sys/streaming_queries`"
        << (filter.empty() ? "" : " WHERE " + filter)
        << (order.empty() ? "" : " ORDER BY " + order)
    );
    UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);

    if (order.ends_with("ASC")) {
        Sort(rows, [&](const auto& lhs, const auto& rhs) {
            return lhs.Name < rhs.Name;
        });
    } else if (order.ends_with("DESC")) {
        Sort(rows, [&](const auto& lhs, const auto& rhs) {
            return lhs.Name > rhs.Name;
        });
    }

    std::vector<TSysViewResult> results;
    CheckScriptResult(result[0], SYS_VIEW_COLUMNS_COUNT, rows.size(), [&](TResultSetParser& resultSet) {
        const auto& row = rows[results.size()];
        auto& result = results.emplace_back();
        UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser("Path").GetOptionalUtf8(), JoinPath(TVector{TString{"/Root"}, TString{row.Name}}));
        UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser("Status").GetOptionalUtf8(), row.Status);
        UNIT_ASSERT_STRING_CONTAINS(*resultSet.ColumnParser("Issues").GetOptionalUtf8(), row.Issues);

        const bool expectExecutions = row.Run && IsIn({"RUNNING", "COMPLETED", "CANCELLED", "FAILED"}, row.Status);
        if (expectExecutions || row.CheckPlan) {
            UNIT_ASSERT_STRING_CONTAINS(*resultSet.ColumnParser("Plan").GetOptionalUtf8(), TStringBuilder() << "Write " << PQ_SOURCE);
            UNIT_ASSERT_STRING_CONTAINS(*resultSet.ColumnParser("Ast").GetOptionalUtf8(), row.Ast ? *row.Ast : JoinPath({"/Root", PQ_SOURCE}));
        }

        UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser("Text").GetOptionalUtf8(), row.Text.value_or(GetQueryText(row.Name)));
        UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser("Run").GetOptionalBool(), row.Run);
        UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser("ResourcePool").GetOptionalUtf8(), row.Pool);
        UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser("RetryCount").GetOptionalUint64(), row.RetryCount);

        if (row.LastFailAt) {
            const auto delta = abs(resultSet.ColumnParser("LastFailAt").GetOptionalTimestamp()->SecondsFloat() - row.LastFailAt->SecondsFloat());
            UNIT_ASSERT_GE(5, delta);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("LastFailAt").GetOptionalTimestamp().has_value(), row.RetryCount > 0);
        }

        if (row.SuspendedUntil) {
            const auto delta = abs(resultSet.ColumnParser("SuspendedUntil").GetOptionalTimestamp()->SecondsFloat() - row.SuspendedUntil->SecondsFloat());
            UNIT_ASSERT_GE(5, delta);
        } else {
            UNIT_ASSERT(!resultSet.ColumnParser("SuspendedUntil").GetOptionalTimestamp());
        }

        result.ExecutionId = *resultSet.ColumnParser("LastExecutionId").GetOptionalUtf8();
        UNIT_ASSERT_VALUES_EQUAL(!result.ExecutionId.empty(), expectExecutions);

        const auto previousExecutionIds = *resultSet.ColumnParser("PreviousExecutionIds").GetOptionalUtf8();
        NJson::TJsonValue value;
        UNIT_ASSERT(NJson::ReadJsonTree(previousExecutionIds, &value));
        UNIT_ASSERT_VALUES_EQUAL(value.GetType(), NJson::JSON_ARRAY);

        const auto executionsSize = value.GetIntegerRobust();
        result.PreviousExecutionIds.reserve(executionsSize);
        for (i64 i = 0; i < executionsSize; ++i) {
            const NJson::TJsonValue* executionId = nullptr;
            value.GetValuePointer(i, &executionId);
            Y_ENSURE(executionId);

            result.PreviousExecutionIds.emplace_back(executionId->GetString());
        }
    });

    return results;
}

std::string TStreamingSysViewTestFixture::GetQueryText(const std::string& name) const {
    UNIT_ASSERT_C(!InputTopic.empty(), "Setup is not called");

    return fmt::format(
        R"sql(
            ;INSERT INTO `{pq_source}`.`{output_topic}`
            /* {query_name} */
            SELECT * FROM `{pq_source}`.`{input_topic}`;
        )sql",
        "query_name"_a = name,
        "pq_source"_a = PQ_SOURCE,
        "input_topic"_a = InputTopic,
        "output_topic"_a = OutputTopic
    );
}

TTestTopicLoader::TTestTopicLoader(const std::string& endpoint, const std::string& database, const std::string& topic, NThreading::TFuture<void> feature)
    : Client(TDriver(TDriverConfig()
        .SetEndpoint(endpoint)
        .SetDatabase(database)
    ))
    , WriteSession(Client.CreateWriteSession(NYdb::NTopic::TWriteSessionSettings().Path(topic)))
    , Feature(feature)
    , Message(1_KB, 'x')
{}


void TTestTopicLoader::Bootstrap() {
    Become(&TTestTopicLoader::StateFunc);
    Schedule(Timeout, new TEvents::TEvWakeup());
    WriteMessages();
}

void TTestTopicLoader::WriteMessages() {
    if (Feature.HasValue() || Timeout <= TInstant::Now()) {
        PassAway();
        WriteSession->Close(TDuration::Zero());
        return;
    }

    const auto event = WriteSession->GetEvent();
    if (!event) {
        WriteSession->WaitEvent().Subscribe([actorSystem = ActorContext().ActorSystem(), selfId = SelfId()](const auto&) {
            actorSystem->Send(selfId, new TEvents::TEvWakeup());
        });
        return;
    }

    if (std::holds_alternative<NYdb::NTopic::TSessionClosedEvent>(*event)) {
        const auto& status = std::get<NYdb::NTopic::TSessionClosedEvent>(*event);
        UNIT_FAIL(status.GetStatus() << ", issues: " << status.GetIssues().ToOneLineString());
    }

    if (std::holds_alternative<NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(*event)) {
        WriteSession->Write(
            std::move(std::get<NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(*event).ContinuationToken),
            Message
        );
    }

    Schedule(TDuration::Zero(), new TEvents::TEvWakeup());
}

TTabletKiller::TTabletKiller(ui64 tabletId, TDuration killerInterval)
    : TabletId(tabletId)
    , KillerInterval(killerInterval)
{}

void TTabletKiller::Bootstrap() {
    Become(&TThis::StateFunc);
    Schedule(KillerInterval, new TEvents::TEvWakeup());
}

void TTabletKiller::KillTablet() const {
    RestartTablet(*ActorContext().ActorSystem(), TabletId);
    Schedule(KillerInterval, new TEvents::TEvWakeup());
}

} // namespace NKikimr::NKqp
