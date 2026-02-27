#include <ydb/core/cms/console/console.h>
#include <ydb/core/external_sources/external_source.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/kqp/proxy_service/kqp_script_executions.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <ydb/core/testlib/test_pq_client.h>
#include <ydb/library/testlib/common/test_utils.h>
#include <ydb/library/testlib/pq_helpers/mock_pq_gateway.h>
#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>
#include <ydb/library/testlib/solomon_helpers/solomon_emulator_helpers.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/error.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/ut_helpers/connector_client_mock.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>

#include <fmt/format.h>

#include <library/cpp/protobuf/interop/cast.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NKikimr::NKqp::NFederatedQueryTest;
using namespace fmt::literals;
using namespace NYql::NConnector::NTest;
using namespace NYql::NConnector::NApi;
using namespace NTestUtils;

namespace {

struct TScriptQuerySettings {
    bool SaveState = false;
    NKikimrKqp::TScriptExecutionRetryState::TMapping RetryMapping;
    std::optional<NKikimrKqp::TQueryPhysicalGraph> PhysicalGraph;
    TDuration Timeout = TDuration::Seconds(30);
    TString CheckpointId = CreateGuidAsString();
};

struct TColumn {
    TString Name;
    Ydb::Type::PrimitiveTypeId Type;
};

struct TMockConnectorTableDescriptionSettings {
    TString TableName;
    std::vector<TColumn> Columns;
    ui64 DescribeCount = 1;
    ui64 ListSplitsCount = 1;
    bool ValidateListSplitsArgs = true;
};

struct TMockConnectorReadSplitsSettings {
    TString TableName;
    std::vector<TColumn> Columns;
    ui64 NumberReadSplits;
    bool ValidateReadSplitsArgs = true;
    std::function<std::shared_ptr<arrow::RecordBatch>()> ResultFactory;
};

class TStreamingTestFixture : public NUnitTest::TBaseFixture {
    using TBase = NUnitTest::TBaseFixture;

public:
    // External YDB recipe
    inline static const TString YDB_ENDPOINT = GetEnv("YDB_ENDPOINT");
    inline static const TString YDB_DATABASE = GetEnv("YDB_DATABASE");

    // Local kikimr test cluster
    inline static constexpr char TEST_DATABASE[] = "/Root";

    inline static constexpr TDuration TEST_OPERATION_TIMEOUT = TDuration::Seconds(10);

public:
    // Local kikimr settings

    NKikimrConfig::TAppConfig& SetupAppConfig() {
        UNIT_ASSERT_C(!AppConfig, "AppConfig is already initialized");
        EnsureNotInitialized("AppConfig");

        auto& result = AppConfig.emplace();
        result.MutableTableServiceConfig()->SetDqChannelVersion(1u);
        return result;
    }

    TIntrusivePtr<IMockPqGateway> SetupMockPqGateway() {
        UNIT_ASSERT_C(!PqGateway, "PqGateway is already initialized");
        EnsureNotInitialized("MockPqGateway");

        const auto mockPqGateway = CreateMockPqGateway({.OperationTimeout = TEST_OPERATION_TIMEOUT});
        PqGateway = mockPqGateway;

        return mockPqGateway;
    }

    std::shared_ptr<TConnectorClientMock> SetupMockConnectorClient() {
        UNIT_ASSERT_C(!ConnectorClient, "ConnectorClient is already initialized");
        EnsureNotInitialized("ConnectorClient");

        auto mockConnectorClient = std::make_shared<TConnectorClientMock>();
        ConnectorClient = mockConnectorClient;

        return mockConnectorClient;
    }

    // Local kikimr test cluster

    std::shared_ptr<TKikimrRunner> GetKikimrRunner() {
        if (!Kikimr) {
            if (!AppConfig) {
                AppConfig.emplace();
            }

            auto& featureFlags = *AppConfig->MutableFeatureFlags();
            featureFlags.SetEnableStreamingQueries(true);
            featureFlags.SetEnableSchemaSecrets(true);
            featureFlags.SetEnableResourcePools(true);

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
                .CredentialsFactory = CreateCredentialsFactory(),
                .PqGateway = PqGateway,
                .CheckpointPeriod = CheckpointPeriod,
                .LogSettings = LogSettings,
                .UseLocalCheckpointsInStreamingQueries = true,
                .InternalInitFederatedQuerySetupFactory = InternalInitFederatedQuerySetupFactory,
            });

            Kikimr->GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
            Kikimr->GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableStreamingQueries(true);
            Kikimr->GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableResourcePools(true);
        }

        return Kikimr;
    }

    TTestActorRuntime& GetRuntime() {
        return *GetKikimrRunner()->GetTestServer().GetRuntime();
    }

    std::shared_ptr<TDriver> GetInternalDriver() {
        if (!InternalDriver) {
            InternalDriver = std::make_shared<TDriver>(GetKikimrRunner()->GetDriver());
        }

        return InternalDriver;
    }

    std::shared_ptr<TQueryClient> GetQueryClient() {
        if (!QueryClient) {
            QueryClient = std::make_shared<TQueryClient>(
                GetKikimrRunner()->GetQueryClient(QueryClientSettings)
            );
        }

        return QueryClient;
    }

    std::shared_ptr<NYdb::NTable::TTableClient> GetTableClient() {
        if (!TableClient) {
            TableClient = std::make_shared<NYdb::NTable::TTableClient>(GetKikimrRunner()->GetTableClient());
        }

        return TableClient;
    }

    std::shared_ptr<NOperation::TOperationClient> GetOperationClient() {
        if (!OperationClient) {
            OperationClient = std::make_shared<NOperation::TOperationClient>(*GetInternalDriver());
        }

        return OperationClient;
    }

    std::shared_ptr<NYdb::NTable::TSession> GetTableClientSession() {
        if (!TableClientSession) {
            TableClientSession = std::make_shared<NYdb::NTable::TSession>(
                GetTableClient()->CreateSession().ExtractValueSync().GetSession()
            );
        }

        return TableClientSession;
    }

    std::shared_ptr<NKikimr::NPersQueueTests::TFlatMsgBusPQClient> GetLocalFlatMsgBusPQClient() {
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

    void KillTopicPqrbTablet(const TString& topicPath) {
        const auto tabletClient = GetLocalFlatMsgBusPQClient();
        const auto& describeResult = tabletClient->Ls(JoinPath({"/Root", topicPath}));
        UNIT_ASSERT_C(describeResult->Record.GetPathDescription().HasPersQueueGroup(), describeResult->Record);

        const auto& persQueueGroup = describeResult->Record.GetPathDescription().GetPersQueueGroup();
        tabletClient->KillTablet(GetKikimrRunner()->GetTestServer(), persQueueGroup.GetBalancerTabletID());
    }

    // External YDB recipe

    std::shared_ptr<TDriver> GetExternalDriver() {
        if (!ExternalDriver) {
            ExternalDriver = std::make_shared<TDriver>(TDriverConfig()
                .SetEndpoint(YDB_ENDPOINT)
                .SetDatabase(YDB_DATABASE));
        }

        return ExternalDriver;
    }

    std::shared_ptr<NTopic::TTopicClient> GetTopicClient(bool local = false) {
        if (local && !LocalTopicClient) {
            LocalTopicClient = std::make_shared<NTopic::TTopicClient>(*GetInternalDriver(), TopicClientSettings);
        }

        if (!TopicClient) {
            TopicClient = std::make_shared<NTopic::TTopicClient>(*GetExternalDriver(), NTopic::TTopicClientSettings()
                .DiscoveryEndpoint(YDB_ENDPOINT)
                .Database(YDB_DATABASE));
        }

        return local ? LocalTopicClient : TopicClient;
    }

    std::shared_ptr<TQueryClient> GetExternalQueryClient() {
        if (!ExternalQueryClient) {
            ExternalQueryClient = std::make_shared<TQueryClient>(*GetExternalDriver(), TClientSettings()
                .DiscoveryEndpoint(YDB_ENDPOINT)
                .Database(YDB_DATABASE));
        }

        return ExternalQueryClient;
    }

    std::vector<TResultSet> ExecExternalQuery(const TString& query, EStatus expectedStatus = EStatus::SUCCESS) {
        auto result = GetExternalQueryClient()->ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), expectedStatus, result.GetIssues().ToOneLineString());
        return result.GetResultSets();
    }

    // Topic client SDK (external YDB recipe)

    void CreateTopic(const TString& topicName, std::optional<NTopic::TCreateTopicSettings> settings = std::nullopt, bool local = false) {
        if (!settings) {
            settings.emplace()
                .PartitioningSettings(1, 1)
                .BeginAddConsumer("test_consumer");
        }

        const auto result = GetTopicClient(local)->CreateTopic(topicName, *settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
    }

    void WriteTopicMessage(const TString& topicName, const TString& message, ui64 partition = 0, bool local = false) {
        auto writeSession = GetTopicClient(local)->CreateSimpleBlockingWriteSession(NTopic::TWriteSessionSettings()
            .Path(topicName)
            .PartitionId(partition));

        writeSession->Write(NTopic::TWriteMessage(message));
        writeSession->Close();
    }

    void WriteTopicMessages(const TString& topicName, const std::vector<TString>& messages, ui64 partition = 0) {
        for (const auto& message : messages) {
            WriteTopicMessage(topicName, message, partition);
        }
    }

    void ReadTopicMessage(const TString& topicName, const TString& expectedMessage, TInstant disposition = TInstant::Now() - TDuration::Seconds(100), bool local = false) {
        ReadTopicMessages(topicName, {expectedMessage}, disposition, /* sort */ false, local);
    }

    void ReadTopicMessages(const TString& topicName, TVector<TString> expectedMessages, TInstant disposition = TInstant::Now() - TDuration::Seconds(100), bool sort = false, bool local = false) {
        NTopic::TReadSessionSettings readSettings;
        readSettings
            .WithoutConsumer()
            .AppendTopics(
                NTopic::TTopicReadSettings(topicName).ReadFromTimestamp(disposition)
                    .AppendPartitionIds(0)
            );

        readSettings.EventHandlers_.StartPartitionSessionHandler(
            [](NTopic::TReadSessionEvent::TStartPartitionSessionEvent& event) {
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
            if (const auto dataEvent = std::get_if<NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
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

    void TestReadTopicBasic(const TString& testSuffix) {
        const TString sourceName = "sourceName" + testSuffix;
        const TString topicName = "topicName" + testSuffix;
        CreateTopic(topicName);

        CreatePqSourceBasicAuth(sourceName, UseSchemaSecrets());

        const auto scriptExecutionOperation = ExecScript(fmt::format(R"(
            SELECT key || "{id}", value FROM `{source}`.`{topic}` WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA = (
                    key String NOT NULL,
                    value String NOT NULL
                )
            )
            LIMIT 1;
            )",
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

    void ExecSchemeQuery(const TString& query, EStatus expectedStatus = EStatus::SUCCESS) {
        const auto result = GetTableClientSession()->ExecuteSchemeQuery(query).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), expectedStatus, result.GetIssues().ToOneLineString());
    }

    // Query client SDK

    std::vector<TResultSet> ExecQuery(const TString& query, EStatus expectedStatus = EStatus::SUCCESS, const TString& expectedError = "", std::function<void(const TString&)> astValidator = nullptr) {
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
            astValidator(TString(*ast));
        }

        if (expectedError) {
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), expectedError);
        }

        return result.GetResultSets();
    }

    void CreatePqSource(const TString& pqSourceName) {
        ExecQuery(fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE `{pq_source}` WITH (
                SOURCE_TYPE = "Ydb",
                LOCATION = "{pq_location}",
                DATABASE_NAME = "{pq_database_name}",
                AUTH_METHOD = "NONE"
            );)",
            "pq_source"_a = pqSourceName,
            "pq_location"_a = YDB_ENDPOINT,
            "pq_database_name"_a = YDB_DATABASE
        ));
    }

    void CreatePqSourceBasicAuth(const TString& pqSourceName, const bool useSchemaSecrets = false) {
        const TString secretName = useSchemaSecrets ? "/Root/secret_local_password" : "secret_local_password";
        if (useSchemaSecrets) {
            ExecQuery(fmt::format(R"(
                CREATE SECRET `{secret_name}` WITH (value = "1234");
                )",
                "secret_name"_a = secretName
            ));
        } else {
            ExecQuery(fmt::format(R"(
                CREATE OBJECT `{secret_name}` (TYPE SECRET) WITH (value = "1234");
                )",
                "secret_name"_a = secretName
            ));
        }

        ExecQuery(fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE `{pq_source}` WITH (
                SOURCE_TYPE = "Ydb",
                LOCATION = "{pq_location}",
                DATABASE_NAME = "{pq_database_name}",
                AUTH_METHOD = "BASIC",
                LOGIN = "root",
                PASSWORD_SECRET_NAME = "{secret_name}"
            );)",
            "pq_source"_a = pqSourceName,
            "pq_location"_a = YDB_ENDPOINT,
            "pq_database_name"_a = YDB_DATABASE,
            "secret_name"_a = secretName
        ));
    }

    void CreateS3Source(const TString& bucket, const TString& s3SourceName) {
        ExecQuery(fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE `{s3_source}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{s3_location}",
                AUTH_METHOD="NONE"
            );)",
            "s3_source"_a = s3SourceName,
            "s3_location"_a = GetBucketLocation(bucket)
        ));
    }

    void CreateYdbSource(const TString& ydbSourceName) {
        ExecQuery(fmt::format(R"(
            CREATE SECRET ydb_source_secret WITH (value = "{token}");
            CREATE EXTERNAL DATA SOURCE `{ydb_source}` WITH (
                SOURCE_TYPE = "Ydb",
                LOCATION = "{ydb_location}",
                DATABASE_NAME = "{ydb_database_name}",
                AUTH_METHOD = "TOKEN",
                TOKEN_SECRET_PATH = "ydb_source_secret",
                USE_TLS = "FALSE"
            );)",
            "ydb_source"_a = ydbSourceName,
            "ydb_location"_a = YDB_ENDPOINT,
            "ydb_database_name"_a = YDB_DATABASE,
            "token"_a = BUILTIN_ACL_ROOT
        ));
    }

    void CreateSolomonSource(const TString& solomonSourceName) {
        ExecQuery(fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE `{solomon_source}` WITH (
                SOURCE_TYPE = "Solomon",
                LOCATION = "localhost:{solomon_port}",
                AUTH_METHOD = "NONE",
                USE_TLS = "false"
            );)",
            "solomon_source"_a = solomonSourceName,
            "solomon_port"_a = getenv("SOLOMON_HTTP_PORT")
        ));
    }

    // Script executions (using query client SDK)

    TOperation::TOperationId ExecScript(const TString& query, std::optional<TExecuteScriptSettings> settings = std::nullopt, bool waitRunning = true) {
        if (!settings) {
            settings.emplace()
                .StatsMode(EStatsMode::Profile);
        }

        const auto operation = GetQueryClient()->ExecuteScript(query, *settings).ExtractValueSync();
        const auto& status = operation.Status();
        UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToOneLineString());

        if (waitRunning) {
            WaitScriptExecution(operation.Id(), EExecStatus::Running);
            Sleep(TDuration::Seconds(1));
        }

        return operation.Id();
    }

    TScriptExecutionOperation GetScriptExecutionOperation(const TOperation::TOperationId& operationId, bool checkStatus = true) {
        const auto operation = GetOperationClient()->Get<NYdb::NQuery::TScriptExecutionOperation>(operationId).GetValueSync();

        if (checkStatus) {
            const auto& status = operation.Status();
            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToOneLineString());
        }

        return operation;
    }

    TScriptExecutionOperation WaitScriptExecution(const TOperation::TOperationId& operationId, EExecStatus finalStatus = EExecStatus::Completed, bool waitRetry = false) {
        const bool waitForFinalStatus = !waitRetry && IsIn({EExecStatus::Completed, EExecStatus::Failed}, finalStatus);

        std::optional<TScriptExecutionOperation> operation;
        WaitFor(TEST_OPERATION_TIMEOUT, TStringBuilder() << "script execution status" << finalStatus, [&](TString& error) {
            operation = GetScriptExecutionOperation(operationId, /* checkStatus */ false);

            const auto execStatus = operation->Metadata().ExecStatus;
            if (execStatus == finalStatus && (!waitForFinalStatus || operation->Ready())) {
                return true;
            }

            const auto& status = operation->Status();
            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, TStringBuilder() << "Status: " << execStatus << ", Issues: " << status.GetIssues().ToOneLineString());
            UNIT_ASSERT_C(!operation->Ready(), "Operation unexpectedly ready in status " << execStatus << " (expected status " << finalStatus << ")");

            error = TStringBuilder() << "operation status: " << execStatus << ", ready: " << operation->Ready();
            return false;
        });

        const auto& status = operation->Status();
        const auto execStatus = operation->Metadata().ExecStatus;
        UNIT_ASSERT_VALUES_EQUAL_C(execStatus, finalStatus, status.GetIssues().ToOneLineString());

        if (finalStatus != EExecStatus::Failed) {
            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToOneLineString());
        }

        UNIT_ASSERT_VALUES_EQUAL(operation->Ready(), waitForFinalStatus);
        return *operation;
    }

    TScriptExecutionOperation ExecAndWaitScript(const TString& query, EExecStatus finalStatus = EExecStatus::Completed, std::optional<TExecuteScriptSettings> settings = std::nullopt) {
        return WaitScriptExecution(ExecScript(query, settings, /* waitRunning */ false), finalStatus, false);
    }

    TResultSet FetchScriptResult(const TOperation::TOperationId& operationId, ui64 resultSetId = 0) {
        WaitScriptExecution(operationId);

        auto result = GetQueryClient()->FetchScriptResults(operationId, resultSetId).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

        return result.ExtractResultSet();
    }

    void CheckScriptResult(const TResultSet& result, ui64 columnsCount, ui64 rowsCount, std::function<void(TResultSetParser&)> validator) {
        TResultSetParser resultSet(result);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), columnsCount);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), rowsCount);

        for (ui64 i = 0; i < rowsCount; ++i) {
            UNIT_ASSERT_C(resultSet.TryNextRow(), i);
            validator(resultSet);
        }
    }

    void CheckScriptResult(const TOperation::TOperationId& operationId, ui64 columnsCount, ui64 rowsCount, std::function<void(TResultSetParser&)> validator, ui64 resultSetId = 0) {
        CheckScriptResult(FetchScriptResult(operationId, resultSetId), columnsCount, rowsCount, validator);
    }

    void CancelScriptExecution(const TOperation::TOperationId& operationId) {
        const auto& result = GetOperationClient()->Cancel(operationId).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
    }

    // Script executions (native events API)

    std::pair<TString, TOperation::TOperationId> ExecScriptNative(const TString& query, const TScriptQuerySettings& settings = {}, bool waitRunning = true) {
        auto& runtime = GetRuntime();

        NKikimrKqp::TEvQueryRequest queryProto;
        queryProto.SetUserToken(NACLib::TUserToken(BUILTIN_ACL_ROOT, TVector<NACLib::TSID>{runtime.GetAppData().AllAuthenticatedUsers}).SerializeAsString());

        auto& req = *queryProto.MutableRequest();
        req.SetDatabase(TEST_DATABASE);
        req.SetQuery(query);
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

    NKikimrKqp::TScriptExecutionRetryState::TMapping CreateRetryMapping(const std::vector<Ydb::StatusIds::StatusCode>& statuses, TDuration backoffDuration = TDuration::Seconds(5)) {
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

    NKikimrKqp::TQueryPhysicalGraph LoadPhysicalGraph(const TString& executionId) {
        auto& runtime = GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();
        runtime.Register(CreateGetScriptExecutionPhysicalGraphActor(edgeActor, TEST_DATABASE, executionId));

        const auto graph = runtime.GrabEdgeEvent<TEvGetScriptPhysicalGraphResponse>(edgeActor, TEST_OPERATION_TIMEOUT);
        UNIT_ASSERT_C(graph, "Empty graph response");
        UNIT_ASSERT_VALUES_EQUAL_C(graph->Get()->Status, Ydb::StatusIds::SUCCESS, graph->Get()->Issues.ToOneLineString());

        const auto& graphProto = graph->Get()->PhysicalGraph;
        UNIT_ASSERT(graphProto);

        return *graphProto;
    }

    void CheckScriptExecutionsCount(ui64 expectedExecutionsCount, ui64 expectedLeasesCount) {
        const auto& result = ExecQuery(R"(
            SELECT COUNT(*) FROM `.metadata/script_executions`;
            SELECT COUNT(*) FROM `.metadata/script_execution_leases`;
            )");

        UNIT_ASSERT_VALUES_EQUAL(result.size(), 2);

        CheckScriptResult(result[0], 1, 1, [&](TResultSetParser& resultSet) {
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUint64(), expectedExecutionsCount);
        });

        CheckScriptResult(result[1], 1, 1, [&](TResultSetParser& resultSet) {
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUint64(), expectedLeasesCount);
        });
    }

    void WaitCheckpointUpdate(const TString& checkpointId) {
        std::optional<uint64_t> minSeqNo;
        WaitFor(TEST_OPERATION_TIMEOUT, "checkpoint update", [&](TString& error) {
            const auto& result = ExecQuery(fmt::format(R"(
                SELECT MIN(seq_no) AS seq_no FROM `.metadata/streaming/checkpoints/checkpoints_metadata`
                WHERE graph_id = "{checkpoint_id}";
            )", "checkpoint_id"_a = checkpointId));
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

    static NYql::TGenericDataSourceInstance GetMockConnectorSourceInstance() {
        NYql::TGenericDataSourceInstance dataSourceInstance;
        dataSourceInstance.set_kind(NYql::YDB);
        dataSourceInstance.set_database(YDB_DATABASE);
        dataSourceInstance.set_use_tls(false);
        dataSourceInstance.set_protocol(NYql::NATIVE);

        auto& endpoint = *dataSourceInstance.mutable_endpoint();
        TIpPort port;
        NHttp::CrackAddress(YDB_ENDPOINT, *endpoint.mutable_host(), port);
        endpoint.set_port(port);

        auto& iamToken = *dataSourceInstance.mutable_credentials()->mutable_token();
        iamToken.set_type("IAM");
        iamToken.set_value(BUILTIN_ACL_ROOT);

        return dataSourceInstance;
    }

    template <typename TRequestBuilder>
    static void FillMockConnectorRequestColumns(TRequestBuilder& builder, const std::vector<TColumn>& columns) {
        for (const auto& column : columns) {
            builder.Column(column.Name, column.Type);
        }
    }

    // Should be called at most once
    static void SetupMockConnectorTableDescription(std::shared_ptr<TConnectorClientMock> mockClient, const TMockConnectorTableDescriptionSettings& settings) {
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
    static void SetupMockConnectorTableData(std::shared_ptr<TConnectorClientMock> mockClient, const TMockConnectorReadSplitsSettings& settings) {
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

    static std::function<void(const TString)> AstChecker(ui64 txCount, ui64 stagesCount) {
        const auto stringCounter = [](const TString& str, const TString& subStr) {
            ui64 count = 0;
            for (size_t i = str.find(subStr); i != TString::npos; i = str.find(subStr, i + subStr.size())) {
                ++count;
            }
            return count;
        };

        return [txCount, stagesCount, stringCounter](const TString& ast) {
            UNIT_ASSERT_VALUES_EQUAL(stringCounter(ast, "KqpPhysicalTx"), txCount);
            UNIT_ASSERT_VALUES_EQUAL(stringCounter(ast, "DqPhyStage"), stagesCount);
        };
    }

private:
    void EnsureNotInitialized(const TString& info) {
        UNIT_ASSERT_C(!Kikimr, "Kikimr runner is already initialized, can not setup " << info);
    }

    void TearDown(NUnitTest::TTestContext& context) final {
        PqGateway.Reset();
        TBase::TearDown(context);
    }

    virtual bool UseSchemaSecrets() {
        return false;
    }

protected:
    TDuration CheckpointPeriod = TDuration::MilliSeconds(200);
    TTestLogSettings LogSettings;
    bool InternalInitFederatedQuerySetupFactory = false;
    TClientSettings QueryClientSettings = TClientSettings().AuthToken(BUILTIN_ACL_ROOT);
    NTopic::TTopicClientSettings TopicClientSettings = NTopic::TTopicClientSettings().AuthToken(BUILTIN_ACL_ROOT);

private:
    std::optional<NKikimrConfig::TAppConfig> AppConfig;
    TIntrusivePtr<NYql::IPqGateway> PqGateway;
    NYql::NConnector::IClient::TPtr ConnectorClient;
    std::shared_ptr<TKikimrRunner> Kikimr;

    std::shared_ptr<TDriver> InternalDriver;
    std::shared_ptr<NOperation::TOperationClient> OperationClient;
    std::shared_ptr<TQueryClient> QueryClient;
    std::shared_ptr<NYdb::NTable::TTableClient> TableClient;
    std::shared_ptr<NYdb::NTable::TSession> TableClientSession;
    std::shared_ptr<NTopic::TTopicClient> LocalTopicClient;
    std::shared_ptr<NKikimr::NPersQueueTests::TFlatMsgBusPQClient> LocalFlatMsgBusPQClient;

    // Attached to database from recipe (YDB_ENDPOINT / YDB_DATABASE)
    std::shared_ptr<TDriver> ExternalDriver;
    std::shared_ptr<NTopic::TTopicClient> TopicClient;
    std::shared_ptr<TQueryClient> ExternalQueryClient;
};

class TStreamingWithSchemaSecretsTestFixture : public TStreamingTestFixture {
public:
    bool UseSchemaSecrets() override {
        return true;
    }
};

class TStreamingSysViewTestFixture : public TStreamingTestFixture {
public:
    inline static constexpr ui64 SYS_VIEW_COLUMNS_COUNT = 13;
    inline static constexpr char INPUT_TOPIC_NAME[] = "sysViewInput";
    inline static constexpr char OUTPUT_TOPIC_NAME[] = "sysViewOutput";
    inline static constexpr char PQ_SOURCE[] = "sysViewSourceName";
    inline static constexpr TDuration STATS_WAIT_DURATION = TDuration::Seconds(2);

public:
    void Setup() {
        LogSettings.AddLogPriority(NKikimrServices::SYSTEM_VIEWS, NLog::PRI_DEBUG);
        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        UNIT_ASSERT_C(!InputTopic, "Setup called twice");
        InputTopic = TStringBuilder() << INPUT_TOPIC_NAME << Name_;
        OutputTopic = TStringBuilder() << OUTPUT_TOPIC_NAME << Name_;
        CreateTopic(InputTopic);
        CreateTopic(OutputTopic);
        CreatePqSource(PQ_SOURCE);
    }

    void StartQuery(const TString& name) {
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS DO BEGIN{text}END DO)",
            "query_name"_a = name,
            "text"_a = GetQueryText(name)
        ));
    }

    struct TSysViewRow {
        TString Name;
        TString Status = "RUNNING";
        TString Issues = "{}";
        std::optional<TString> Ast;
        std::optional<TString> Text;
        bool Run = true;
        TString Pool = "default";
        ui64 RetryCount = 0;
        std::optional<TInstant> LastFailAt;
        std::optional<TInstant> SuspendedUntil;
        bool CheckPlan = false;
    };

    struct TSysViewResult {
        TString ExecutionId;
        std::vector<TString> PreviousExecutionIds;
    };

    std::vector<TSysViewResult> CheckSysView(std::vector<TSysViewRow> rows, const TString& filter = "", const TString& order = "Path ASC") {
        const auto& result = ExecQuery(TStringBuilder()
            << "SELECT * FROM `.sys/streaming_queries`"
            << (filter ? " WHERE " + filter : "")
            << (order ? " ORDER BY " + order : "")
        );
        UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);

        if (order.EndsWith("ASC")) {
            Sort(rows, [&](const auto& lhs, const auto& rhs) {
                return lhs.Name < rhs.Name;
            });
        } else if (order.EndsWith("DESC")) {
            Sort(rows, [&](const auto& lhs, const auto& rhs) {
                return lhs.Name > rhs.Name;
            });
        }

        std::vector<TSysViewResult> results;
        CheckScriptResult(result[0], SYS_VIEW_COLUMNS_COUNT, rows.size(), [&](TResultSetParser& resultSet) {
            const auto& row = rows[results.size()];
            auto& result = results.emplace_back();
            UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser("Path").GetOptionalUtf8(), JoinPath({"/Root", row.Name}));
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

protected:
    TString GetQueryText(const TString& name) const {
        UNIT_ASSERT_C(InputTopic, "Setup is not called");

        return fmt::format(R"(
            ;INSERT INTO `{pq_source}`.`{output_topic}`
            /* {query_name} */
            SELECT * FROM `{pq_source}`.`{input_topic}`;)",
            "query_name"_a = name,
            "pq_source"_a = PQ_SOURCE,
            "input_topic"_a = InputTopic,
            "output_topic"_a = OutputTopic
        );
    }

protected:
    TString InputTopic;
    TString OutputTopic;
};

class TTestTopicLoader : public TActorBootstrapped<TTestTopicLoader> {
public:
    TTestTopicLoader(const TString& endpoint, const TString& database, const TString& topic, NThreading::TFuture<void> feature)
        : Client(TDriver(TDriverConfig()
            .SetEndpoint(endpoint)
            .SetDatabase(database)
        ))
        , WriteSession(Client.CreateWriteSession(NTopic::TWriteSessionSettings().Path(topic)))
        , Feature(feature)
        , Message(1_KB, 'x')
    {}

    STRICT_STFUNC(StateFunc,
        sFunc(TEvents::TEvWakeup, WriteMessages)
    )

    void Bootstrap() {
        Become(&TTestTopicLoader::StateFunc);
        Schedule(Timeout, new TEvents::TEvWakeup());
        WriteMessages();
    }

    void WriteMessages() {
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

        if (std::holds_alternative<NTopic::TSessionClosedEvent>(*event)) {
            const auto& status = std::get<NTopic::TSessionClosedEvent>(*event);
            UNIT_FAIL(status.GetStatus() << ", issues: " << status.GetIssues().ToOneLineString());
        }

        if (std::holds_alternative<NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(*event)) {
            WriteSession->Write(
                std::move(std::get<NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(*event).ContinuationToken),
                Message
            );
        }

        Schedule(TDuration::Zero(), new TEvents::TEvWakeup());
    }

private:
    NTopic::TTopicClient Client;
    const std::shared_ptr<NTopic::IWriteSession> WriteSession;
    const NThreading::TFuture<void> Feature;
    const TString Message;
    const TInstant Timeout = TInstant::Now() + TDuration::Seconds(60);
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpFederatedQueryDatastreams) {
    Y_UNIT_TEST_F(CreateExternalDataSource, TStreamingTestFixture) {
        CreatePqSource("sourceName");

        // DataStreams is not allowed.
        ExecSchemeQuery(TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `sourceName2` WITH (
                SOURCE_TYPE="DataStreams",
                LOCATION=")" << YDB_ENDPOINT << R"(",
                DATABASE_NAME=")" << YDB_DATABASE << R"(",
                AUTH_METHOD="NONE"
            );)", EStatus::SCHEME_ERROR);

        // YdbTopics is not allowed.
        ExecSchemeQuery(TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `sourceName2` WITH (
                SOURCE_TYPE=")" << ToString(NYql::EDatabaseType::YdbTopics) << R"(",
                LOCATION=")" << YDB_ENDPOINT << R"(",
                DATABASE_NAME=")" << YDB_DATABASE << R"(",
                AUTH_METHOD="NONE"
            );)", EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST_F(CreateExternalDataSourceBasic, TStreamingTestFixture) {
        CreatePqSourceBasicAuth("sourceName");
    }

    Y_UNIT_TEST_F(FailedWithoutAvailableExternalDataSourcesYdb, TStreamingTestFixture) {
        SetupAppConfig().MutableQueryServiceConfig()->SetAllExternalDataSourcesAreAvailable(false);

        ExecSchemeQuery(TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `sourceName` WITH (
                SOURCE_TYPE="Ydb",
                LOCATION=")" << YDB_ENDPOINT << R"(",
                DATABASE_NAME=")" << YDB_DATABASE << R"(",
                AUTH_METHOD="NONE"
            );)", EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST_F(CheckAvailableExternalDataSourcesYdb, TStreamingTestFixture) {
        auto& cfg = *SetupAppConfig().MutableQueryServiceConfig();
        cfg.AddAvailableExternalDataSources("Ydb");
        cfg.SetAllExternalDataSourcesAreAvailable(false);

        CreatePqSource("sourceName");
    }

    Y_UNIT_TEST_F(ReadTopicFailedWithoutAvailableExternalDataSourcesYdbTopics, TStreamingTestFixture) {
        auto& cfg = *SetupAppConfig().MutableQueryServiceConfig();
        cfg.AddAvailableExternalDataSources("Ydb");
        cfg.SetAllExternalDataSourcesAreAvailable(false);

        TString sourceName = "sourceName";
        CreatePqSource(sourceName);

        TString topicName = "topicName";
        CreateTopic(topicName);

        const auto scriptExecutionOperation = ExecAndWaitScript(fmt::format(R"(
            SELECT * FROM `{source}`.`{topic}` WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA = (
                    key String NOT NULL,
                    value String NOT NULL
                )
            )
            LIMIT 1;
            )",
            "source"_a=sourceName,
            "topic"_a=topicName
        ), EExecStatus::Failed);

        const auto& status = scriptExecutionOperation.Status();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::GENERIC_ERROR, status.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRING_CONTAINS(status.GetIssues().ToString(), "Unsupported. Failed to load metadata for table: /Root/sourceName.[topicName] data source generic doesn't exist");
    }

    Y_UNIT_TEST_F(ReadTopicEndpointValidationWithoutAvailableExternalDataSourcesYdbTopics, TStreamingTestFixture) {
        auto& cfg = *SetupAppConfig().MutableQueryServiceConfig();
        cfg.AddAvailableExternalDataSources("Ydb");
        cfg.SetAllExternalDataSourcesAreAvailable(false);

        constexpr char sourceName[] = "sourceName";
        CreatePqSource(sourceName);

        // Execute script without existing topic
        const auto scriptExecutionOperation = ExecAndWaitScript(fmt::format(R"(
            SELECT * FROM `{source}`.`topicName` WITH (STREAMING = "TRUE")
            )",
            "source"_a=sourceName
        ), EExecStatus::Failed);

        const auto& status = scriptExecutionOperation.Status();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::GENERIC_ERROR, status.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRING_CONTAINS(status.GetIssues().ToString(), "Unsupported. Failed to load metadata for table: /Root/sourceName.[topicName] data source generic doesn't exist");
    }

    Y_UNIT_TEST_F(ReadTopicEndpointValidation, TStreamingTestFixture) {
        constexpr char sourceName[] = "sourceName";
        CreatePqSource(sourceName);

        // Execute script without existing topic
        const auto scriptExecutionOperation = ExecAndWaitScript(fmt::format(R"(
            SELECT * FROM `{source}`.`topicName` WITH (STREAMING = "TRUE")
            )",
            "source"_a=sourceName
        ), EExecStatus::Failed);

        const auto& status = scriptExecutionOperation.Status();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::GENERIC_ERROR, status.GetIssues().ToOneLineString());
        const auto& issues = status.GetIssues().ToString();
        UNIT_ASSERT_STRING_CONTAINS(issues, "Couldn't determine external YDB entity type");
        UNIT_ASSERT_STRING_CONTAINS(issues, "Describe path 'local/topicName' in external YDB database '/local'");
    }

    Y_UNIT_TEST_F(ReadTopic, TStreamingTestFixture) {
        auto& cfg = *SetupAppConfig().MutableQueryServiceConfig();
        cfg.AddAvailableExternalDataSources("Ydb");
        cfg.AddAvailableExternalDataSources("YdbTopics");
        cfg.SetAllExternalDataSourcesAreAvailable(false);

        TString sourceName = "sourceName";
        TString topicName = "topicName";
        TString tableName = "tableName";
        ui32 partitionCount = 10;

        CreateTopic(topicName, NTopic::TCreateTopicSettings()
            .PartitioningSettings(partitionCount, partitionCount));

        CreatePqSource(sourceName);

        const auto scriptExecutionOperation = ExecScript(fmt::format(R"(
            SELECT * FROM `{source}`.`{topic}` WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA = (
                    key String NOT NULL,
                    value String NOT NULL
                )
            )
            LIMIT {partition_count};
            )",
            "source"_a=sourceName,
            "topic"_a=topicName,
            "partition_count"_a=partitionCount
        ));

        for (ui32 i = 0; i < partitionCount; ++i) {
            WriteTopicMessage(topicName, R"({"key": "key1", "value": "value1"})", i);
        }

        CheckScriptResult(scriptExecutionOperation, 2, partitionCount, [](TResultSetParser& result) {
            UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser(0).GetString(), "key1");
            UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser(1).GetString(), "value1");
        });
    }

    Y_UNIT_TEST_F(ReadTopicBasicNewSecrets, TStreamingWithSchemaSecretsTestFixture) {
        TestReadTopicBasic("-with-new-secret");
    }

    Y_UNIT_TEST_F(ReadTopicBasicOldSecrets, TStreamingTestFixture) {
        TestReadTopicBasic("-with-old-secret");
    }

    Y_UNIT_TEST_F(ReadTopicExplainBasic, TStreamingTestFixture) {
        const TString sourceName = "sourceName";
        const TString topicName = "topicName";
        CreateTopic(topicName);

        CreatePqSourceBasicAuth(sourceName);

        const auto result = GetQueryClient()->ExecuteQuery(fmt::format(
            R"(SELECT * FROM `{source}`.`{topic}` WITH (STREAMING = "TRUE"))",
            "source"_a=sourceName,
            "topic"_a=topicName
        ), TTxControl::NoTx(), TExecuteQuerySettings().ExecMode(EExecMode::Explain)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        const auto& stats = result.GetStats();
        UNIT_ASSERT(stats);

        const auto& plan = stats->GetPlan();
        UNIT_ASSERT(plan);
        UNIT_ASSERT_STRING_CONTAINS(*plan, sourceName);

        const auto& ast = stats->GetAst();
        UNIT_ASSERT(ast);
        UNIT_ASSERT_STRING_CONTAINS(*ast, sourceName);
    }

    Y_UNIT_TEST_F(InsertTopicBasic, TStreamingTestFixture) {
        SetupAppConfig().MutableQueryServiceConfig()->SetProgressStatsPeriodMs(1000);

        TString sourceName = "sourceName";
        TString inputTopicName = "inputTopicName";
        TString outputTopicName = "outputTopicName";
        TString tableName = "tableName";

        CreateTopic(outputTopicName);
        CreateTopic(inputTopicName);

        CreatePqSourceBasicAuth(sourceName);

        const auto scriptExecutionOperation = ExecScript(fmt::format(R"(
            $input = SELECT key, value FROM `{source}`.`{input_topic}` WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA = (
                    key String NOT NULL,
                    value String NOT NULL
                )
            );
            INSERT INTO `{source}`.`{output_topic}`
                SELECT key || value FROM $input;
            )",
            "source"_a=sourceName,
            "input_topic"_a=inputTopicName,
            "output_topic"_a=outputTopicName
        ));

        WriteTopicMessage(inputTopicName, R"({"key": "key1", "value": "value1"})");
        ReadTopicMessage(outputTopicName, "key1value1");

        WaitFor(TDuration::Seconds(5), "operation AST", [&](TString& error) {
            const auto& operation = GetScriptExecutionOperation(scriptExecutionOperation);
            const auto& metadata = operation.Metadata();
            if (const auto& ast = metadata.ExecStats.GetAst()) {
                UNIT_ASSERT_STRING_CONTAINS(*ast, sourceName);
                return true;
            }

            error = TStringBuilder() << "AST is not available, status: " << metadata.ExecStatus;
            return false;
        });

        CancelScriptExecution(scriptExecutionOperation);
    }

    Y_UNIT_TEST_F(ReadTopicWithColumnOrder, TStreamingTestFixture) {
        constexpr char topicName[] = "readTopicWithColumnOrder";
        CreateTopic(topicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        const auto op = ExecScript(fmt::format(R"(
            PRAGMA OrderedColumns;
            SELECT * FROM `{source}`.`{topic}` WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA (
                    key String NOT NULL,
                    value String NOT NULL
                )
            ) LIMIT 1;

            SELECT * FROM `{source}`.`{topic}` WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA (
                    value String NOT NULL,
                    key String NOT NULL
                )
            ) LIMIT 1;
            )",
            "source"_a=pqSourceName,
            "topic"_a=topicName
        ));

        WriteTopicMessage(topicName, R"({"key":"key1", "value": "value1"})");

        CheckScriptResult(op, 2, 1, [](TResultSetParser& result) {
            UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser(0).GetString(), "key1");
            UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser(1).GetString(), "value1");
        }, 0);

        CheckScriptResult(op, 2, 1, [](TResultSetParser& result) {
            UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser(0).GetString(), "value1");
            UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser(1).GetString(), "key1");
        }, 1);
    }

    Y_UNIT_TEST_F(ReadTopicWithDefaultSchema, TStreamingTestFixture) {
        constexpr char topicName[] = "readTopicWithDefaultSchema";
        CreateTopic(topicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        const auto op = ExecScript(fmt::format(R"(
            PRAGMA OrderedColumns;
            SELECT * FROM `{source}`.`{topic}` WITH (STREAMING = "TRUE") LIMIT 1;
            )",
            "source"_a=pqSourceName,
            "topic"_a=topicName
        ));

        WriteTopicMessage(topicName, R"({"key":"key1", "value": "value1"})");

        CheckScriptResult(op, 1, 1, [](TResultSetParser& result) {
            UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser(0).GetString(), R"({"key":"key1", "value": "value1"})");
        });
    }

    Y_UNIT_TEST_F(RestoreScriptPhysicalGraphBasic, TStreamingTestFixture) {
        constexpr char writeBucket[] = "test_bucket_restore_script_physical_graph";
        CreateBucket(writeBucket);

        constexpr char topicName[] = "restoreScriptTopic";
        CreateTopic(topicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char s3SinkName[] = "s3Sink";
        CreateS3Source(writeBucket, s3SinkName);

        const auto executeQuery = [&](TScriptQuerySettings settings) {
            const auto& [executionId, operationId] = ExecScriptNative(fmt::format(R"(
                INSERT INTO `{s3_sink}`.`folder/` WITH (FORMAT = "json_each_row")
                SELECT * FROM `{source}`.`{topic}` WITH (
                    STREAMING = "TRUE",
                    FORMAT = "json_each_row",
                    SCHEMA = (
                        key String NOT NULL,
                        value String NOT NULL
                    )
                ) LIMIT 1;)",
                "s3_sink"_a = s3SinkName,
                "source"_a = pqSourceName,
                "topic"_a = topicName
            ), settings);

            WriteTopicMessage(topicName, R"({"key": "key1", "value": "value1"})");
            WaitScriptExecution(operationId);

            return executionId;
        };

        const auto executionId = executeQuery({.SaveState = true});
        const TString sampleResult = "{\"key\":\"key1\",\"value\":\"value1\"}\n";
        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(writeBucket), sampleResult);

        ExecQuery(fmt::format(R"(
            DROP EXTERNAL DATA SOURCE `{s3_sink}`;
            DROP EXTERNAL DATA SOURCE `{pq_source}`;)",
            "s3_sink"_a = s3SinkName,
            "pq_source"_a = pqSourceName
        ));

        executeQuery({.PhysicalGraph = LoadPhysicalGraph(executionId)});
        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(writeBucket), TStringBuilder() << sampleResult << sampleResult);
    }

    Y_UNIT_TEST_F(RestoreScriptPhysicalGraphGroupByHop, TStreamingTestFixture) {
        constexpr char sourceTopicName[] = "restoreScriptGroupByHopTopicSource";
        constexpr char sinkTopicName[] = "restoreScriptGroupByHopTopicSink";
        CreateTopic(sourceTopicName);
        CreateTopic(sinkTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        TVector<TString> expectedMessages;
        const auto executeQuery = [&](TScriptQuerySettings settings) {
            const auto& [executionId, operationId] = ExecScriptNative(fmt::format(R"(
                $input = SELECT * FROM `{source}`.`{source_topic}` WITH (
                    STREAMING = "TRUE",
                    FORMAT = "json_each_row",
                    SCHEMA (
                        time String,
                        event String NOT NULL
                    )
                );

                INSERT INTO `{source}`.`{sink_topic}`
                SELECT
                    event
                FROM $input
                GROUP BY
                    HOP (CAST(time AS Timestamp), "PT1H", "PT1H", "PT0H"),
                    event
                LIMIT 1;)",
                "source_topic"_a = sourceTopicName,
                "sink_topic"_a = sinkTopicName,
                "source"_a = pqSourceName
            ), settings);

            WriteTopicMessages(sourceTopicName, {
                R"({"time": "2025-08-24T00:00:00.000000Z", "event": "A"})",
                R"({"time": "2025-08-25T00:00:00.000000Z", "event": "B"})"
            });

            expectedMessages.emplace_back("A");
            ReadTopicMessages(sinkTopicName, expectedMessages);

            WaitScriptExecution(operationId);

            return executionId;
        };

        const auto executionId = executeQuery({.SaveState = true});

        ExecQuery(fmt::format(R"(
            DROP EXTERNAL DATA SOURCE `{source}`;)",
            "source"_a = pqSourceName
        ));

        executeQuery({.PhysicalGraph = LoadPhysicalGraph(executionId)});
    }

    Y_UNIT_TEST_F(RestoreScriptPhysicalGraphOnRetry, TStreamingTestFixture) {
        const auto pqGateway = SetupMockPqGateway();

        constexpr char writeBucket[] = "test_bucket_restore_script_physical_graph_on_retry";
        CreateBucket(writeBucket);

        constexpr char topicName[] = "restoreScriptTopicOnRetry";
        CreateTopic(topicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char s3SinkName[] = "s3Sink";
        CreateS3Source(writeBucket, s3SinkName);

        const auto& [_, operationId] = ExecScriptNative(fmt::format(R"(
            PRAGMA s3.AtomicUploadCommit = "true";

            INSERT INTO `{s3_sink}`.`folder/` WITH (FORMAT = "json_each_row")
            SELECT * FROM `{source}`.`{topic}` WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA = (
                    key String NOT NULL,
                    value String NOT NULL
                )
            ) LIMIT 1;)",
            "s3_sink"_a = s3SinkName,
            "source"_a = pqSourceName,
            "topic"_a = topicName
        ), {
            .SaveState = true,
            .RetryMapping = CreateRetryMapping({Ydb::StatusIds::BAD_REQUEST})
        });

        pqGateway->WaitReadSession(topicName)->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});
        WaitScriptExecution(operationId, EExecStatus::Failed, true);

        ExecQuery(fmt::format(R"(
            DROP EXTERNAL DATA SOURCE `{s3_sink}`;
            DROP EXTERNAL DATA SOURCE `{pq_source}`;)",
            "s3_sink"_a = s3SinkName,
            "pq_source"_a = pqSourceName
        ));

        pqGateway->WaitReadSession(topicName)->AddDataReceivedEvent(1, R"({"key": "key1", "value": "value1"})");
        WaitScriptExecution(operationId);

        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(writeBucket), "{\"key\":\"key1\",\"value\":\"value1\"}\n");
        UNIT_ASSERT_VALUES_EQUAL(GetUncommittedUploadsCount(writeBucket), 0);
    }

    Y_UNIT_TEST_F(RestoreScriptPhysicalGraphOnRetryWithCheckpoints, TStreamingTestFixture) {
        const auto pqGateway = SetupMockPqGateway();

        constexpr char inputTopicName[] = "inputTopicName";
        CreateTopic(inputTopicName);
        constexpr char outputTopicName[] = "outputTopicName";
        CreateTopic(outputTopicName);

        constexpr char sourceName[] = "sourceName";
        CreatePqSource(sourceName);

        const TString checkpointId = CreateGuidAsString();
        const auto& [executionId, operationId] = ExecScriptNative(fmt::format(R"(
                $input = SELECT key, value FROM `{source}`.`{input_topic}` WITH (
                    STREAMING = "TRUE",
                    FORMAT = "json_each_row",
                    SCHEMA = (
                        key String NOT NULL,
                        value String NOT NULL
                    )
                );
                INSERT INTO `{source}`.`{output_topic}`
                    SELECT key || value FROM $input;)",
                "source"_a = sourceName,
                "input_topic"_a = inputTopicName,
                "output_topic"_a = outputTopicName
            ), {
                .SaveState = true,
                .RetryMapping = CreateRetryMapping({Ydb::StatusIds::BAD_REQUEST}),
                .CheckpointId = checkpointId
            });

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");
        ExecQuery("GRANT ALL ON `/Root/.metadata` TO `" BUILTIN_ACL_ROOT "`");
        ExecQuery("GRANT ALL ON `/Root/.metadata/streaming` TO `" BUILTIN_ACL_ROOT "`");
        WaitCheckpointUpdate(checkpointId);

        auto readSession = pqGateway->WaitReadSession(inputTopicName);
        auto writeSession = pqGateway->WaitWriteSession(outputTopicName);
        readSession->AddDataReceivedEvent(1, R"({"key": "key1", "value": "value1"})");
        readSession->AddDataReceivedEvent(2, R"({"key": "key2", "value": "value2"})");
        readSession->AddDataReceivedEvent(3, R"({"key": "key3", "value": "value3"})");
        WaitCheckpointUpdate(checkpointId);
        readSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});

        WaitScriptExecution(operationId, EExecStatus::Failed, true);
        WaitCheckpointUpdate(checkpointId);

        pqGateway->WaitReadSession(inputTopicName)->AddDataReceivedEvent(4, R"({"key": "key4", "value": "value4"})");
        writeSession = pqGateway->WaitWriteSession(outputTopicName);
        writeSession->ExpectMessage("key4value4");

        CancelScriptExecution(operationId);
    }

    Y_UNIT_TEST_F(CheckpointsPropagationWithGroupByHop, TStreamingTestFixture) {
        LogSettings.Freeze = true;
        CheckpointPeriod = TDuration::Seconds(5);

        constexpr char inputTopicName[] = "inputTopicName";
        constexpr char outputTopicName[] = "outputTopicName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char sourceName[] = "sourceName";
        CreatePqSource(sourceName);

        const TString checkpointId = CreateGuidAsString();
        const auto& [executionId, operationId] = ExecScriptNative(fmt::format(R"(
            INSERT INTO `{source}`.`{output_topic}`
            SELECT event FROM `{source}`.`{input_topic}` WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA (
                    time String,
                    event String NOT NULL
                )
            )
            GROUP BY HOP (CAST(time AS Timestamp), "PT1H", "PT1H", "PT0H"), event;)",
            "source"_a = sourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ), {
            .SaveState = true,
            .CheckpointId = checkpointId
        });

        WriteTopicMessages(inputTopicName, {
            R"({"time": "2025-08-24T00:00:00.000000Z", "event": "A"})",
            R"({"time": "2025-08-25T00:00:00.000000Z", "event": "B"})"
        });
        ReadTopicMessage(outputTopicName, "A");
        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");
        ExecQuery("GRANT ALL ON `/Root/.metadata` TO `" BUILTIN_ACL_ROOT "`");
        ExecQuery("GRANT ALL ON `/Root/.metadata/streaming` TO `" BUILTIN_ACL_ROOT "`");
        WaitCheckpointUpdate(checkpointId);

        const auto& result = ExecQuery(fmt::format(R"(
            SELECT COUNT(*) AS states_count FROM (
                SELECT DISTINCT task_id FROM `.metadata/streaming/checkpoints/states`
                WHERE graph_id = "{checkpoint_id}"
            )
        )", "checkpoint_id"_a = checkpointId));
        UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);

        CheckScriptResult(result[0], 1, 1, [](TResultSetParser& resultSet) {
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUint64(), 2);
        });

        WaitFor(TDuration::Seconds(10), "operation stats", [&](TString& error) {
            const auto metadata = GetScriptExecutionOperation(operationId).Metadata();
            const auto& plan = metadata.ExecStats.GetPlan();
            if (plan && plan->contains("MultiHop_NewHopsCount")) {
                return true;
            }

            error = TStringBuilder() << "plan is not available, status: " << metadata.ExecStatus << ", plan: " << plan.value_or("");
            return false;
        });
    }

    Y_UNIT_TEST_F(CheckpointsOnNotDrainedChannels, TStreamingTestFixture) {
        LogSettings.Freeze = true;

        CheckpointPeriod = TDuration::Seconds(3);
        const auto pqGateway = SetupMockPqGateway();

        constexpr char inputTopicName[] = "inputTopicName";
        constexpr char outputTopicName[] = "outputTopicName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char sourceName[] = "sourceName";
        CreatePqSource(sourceName);

        const TString checkpointId = CreateGuidAsString();
        const auto& [executionId, operationId] = ExecScriptNative(fmt::format(R"(
            INSERT INTO `{source}`.`{output_topic}`
            SELECT event FROM `{source}`.`{input_topic}` WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA (
                    time String,
                    event String NOT NULL
                )
            )
            GROUP BY HOP (CAST(time AS Timestamp), "PT1H", "PT1H", "PT0H"), event;)",
            "source"_a = sourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ), {
            .SaveState = true,
            .CheckpointId = checkpointId
        });
        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");
        ExecQuery("GRANT ALL ON `/Root/.metadata` TO `" BUILTIN_ACL_ROOT "`");
        ExecQuery("GRANT ALL ON `/Root/.metadata/streaming` TO `" BUILTIN_ACL_ROOT "`");

        auto writeSession = pqGateway->WaitWriteSession(outputTopicName);
        WaitCheckpointUpdate(checkpointId);
        writeSession->Lock();

        auto readSession = pqGateway->WaitReadSession(inputTopicName);
        const TString value(1_KB, 'x');
        TInstant time = TInstant::Now();
        for (ui64 i = 0; i < 100000; ++i, time += TDuration::Hours(2)) {
            readSession->AddDataReceivedEvent(i, TStringBuilder() << R"({"time": ")" << time.ToString() << R"(", "event": ")" << value << R"("})");
        }

        Sleep(TDuration::Seconds(6));
        writeSession->Unlock();

        for (ui64 i = 0; i < 3; ++i) {
            WaitCheckpointUpdate(checkpointId);
        }
    }

    Y_UNIT_TEST_F(S3RuntimeListingDisabledForStreamingQueries, TStreamingTestFixture) {
        constexpr char sourceBucket[] = "test_bucket_disable_runtime_listing";
        constexpr char objectPath[] = "test_bucket_object.json";
        constexpr char objectContent[] = R"({"data": "x"})";
        CreateBucketWithObject(sourceBucket, objectPath, objectContent);

        constexpr char s3SourceName[] = "s3Source";
        CreateS3Source(sourceBucket, s3SourceName);

        const auto& [_, operationId] = ExecScriptNative(fmt::format(R"(
            PRAGMA s3.UseRuntimeListing = "true";
            INSERT INTO `{s3_source}`.`path/` WITH (
                FORMAT = "json_each_row"
            ) SELECT * FROM `{s3_source}`.`{object_path}` WITH (
                FORMAT = "json_each_row",
                SCHEMA (
                    data String NOT NULL
                )
            )
        )", "s3_source"_a = s3SourceName, "object_path"_a = objectPath), {
            .SaveState = true
        }, /* waitRunning */ false);

        const auto& readyOp = WaitScriptExecution(operationId);
        UNIT_ASSERT_STRING_CONTAINS(readyOp.Status().GetIssues().ToString(), "Runtime listing is not supported for streaming queries, pragma value was ignored");
        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(sourceBucket), "{\"data\":\"x\"}\n{\"data\": \"x\"}");
    }

    Y_UNIT_TEST_F(S3AtomicUploadCommitDisabledForStreamingQueries, TStreamingTestFixture) {
        constexpr char sourceBucket[] = "test_bucket_disable_atomic_upload_commit";
        constexpr char objectPath[] = "test_bucket_object.json";
        constexpr char objectContent[] = R"({"data": "x"})";
        CreateBucketWithObject(sourceBucket, objectPath, objectContent);

        constexpr char s3SourceName[] = "s3Source";
        CreateS3Source(sourceBucket, s3SourceName);

        const auto& [_, operationId] = ExecScriptNative(fmt::format(R"(
            PRAGMA s3.AtomicUploadCommit = "true";
            INSERT INTO `{s3_source}`.`path/` WITH (
                FORMAT = "json_each_row"
            ) SELECT * FROM `{s3_source}`.`{object_path}` WITH (
                FORMAT = "json_each_row",
                SCHEMA (
                    data String NOT NULL
                )
            )
        )", "s3_source"_a = s3SourceName, "object_path"_a = objectPath), {
            .SaveState = true
        }, /* waitRunning */ false);

        const auto& readyOp = WaitScriptExecution(operationId);
        UNIT_ASSERT_STRING_CONTAINS(readyOp.Status().GetIssues().ToString(), "Atomic upload commit is not supported for streaming queries, pragma value was ignored");
        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(sourceBucket), "{\"data\":\"x\"}\n{\"data\": \"x\"}");
    }

    Y_UNIT_TEST_F(S3PartitioningKeysFlushTimeout, TStreamingTestFixture) {
        const auto pqGateway = SetupMockPqGateway();
        constexpr char sourceBucket[] = "test_bucket_partitioning_keys_flush";
        constexpr char s3SourceName[] = "s3Source";
        CreateBucket(sourceBucket);
        CreateS3Source(sourceBucket, s3SourceName);

        constexpr char inputTopicName[] = "inputTopicName";
        constexpr char pqSourceName[] = "pqSourceName";
        CreateTopic(inputTopicName);
        CreatePqSource(pqSourceName);

        const auto& [_, operationId] = ExecScriptNative(fmt::format(R"(
            PRAGMA s3.OutputKeyFlushTimeout = "1s";
            PRAGMA ydb.DisableCheckpoints = "TRUE";
            PRAGMA ydb.MaxTasksPerStage = "1";

            INSERT INTO `{s3_source}`.`path/` WITH (
                FORMAT = json_each_row,
                PARTITIONED_BY = key
            ) SELECT * FROM `{pq_source}`.`{input_topic}` WITH (
                STREAMING = "TRUE",
                FORMAT = json_each_row,
                SCHEMA (
                    data String NOT NULL,
                    key Uint64 NOT NULL
                )
            )
        )", "s3_source"_a = s3SourceName, "pq_source"_a = pqSourceName, "input_topic"_a = inputTopicName), {
            .SaveState = true
        });

        auto readSession = pqGateway->WaitReadSession(inputTopicName);
        readSession->AddDataReceivedEvent(0, R"({"data": "x", "key": 0})");

        Sleep(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(sourceBucket), "");

        readSession->AddDataReceivedEvent(1, R"({"data": "y", "key": 1})");

        Sleep(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(sourceBucket), "{\"data\":\"x\"}\n");
    }

    Y_UNIT_TEST_F(CrossJoinWithNotExistingDataSource, TStreamingTestFixture) {
        const auto connectorClient = SetupMockConnectorClient();

        constexpr char ydbSourceName[] = "ydbSourceName";
        CreateYdbSource(ydbSourceName);

        constexpr char ydbTable[] = "unknownSourceLookup";
        ExecExternalQuery(fmt::format(R"(
            CREATE TABLE `{table}` (
                fqdn String,
                payload String,
                PRIMARY KEY (fqdn)
            ))",
            "table"_a = ydbTable
        ));

        {   // Prepare connector mock
            const std::vector<TColumn> columns = {
                {"fqdn", Ydb::Type::STRING},
                {"payload", Ydb::Type::STRING}
            };
            SetupMockConnectorTableDescription(connectorClient, {
                .TableName = ydbTable,
                .Columns = columns,
                .DescribeCount = 1,
                .ListSplitsCount = 0
            });
        }

        ExecQuery(fmt::format(R"(
                SELECT
                    *
                FROM `unknown-datasource`.`unknown-topic` WITH (
                    FORMAT = raw,
                    SCHEMA (Data String NOT NULL)
                ) AS p
                CROSS JOIN (
                    SELECT * FROM `{ydb_source}`.`{table}`
                ) AS l
            )",
            "ydb_source"_a = ydbSourceName,
            "table"_a = ydbTable
        ), EStatus::SCHEME_ERROR, "Cannot find table '/Root/unknown-datasource.[unknown-topic]' because it does not exist or you do not have access permissions");
    }


    Y_UNIT_TEST_TWIN_F(ReplicatedFederativeWriting, UseColumnTable, TStreamingTestFixture) {
        auto& config = SetupAppConfig();
        config.MutableTableServiceConfig()->SetEnableDataShardCreateTableAs(true);
        config.MutableTableServiceConfig()->SetEnableHtapTx(true);
        constexpr char firstOutputTopic[] = "replicatedWritingOutputTopicName1";
        constexpr char secondOutputTopic[] = "replicatedWritingOutputTopicName2";
        constexpr char pqSource[] = "pqSourceName";
        CreateTopic(firstOutputTopic);
        CreateTopic(secondOutputTopic);
        CreatePqSource(pqSource);

        constexpr char solomonSink[] = "solomonSinkName";
        CreateSolomonSource(solomonSink);

        constexpr char sourceTable[] = "source";
        constexpr char rowSinkTable[] = "rowSink";
        constexpr char columnSinkTable[] = "columnSink";
        ExecQuery(fmt::format(R"(
            CREATE TABLE `{source_table}` (
                Data String NOT NULL,
                PRIMARY KEY (Data)
            ) {source_settings};
            CREATE TABLE `{row_table}` (
                B Utf8 NOT NULL,
                PRIMARY KEY (B)
            );
            CREATE TABLE `{column_table}` (
                C String NOT NULL,
                PRIMARY KEY (C)
            ) WITH (
                STORE = COLUMN
            );)",
            "source_table"_a = sourceTable,
            "source_settings"_a = UseColumnTable ? "WITH (STORE = COLUMN)" : "",
            "row_table"_a = rowSinkTable,
            "column_table"_a = columnSinkTable
        ));

        ExecQuery(fmt::format(R"(
            UPSERT INTO `{table}`
                (Data)
            VALUES
                ("{{\"Val\": \"ABC\"}}");)",
            "table"_a = sourceTable
        ));

        TInstant disposition = TInstant::Now();

        // Double PQ insert
        {
            ExecQuery(fmt::format(R"(
                $rows = SELECT Data FROM `{source_table}`;
                INSERT INTO `{pq_source}`.`{output_topic1}` SELECT Unwrap(CAST("[" || Data || "]" AS Json)) AS A FROM $rows;
                INSERT INTO `{pq_source}`.`{output_topic2}` SELECT Unwrap(CAST(Data || "-B" AS String)) AS B FROM $rows;)",
                "source_table"_a = sourceTable,
                "pq_source"_a = pqSource,
                "output_topic1"_a = firstOutputTopic,
                "output_topic2"_a = secondOutputTopic
            ), EStatus::SUCCESS, "", AstChecker(1, 1));

            ReadTopicMessage(firstOutputTopic, R"([{"Val": "ABC"}])", disposition);
            ReadTopicMessage(secondOutputTopic, R"({"Val": "ABC"}-B)", disposition);
            disposition = TInstant::Now();
        }

        // Double solomon insert
        {
            const TSolomonLocation firstSoLocation = {
                .ProjectId = "cloudId1",
                .FolderId = "folderId1",
                .Service = "custom1",
                .IsCloud = false,
            };
            const TSolomonLocation secondSoLocation = {
                .ProjectId = "cloudId2",
                .FolderId = "folderId2",
                .Service = "custom2",
                .IsCloud = false,
            };

            CleanupSolomon(firstSoLocation);
            CleanupSolomon(secondSoLocation);

            ExecQuery(fmt::format(R"(
                $rows = SELECT Data FROM `{source_table}`;

                INSERT INTO `{solomon_sink}`.`{first_solomon_project}/{first_solomon_folder}/{first_solomon_service}`
                SELECT Unwrap(CAST("[" || Data || "]" AS Json)) AS sensor, 1 AS value, Timestamp("2025-03-12T14:40:39Z") AS ts FROM $rows;

                INSERT INTO `{solomon_sink}`.`{second_solomon_project}/{second_solomon_folder}/{second_solomon_service}`
                SELECT Unwrap(CAST(Data || "-B" AS String)) AS sensor, 2 AS value, Timestamp("2025-03-12T14:40:39Z") AS ts FROM $rows;)",
                "source_table"_a = sourceTable,
                "solomon_sink"_a = solomonSink,
                "first_solomon_project"_a = firstSoLocation.ProjectId,
                "first_solomon_folder"_a = firstSoLocation.FolderId,
                "first_solomon_service"_a = firstSoLocation.Service,
                "second_solomon_project"_a = secondSoLocation.ProjectId,
                "second_solomon_folder"_a = secondSoLocation.FolderId,
                "second_solomon_service"_a = secondSoLocation.Service
            ), EStatus::SUCCESS, "", AstChecker(1, 1));

            TString expectedMetrics = R"([
  {
    "labels": [
      [
        "name",
        "value"
      ],
      [
        "sensor",
        "[{\"Val\": \"ABC\"}]"
      ]
    ],
    "ts": 1741790439,
    "value": 1
  }
])";
            UNIT_ASSERT_STRINGS_EQUAL(GetSolomonMetrics(firstSoLocation), expectedMetrics);

            expectedMetrics = R"([
  {
    "labels": [
      [
        "name",
        "value"
      ],
      [
        "sensor",
        "{\"Val\": \"ABC\"}-B"
      ]
    ],
    "ts": 1741790439,
    "value": 2
  }
])";
            UNIT_ASSERT_STRINGS_EQUAL(GetSolomonMetrics(secondSoLocation), expectedMetrics);
        }

        // Mixed external and kqp writing
        {
            ExecQuery(fmt::format(R"(
                $rows = SELECT Data FROM `{source_table}`;
                UPSERT INTO `{column_table}` SELECT Unwrap(CAST(Data || "-C" AS String)) AS C FROM $rows;
                INSERT INTO `{pq_source}`.`{output_topic}` SELECT Unwrap(CAST("[" || Data || "]" AS Json)) AS A FROM $rows;
                UPSERT INTO `{row_table}` SELECT Unwrap(CAST(Data || "-B" AS Utf8)) AS B FROM $rows;)",
                "source_table"_a = sourceTable,
                "pq_source"_a = pqSource,
                "output_topic"_a = firstOutputTopic,
                "row_table"_a = rowSinkTable,
                "column_table"_a = columnSinkTable
            ), EStatus::SUCCESS, "", AstChecker(2, 4));

            ReadTopicMessage(firstOutputTopic, R"([{"Val": "ABC"}])", disposition);
            disposition = TInstant::Now();

            const auto& results = ExecQuery(fmt::format(R"(
                SELECT * FROM `{row_table}`;
                SELECT * FROM `{column_table}`;)",
                "row_table"_a = rowSinkTable,
                "column_table"_a = columnSinkTable
            ));
            UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);

            CheckScriptResult(results[0], 1, 1, [&](TResultSetParser& resultSet) {
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("B").GetUtf8(), R"({"Val": "ABC"}-B)");
            });

            CheckScriptResult(results[1], 1, 1, [&](TResultSetParser& resultSet) {
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("C").GetString(), R"({"Val": "ABC"}-C)");
            });
        }
    }

    Y_UNIT_TEST_F(ReadFromLocalTopicsWithAuth, TStreamingTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;

        auto& config = SetupAppConfig();
        config.MutableFeatureFlags()->SetEnableTopicsSqlIoOperations(true);
        config.MutableTableServiceConfig()->SetEnableDataShardCreateTableAs(true);
        config.MutablePQConfig()->SetRequireCredentialsInNewProtocol(true);

        constexpr char inputTopic[] = "inputTopicName";
        constexpr char outputTopic[] = "outputTopicName";
        CreateTopic(inputTopic, std::nullopt, /* local */ true);
        CreateTopic(outputTopic, std::nullopt, /* local */ true);

        auto asyncResult = GetQueryClient()->ExecuteQuery(fmt::format(R"(
                PRAGMA pq.Consumer = "test_consumer";
                INSERT INTO `{output_topic}`
                SELECT * FROM `{input_topic}` WITH (
                    STREAMING = "TRUE"
                ) LIMIT 2
            )",
            "input_topic"_a = inputTopic,
            "output_topic"_a = outputTopic
        ), TTxControl::NoTx());

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(inputTopic, "data", 0, /* local */ true);
        ReadTopicMessage(outputTopic, "data", TInstant::Now() - TDuration::Seconds(100), /* local */ true);

        // Force session reconnect
        KillTopicPqrbTablet(inputTopic);

        const auto disposition = TInstant::Now();
        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(inputTopic, "data2", 0, /* local */ true);
        ReadTopicMessage(outputTopic, "data2", disposition, /* local */ true);

        const auto result = asyncResult.ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
    }

    Y_UNIT_TEST_F(ScalarFederativeWriting, TStreamingTestFixture) {
        constexpr char firstOutputTopic[] = "replicatedWritingOutputTopicName1";
        constexpr char secondOutputTopic[] = "replicatedWritingOutputTopicName2";
        constexpr char pqSource[] = "pqSourceName";
        CreateTopic(firstOutputTopic);
        CreateTopic(secondOutputTopic);
        CreatePqSource(pqSource);

        constexpr char solomonSink[] = "solomonSinkName";
        CreateSolomonSource(solomonSink);

        const TSolomonLocation soLocation = {
            .ProjectId = "cloudId1",
            .FolderId = "folderId1",
            .Service = "custom1",
            .IsCloud = false,
        };
        CleanupSolomon(soLocation);
        ExecQuery(fmt::format(R"(
            INSERT INTO `{pq_source}`.`{output_topic1}` SELECT "TestData1";
            INSERT INTO `{pq_source}`.`{output_topic2}` SELECT "TestData2" AS Data;
            INSERT INTO `{pq_source}`.`{output_topic2}`(Data) VALUES ("TestData2");

            INSERT INTO `{solomon_sink}`.`{solomon_project}/{solomon_folder}/{solomon_service}`
            SELECT
                13333 AS value,
                "test-insert" AS sensor,
                Timestamp("2025-03-12T14:40:39Z") AS ts;

            INSERT INTO `{solomon_sink}`.`{solomon_project}/{solomon_folder}/{solomon_service}`
                (value, sensor, ts)
            VALUES
                (23333, "test-insert-2", Timestamp("2025-03-12T14:40:39Z"));)",
            "pq_source"_a = pqSource,
            "output_topic1"_a = firstOutputTopic,
            "output_topic2"_a = secondOutputTopic,
            "solomon_sink"_a = solomonSink,
            "solomon_project"_a = soLocation.ProjectId,
            "solomon_folder"_a = soLocation.FolderId,
            "solomon_service"_a = soLocation.Service
        ), EStatus::SUCCESS, "", AstChecker(2, 5));

        ReadTopicMessage(firstOutputTopic, "TestData1");
        ReadTopicMessages(secondOutputTopic, {"TestData2", "TestData2"});

        TString expectedMetrics = R"([
  {
    "labels": [
      [
        "name",
        "value"
      ],
      [
        "sensor",
        "test-insert"
      ]
    ],
    "ts": 1741790439,
    "value": 13333
  },
  {
    "labels": [
      [
        "name",
        "value"
      ],
      [
        "sensor",
        "test-insert-2"
      ]
    ],
    "ts": 1741790439,
    "value": 23333
  }
])";
        UNIT_ASSERT_VALUES_EQUAL(GetSolomonMetrics(soLocation), expectedMetrics);
    }
}

Y_UNIT_TEST_SUITE(KqpStreamingQueriesDdl) {
    Y_UNIT_TEST_F(CreateAndAlterStreamingQuery, TStreamingWithSchemaSecretsTestFixture) {
        constexpr char inputTopicName[] = "createAndAlterStreamingQueryInputTopic";
        constexpr char outputTopicName[] = "createAndAlterStreamingQueryOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE SECRET test_secret WITH (value = "1234");
            CREATE TABLE test_table1 (Key Int32 NOT NULL, PRIMARY KEY (Key));
            GRANT ALL ON `/Root/test_table1` TO `test@builtin`;
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT key || value FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        key String NOT NULL,
                        value String NOT NULL
                    )
                )
                WHERE value REGEXP ".*v.*a.*l.*"
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        {
            const auto tableDesc = Navigate(GetRuntime(), GetRuntime().AllocateEdgeActor(), "/Root/test_table1", NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
            const auto& table = tableDesc->ResultSet.at(0);
            UNIT_ASSERT_VALUES_EQUAL(table.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindTable);
            UNIT_ASSERT(table.SecurityObject->CheckAccess(NACLib::GenericFull, NACLib::TUserToken("test@builtin", {})));
        }

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, R"({"key": "key1", "value": "value1"})");
        ReadTopicMessages(outputTopicName, {"key1value1"});

        ExecQuery(fmt::format(R"(
            CREATE TABLE test_table2 (Key Int32 NOT NULL, PRIMARY KEY (Key));
            ALTER STREAMING QUERY `{query_name}` SET (
                FORCE = TRUE
            ) AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT value || key FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        key String NOT NULL,
                        value String NOT NULL
                    )
                )
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(2, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, R"({"key": "key2", "value": "value2"})");
        ReadTopicMessages(outputTopicName, {"key1value1", "value2key2"});

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            );)",
            "query_name"_a = queryName
        ));

        CheckScriptExecutionsCount(2, 0);
    }

    Y_UNIT_TEST_F(CreateAndDropStreamingQuery, TStreamingTestFixture) {
        constexpr char inputTopicName[] = "createAndDropStreamingQueryInputTopic";
        constexpr char outputTopicName[] = "createAndDropStreamingQueryOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT key || value FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        key String NOT NULL,
                        value String NOT NULL
                    )
                )
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, R"({"key": "key1", "value": "value1"})");
        ReadTopicMessages(outputTopicName, {"key1value1"});

        ExecQuery(fmt::format(R"(
            DROP STREAMING QUERY `{query_name}`;)",
            "query_name"_a = queryName
        ));

        CheckScriptExecutionsCount(0, 0);
    }

    Y_UNIT_TEST_F(MaxPartitionReadSkewWithRestartAndCheckpoint, TStreamingTestFixture) {
        constexpr ui32 partitionCount = 10;
        constexpr char inputTopicName[] = "maxPartitionReadSkewRestartInputTopic";
        constexpr char outputTopicName[] = "maxPartitionReadSkewRestartOutputTopic";
        CreateTopic(inputTopicName, NTopic::TCreateTopicSettings()
            .PartitioningSettings(partitionCount, partitionCount));
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA pq.MaxPartitionReadSkew = "10s";
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT time FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (time String NOT NULL)
                )
                WHERE time LIKE "%lunch%";
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        TVector<TString> firstBatch;
        for (ui32 p = 0; p < partitionCount; ++p) {
            firstBatch.push_back(fmt::format("lunch time {}", p));
            WriteTopicMessage(inputTopicName, fmt::format(R"({{"time": "lunch time {}"}})", p), p);
        }
        ReadTopicMessages(outputTopicName, firstBatch, TInstant::Now() - TDuration::Seconds(100), /* sort */ true);

        Sleep(CheckpointPeriod * 3);

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (RUN = FALSE);)",
            "query_name"_a = queryName
        ));

        CheckScriptExecutionsCount(1, 0);
        Sleep(TDuration::MilliSeconds(500));

        TVector<TString> secondBatch;
        for (ui32 p = 0; p < partitionCount; ++p) {
            secondBatch.push_back(fmt::format("next lunch time {}", p));
            WriteTopicMessage(inputTopicName, fmt::format(R"({{"time": "next lunch time {}"}})", p), p);
        }

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (RUN = TRUE);)",
            "query_name"_a = queryName
        ));

        CheckScriptExecutionsCount(2, 1);  // 2 executions (initial + restarted), 1 lease (running)
        TVector<TString> allExpected;
        for (const auto& s : firstBatch) {
            allExpected.push_back(s);
        }
        for (const auto& s : secondBatch) {
            allExpected.push_back(s);
        }
        ReadTopicMessages(outputTopicName, allExpected, TInstant::Now() - TDuration::Seconds(100), /* sort */ true);
    }

    Y_UNIT_TEST_F(IdleTimeoutPartitionSessionBalancer, TStreamingTestFixture) {
        constexpr ui32 partitionCount = 2;
        constexpr char inputTopicName[] = "idleTimeoutBalancerInputTopic";
        constexpr char outputTopicName[] = "idleTimeoutBalancerOutputTopic";
        CreateTopic(inputTopicName, NTopic::TCreateTopicSettings()
            .PartitioningSettings(partitionCount, partitionCount));
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA pq.MaxPartitionReadSkew = "10s";
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT key || value FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        key String NOT NULL,
                        value String NOT NULL
                    ),
                    WATERMARK_IDLE_TIMEOUT = "PT5S"
                );
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        TVector<TString> expectedOutputs;
        for (ui32 i = 0; i < 10; ++i) {
            TString value = fmt::format("v{}", i);
            expectedOutputs.push_back("k" + value);
            WriteTopicMessage(inputTopicName, fmt::format(R"({{"key": "k", "value": "{}"}})", value), 0);
            ReadTopicMessages(outputTopicName, expectedOutputs);
        }
    }

    Y_UNIT_TEST_F(MaxStreamingQueryExecutionsLimit, TStreamingTestFixture) {
        constexpr ui64 executionsLimit = 3;
        constexpr char inputTopicName[] = "maxStreamingQueryExecutionsLimitInputTopic";
        constexpr char outputTopicName[] = "maxStreamingQueryExecutionsLimitOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT key || value FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        key String NOT NULL,
                        value String NOT NULL
                    )
                )
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, R"({"key": "key1", "value": "value1"})");
        ReadTopicMessages(outputTopicName, {"key1value1"});

        TVector<TString> messages = {"key1value1"};
        messages.reserve(2 * executionsLimit + 1);
        for (ui64 i = 0; i < 2 * executionsLimit; ++i) {
            ExecQuery(fmt::format(R"(
                ALTER STREAMING QUERY `{query_name}` SET (
                    FORCE = TRUE
                ) AS
                DO BEGIN
                    INSERT INTO `{pq_source}`.`{output_topic}`
                    SELECT value || key FROM `{pq_source}`.`{input_topic}` WITH (
                        FORMAT = "json_each_row",
                        SCHEMA (
                            key String NOT NULL,
                            value String NOT NULL
                        )
                    )
                END DO;)",
                "query_name"_a = queryName,
                "pq_source"_a = pqSourceName,
                "input_topic"_a = inputTopicName,
                "output_topic"_a = outputTopicName
            ));

            const ui64 id = i + 2;
            CheckScriptExecutionsCount(std::min(id, executionsLimit + 1), 1);
            Sleep(TDuration::Seconds(1));

            WriteTopicMessage(inputTopicName, TStringBuilder() << "{\"key\":\"key" << id << "\", \"value\": \"value" << id << "\"}");

            messages.emplace_back(TStringBuilder() << "value" << id << "key" << id);
            ReadTopicMessages(outputTopicName, messages);
        }
    }

    Y_UNIT_TEST_F(CreateStreamingQueryWithDefineAction, TStreamingTestFixture) {
        constexpr char inputTopicName[] = "createAndAlterStreamingQueryInputTopic";
        constexpr char outputTopicName[] = "createAndAlterStreamingQueryOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                DEFINE ACTION $start_query($add) AS
                    INSERT INTO `{pq_source}`.`{output_topic}`
                    SELECT key || value || $add FROM `{pq_source}`.`{input_topic}` WITH (
                        FORMAT = "json_each_row",
                        SCHEMA (
                            key String NOT NULL,
                            value String NOT NULL
                        )
                    )
                END DEFINE;

                DO $start_query("Add1")
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, R"({"key": "key1", "value": "value1"})");
        ReadTopicMessages(outputTopicName, {"key1value1Add1"});
    }

    Y_UNIT_TEST_F(CreateStreamingQueryMatchRecognize, TStreamingTestFixture) {
        constexpr char inputTopicName[] = "createStreamingQueryMatchRecognizeInputTopic";
        constexpr char outputTopicName[] = "createStreamingQueryMatchRecognizeOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;
                PRAGMA FeatureR010="prototype";

                $matches = SELECT * FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        key Uint64 NOT NULL,
                        value String NOT NULL
                    )
                ) MATCH_RECOGNIZE(
                    MEASURES
                        LAST(V1.key) as v1,
                        LAST(V4.key) as v4
                    ONE ROW PER MATCH
                    PATTERN (V1 V? V4)
                    DEFINE
                        V1 as V1.value = "value1",
                        V as True,
                        V4 as V4.value = "value4"
                );

                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT UNWRAP(CAST(v1 AS String) || "-" || CAST(v4 AS String)) FROM $matches;
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessages(inputTopicName, {
            R"({"key": 1, "value": "value1"})",
            R"({"key": 2, "value": "value2"})",
            R"({"key": 4, "value": "value4"})",
        });
        ReadTopicMessages(outputTopicName, {"1-4"});
    }

    Y_UNIT_TEST_F(StreamingQueryReplaceAfterError, TStreamingTestFixture) {
        constexpr char inputTopicName[] = "createAndAlterStreamingQueryInputTopic";
        constexpr char outputTopicName[] = "createAndAlterStreamingQueryOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.OverridePlanner = @@ [
                    {{ "tx": 0, "stage": 1, "tasks": 32 }}
                ] @@;
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ), EStatus::GENERIC_ERROR, "Invalid override planner settings");

        CheckScriptExecutionsCount(1, 0);

        const auto streamingQueryDesc = Navigate(GetRuntime(), GetRuntime().AllocateEdgeActor(), JoinPath({"Root", queryName}), NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
        const auto& streamingQuery = streamingQueryDesc->ResultSet.at(0);
        UNIT_ASSERT_VALUES_EQUAL(streamingQuery.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindStreamingQuery);
        UNIT_ASSERT(streamingQuery.StreamingQueryInfo);
        UNIT_ASSERT_VALUES_EQUAL(streamingQuery.StreamingQueryInfo->Description.GetName(), queryName);

        ExecQuery(fmt::format(R"(
            CREATE OR REPLACE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.OverridePlanner = @@ [
                    {{ "tx": 0, "stage": 0, "tasks": 32 }}
                ] @@;
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(2, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, "test message");
        ReadTopicMessage(outputTopicName, "test message");
    }

    Y_UNIT_TEST_F(StreamingQueryTextChangeWithCreateOrReplace, TStreamingTestFixture) {
        constexpr char inputTopicName[] = "createAndReplaceStreamingQueryInputTopic";
        constexpr char outputTopicName[] = "createAndReplaceStreamingQueryOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT key || value FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        key String NOT NULL,
                        value String NOT NULL
                    )
                )
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, R"({"key": "key1", "value": "value1"})");
        ReadTopicMessages(outputTopicName, {"key1value1"});

        ExecQuery(fmt::format(R"(
            CREATE OR REPLACE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT value || key FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        key String NOT NULL,
                        value String NOT NULL
                    )
                )
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(2, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, R"({"key": "key2", "value": "value2"})");
        ReadTopicMessages(outputTopicName, {"key1value1", "value2key2"});
    }

    Y_UNIT_TEST_F(StreamingQueryCreateOrReplaceFailure, TStreamingTestFixture) {
        constexpr char inputTopicName[] = "createOrReplaceStreamingQueryFailInputTopic";
        constexpr char outputTopicName[] = "createOrReplaceStreamingQueryFailOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, "key1value1");
        ReadTopicMessages(outputTopicName, {"key1value1"});

        ExecQuery(fmt::format(R"(
            CREATE OR REPLACE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.OverridePlanner = @@ [
                    {{ "tx": 0, "stage": 10, "tasks": 1 }}
                ] @@;
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ), EStatus::GENERIC_ERROR, "Invalid override planner settings");

        CheckScriptExecutionsCount(2, 0);
    }

    Y_UNIT_TEST_F(StreamingQueryWithSolomonInsert, TStreamingTestFixture) {
        const auto pqGateway = SetupMockPqGateway();

        constexpr char inputTopicName[] = "streamingQuerySolomonInsertInputTopic";
        CreateTopic(inputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char solomonSinkName[] = "sinkName";
        CreateSolomonSource(solomonSinkName);

        constexpr char queryName[] = "streamingQuery";
        const TSolomonLocation soLocation = {
            .ProjectId = "cloudId1",
            .FolderId = "folderId1",
            .Service = "custom",
            .IsCloud = false,
        };
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{solomon_sink}`.`{solomon_project}/{solomon_folder}/{solomon_service}`
                SELECT
                    Unwrap(CAST(Data AS Uint64)) AS value,
                    "test-solomon-insert" AS sensor,
                    Timestamp("2025-03-12T14:40:39Z") AS ts
                FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "solomon_sink"_a = solomonSinkName,
            "solomon_project"_a = soLocation.ProjectId,
            "solomon_folder"_a = soLocation.FolderId,
            "solomon_service"_a = soLocation.Service,
            "input_topic"_a = inputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);

        CleanupSolomon(soLocation);
        auto readSession = pqGateway->WaitReadSession(inputTopicName);
        readSession->AddDataReceivedEvent(0, "1234");

        Sleep(TDuration::Seconds(2));

        TString expectedMetrics = R"([
  {
    "labels": [
      [
        "name",
        "value"
      ],
      [
        "sensor",
        "test-solomon-insert"
      ]
    ],
    "ts": 1741790439,
    "value": 1234
  }
])";
        UNIT_ASSERT_STRINGS_EQUAL(GetSolomonMetrics(soLocation), expectedMetrics);
        CleanupSolomon(soLocation);

        readSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});

        pqGateway->WaitReadSession(inputTopicName)->AddDataReceivedEvent(1, "4321");
        Sleep(TDuration::Seconds(2));

        expectedMetrics = R"([
  {
    "labels": [
      [
        "name",
        "value"
      ],
      [
        "sensor",
        "test-solomon-insert"
      ]
    ],
    "ts": 1741790439,
    "value": 4321
  }
])";
        UNIT_ASSERT_STRINGS_EQUAL(GetSolomonMetrics(soLocation), expectedMetrics);
    }

    Y_UNIT_TEST_F(StreamingQueryWithS3Insert, TStreamingTestFixture) {
        const auto pqGateway = SetupMockPqGateway();

        constexpr char inputTopicName[] = "streamingQueryS3InsertInputTopic";
        constexpr char pqSourceName[] = "sourceName";
        CreateTopic(inputTopicName);
        CreatePqSource(pqSourceName);

        constexpr char sourceBucket[] = "test_bucket_streaming_query_s3_insert";
        constexpr char s3SinkName[] = "sinkName";
        CreateBucket(sourceBucket);
        CreateS3Source(sourceBucket, s3SinkName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{s3_sink}`.`test/` WITH (
                    FORMAT = raw
                ) SELECT
                    Data
                FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "s3_sink"_a = s3SinkName,
            "input_topic"_a = inputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);

        auto readSession = pqGateway->WaitReadSession(inputTopicName);
        readSession->AddDataReceivedEvent(0, "1234");
        Sleep(TDuration::Seconds(2));

        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(sourceBucket), "1234");

        readSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});

        pqGateway->WaitReadSession(inputTopicName)->AddDataReceivedEvent(1, "4321");
        Sleep(TDuration::Seconds(2));

        if (const auto& s3Data = GetAllObjects(sourceBucket); !IsIn({"12344321", "43211234"}, s3Data)) {
            UNIT_FAIL("Unexpected S3 data: " << s3Data);
        }

        const auto& keys = GetObjectKeys(sourceBucket);
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 2);
        for (const auto& key : keys) {
            UNIT_ASSERT_STRING_CONTAINS(key, "test/");
            UNIT_ASSERT_C(!key.substr(5).Contains("/"), key);
        }
    }

    Y_UNIT_TEST_F(StreamingQueryWithS3Join, TStreamingTestFixture) {
        // Test that defaults are overridden for streaming queries
        auto& setting = *SetupAppConfig().MutableKQPConfig()->AddSettings();
        setting.SetName("HashJoinMode");
        setting.SetValue("grace");

        const auto pqGateway = SetupMockPqGateway();

        constexpr char sourceBucket[] = "test_streaming_query_with_s3_join";
        constexpr char objectContent[] = R"(
{"fqdn": "host1.example.com", "payload": "P1"}
{"fqdn": "host2.example.com", "payload": "P2"}
{"fqdn": "host3.example.com", "payload": "P3"})";
        CreateBucketWithObject(sourceBucket, "path/test_object.json", objectContent);

        constexpr char inputTopicName[] = "inputTopicName";
        constexpr char outputTopicName[] = "outputTopicName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "pqSourceName";
        constexpr char s3SourceName[] = "s3Source";
        CreatePqSource(pqSourceName);
        CreateS3Source(sourceBucket, s3SourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.HashJoinMode = "map";
                $s3_lookup = SELECT * FROM `{s3_source}`.`path/` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        fqdn String,
                        payload String
                    )
                );

                $pq_source = SELECT * FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        time Int32 NOT NULL,
                        event String,
                        host String
                    )
                );

                $joined = SELECT l.payload AS payload, p.* FROM $pq_source AS p
                LEFT JOIN $s3_lookup AS l
                ON (l.fqdn = p.host);

                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT Unwrap(event || "-" || payload) FROM $joined
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "s3_source"_a = s3SourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);

        auto readSession = pqGateway->WaitReadSession(inputTopicName);
        const std::vector<IMockPqReadSession::TMessage> sampleMessages = {
            {0, R"({"time": 0, "event": "A", "host": "host1.example.com"})"},
            {1, R"({"time": 1, "event": "B", "host": "host3.example.com"})"},
            {2, R"({"time": 2, "event": "A", "host": "host1.example.com"})"},
        };
        readSession->AddDataReceivedEvent(sampleMessages);

        const std::vector<TString> sampleResult = {"A-P1", "B-P3", "A-P1"};
        pqGateway->WaitWriteSession(outputTopicName)->ExpectMessages(sampleResult);

        readSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});

        pqGateway->WaitReadSession(inputTopicName)->AddDataReceivedEvent(sampleMessages);
        pqGateway->WaitWriteSession(outputTopicName)->ExpectMessages(sampleResult);
    }

    Y_UNIT_TEST_F(StreamingQueryWithYdbJoin, TStreamingTestFixture) {
        // Test that defaults are overridden for streaming queries
        auto& setting = *SetupAppConfig().MutableKQPConfig()->AddSettings();
        setting.SetName("HashJoinMode");
        setting.SetValue("grace");

        const auto connectorClient = SetupMockConnectorClient();
        const auto pqGateway = SetupMockPqGateway();

        constexpr char inputTopicName[] = "inputTopicName";
        constexpr char outputTopicName[] = "outputTopicName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "pqSourceName";
        constexpr char ydbSourceName[] = "ydbSourceName";
        CreatePqSource(pqSourceName);
        CreateYdbSource(ydbSourceName);

        constexpr char ydbTable[] = "lookup";
        ExecExternalQuery(fmt::format(R"(
            CREATE TABLE `{table}` (
                fqdn String,
                payload String,
                PRIMARY KEY (fqdn)
            ))",
            "table"_a = ydbTable
        ));

        {   // Prepare connector mock

            const std::vector<TColumn> columns = {
                {"fqdn", Ydb::Type::STRING},
                {"payload", Ydb::Type::STRING}
            };
            SetupMockConnectorTableDescription(connectorClient, {
                .TableName = ydbTable,
                .Columns = columns,
                .DescribeCount = 2,
                // For stream queries type annotation is executed twice, but
                // now List Split is done after type annotation optimization.
                // That is why only single call to List Split is expected.
                .ListSplitsCount = 1
            });

            const std::vector<std::string> fqdnColumn = {"host1.example.com", "host2.example.com", "host3.example.com"};
            const std::vector<std::string> payloadColumn = {"P1", "P2", "P3"};
            SetupMockConnectorTableData(connectorClient, {
                .TableName = ydbTable,
                .Columns = columns,
                .NumberReadSplits = 2,
                .ResultFactory = [&]() {
                    return MakeRecordBatch(
                        MakeArray<arrow::BinaryBuilder>("fqdn", fqdnColumn, arrow::binary()),
                        MakeArray<arrow::BinaryBuilder>("payload", payloadColumn, arrow::binary())
                    );
                }
            });
        }

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.HashJoinMode = "map";
                $ydb_lookup = SELECT * FROM `{ydb_source}`.`{ydb_table}`;

                $pq_source = SELECT * FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        time Int32 NOT NULL,
                        event String,
                        host String
                    )
                );

                $joined = SELECT l.payload AS payload, p.* FROM $pq_source AS p
                LEFT JOIN $ydb_lookup AS l
                ON (l.fqdn = p.host);

                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT Unwrap(event || "-" || payload) FROM $joined
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "ydb_source"_a = ydbSourceName,
            "ydb_table"_a = ydbTable,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);

        auto readSession = pqGateway->WaitReadSession(inputTopicName);
        const std::vector<IMockPqReadSession::TMessage> sampleMessages = {
            {0, R"({"time": 0, "event": "A", "host": "host1.example.com"})"},
            {1, R"({"time": 1, "event": "B", "host": "host3.example.com"})"},
            {2, R"({"time": 2, "event": "A", "host": "host1.example.com"})"},
        };
        readSession->AddDataReceivedEvent(sampleMessages);

        const std::vector<TString> sampleResult = {"A-P1", "B-P3", "A-P1"};
        pqGateway->WaitWriteSession(outputTopicName)->ExpectMessages(sampleResult);

        readSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});

        pqGateway->WaitReadSession(inputTopicName)->AddDataReceivedEvent(sampleMessages);
        pqGateway->WaitWriteSession(outputTopicName)->ExpectMessages(sampleResult);
    }

    Y_UNIT_TEST_F(StreamingQueryWithDoubleYdbJoin, TStreamingTestFixture) {
        const auto connectorClient = SetupMockConnectorClient();
        const auto pqGateway = SetupMockPqGateway();

        constexpr char inputTopicName[] = "doubleYdbJoinInputTopicName";
        constexpr char outputTopicName[] = "doubleYdbJoinOutputTopicName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "pqSourceName";
        constexpr char ydbSourceName[] = "ydbSourceName";
        CreatePqSource(pqSourceName);
        CreateYdbSource(ydbSourceName);

        constexpr char ydbTable[] = "doubleYdbJoinLookup";
        ExecExternalQuery(fmt::format(R"(
            CREATE TABLE `{table}` (
                fqdn String,
                PRIMARY KEY (fqdn)
            ))",
            "table"_a = ydbTable
        ));

        {   // Prepare connector mock
            const std::vector<TColumn> columns = {{"fqdn", Ydb::Type::STRING}};
            SetupMockConnectorTableDescription(connectorClient, {
                .TableName = ydbTable,
                .Columns = columns,
                .DescribeCount = 2,
                .ListSplitsCount = 1
            });

            const std::vector<std::string> fqdnColumn = {"host1", "host2"};
            SetupMockConnectorTableData(connectorClient, {
                .TableName = ydbTable,
                .Columns = columns,
                .NumberReadSplits = 4, // Read from ydb source is not deduplicated because spilling is disabled for streaming queries
                .ResultFactory = [&]() {
                    return MakeRecordBatch(MakeArray<arrow::BinaryBuilder>("fqdn", fqdnColumn, arrow::binary()));
                }
            });
        }

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT
                    p.Data || "-" || la.fqdn || "-" || lb.fqdn
                FROM `{pq_source}`.`{input_topic}` AS p
                CROSS JOIN `{ydb_source}`.`{ydb_table}` AS la
                CROSS JOIN `{ydb_source}`.`{ydb_table}` AS lb
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "ydb_source"_a = ydbSourceName,
            "ydb_table"_a = ydbTable,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);

        auto readSession = pqGateway->WaitReadSession(inputTopicName);
        readSession->AddDataReceivedEvent(0, "data1");

        pqGateway->WaitWriteSession(outputTopicName)->ExpectMessages({
            "data1-host1-host2",
            "data1-host2-host1",
            "data1-host1-host1",
            "data1-host2-host2"
        }, /* sort  */ true);

        readSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});

        pqGateway->WaitReadSession(inputTopicName)->AddDataReceivedEvent(1, "data2");
        pqGateway->WaitWriteSession(outputTopicName)->ExpectMessages({
            "data2-host1-host2",
            "data2-host2-host1",
            "data2-host1-host1",
            "data2-host2-host2"
        }, /* sort  */ true);
    }

    Y_UNIT_TEST_TWIN_F(StreamingQueryWithStreamLookupJoin, WithFeatureFlag, TStreamingTestFixture) {
        {
            auto& setupAppConfig = SetupAppConfig();
            setupAppConfig.MutableQueryServiceConfig()->SetProgressStatsPeriodMs(0);
            if (WithFeatureFlag) {
                setupAppConfig.MutableTableServiceConfig()->SetEnableDqSourceStreamLookupJoin(true);
            }
        }

        const auto connectorClient = SetupMockConnectorClient();
        const auto pqGateway = SetupMockPqGateway();

        constexpr char inputTopicName[] = "sljInputTopicName";
        constexpr char outputTopicName[] = "sljOutputTopicName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "pqSourceName";
        constexpr char ydbSourceName[] = "ydbSourceName";
        CreatePqSource(pqSourceName);
        CreateYdbSource(ydbSourceName);

        constexpr char ydbTable[] = "lookup";
        ExecExternalQuery(fmt::format(R"(
            CREATE TABLE `{table}` (
                fqdn String,
                payload String,
                PRIMARY KEY (fqdn)
            ))",
            "table"_a = ydbTable
        ));

        {   // Prepare connector mock
            const std::vector<TColumn> columns = {
                {"fqdn", Ydb::Type::STRING},
                {"payload", Ydb::Type::STRING}
            };
            SetupMockConnectorTableDescription(connectorClient, {
                .TableName = ydbTable,
                .Columns = columns,
                .DescribeCount = 2,
                // Now List Split is done after type annotation, that is the
                // reason why this value equal to 4 not 5
                .ListSplitsCount = WithFeatureFlag ? 4 : 0,
                .ValidateListSplitsArgs = false
            });

            if (WithFeatureFlag) {
                ui64 readSplitsCount = 0;
                const std::vector<std::string> fqdnColumn = {"host1.example.com", "host2.example.com", "host3.example.com"};
                SetupMockConnectorTableData(connectorClient, {
                    .TableName = ydbTable,
                    .Columns = columns,
                    .NumberReadSplits = 3,
                    .ValidateReadSplitsArgs = false,
                    .ResultFactory = [&]() {
                        readSplitsCount += 1;
                        const auto payloadColumn = readSplitsCount < 3
                            ? std::vector<std::string>{"P1", "P2", "P3"}
                            : std::vector<std::string>{"P4", "P5", "P6"};

                        return MakeRecordBatch(
                            MakeArray<arrow::BinaryBuilder>("fqdn", fqdnColumn, arrow::binary()),
                            MakeArray<arrow::BinaryBuilder>("payload", payloadColumn, arrow::binary())
                        );
                    }
                });
            }
        }

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $ydb_lookup = SELECT * FROM `{ydb_source}`.`{ydb_table}`;

                $pq_source = SELECT * FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        time Int32 NOT NULL,
                        event String,
                        host String
                    )
                );

                $joined = SELECT l.payload AS payload, p.* FROM $pq_source AS p
                LEFT JOIN /*+ streamlookup(TTL 1) */ ANY $ydb_lookup AS l
                ON (l.fqdn = p.host);

                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT Unwrap(event || "-" || payload) FROM $joined
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "ydb_source"_a = ydbSourceName,
            "ydb_table"_a = ydbTable,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ),
        WithFeatureFlag ? EStatus::SUCCESS : EStatus::GENERIC_ERROR,
        WithFeatureFlag ? "" : "Unsupported join strategy: streamlookup");
        if (!WithFeatureFlag) {
            return;
        }

        CheckScriptExecutionsCount(1, 1);

        auto readSession = pqGateway->WaitReadSession(inputTopicName);
        const std::vector<IMockPqReadSession::TMessage> sampleMessages = {
            {0, R"({"time": 0, "event": "A", "host": "host1.example.com"})"},
            {1, R"({"time": 1, "event": "B", "host": "host3.example.com"})"},
            {2, R"({"time": 2, "event": "A", "host": "host1.example.com"})"},
        };
        readSession->AddDataReceivedEvent(sampleMessages);

        const std::vector<TString> sampleResult = {"A-P1", "B-P3", "A-P1"};
        pqGateway->WaitWriteSession(outputTopicName)->ExpectMessages(sampleResult);

        readSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});

        readSession = pqGateway->WaitReadSession(inputTopicName);
        readSession->AddDataReceivedEvent(sampleMessages);
        auto writeSession = pqGateway->WaitWriteSession(outputTopicName);
        writeSession->ExpectMessages(sampleResult);

        Sleep(TDuration::Seconds(2));
        readSession->AddDataReceivedEvent(sampleMessages);
        writeSession->ExpectMessages({"A-P4", "B-P6", "A-P4"});

        CheckScriptExecutionsCount(1, 1);
        const auto results = ExecQuery(
            "SELECT ast_compressed FROM `.metadata/script_executions`;"
        );
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);
        CheckScriptResult(results[0], 1, 1, [](TResultSetParser& result) {
            const auto& ast = result.ColumnParser(0).GetOptionalString();
            UNIT_ASSERT(ast);
            UNIT_ASSERT_STRING_CONTAINS(*ast, "DqCnStreamLookup");
        });
    }

    Y_UNIT_TEST_F(StreamingQueryWithLocalYdbJoin, TStreamingTestFixture) {
        auto& config = SetupAppConfig();
        config.MutableTableServiceConfig()->SetEnableDataShardCreateTableAs(true);
        config.MutableTableServiceConfig()->SetEnableHtapTx(true);

        constexpr char inputTopicName[] = "streamingQueryWithLocalYdbJoinInputTopic";
        constexpr char outputTopicName[] = "streamingQueryWithLocalYdbJoinOutputTopic";
        constexpr char pqSourceName[] = "pqSourceName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);
        CreatePqSource(pqSourceName);

        constexpr char streamLookupTableName[] = "oltpStreamLookupTable";
        constexpr char oltpTableName[] = "oltpTable";
        constexpr char olapTableName[] = "olapTable";
        ExecQuery(fmt::format(R"(
            CREATE TABLE `{oltp_streamlookup_table}` (
                Key Int32 NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Key)
            );
            CREATE TABLE `{oltp_table}` (
                Key Int32 NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Value)
            );
            CREATE TABLE `{olap_table}` (
                Key Int32 NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Key)
            ) WITH (
                STORE = COLUMN
            );)",
            "oltp_streamlookup_table"_a = streamLookupTableName,
            "oltp_table"_a = oltpTableName,
            "olap_table"_a = olapTableName
        ));

        ExecQuery(fmt::format(R"(
            UPSERT INTO `{oltp_streamlookup_table}`(Key, Value)
            VALUES (1, "oltp_slj1"), (2, "oltp_slj2");

            UPSERT INTO `{oltp_table}`(Key, Value)
            VALUES (1, "oltp1"), (2, "oltp2");

            INSERT INTO `{olap_table}`(Key, Value)
            VALUES (1, "olap1"), (2, "olap2");)",
            "oltp_streamlookup_table"_a = streamLookupTableName,
            "oltp_table"_a = oltpTableName,
            "olap_table"_a = olapTableName
        ));

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.HashJoinMode = "map";
                PRAGMA ydb.DqChannelVersion = "1";

                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT
                    Unwrap(oltp_slj.Value || "-" || oltp.Value || "-" || olap.Value)
                FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = json_each_row,
                    SCHEMA (
                        Key Int32 NOT NULL
                    )
                ) AS topic
                LEFT JOIN `{oltp_streamlookup_table}` AS oltp_slj ON topic.Key = oltp_slj.Key
                LEFT JOIN `{oltp_table}` AS oltp ON topic.Key = oltp.Key
                LEFT JOIN `{olap_table}` AS olap ON topic.Key = olap.Key
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName,
            "oltp_streamlookup_table"_a = streamLookupTableName,
            "oltp_table"_a = oltpTableName,
            "olap_table"_a = olapTableName
        ));
        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, R"({"Key": 1})");
        ReadTopicMessage(outputTopicName, "oltp_slj1-oltp1-olap1");
        Sleep(TDuration::Seconds(1));

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(1, 0);

        WriteTopicMessage(inputTopicName, R"({"Key": 2})");
        const auto disposition = TInstant::Now();

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(2, 1);

        ReadTopicMessage(outputTopicName, "oltp_slj2-oltp2-olap2", disposition);
    }

    Y_UNIT_TEST_F(StreamingQueryWithPrecompute, TStreamingTestFixture) {
        constexpr char inputTopicName[] = "streamingQueryWithPrecomputeInputTopic";
        constexpr char outputTopicName[] = "streamingQueryWithPrecomputeOutputTopic";
        constexpr char pqSourceName[] = "pqSourceName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);
        CreatePqSource(pqSourceName);

        constexpr char tableName[] = "oltpTable";
        ExecQuery(fmt::format(R"(
            CREATE TABLE `{table_name}` (
                Key Int32 NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Key)
            );)",
            "table_name"_a = tableName
        ));

        ExecQuery(fmt::format(R"(
            UPSERT INTO `{table_name}`
                (Key, Value)
            VALUES
                (1, "value-1");)",
            "table_name"_a = tableName
        ));

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $r = SELECT Value FROM `{table_name}`;

                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT
                    Unwrap(Data || "-" || $r)
                FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName,
            "table_name"_a = tableName
        ));
        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, "message-1");
        ReadTopicMessage(outputTopicName, "message-1-value-1");
        Sleep(TDuration::Seconds(1));

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(1, 0);

        ExecQuery(fmt::format(R"(
            UPSERT INTO `{table_name}`
                (Key, Value)
            VALUES
                (1, "value-2");)",
            "table_name"_a = tableName
        ));

        WriteTopicMessage(inputTopicName, "message-2");
        const auto disposition = TInstant::Now();

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(2, 1);

        ReadTopicMessage(outputTopicName, "message-2-value-2", disposition);
    }

    Y_UNIT_TEST_F(StreamingQueryUnderSecureScriptExecutions, TStreamingTestFixture) {
        auto& appConfig = SetupAppConfig();
        appConfig.MutableFeatureFlags()->SetEnableSecureScriptExecutions(true);
        GetRuntime().GetAppData().FeatureFlags.SetEnableSecureScriptExecutions(true);

        constexpr char inputTopicName[] = "streamingQueryUnderSecureScriptExecutionsInputTopic";
        constexpr char outputTopicName[] = "streamingQueryUnderSecureScriptExecutionsOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(inputTopicName, R"({"key": "key1", "value": "value1"})");
        ReadTopicMessage(outputTopicName, R"({"key": "key1", "value": "value1"})");

        NOperation::TOperationClient rootClient(*GetInternalDriver(), TCommonClientSettings().AuthToken(BUILTIN_ACL_ROOT));
        {
            const auto result = rootClient.List<TScriptExecutionOperation>(10).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetList().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetList()[0].Metadata().ExecStatus, EExecStatus::Running);
        }

        NOperation::TOperationClient testClient(*GetInternalDriver(), TCommonClientSettings().AuthToken("test@" BUILTIN_ACL_DOMAIN));
        {
            const auto result = testClient.List<TScriptExecutionOperation>(10).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetList().size(), 0);
        }

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            ))",
            "query_name"_a = queryName
        ));

        {
            const auto result = rootClient.List<TScriptExecutionOperation>(10).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetList().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetList()[0].Metadata().ExecStatus, EExecStatus::Canceled);
        }

        {
            const auto result = testClient.List<TScriptExecutionOperation>(10).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetList().size(), 0);
        }

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");
        ExecQuery("GRANT ALL ON `/Root/.metadata` TO `" BUILTIN_ACL_ROOT "`");
        ExecQuery("GRANT ALL ON `/Root/.metadata/streaming` TO `" BUILTIN_ACL_ROOT "`");

        const auto testNoAccess = [&]() {
            ExecQuery("SELECT COUNT(*) FROM `.metadata/streaming/queries`", EStatus::SCHEME_ERROR, "Cannot find table");
            ExecQuery("SELECT COUNT(*) FROM `.metadata/streaming/checkpoints/checkpoints_metadata`", EStatus::SCHEME_ERROR, "Cannot find table");
        };
        const auto testAccessAllowed = [&]() {
            const auto& resultQueries = ExecQuery("SELECT COUNT(*) FROM `.metadata/streaming/queries`");
            UNIT_ASSERT_VALUES_EQUAL(resultQueries.size(), 1);

            CheckScriptResult(resultQueries[0], 1, 1, [](TResultSetParser& parser) {
                UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser(0).GetUint64(), 1);
            });

            const auto& resultCheckpoints = ExecQuery("SELECT COUNT(*) FROM `.metadata/streaming/checkpoints/checkpoints_metadata`");
            UNIT_ASSERT_VALUES_EQUAL(resultCheckpoints.size(), 1);
        };
        const auto switchAccess = [&](bool allowed) {
            auto& runtime = GetRuntime();
            runtime.GetAppData().FeatureFlags.SetEnableSecureScriptExecutions(!allowed);

            const auto edgeActor = runtime.AllocateEdgeActor();
            appConfig.MutableFeatureFlags()->SetEnableSecureScriptExecutions(!allowed);

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

            Sleep(TDuration::Seconds(1));

            ExecQuery(fmt::format(R"(
                ALTER STREAMING QUERY `{query_name}` SET (
                    RUN = FALSE
                ))",
                "query_name"_a = queryName
            ));
        };

        testNoAccess();

        switchAccess(/* allowed */ true);
        testAccessAllowed();

        switchAccess(/* allowed */ false);
        testNoAccess();
    }

    Y_UNIT_TEST_F(OffsetsRecoveryAfterManualAndInternalRetry, TStreamingTestFixture) {
        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        constexpr char inputTopicName[] = "offsetsRecoveryAfterManualAndInternalRetry,InputTopic";
        constexpr char outputTopicName[] = "offsetsRecoveryAfterManualAndInternalRetry,OutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char consumerName[] = "unknownConsumer";
        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA pq.Consumer = "{consumer_name}";
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName,
            "consumer_name"_a = consumerName
        ));

        WaitFor(TDuration::Seconds(10), "Wait fail", [&](TString& error) {
            const auto& result = ExecQuery("SELECT Issues FROM `.sys/streaming_queries`");
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);

            TString issues;
            CheckScriptResult(result[0], 1, 1, [&](TResultSetParser& resultSet) {
                issues = resultSet.ColumnParser("Issues").GetOptionalUtf8().value_or("");
            });

            error = TStringBuilder() << "Query issues: " << issues;
            return issues.Contains("no read rule provided for consumer 'unknownConsumer' in topic");
        });

        ExecExternalQuery(fmt::format(R"(
            ALTER TOPIC `{input_topic}` ADD CONSUMER `{consumer_name}`;)",
            "input_topic"_a = inputTopicName,
            "consumer_name"_a = consumerName
        ));

        WaitFor(TDuration::Seconds(10), "Wait fail", [&](TString& error) {
            const auto& result = ExecQuery("SELECT Status FROM `.sys/streaming_queries`");
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);

            TString status;
            CheckScriptResult(result[0], 1, 1, [&](TResultSetParser& resultSet) {
                status = *resultSet.ColumnParser("Status").GetOptionalUtf8();
            });

            error = TStringBuilder() << "Query status: " << status;
            return status == "RUNNING";
        });

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(inputTopicName, R"({"key": "key1", "value": "value1"})");
        ReadTopicMessage(outputTopicName, R"({"key": "key1", "value": "value1"})");
        Sleep(TDuration::Seconds(1));

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            );)",
            "query_name"_a = queryName
        ));

        const auto disposition = TInstant::Now();
        WriteTopicMessage(inputTopicName, R"({"key": "key2", "value": "value2"})");

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = queryName
        ));

        ReadTopicMessage(outputTopicName, R"({"key": "key2", "value": "value2"})", disposition);
    }

    Y_UNIT_TEST_F(OffsetsAndStateRecoveryOnInternalRetry, TStreamingTestFixture) {
        QueryClientSettings = TClientSettings();

        // Join with S3 used for introducing temporary failure and force retry on specific key

        constexpr char sourceBucket[] = "test_streaming_query_recovery_on_internal_retry";
        constexpr char objectContent[] = R"(
{"fqdn": "host1.example.com", "payload": "P1"}
{"fqdn": "host2.example.com"                              })";
        constexpr char objectPath[] = "path/test_object.json";
        CreateBucketWithObject(sourceBucket, objectPath, objectContent);

        constexpr char inputTopicName[] = "internalRetryInputTopicName";
        constexpr char outputTopicName[] = "internalRetryOutputTopicName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "pqSourceName";
        constexpr char s3SourceName[] = "s3Source";
        CreatePqSource(pqSourceName);
        CreateS3Source(sourceBucket, s3SourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.HashJoinMode = "map";
                $s3_lookup = SELECT * FROM `{s3_source}`.`path/` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        fqdn String NOT NULL,
                        payload String
                    )
                );

                -- Test that offsets are recovered
                $pq_source = SELECT * FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        time String NOT NULL,
                        event String,
                        host String
                    )
                );

                $joined = SELECT
                    Unwrap(l.payload) AS payload, -- Test failure here
                    p.*
                FROM $pq_source AS p
                LEFT JOIN $s3_lookup AS l
                ON (l.fqdn = p.host);

                -- Test that state also recovered
                $grouped = SELECT
                    event,
                    CAST(SOME(time) AS String) AS time,
                    SOME(payload) AS payload,
                    CAST(COUNT(*) AS String) AS count
                FROM $joined
                GROUP BY
                    HOP (CAST(time AS Timestamp), "PT1H", "PT1H", "PT0H"),
                    event;

                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT Unwrap(event || "-" || time || "-" || payload || "-" || count) FROM $grouped
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "s3_source"_a = s3SourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        // Fill HOP state for key A
        WriteTopicMessages(inputTopicName, {
            R"({"time": "2025-08-24T00:00:00.000000Z", "event": "A", "host": "host1.example.com"})",
            R"({"time": "2025-08-25T00:00:00.000000Z", "event": "A", "host": "host1.example.com"})",
        });
        ReadTopicMessage(outputTopicName, "A-2025-08-24T00:00:00.000000Z-P1-1");

        Sleep(TDuration::Seconds(2));
        auto readDisposition = TInstant::Now();

        // Write failure message for key B
        WriteTopicMessage(inputTopicName, R"({"time": "2025-08-24T00:00:00.000000Z", "event": "B", "host": "host2.example.com"})");

        // Wait script execution retry
        WaitFor(TDuration::Seconds(10), "wait retry", [&](TString& error) {
            const auto& results = ExecQuery(R"(
                SELECT MAX(lease_generation) AS generation FROM `.metadata/script_executions`;
            )");
            UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);

            std::optional<i64> generation;
            CheckScriptResult(results[0], 1, 1, [&](TResultSetParser& result) {
                generation = result.ColumnParser(0).GetOptionalInt64();
            });

            if (!generation || *generation < 2) {
                error = TStringBuilder() << "generation is: " << (generation ? ToString(*generation) : "null");
                return false;
            }

            return true;
        });

        // Resolve query failure
        UploadObject(sourceBucket, objectPath, R"(
{"fqdn": "host1.example.com", "payload": "P1"}
{"fqdn": "host2.example.com", "payload": "P2"             })");
        Sleep(TDuration::Seconds(2));

        // Check that offset is restored
        WriteTopicMessage(inputTopicName, R"({"time": "2025-08-25T00:00:00.000000Z", "event": "B", "host": "host2.example.com"})");
        ReadTopicMessage(outputTopicName, "B-2025-08-24T00:00:00.000000Z-P2-1", readDisposition);

        Sleep(TDuration::Seconds(1));
        readDisposition = TInstant::Now();

        // Check that HOP state is restored
        WriteTopicMessage(inputTopicName, R"({"time": "2025-08-26T00:00:00.000000Z", "event": "A", "host": "host1.example.com"})");
        ReadTopicMessages(outputTopicName, {
            "A-2025-08-25T00:00:00.000000Z-P1-1",
            "B-2025-08-25T00:00:00.000000Z-P2-1"
        }, readDisposition, /* sort */ true);
    }

    struct TTestInfo {
        TString InputTopicName;
        TString OutputTopicName;
        TString PqSourceName;
        TString QueryName;
        TString QueryText;
    };

    TTestInfo SetupCheckpointRecoveryTest(TStreamingTestFixture& self) {
        TTestInfo info = {
            .InputTopicName = TStringBuilder() << "inputTopicName" << self.Name_,
            .OutputTopicName = TStringBuilder() << "outputTopicName" << self.Name_,
            .PqSourceName = "pqSourceName",
            .QueryName = "streamingQuery"
        };
        info.QueryText = fmt::format(R"(
            -- Test that offsets are recovered
            $pq_source = SELECT * FROM `{pq_source}`.`{input_topic}` WITH (
                FORMAT = "json_each_row",
                SCHEMA (
                    time String NOT NULL,
                    event String
                )
            );

            -- Test that state also recovered
            $grouped = SELECT
                event,
                CAST(SOME(time) AS String) AS time,
                CAST(COUNT(*) AS String) AS count
            FROM $pq_source
            GROUP BY
                HOP (CAST(time AS Timestamp), "PT1H", "PT1H", "PT0H"),
                event;

            INSERT INTO `{pq_source}`.`{output_topic}`
            SELECT Unwrap(event || "-" || time || "-" || count) FROM $grouped)",
            "pq_source"_a = info.PqSourceName,
            "input_topic"_a = info.InputTopicName,
            "output_topic"_a = info.OutputTopicName
        );

        self.CreateTopic(info.InputTopicName);
        self.CreateTopic(info.OutputTopicName);
        self.CreatePqSource(info.PqSourceName);

        self.ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                {query_text}
            END DO;)",
            "query_name"_a = info.QueryName,
            "query_text"_a = info.QueryText
        ));
        self.CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        return info;
    }

    Y_UNIT_TEST_F(OffsetsAndStateRecoveryOnManualRestart, TStreamingTestFixture) {
        const auto info = SetupCheckpointRecoveryTest(*this);

        WriteTopicMessages(info.InputTopicName, {
            R"({"time": "2025-08-24T00:00:00.000000Z", "event": "A"})",
            R"({"time": "2025-08-25T00:00:00.000000Z", "event": "A"})",
        });
        ReadTopicMessage(info.OutputTopicName, "A-2025-08-24T00:00:00.000000Z-1");

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            );)",
            "query_name"_a = info.QueryName
        ));
        CheckScriptExecutionsCount(1, 0);

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-25T00:00:00.000000Z", "event": "B"})");
        const auto readDisposition = TInstant::Now();

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = info.QueryName
        ));
        CheckScriptExecutionsCount(2, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-26T00:00:00.000000Z", "event": "A"})");
        ReadTopicMessages(info.OutputTopicName, {
            "A-2025-08-25T00:00:00.000000Z-1",
            "B-2025-08-25T00:00:00.000000Z-1"
        }, readDisposition, /* sort */ true);
    }

    Y_UNIT_TEST_F(OffsetsRecoveryOnQueryTextChangeBasic, TStreamingTestFixture) {
        const auto info = SetupCheckpointRecoveryTest(*this);

        WriteTopicMessages(info.InputTopicName, {
            R"({"time": "2025-08-24T00:00:00.000000Z", "event": "A"})",
            R"({"time": "2025-08-25T00:00:00.000000Z", "event": "A"})",
        });
        ReadTopicMessage(info.OutputTopicName, "A-2025-08-24T00:00:00.000000Z-1");

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                FORCE = TRUE,
                RUN = FALSE
            ) AS
            DO BEGIN
                /* some comment */
                {text}
            END DO;)",
            "query_name"_a = info.QueryName,
            "text"_a = info.QueryText
        ));
        CheckScriptExecutionsCount(1, 0);

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-25T00:00:00.000000Z", "event": "B"})");
        const auto readDisposition = TInstant::Now();

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = info.QueryName
        ));
        CheckScriptExecutionsCount(2, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-26T00:00:00.000000Z", "event": "A"})");
        ReadTopicMessage(info.OutputTopicName, "B-2025-08-25T00:00:00.000000Z-1", readDisposition);
    }

    Y_UNIT_TEST_F(OffsetsRecoveryOnQueryTextChangeCreateOrReplace, TStreamingTestFixture) {
        const auto info = SetupCheckpointRecoveryTest(*this);

        WriteTopicMessages(info.InputTopicName, {
            R"({"time": "2025-08-24T00:00:00.000000Z", "event": "A"})",
            R"({"time": "2025-08-25T00:00:00.000000Z", "event": "A"})",
        });
        ReadTopicMessage(info.OutputTopicName, "A-2025-08-24T00:00:00.000000Z-1");

        ExecQuery(fmt::format(R"(
            CREATE OR REPLACE STREAMING QUERY `{query_name}` WITH (
                RUN = FALSE
            ) AS
            DO BEGIN
                /* some comment */
                {text}
            END DO;)",
            "query_name"_a = info.QueryName,
            "text"_a = info.QueryText
        ));
        CheckScriptExecutionsCount(1, 0);

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-25T00:00:00.000000Z", "event": "B"})");
        const auto readDisposition = TInstant::Now();

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = info.QueryName
        ));
        CheckScriptExecutionsCount(2, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-26T00:00:00.000000Z", "event": "A"})");
        ReadTopicMessage(info.OutputTopicName, "B-2025-08-25T00:00:00.000000Z-1", readDisposition);
    }

    Y_UNIT_TEST_F(OffsetsRecoveryOnQueryTextChangeWithFail, TStreamingTestFixture) {
        const auto info = SetupCheckpointRecoveryTest(*this);

        WriteTopicMessages(info.InputTopicName, {
            R"({"time": "2025-08-24T00:00:00.000000Z", "event": "A"})",
            R"({"time": "2025-08-25T00:00:00.000000Z", "event": "A"})",
        });
        ReadTopicMessage(info.OutputTopicName, "A-2025-08-24T00:00:00.000000Z-1");

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                FORCE = TRUE,
                RESOURCE_POOL = "unknown_pool"
            ) AS
            DO BEGIN
                /* some comment */
                {text}
            END DO;)",
            "query_name"_a = info.QueryName,
            "text"_a = info.QueryText
        ), EStatus::NOT_FOUND, "Resource pool unknown_pool not found or you don't have access permissions");

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-25T00:00:00.000000Z", "event": "B"})");
        const auto readDisposition = TInstant::Now();

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RESOURCE_POOL = "default"
            );)",
            "query_name"_a = info.QueryName
        ));
        CheckScriptExecutionsCount(3, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-26T00:00:00.000000Z", "event": "A"})");
        ReadTopicMessage(info.OutputTopicName, "B-2025-08-25T00:00:00.000000Z-1", readDisposition);
    }

    Y_UNIT_TEST_F(OffsetsAndStateRecoveryAfterQueryTextChange, TStreamingTestFixture) {
        const auto info = SetupCheckpointRecoveryTest(*this);

        WriteTopicMessages(info.InputTopicName, {
            R"({"time": "2025-08-24T00:00:00.000000Z", "event": "A"})",
            R"({"time": "2025-08-25T00:00:00.000000Z", "event": "A"})",
        });
        ReadTopicMessage(info.OutputTopicName, "A-2025-08-24T00:00:00.000000Z-1");

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                FORCE = TRUE,
                RUN = FALSE
            ) AS
            DO BEGIN
                /* some comment */
                {text}
            END DO;)",
            "query_name"_a = info.QueryName,
            "text"_a = info.QueryText
        ));
        CheckScriptExecutionsCount(1, 0);

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-25T00:00:00.000000Z", "event": "B"})");
        auto readDisposition = TInstant::Now();

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = info.QueryName
        ));
        CheckScriptExecutionsCount(2, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-26T00:00:00.000000Z", "event": "A"})");
        ReadTopicMessage(info.OutputTopicName, "B-2025-08-25T00:00:00.000000Z-1", readDisposition);

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            );)",
            "query_name"_a = info.QueryName
        ));
        CheckScriptExecutionsCount(2, 0);

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-26T00:00:00.000000Z", "event": "B"})");
        readDisposition = TInstant::Now();

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = info.QueryName
        ));
        CheckScriptExecutionsCount(3, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-27T00:00:00.000000Z", "event": "A"})");
        ReadTopicMessages(info.OutputTopicName, {
            "A-2025-08-26T00:00:00.000000Z-1",
            "B-2025-08-26T00:00:00.000000Z-1"
        }, readDisposition, /* sort */ true);
    }

    Y_UNIT_TEST_F(CheckpointPropagationWithStreamLookupJoinHanging, TStreamingTestFixture) {
        {
            auto& setupAppConfig = SetupAppConfig();
            setupAppConfig.MutableTableServiceConfig()->SetEnableDqSourceStreamLookupJoin(true);
        }
        const auto connectorClient = SetupMockConnectorClient();

        constexpr char inputTopicName[] = "sljInputTopicName";
        constexpr char outputTopicName[] = "sljOutputTopicName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "pqSourceName";
        constexpr char ydbSourceName[] = "ydbSourceName";
        CreatePqSource(pqSourceName);
        CreateYdbSource(ydbSourceName);

        constexpr char ydbTable[] = "lookup";
        ExecExternalQuery(fmt::format(R"(
            CREATE TABLE `{table}` (
                fqdn String,
                payload String,
                PRIMARY KEY (fqdn)
            ))",
            "table"_a = ydbTable
        ));

        {   // Prepare connector mock
            const std::vector<TColumn> columns = {
                {"fqdn", Ydb::Type::STRING},
                {"payload", Ydb::Type::STRING}
            };
            SetupMockConnectorTableDescription(connectorClient, {
                .TableName = ydbTable,
                .Columns = columns,
                .DescribeCount = 2,
                .ListSplitsCount = 4,
                .ValidateListSplitsArgs = false
            });

            const std::vector<std::string> fqdnColumn = {"host1.example.com", "host2.example.com", "host3.example.com"};
            const std::vector<std::string> payloadColumn = std::vector<std::string>{"P1", "P2", "P3"};
            SetupMockConnectorTableData(connectorClient, {
                .TableName = ydbTable,
                .Columns = columns,
                .NumberReadSplits = 3,
                .ValidateReadSplitsArgs = false,
                .ResultFactory = [&]() {
                    return MakeRecordBatch(
                        MakeArray<arrow::BinaryBuilder>("fqdn", fqdnColumn, arrow::binary()),
                        MakeArray<arrow::BinaryBuilder>("payload", payloadColumn, arrow::binary())
                    );
                }
            });
        }

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $ydb_lookup = SELECT * FROM `{ydb_source}`.`{ydb_table}`;

                $pq_source = SELECT * FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        time Int32 NOT NULL,
                        event String,
                        host String
                    )
                );

                $joined = SELECT l.payload AS payload, p.* FROM $pq_source AS p
                LEFT JOIN /*+ streamlookup(TTL 1) */ ANY $ydb_lookup AS l
                ON (l.fqdn = p.host);

                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT Unwrap(event || "-" || payload) FROM $joined
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "ydb_source"_a = ydbSourceName,
            "ydb_table"_a = ydbTable,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));
        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, R"({"time": 0, "event": "A", "host": "host1.example.com"})");
        ReadTopicMessage(outputTopicName, "A-P1");

        connectorClient->LockReading();
        WriteTopicMessage(inputTopicName, R"({"time": 1, "event": "B", "host": "host3.example.com"})");
        Sleep(TDuration::Seconds(2));
        const auto readDisposition = TInstant::Now();

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(1, 0);

        connectorClient->UnlockReading();

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(2, 1);

        ReadTopicMessage(outputTopicName, "B-P3", readDisposition);
    }

    Y_UNIT_TEST_F(CheckpointPropagationWithS3Insert, TStreamingTestFixture) {
        constexpr char inputTopicName[] = "s3InsertCheckpointsInputTopicName";
        constexpr char pqSourceName[] = "pqSourceName";
        CreateTopic(inputTopicName);
        CreatePqSource(pqSourceName);

        constexpr char sourceBucket[] = "test_bucket_streaming_query_s3_insert_checkpoint_propagation";
        constexpr char s3SinkName[] = "sinkName";
        CreateBucket(sourceBucket);
        CreateS3Source(sourceBucket, s3SinkName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{s3_sink}`.`test/` WITH (
                    FORMAT = raw
                ) SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "s3_sink"_a = s3SinkName,
            "input_topic"_a = inputTopicName
        ));
        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, "data-1");
        Sleep(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(sourceBucket), "data-1");

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(1, 0);

        WriteTopicMessage(inputTopicName, "data-2");

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(2, 1);

        Sleep(TDuration::Seconds(2));
        if (const auto& s3Data = GetAllObjects(sourceBucket); !IsIn({"data-1data-2", "data-2data-1"}, s3Data)) {
            UNIT_FAIL("Unexpected S3 data: " << s3Data);
        }
    }

    void CheckTable(TStreamingTestFixture& self, const TString& tableName, const std::map<TString, TString>& rows) {
        const auto results = self.ExecQuery(fmt::format(
            "SELECT * FROM `{table}` ORDER BY Key",
            "table"_a = tableName
        ));
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);

        auto it = rows.begin();
        self.CheckScriptResult(results[0], 2, rows.size(), [&](TResultSetParser& parser) {
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Key").GetString(), it->first);
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Value").GetString(), it->second);
            ++it;
        });
    }

    Y_UNIT_TEST_F(WritingInLocalYdbTablesWithCheckpoints, TStreamingTestFixture) {
        constexpr char pqSourceName[] = "pqSource";
        CreatePqSource(pqSourceName);

        for (const bool rowTables : {true, false}) {
            const auto inputTopicName = TStringBuilder() << "writingInLocalYdbInputTopicName" << rowTables;
            CreateTopic(inputTopicName);

            const auto ydbTable = TStringBuilder() << "tableSink" << rowTables;
            ExecQuery(fmt::format(R"(
                CREATE TABLE `{table}` (
                    Key String NOT NULL,
                    Value String NOT NULL,
                    PRIMARY KEY (Key)
                ) {settings})",
                "table"_a = ydbTable,
                "settings"_a = rowTables ? "" : "WITH (STORE = COLUMN)"
            ));

            const auto queryName = TStringBuilder() << "streamingQuery" << rowTables;
            ExecQuery(fmt::format(R"(
                CREATE STREAMING QUERY `{query_name}` AS
                DO BEGIN
                    UPSERT INTO `{ydb_table}`
                    SELECT * FROM `{pq_source}`.`{input_topic}` WITH (
                        FORMAT = json_each_row,
                        SCHEMA (
                            Key String NOT NULL,
                            Value String NOT NULL
                        )
                    )
                END DO;)",
                "query_name"_a = queryName,
                "pq_source"_a = pqSourceName,
                "input_topic"_a = inputTopicName,
                "ydb_table"_a = ydbTable
            ));

            CheckScriptExecutionsCount(1, 1);
            Sleep(TDuration::Seconds(1));

            WriteTopicMessage(inputTopicName, R"({"Key": "message1", "Value": "value1"})");
            Sleep(TDuration::Seconds(1));
            CheckTable(*this, ydbTable, {{"message1", "value1"}});

            ExecQuery(fmt::format(R"(
                ALTER STREAMING QUERY `{query_name}` SET (
                    RUN = FALSE
                );)",
                "query_name"_a = queryName
            ));
            CheckScriptExecutionsCount(1, 0);
            CheckTable(*this, ydbTable, {{"message1", "value1"}});

            Sleep(TDuration::Seconds(1));
            WriteTopicMessage(inputTopicName, R"({"Key": "message2", "Value": "value2"})");

            ExecQuery(fmt::format(R"(
                ALTER STREAMING QUERY `{query_name}` SET (
                    RUN = TRUE
                );)",
                "query_name"_a = queryName
            ));
            CheckScriptExecutionsCount(2, 1);
            Sleep(TDuration::Seconds(1));

            WriteTopicMessage(inputTopicName, R"({"Key": "message1", "Value": "value3"})");
            Sleep(TDuration::Seconds(1));
            CheckTable(*this, ydbTable, {{"message1", "value3"}, {"message2", "value2"}});

            ExecQuery(fmt::format(
                "DROP STREAMING QUERY `{query_name}`",
                "query_name"_a = queryName
            ));
            CheckScriptExecutionsCount(0, 0);
        }
    }

    Y_UNIT_TEST_F(WritingInLocalYdbTablesWithLimit, TStreamingTestFixture) {
        constexpr char pqSourceName[] = "pqSource";
        CreatePqSource(pqSourceName);

        for (const bool rowTables : {true, false}) {
            const auto inputTopicName = TStringBuilder() << "writingInLocalYdbWithLimitInputTopicName" << rowTables;
            CreateTopic(inputTopicName);

            const auto ydbTable = TStringBuilder() << "tableSink" << rowTables;
            ExecQuery(fmt::format(R"(
                CREATE TABLE `{table}` (
                    Key String NOT NULL,
                    Value String NOT NULL,
                    PRIMARY KEY (Key)
                ) {settings})",
                "table"_a = ydbTable,
                "settings"_a = rowTables ? "" : "WITH (STORE = COLUMN)"
            ));

            const auto queryName = TStringBuilder() << "streamingQuery" << rowTables;
            ExecQuery(fmt::format(R"(
                CREATE STREAMING QUERY `{query_name}` AS
                DO BEGIN
                    UPSERT INTO `{ydb_table}`
                    SELECT * FROM `{pq_source}`.`{input_topic}` WITH (
                        FORMAT = json_each_row,
                        SCHEMA (
                            Key String NOT NULL,
                            Value String NOT NULL
                        )
                    ) LIMIT 1
                END DO;)",
                "query_name"_a = queryName,
                "pq_source"_a = pqSourceName,
                "input_topic"_a = inputTopicName,
                "ydb_table"_a = ydbTable
            ));

            CheckScriptExecutionsCount(1, 1);
            Sleep(TDuration::Seconds(1));

            WriteTopicMessage(inputTopicName, R"({"Key": "message1", "Value": "value1"})");
            Sleep(TDuration::Seconds(1));
            CheckTable(*this, ydbTable, {{"message1", "value1"}});

            Sleep(TDuration::Seconds(1));
            CheckScriptExecutionsCount(1, 0);

            ExecQuery(fmt::format(
                "DROP STREAMING QUERY `{query_name}`",
                "query_name"_a = queryName
            ));
            CheckScriptExecutionsCount(0, 0);
        }
    }

    Y_UNIT_TEST_F(WritingInLocalYdbTablesWithProjection, TStreamingTestFixture) {
        constexpr char pqSourceName[] = "pqSource";
        CreatePqSource(pqSourceName);

        for (const bool rowTables : {true, false}) {
            const auto inputTopicName = TStringBuilder() << "writingInLocalYdbWithLimitInputTopicName" << rowTables;
            CreateTopic(inputTopicName);

            const auto ydbTable = TStringBuilder() << "tableSink" << rowTables;
            ExecQuery(fmt::format(R"(
                CREATE TABLE `{table}` (
                    Key String NOT NULL,
                    Value String NOT NULL,
                    PRIMARY KEY (Key)
                ) {settings})",
                "table"_a = ydbTable,
                "settings"_a = rowTables ? "" : "WITH (STORE = COLUMN)"
            ));

            const auto queryName = TStringBuilder() << "streamingQuery" << rowTables;
            ExecQuery(fmt::format(R"(
                CREATE STREAMING QUERY `{query_name}` AS
                DO BEGIN
                    UPSERT INTO `{ydb_table}`
                    SELECT (Key || "x") AS Key, Value FROM `{pq_source}`.`{input_topic}` WITH (
                        FORMAT = json_each_row,
                        SCHEMA (
                            Key String NOT NULL,
                            Value String NOT NULL
                        )
                    ) LIMIT 1
                END DO;)",
                "query_name"_a = queryName,
                "pq_source"_a = pqSourceName,
                "input_topic"_a = inputTopicName,
                "ydb_table"_a = ydbTable
            ));

            CheckScriptExecutionsCount(1, 1);
            Sleep(TDuration::Seconds(1));

            WriteTopicMessage(inputTopicName, R"({"Key": "message1", "Value": "value1"})");
            Sleep(TDuration::Seconds(1));
            CheckTable(*this, ydbTable, {{"message1x", "value1"}});

            Sleep(TDuration::Seconds(1));
            CheckScriptExecutionsCount(1, 0);

            ExecQuery(fmt::format(
                "DROP STREAMING QUERY `{query_name}`",
                "query_name"_a = queryName
            ));
            CheckScriptExecutionsCount(0, 0);
        }
    }

    Y_UNIT_TEST_F(DropStreamingQueryUnderLoad, TStreamingTestFixture) {
        LogSettings.Freeze = true;
        SetupAppConfig().MutableQueryServiceConfig()->SetProgressStatsPeriodMs(1);

        constexpr char inputTopicName[] = "inputTopic";
        constexpr char outputTopicName[] = "outputTopic";
        constexpr char pqSourceName[] = "pqSource";
        ExecQuery(fmt::format(R"(
            CREATE TOPIC `{input_topic}` WITH (
                min_active_partitions = 100,
                partition_count_limit = 100
            );
            CREATE TOPIC `{output_topic}` WITH (
                min_active_partitions = 100,
                partition_count_limit = 100
            );
            CREATE EXTERNAL DATA SOURCE `{pq_source}` WITH (
                SOURCE_TYPE = "Ydb",
                LOCATION = "{pq_location}",
                DATABASE_NAME = "{pq_database_name}",
                AUTH_METHOD = "NONE"
            );)",
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName,
            "pq_source"_a = pqSourceName,
            "pq_location"_a = GetKikimrRunner()->GetEndpoint(),
            "pq_database_name"_a = "/Root"
        ));

        const auto queryName = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.MaxTasksPerStage = "100";
                PRAGMA ydb.OverridePlanner = @@ [
                    {{ "tx": 0, "stage": 0, "tasks": 100 }}
                ] @@;
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        auto promise = NThreading::NewPromise();
        Y_DEFER {
            promise.SetValue();
        };

        for (ui32 i = 0; i < 10; ++i) {
            GetRuntime().Register(new TTestTopicLoader(GetKikimrRunner()->GetEndpoint(), "/Root", inputTopicName, promise.GetFuture()));
        }

        Sleep(TDuration::Seconds(2));
        CheckScriptExecutionsCount(1, 1);

        ExecQuery(fmt::format(R"(
            DROP STREAMING QUERY `{query_name}`;)",
            "query_name"_a = queryName
        ));

        CheckScriptExecutionsCount(0, 0);
    }

    Y_UNIT_TEST_F(CreateStreamingQueryUnderTimeout, TStreamingWithSchemaSecretsTestFixture) {
        auto& config = *SetupAppConfig().MutableQueryServiceConfig();
        config.SetQueryTimeoutDefaultSeconds(3);
        config.SetScriptOperationTimeoutDefaultSeconds(3);

        constexpr char inputTopicName[] = "createStreamingQueryUnderTimeoutInputTopic";
        constexpr char outputTopicName[] = "createStreamingQueryUnderTimeoutOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));
        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(5));

        WriteTopicMessage(inputTopicName, "data1");
        ReadTopicMessage(outputTopicName, "data1");
        Sleep(TDuration::Seconds(5));

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");
        const auto& result = ExecQuery("SELECT RetryCount FROM `.sys/streaming_queries`");
        UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);
        CheckScriptResult(result[0], 1, 1, [&](TResultSetParser& resultSet) {
            UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser("RetryCount").GetOptionalUint64(), 0);
        });
    }

    Y_UNIT_TEST_F(StreamingQueryDisposition, TStreamingWithSchemaSecretsTestFixture) {
        constexpr char inputTopicName[] = "createStreamingQueryUnderTimeoutInputTopic";
        constexpr char outputTopicName[] = "createStreamingQueryUnderTimeoutOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        ui64 dataIdx = 0;
        WriteTopicMessage(inputTopicName, TStringBuilder() << "data" << ++dataIdx);
        Sleep(TDuration::Seconds(1));

        const auto readDisposition = TInstant::Now();
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, TStringBuilder() << "data" << ++dataIdx);
        Sleep(TDuration::Seconds(1));

        // Test OLDEST disposition
        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` WITH (
                STREAMING_DISPOSITION = OLDEST
            ) AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));
        ui64 executionsCount = 0;
        CheckScriptExecutionsCount(++executionsCount, 1);

        ReadTopicMessages(outputTopicName, {"data1", "data2"});
        auto writeDisposition = TInstant::Now();

        // Test FROM_TIME disposition
        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                STREAMING_DISPOSITION = (
                    FROM_TIME = "{disposition}"
                )
            );)",
            "query_name"_a = queryName,
            "disposition"_a = readDisposition.ToString()
        ));
        CheckScriptExecutionsCount(++executionsCount, 1);

        ReadTopicMessage(outputTopicName, "data2", writeDisposition);
        writeDisposition = TInstant::Now();

        // Test TIME_AGO disposition
        const auto duration = TInstant::Now() - readDisposition;
        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                STREAMING_DISPOSITION = (
                    TIME_AGO = "PT{disposition}S"
                )
            );)",
            "query_name"_a = queryName,
            "disposition"_a = TStringBuilder() << duration.Seconds() << "." << duration.MicroSecondsOfSecond()
        ));
        CheckScriptExecutionsCount(++executionsCount, 1);

        ReadTopicMessage(outputTopicName, "data2", writeDisposition);
        writeDisposition = TInstant::Now();

        // Test checkpoint dispositions
        for (const TString& disposition : {"", "FROM_CHECKPOINT", "FROM_CHECKPOINT_FORCE"}) {
            Sleep(TDuration::Seconds(1));
            ExecQuery(fmt::format(R"(
                ALTER STREAMING QUERY `{query_name}` SET (
                    RUN = FALSE
                );)",
                "query_name"_a = queryName
            ));
            CheckScriptExecutionsCount(std::min(executionsCount, (ui64)4), 0);

            WriteTopicMessage(inputTopicName, TStringBuilder() << "data" << ++dataIdx);

            ExecQuery(fmt::format(R"(
                ALTER STREAMING QUERY `{query_name}` SET (
                    RUN = TRUE,
                    {disposition}
                );)",
                "query_name"_a = queryName,
                "disposition"_a = disposition ? TStringBuilder() << "STREAMING_DISPOSITION = " << disposition : TStringBuilder()
            ));
            CheckScriptExecutionsCount(std::min(++executionsCount, (ui64)4), 1);

            ReadTopicMessage(outputTopicName, TStringBuilder() << "data" << dataIdx, writeDisposition);
            writeDisposition = TInstant::Now();
        }

        // Test fresh disposition
        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                STREAMING_DISPOSITION = FRESH
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(std::min(++executionsCount, (ui64)4), 1);

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(inputTopicName, TStringBuilder() << "data" << ++dataIdx);
        ReadTopicMessage(outputTopicName, TStringBuilder() << "data" << dataIdx, writeDisposition);
    }

    Y_UNIT_TEST_F(StreamingQueryWithMultipleWrites, TStreamingWithSchemaSecretsTestFixture) {
        auto& config = SetupAppConfig();
        config.MutableTableServiceConfig()->SetEnableDataShardCreateTableAs(true);
        config.MutableTableServiceConfig()->SetEnableHtapTx(true);

        constexpr char inputTopic[] = "createStreamingQueryWithMultipleWritesInputTopic";
        constexpr char outputTopic1[] = "createStreamingQueryWithMultipleWritesOutputTopic1";
        constexpr char outputTopic2[] = "createStreamingQueryWithMultipleWritesOutputTopic2";
        constexpr char pqSource[] = "sourceName";
        CreateTopic(inputTopic);
        CreateTopic(outputTopic1);
        CreateTopic(outputTopic2);
        CreatePqSource(pqSource);

        constexpr char sinkBucket[] = "test_bucket_streaming_query_multi_insert";
        constexpr char s3SinkName[] = "s3SinkName";
        CreateBucket(sinkBucket);
        CreateS3Source(sinkBucket, s3SinkName);

        constexpr char solomonSink[] = "solomonSinkName";
        CreateSolomonSource(solomonSink);

        constexpr char rowSinkTable[] = "rowSink";
        constexpr char columnSinkTable[] = "columnSink";
        ExecQuery(fmt::format(R"(
            CREATE TABLE `{row_table}` (
                B Utf8 NOT NULL,
                PRIMARY KEY (B)
            );
            CREATE TABLE `{column_table}` (
                C String NOT NULL,
                PRIMARY KEY (C)
            ) WITH (
                STORE = COLUMN
            );)",
            "row_table"_a = rowSinkTable,
            "column_table"_a = columnSinkTable
        ));

        constexpr char queryName[] = "streamingQuery";
        const TSolomonLocation soLocation = {
            .ProjectId = "cloudId1",
            .FolderId = "folderId1",
            .Service = "custom",
            .IsCloud = false,
        };
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $rows = SELECT * FROM `{pq_source}`.`{input_topic}`;

                INSERT INTO `{pq_source}`.`{output_topic1}` SELECT Data || "-A" AS X FROM $rows;

                INSERT INTO `{pq_source}`.`{output_topic2}` SELECT Data || "-B" AS Y FROM $rows;

                UPSERT INTO `{row_table}` SELECT Unwrap(CAST(Data || "-C" AS Utf8)) AS B FROM $rows;

                UPSERT INTO `{column_table}` SELECT Data || "-D" AS C FROM $rows;

                INSERT INTO `{s3_sink}`.`test/` WITH (
                    FORMAT = raw
                ) SELECT Data || "-E" AS D FROM $rows;

                INSERT INTO `{solomon_sink}`.`{solomon_project}/{solomon_folder}/{solomon_service}`
                SELECT
                    42 AS value,
                    Data || "-F" AS sensor,
                    Timestamp("2025-03-12T14:40:39Z") AS ts
                FROM $rows;
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSource,
            "input_topic"_a = inputTopic,
            "output_topic1"_a = outputTopic1,
            "output_topic2"_a = outputTopic2,
            "row_table"_a = rowSinkTable,
            "column_table"_a = columnSinkTable,
            "s3_sink"_a = s3SinkName,
            "solomon_sink"_a = solomonSink,
            "solomon_project"_a = soLocation.ProjectId,
            "solomon_folder"_a = soLocation.FolderId,
            "solomon_service"_a = soLocation.Service
        ));
        CheckScriptExecutionsCount(1, 1);

        CleanupSolomon(soLocation);
        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(inputTopic, "test");
        ReadTopicMessage(outputTopic1, "test-A");
        ReadTopicMessage(outputTopic2, "test-B");

        const auto& results = ExecQuery(fmt::format(R"(
            SELECT * FROM `{row_table}`;
            SELECT * FROM `{column_table}`;)",
            "row_table"_a = rowSinkTable,
            "column_table"_a = columnSinkTable
        ));
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);

        CheckScriptResult(results[0], 1, 1, [&](TResultSetParser& resultSet) {
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("B").GetUtf8(), "test-C");
        });

        CheckScriptResult(results[1], 1, 1, [&](TResultSetParser& resultSet) {
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("C").GetString(), "test-D");
        });

        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(sinkBucket), "test-E");

        const TString expectedMetrics = R"([
  {
    "labels": [
      [
        "name",
        "value"
      ],
      [
        "sensor",
        "test-F"
      ]
    ],
    "ts": 1741790439,
    "value": 42
  }
])";
        UNIT_ASSERT_STRINGS_EQUAL(GetSolomonMetrics(soLocation), expectedMetrics);
    }

    Y_UNIT_TEST_F(DropStreamingQueryDuringRetries, TStreamingWithSchemaSecretsTestFixture) {
        constexpr char topic[] = "dropStreamingQueryDuringRetriesTopic";
        constexpr char pqSource[] = "pqSource";
        CreateTopic(topic);
        CreatePqSource(pqSource);

        const auto queryName = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA pq.Consumer = "test-consumer";
                INSERT INTO `{pq_source}`.`{topic}`
                SELECT * FROM `{pq_source}`.`{topic}`;
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSource,
            "topic"_a = topic
        ));

        Sleep(TDuration::Seconds(3));

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");
        {
            const auto& result = ExecQuery("SELECT RetryCount, SuspendedUntil FROM `.sys/streaming_queries`");
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);
            CheckScriptResult(result[0], 2, 1, [&](TResultSetParser& resultSet) {
                UNIT_ASSERT_GE(*resultSet.ColumnParser("RetryCount").GetOptionalUint64(), 1);
                UNIT_ASSERT(*resultSet.ColumnParser("SuspendedUntil").GetOptionalTimestamp());
            });
        }

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (RUN = FALSE);)",
            "query_name"_a = queryName
        ));

        {
            const auto& result = ExecQuery("SELECT Status FROM `.sys/streaming_queries`");
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);
            CheckScriptResult(result[0], 1, 1, [&](TResultSetParser& resultSet) {
                UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser("Status").GetOptionalUtf8(), "FAILED");
            });
        }

        ExecQuery(fmt::format(R"(
            DROP STREAMING QUERY `{query_name}`;)",
            "query_name"_a = queryName
        ));

        CheckScriptExecutionsCount(0, 0);
    }
}

Y_UNIT_TEST_SUITE(KqpStreamingQueriesSysView) {
    Y_UNIT_TEST_F(ReadNotCreatedSysView, TStreamingSysViewTestFixture) {
        Setup();
        CheckSysView({});
    }

    Y_UNIT_TEST_F(ReadRangesForSysView, TStreamingSysViewTestFixture) {
        Setup();

        StartQuery("A");
        StartQuery("B");
        StartQuery("C");
        StartQuery("D");
        StartQuery("E");
        Sleep(STATS_WAIT_DURATION);

        CheckSysView({
            {"A"}, {"B"}, {"C"}, {"D"}, {"E"}
        });

        CheckSysView({
            {"B"}, {"C"}, {"D"}
        }, "Path > '/Root/A' AND Path < '/Root/E'");

        CheckSysView({
            {"C"}, {"D"}, {"E"}
        }, "Path > '/Root/B'");

        CheckSysView({
            {"A"}, {"B"}
        }, "Path < '/Root/C'");

        CheckSysView({
            {"C"}, {"D"}, {"E"}
        }, "Path >= '/Root/C' AND Path <= '/Root/E'");

        CheckSysView({
            {"D"}, {"E"}
        }, "Path >= '/Root/D'");

        CheckSysView({
            {"A"}, {"B"}
        }, "Path <= '/Root/B'");

        CheckSysView({
            {"C"}
        }, "Path = '/Root/C'");
    }

    Y_UNIT_TEST_F(ReadWithoutAuth, TStreamingSysViewTestFixture) {
        QueryClientSettings = TClientSettings();
        Setup();

        StartQuery("A");
        StartQuery("B");
        StartQuery("C");
        Sleep(STATS_WAIT_DURATION);

        CheckSysView({
            {"A"}, {"B"}, {"C"}
        });
    }

    Y_UNIT_TEST_F(SortOrderForSysView, TStreamingSysViewTestFixture) {
        Setup();

        StartQuery("A");
        StartQuery("B");
        StartQuery("C");
        StartQuery("D");
        Sleep(STATS_WAIT_DURATION);

        CheckSysView({
            {"A"}, {"B"}, {"C"}, {"D"}
        }, "", "Path ASC");

        CheckSysView({
            {"C"}, {"D"}
        }, "Path >= '/Root/C'", "Path ASC");

        CheckSysView({
            {"D"}, {"C"}, {"B"}, {"A"}
        }, "", "Path DESC");

        CheckSysView({
            {"D"}, {"C"}
        }, "Path >= '/Root/C'", "Path DESC");
    }

    Y_UNIT_TEST_F(SysViewColumnsValues, TStreamingSysViewTestFixture) {
        const auto pqGateway = SetupMockPqGateway();
        Setup();

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            GRANT ALL ON `/Root` TO `root@builtin`;
            CREATE RESOURCE POOL MyResourcePool WITH (CONCURRENT_QUERY_LIMIT = "-1");
            CREATE STREAMING QUERY `{query_name}` WITH (
                RUN = FALSE,
                RESOURCE_POOL = "MyResourcePool"
            ) AS DO BEGIN{text}END DO)",
            "query_name"_a = queryName,
            "text"_a = GetQueryText(queryName)
        ));

        CheckSysView({{
            .Name = queryName,
            .Status = "CREATED",
            .Run = false,
            .Pool = "MyResourcePool",
        }});

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            ))",
            "query_name"_a = queryName
        ));

        auto readSession = pqGateway->WaitReadSession(InputTopic);
        readSession->AddDataReceivedEvent(0, "test0");
        pqGateway->WaitWriteSession(OutputTopic)->ExpectMessage("test0");

        {
            Sleep(STATS_WAIT_DURATION);
            const auto result = CheckSysView({{
                .Name = queryName,
                .Status = "RUNNING",
                .Run = true,
                .Pool = "MyResourcePool",
            }})[0];

            UNIT_ASSERT_VALUES_EQUAL(result.PreviousExecutionIds.size(), 0);
            const auto operation = GetScriptExecutionOperation(OperationIdFromExecutionId(result.ExecutionId));
            UNIT_ASSERT_VALUES_EQUAL(operation.Metadata().ExecStatus, EExecStatus::Running);
        }

        auto failAt = TInstant::Now();
        readSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});
        readSession = pqGateway->WaitReadSession(InputTopic);
        readSession->AddDataReceivedEvent(0, "test0");
        pqGateway->WaitWriteSession(OutputTopic)->ExpectMessage("test0");

        {
            Sleep(TDuration::Seconds(1));
            const auto result = CheckSysView({{
                .Name = queryName,
                .Status = "RUNNING",
                .Issues = "Test pq session failure",
                .Run = true,
                .Pool = "MyResourcePool",
                .RetryCount = 1,
                .LastFailAt = failAt,
            }})[0];

            UNIT_ASSERT_VALUES_EQUAL(result.PreviousExecutionIds.size(), 0);
            const auto operation = GetScriptExecutionOperation(OperationIdFromExecutionId(result.ExecutionId));
            UNIT_ASSERT_VALUES_EQUAL(operation.Metadata().ExecStatus, EExecStatus::Running);
        }

        failAt = TInstant::Now();
        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            ))",
            "query_name"_a = queryName
        ));

        {
            const auto result = CheckSysView({{
                .Name = queryName,
                .Status = "STOPPED",
                .Issues = "Request was canceled by user",
                .Run = false,
                .Pool = "MyResourcePool",
                .RetryCount = 1,
                .LastFailAt = failAt,
                .CheckPlan = true,
            }})[0];

            UNIT_ASSERT_VALUES_EQUAL(result.PreviousExecutionIds.size(), 1);
            UNIT_ASSERT(!result.ExecutionId);
            const auto operation = GetScriptExecutionOperation(OperationIdFromExecutionId(result.PreviousExecutionIds[0]), /* checkStatus */ false);
            const auto& status = operation.Status();
            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::CANCELLED, status.GetIssues().ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL(operation.Metadata().ExecStatus, EExecStatus::Canceled);
        }
    }

    Y_UNIT_TEST_F(SysViewForFinishedStreamingQueries, TStreamingSysViewTestFixture) {
        Setup();

        const std::vector<TString> texts = {
            fmt::format(R"(
                ;INSERT INTO `{pq_source}`.`{output_topic}`
                /* A */
                SELECT * FROM `{pq_source}`.`{input_topic}`
                LIMIT 1;)",
                "pq_source"_a = PQ_SOURCE,
                "output_topic"_a = OutputTopic,
                "input_topic"_a = InputTopic
            ), fmt::format(R"(
                ;PRAGMA ydb.OverridePlanner = @@ [
                    {{ "tx": 0, "stage": 10, "tasks": 1 }}
                ] @@;
                INSERT INTO `{pq_source}`.`{output_topic}`
                /* B */
                SELECT * FROM `{pq_source}`.`{input_topic}`;)",
                "pq_source"_a = PQ_SOURCE,
                "output_topic"_a = OutputTopic,
                "input_topic"_a = InputTopic
            )
        };

        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY A AS DO BEGIN{text}END DO)",
            "text"_a = texts[0]
        ));

        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY B AS DO BEGIN{text}END DO)",
            "text"_a = texts[1]
        ), EStatus::GENERIC_ERROR);

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(InputTopic, "test0");
        ReadTopicMessage(OutputTopic, "test0");
        Sleep(TDuration::Seconds(1));

        CheckSysView({{
            .Name = "A",
            .Status = "COMPLETED",
            .Text = texts[0],
            .CheckPlan = true,
        }, {
            .Name = "B",
            .Status = "CREATED",
            .Text = texts[1],
        }});
    }

    Y_UNIT_TEST_F(SysViewForSuspendedStreamingQueries, TStreamingSysViewTestFixture) {
        Setup();

        const TString text = fmt::format(R"(
            PRAGMA pq.Consumer = "unknown";
            INSERT INTO `{pq_source}`.`{output_topic}`
            SELECT * FROM `{pq_source}`.`{input_topic}`;)",
            "pq_source"_a = PQ_SOURCE,
            "output_topic"_a = OutputTopic,
            "input_topic"_a = InputTopic
        );

        const auto start = TInstant::Now();
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY A AS DO BEGIN{text}END DO)",
            "text"_a = text
        ));

        const auto timeout = TDuration::Seconds(20);
        WaitFor(timeout, "Wait query suspend", [&](TString& error) {
            const auto& result = ExecQuery("SELECT Status, SuspendedUntil FROM `.sys/streaming_queries`");
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);

            TString status;
            std::optional<TInstant> suspendedUntil;
            CheckScriptResult(result[0], 2, 1, [&](TResultSetParser& resultSet) {
                status = *resultSet.ColumnParser("Status").GetOptionalUtf8();
                suspendedUntil = resultSet.ColumnParser("SuspendedUntil").GetOptionalTimestamp();
            });

            if (status != "SUSPENDED") {
                error = TStringBuilder() << "Query status is not SUSPENDED, but " << status;
                return false;
            }

            const auto delta = abs(suspendedUntil->SecondsFloat() - start.SecondsFloat());
            UNIT_ASSERT_GE(timeout.SecondsFloat(), delta);
            return true;
        });
    }

    Y_UNIT_TEST_F(ReadPartOfSysViewColumns, TStreamingSysViewTestFixture) {
        Setup();

        StartQuery("A");

        const auto& result = ExecQuery(R"(
            SELECT
                Path,
                Text,
                Run,
                ResourcePool
            FROM `.sys/streaming_queries`
        )");
        UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);

        std::vector<TSysViewResult> results;
        CheckScriptResult(result[0], 4, 1, [&](TResultSetParser& resultSet) {
            UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser("Path").GetOptionalUtf8(), "/Root/A");
            UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser("Text").GetOptionalUtf8(), GetQueryText("A"));
            UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser("Run").GetOptionalBool(), true);
            UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser("ResourcePool").GetOptionalUtf8(), "default");
        });
    }

    Y_UNIT_TEST_F(ReadSysViewWithRowCountBackPressure, TStreamingSysViewTestFixture) {
        LogSettings.Freeze = true;
        SetupAppConfig().MutableTableServiceConfig()->MutableResourceManager()->SetChannelBufferSize(1_KB);
        Setup();

        constexpr ui64 NUMBER_OF_QUERIES = 2010;

        std::vector<TSysViewRow> rows;
        rows.reserve(NUMBER_OF_QUERIES);
        for (ui64 i = 0; i < NUMBER_OF_QUERIES; ++i) {
            const auto name = TStringBuilder() << "query-" << i;
            ExecQuery(fmt::format(R"(
                CREATE STREAMING QUERY `{name}` WITH (
                    RUN = FALSE
                ) AS DO BEGIN{text}END DO)",
                "name"_a = name,
                "text"_a = GetQueryText(name)
            ));
            rows.push_back({
                .Name = name,
                .Status = "CREATED",
                .Run = false,
            });
        }

        CheckSysView(rows);
    }

    Y_UNIT_TEST_F(ReadSysViewWithRowSizeBackPressure, TStreamingSysViewTestFixture) {
        LogSettings.Freeze = true;
        SetupAppConfig().MutableTableServiceConfig()->MutableResourceManager()->SetChannelBufferSize(1_KB);
        Setup();

        constexpr ui64 NUMBER_OF_QUERIES = 50;
        const TString payload(100_KB, 'x');

        std::vector<TSysViewRow> rows;
        rows.reserve(NUMBER_OF_QUERIES);
        for (ui64 i = 0; i < NUMBER_OF_QUERIES; ++i) {
            const auto name = TStringBuilder() << "query-" << i;
            const auto text = TStringBuilder() << GetQueryText(name) << " /* " << payload << " */";
            ExecQuery(fmt::format(R"(
                CREATE STREAMING QUERY `{name}` WITH (
                    RUN = FALSE
                ) AS DO BEGIN{text}END DO)",
                "name"_a = name,
                "text"_a = text
            ));
            rows.push_back({
                .Name = name,
                .Status = "CREATED",
                .Text = text,
                .Run = false,
            });
        }

        CheckSysView(rows);
    }

    Y_UNIT_TEST_F(ReadSysViewWithMetadataSizeBackPressure, TStreamingSysViewTestFixture) {
        LogSettings.Freeze = true;
        SetupAppConfig().MutableTableServiceConfig()->MutableResourceManager()->SetChannelBufferSize(1_KB);
        Setup();

        constexpr ui64 NUMBER_OF_QUERIES = 50;
        const TString payload(50_KB, 'x');

        std::vector<TSysViewRow> rows;
        TVector<TString> resultMessages;
        rows.reserve(NUMBER_OF_QUERIES);
        resultMessages.reserve(NUMBER_OF_QUERIES);
        for (ui64 i = 0; i < NUMBER_OF_QUERIES; ++i) {
            const auto name = TStringBuilder() << "query-" << i;
            const TString text = fmt::format(R"(
                ;INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT Data || "{payload}" FROM `{pq_source}`.`{input_topic}`
                LIMIT 1;)",
                "payload"_a = payload,
                "pq_source"_a = PQ_SOURCE,
                "input_topic"_a = InputTopic,
                "output_topic"_a = OutputTopic
            );
            ExecQuery(fmt::format(R"(
                CREATE STREAMING QUERY `{name}`
                AS DO BEGIN{text}END DO)",
                "name"_a = name,
                "text"_a = text
            ));
            rows.push_back({
                .Name = name,
                .Status = "COMPLETED",
                .Ast = payload,
                .Text = text,
                .Run = true,
            });
            resultMessages.emplace_back(TStringBuilder() << "test" << payload);
        }

        WriteTopicMessage(InputTopic, "test");
        ReadTopicMessages(OutputTopic, resultMessages);

        WaitFor(TDuration::Seconds(60), "Wait for queries to complete", [&](TString& error) {
            const auto& result = ExecQuery("SELECT COUNT(*) FROM `.metadata/script_execution_leases`");
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);

            ui64 count = 0;
            CheckScriptResult(result[0], 1, 1, [&](TResultSetParser& resultSet) {
                count = resultSet.ColumnParser(0).GetUint64();
            });

            error = TStringBuilder() << "running queries: " << count;
            return count == 0;
        });

        Sleep(STATS_WAIT_DURATION);
        CheckSysView(rows);
    }
}

} // namespace NKikimr::NKqp
