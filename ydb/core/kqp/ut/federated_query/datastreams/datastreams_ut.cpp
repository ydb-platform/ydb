#include <ydb/core/external_sources/external_source.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/kqp/proxy_service/kqp_script_executions.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/federated_query/common/common.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>

#include <ydb/library/testlib/pq_helpers/mock_pq_gateway.h>
#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>

#include <fmt/format.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NKikimr::NKqp::NFederatedQueryTest;
using namespace fmt::literals;
using namespace NTestUtils;

namespace {

struct TScriptQuerySettings {
    bool SaveState = false;
    NKikimrKqp::TScriptExecutionRetryState::TMapping RetryMapping;
    std::optional<NKikimrKqp::TQueryPhysicalGraph> PhysicalGraph;
    TDuration Timeout = TDuration::Seconds(30);
};

class TStreamingTestFixture : public NUnitTest::TBaseFixture {
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

        return AppConfig.emplace();
    }

    TIntrusivePtr<IMockPqGateway> SetupMockPqGateway() {
        UNIT_ASSERT_C(!PqGateway, "PqGateway is already initialized");
        EnsureNotInitialized("MockPqGateway");

        const auto mockPqGateway = CreateMockPqGateway();
        PqGateway = mockPqGateway;

        return mockPqGateway;
    }

    // Local kikimr test cluster

    std::shared_ptr<TKikimrRunner> GetKikimrRunner() {
        if (!Kikimr) {
            if (!AppConfig) {
                AppConfig.emplace();
            }

            AppConfig->MutableQueryServiceConfig()->SetProgressStatsPeriodMs(1000);

            Kikimr = MakeKikimrRunner(true, nullptr, nullptr, AppConfig, NYql::NDq::CreateS3ActorsFactory(), {
                .PqGateway = PqGateway
            });
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
            QueryClient = std::make_shared<TQueryClient>(GetKikimrRunner()->GetQueryClient());
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

    // External YDB recipe

    std::shared_ptr<TDriver> GetExternalDriver() {
        if (!ExternalDriver) {
            ExternalDriver = std::make_shared<TDriver>(TDriverConfig()
                .SetEndpoint(YDB_ENDPOINT)
                .SetDatabase(YDB_DATABASE));
        }

        return ExternalDriver;
    }

    std::shared_ptr<NTopic::TTopicClient> GetTopicClient() {
        if (!TopicClient) {
            TopicClient = std::make_shared<NTopic::TTopicClient>(*GetExternalDriver(), NTopic::TTopicClientSettings()
                .DiscoveryEndpoint(YDB_ENDPOINT)
                .Database(YDB_DATABASE));
        }

        return TopicClient;
    }

    // Topic client SDK (external YDB recipe)

    void CreateTopic(const TString& topicName, std::optional<NTopic::TCreateTopicSettings> settings = std::nullopt) {
        if (!settings) {
            settings.emplace()
                .PartitioningSettings(1, 1);
        }

        const auto result = GetTopicClient()->CreateTopic(topicName, *settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
    }

    void WriteTopicMessage(const TString& topicName, const TString& message, ui64 partition = 0) {
        auto writeSession = GetTopicClient()->CreateSimpleBlockingWriteSession(NTopic::TWriteSessionSettings()
            .Path(topicName)
            .PartitionId(partition));

        writeSession->Write(NTopic::TWriteMessage(message));
        writeSession->Close();
    }

    void ReadTopicMessages(const TString& topicName, const TVector<TString>& expectedMessages, TDuration disposition = TDuration::Seconds(100)) {
        NTopic::TReadSessionSettings readSettings;
        readSettings
            .WithoutConsumer()
            .AppendTopics(
                NTopic::TTopicReadSettings(topicName).ReadFromTimestamp(TInstant::Now() - disposition)
                    .AppendPartitionIds(0)
            );

        readSettings.EventHandlers_.StartPartitionSessionHandler(
            [](NTopic::TReadSessionEvent::TStartPartitionSessionEvent& event) {
                event.Confirm(0);
            }
        );

        auto readSession = GetTopicClient()->CreateReadSession(readSettings);
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

            UNIT_ASSERT_GE(expectedMessages.size(), received.size());

            error = TStringBuilder() << "got new event, received #" << received.size() << " / " << expectedMessages.size() << " messages";
            return false;
        });

        UNIT_ASSERT_VALUES_EQUAL(received.size(), expectedMessages.size());
        for (size_t i = 0; i < received.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(received[i], expectedMessages[i]);
        }
    }

    // Table client SDK

    void ExecSchemeQuery(const TString& query, EStatus expectedStatus = EStatus::SUCCESS) {
        const auto result = GetTableClientSession()->ExecuteSchemeQuery(query).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), expectedStatus, result.GetIssues().ToOneLineString());
    }

    // Query client SDK

    std::vector<TResultSet> ExecQuery(const TString& query, EStatus expectedStatus = EStatus::SUCCESS) {
        auto result = GetQueryClient()->ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), expectedStatus, result.GetIssues().ToOneLineString());
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

    void CreatePqSourceBasicAuth(const TString& pqSourceName) {
        ExecQuery(fmt::format(R"(
            CREATE OBJECT secret_local_password (TYPE SECRET) WITH (value = "1234");
            CREATE EXTERNAL DATA SOURCE `{pq_source}` WITH (
                SOURCE_TYPE = "Ydb",
                LOCATION = "{pq_location}",
                DATABASE_NAME = "{pq_database_name}",
                AUTH_METHOD = "BASIC",
                LOGIN = "root",
                PASSWORD_SECRET_NAME = "secret_local_password"
            );)",
            "pq_source"_a = pqSourceName,
            "pq_location"_a = YDB_ENDPOINT,
            "pq_database_name"_a = YDB_DATABASE
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

    void WaitScriptExecution(const TOperation::TOperationId& operationId, EExecStatus finalStatus = EExecStatus::Completed, bool waitRetry = false) {
        std::optional<TScriptExecutionOperation> operation;
        WaitFor(TEST_OPERATION_TIMEOUT, TStringBuilder() << "script execution status" << finalStatus, [&](TString& error) {
            operation = GetScriptExecutionOperation(operationId, /* checkStatus */ false);

            const auto execStatus = operation->Metadata().ExecStatus;
            if (execStatus == finalStatus) {
                return true;
            }

            const auto& status = operation->Status();
            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToOneLineString());
            UNIT_ASSERT_C(!operation->Ready(), "Operation unexpectedly ready in status " << execStatus << " (expected status " << finalStatus << ")");

            error = TStringBuilder() << "operation status " << execStatus;
            return false;
        });

        const auto& status = operation->Status();
        const auto execStatus = operation->Metadata().ExecStatus;
        UNIT_ASSERT_VALUES_EQUAL_C(execStatus, finalStatus, status.GetIssues().ToOneLineString());

        if (finalStatus != EExecStatus::Failed) {
            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToOneLineString());
        }

        if (!waitRetry) {
            UNIT_ASSERT_VALUES_EQUAL(operation->Ready(), IsIn({EExecStatus::Completed, EExecStatus::Failed}, execStatus));
        } else {
            UNIT_ASSERT_C(!operation->Ready(), "Operation unexpectedly ready for waiting retry");
        }
    }

    void ExecAndWaitScript(const TString& query, EExecStatus finalStatus = EExecStatus::Completed, std::optional<TExecuteScriptSettings> settings = std::nullopt) {
        WaitScriptExecution(ExecScript(query, settings), finalStatus, false);
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

        auto ev = std::make_unique<TEvKqp::TEvScriptRequest>();
        ev->Record = queryProto;
        ev->ForgetAfter = settings.Timeout;
        ev->ResultsTtl = settings.Timeout;
        ev->RetryMapping = {settings.RetryMapping};
        ev->SaveQueryPhysicalGraph = settings.SaveState;
        ev->QueryPhysicalGraph = settings.PhysicalGraph;

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

        auto& policy = *retryMapping.MutableBackoffPolicy();
        policy.SetRetryPeriodMs(backoffDuration.MilliSeconds());
        policy.SetBackoffPeriodMs(backoffDuration.MilliSeconds());
        policy.SetRetryRateLimit(1);

        return retryMapping;
    }

    NKikimrKqp::TQueryPhysicalGraph LoadPhysicalGraph(const TString& executionId) {
        auto& runtime = GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();
        runtime.Register(CreateGetScriptExecutionPhysicalGraphActor(edgeActor, TEST_DATABASE, executionId));

        const auto graph = runtime.GrabEdgeEvent<TEvGetScriptPhysicalGraphResponse>(edgeActor, TDuration::Seconds(10));
        UNIT_ASSERT_C(graph, "Empty graph response");
        UNIT_ASSERT_VALUES_EQUAL_C(graph->Get()->Status, Ydb::StatusIds::SUCCESS, graph->Get()->Issues.ToOneLineString());

        return graph->Get()->PhysicalGraph;
    }

    // Utils

    static void WaitFor(TDuration timeout, const TString& description, std::function<bool(TString&)> callback) {
        TInstant start = TInstant::Now();
        TString errorString;
        while (TInstant::Now() - start <= timeout) {
            if (callback(errorString)) {
                return;
            }

            Cerr << "Wait " << description << " " << TInstant::Now() - start << ": " << errorString << "\n";
            Sleep(TDuration::Seconds(1));
        }

        UNIT_FAIL("Waiting " << description << " timeout. Spent time " << TInstant::Now() - start << " exceeds limit " << timeout << ", last error: " << errorString);
    }

private:
    void EnsureNotInitialized(const TString& info) {
        UNIT_ASSERT_C(!Kikimr, "Kikimr runner is already initialized, can not setup " << info);
    }

private:
    std::optional<NKikimrConfig::TAppConfig> AppConfig;
    TIntrusivePtr<NYql::IPqGateway> PqGateway;
    std::shared_ptr<TKikimrRunner> Kikimr;

    std::shared_ptr<TDriver> InternalDriver;
    std::shared_ptr<NOperation::TOperationClient> OperationClient;
    std::shared_ptr<TQueryClient> QueryClient;
    std::shared_ptr<NYdb::NTable::TTableClient> TableClient;
    std::shared_ptr<NYdb::NTable::TSession> TableClientSession;

    // Attached to database from recipe (YDB_ENDPOINT / YDB_DATABASE)
    std::shared_ptr<TDriver> ExternalDriver;
    std::shared_ptr<NTopic::TTopicClient> TopicClient;
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

        ExecAndWaitScript(fmt::format(R"(
            SELECT * FROM `{source}`.`{topic}`
                WITH (
                    FORMAT="json_each_row",
                    SCHEMA=(
                        key String NOT NULL,
                        value String NOT NULL
                    ))
                LIMIT 1;
            )",
            "source"_a=sourceName,
            "topic"_a=topicName
        ), EExecStatus::Failed);
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
            SELECT * FROM `{source}`.`{topic}`
                WITH (
                    FORMAT="json_each_row",
                    SCHEMA=(
                        key String NOT NULL,
                        value String NOT NULL
                    ))
                LIMIT {partition_count};
            )",
            "source"_a=sourceName,
            "topic"_a=topicName,
            "partition_count"_a=partitionCount
        ));

        for (ui32 i = 0; i < partitionCount; ++i) {
            WriteTopicMessage(topicName, R"({"key":"key1", "value": "value1"})", i);
        }

        CheckScriptResult(scriptExecutionOperation, 2, partitionCount, [](TResultSetParser& result) {
            UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser(0).GetString(), "key1");
            UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser(1).GetString(), "value1");
        });
    }

    Y_UNIT_TEST_F(ReadTopicBasic, TStreamingTestFixture) {
        TString sourceName = "sourceName";
        TString topicName = "topicName";
        TString tableName = "tableName";

        CreateTopic(topicName);

        CreatePqSourceBasicAuth(sourceName);

        const auto scriptExecutionOperation = ExecScript(fmt::format(R"(
            SELECT * FROM `{source}`.`{topic}`
                WITH (
                    FORMAT="json_each_row",
                    SCHEMA=(
                        key String NOT NULL,
                        value String NOT NULL
                    ))
                LIMIT 1;
            )",
            "source"_a=sourceName,
            "topic"_a=topicName
        ));

        WriteTopicMessage(topicName, R"({"key":"key1", "value": "value1"})");

        CheckScriptResult(scriptExecutionOperation, 2, 1, [](TResultSetParser& result) {
            UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser(0).GetString(), "key1");
            UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser(1).GetString(), "value1");
        });
    }

    Y_UNIT_TEST_F(InsertTopicBasic, TStreamingTestFixture) {
        TString sourceName = "sourceName";
        TString inputTopicName = "inputTopicName";
        TString outputTopicName = "outputTopicName";
        TString tableName = "tableName";

        CreateTopic(outputTopicName);
        CreateTopic(inputTopicName);

        CreatePqSourceBasicAuth(sourceName);

        const auto scriptExecutionOperation = ExecScript(fmt::format(R"(
            $input = SELECT key, value FROM `{source}`.`{input_topic}`
                WITH (
                    FORMAT="json_each_row",
                    SCHEMA=(
                        key String NOT NULL,
                        value String  NOT NULL
                    ));
            INSERT INTO `{source}`.`{output_topic}`
                SELECT key || value FROM $input;
            )",
            "source"_a=sourceName,
            "input_topic"_a=inputTopicName,
            "output_topic"_a=outputTopicName
        ));

        WriteTopicMessage(inputTopicName, R"({"key":"key1", "value": "value1"})");
        ReadTopicMessages(outputTopicName, {"key1value1"});

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

            WriteTopicMessage(topicName, R"({"key":"key1", "value": "value1"})");
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

            WriteTopicMessage(sourceTopicName, R"({"time": "2025-08-24T00:00:00.000000Z", "event": "A"})");
            WriteTopicMessage(sourceTopicName, R"({"time": "2025-08-25T00:00:00.000000Z", "event": "B"})");

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

        size_t numberEvents = 0;
        pqGateway->AddEventProvider(topicName, [&](TMockPqSession meta) -> NTopic::TReadSessionEvent::TEvent {
            numberEvents++;

            if (numberEvents == 1) {
                return NTopic::TSessionClosedEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});
            }

            return MakePqMessage(numberEvents, R"({"key":"key1", "value": "value1"})", meta);
        });

        const auto& [_, operationId] = ExecScriptNative(fmt::format(R"(
            PRAGMA s3.AtomicUploadCommit = "true";

            INSERT INTO `{s3_sink}`.`folder/` WITH (FORMAT = "json_each_row")
            SELECT * FROM `{source}`.`{topic}` WITH (
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
        }, false);

        WaitScriptExecution(operationId, EExecStatus::Failed, true);

        ExecQuery(fmt::format(R"(
            DROP EXTERNAL DATA SOURCE `{s3_sink}`;
            DROP EXTERNAL DATA SOURCE `{pq_source}`;)",
            "s3_sink"_a = s3SinkName,
            "pq_source"_a = pqSourceName
        ));

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

        auto kikimr = NFederatedQueryTest::MakeKikimrRunner(true, nullptr, nullptr, std::nullopt, NYql::NDq::CreateS3ActorsFactory(), {
            .PqGateway = pqGateway
        });

        constexpr char sourceName[] = "sourceName";
        CreatePqSource(sourceName);

        const auto& [_, operationId] = ExecScriptNative(fmt::format(R"(
                $input = SELECT key, value FROM `{source}`.`{input_topic}`
                WITH (
                    FORMAT="json_each_row",
                    SCHEMA=(
                        key String NOT NULL,
                        value String  NOT NULL
                    ));
                INSERT INTO `{source}`.`{output_topic}`
                    SELECT key || value FROM $input;)",
                "source"_a = sourceName,
                "input_topic"_a = inputTopicName,
                "output_topic"_a = outputTopicName
            ), {
                .SaveState = true,
                .RetryMapping = CreateRetryMapping({Ydb::StatusIds::BAD_REQUEST})
            }, false);

        Sleep(TDuration::MilliSeconds(3000));

        pqGateway->AddEvent(inputTopicName, MakePqMessage(1, R"({"key":"key1", "value": "value1"})", {.Session = CreatePartitionSession()}));
        pqGateway->AddEvent(inputTopicName, MakePqMessage(2, R"({"key":"key2", "value": "value2"})", {.Session = CreatePartitionSession()}));
        pqGateway->AddEvent(inputTopicName, MakePqMessage(3, R"({"key":"key3", "value": "value3"})", {.Session = CreatePartitionSession()}));
        Sleep(TDuration::MilliSeconds(1000));
        pqGateway->AddEvent(inputTopicName, NTopic::TSessionClosedEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")}));

        Sleep(TDuration::MilliSeconds(10000));
        pqGateway->AddEvent(inputTopicName, MakePqMessage(4, R"({"key":"key4", "value": "value4"})", {.Session = CreatePartitionSession()}));

        bool success = false;
        while (true) {
            auto writeResult = pqGateway->GetWriteSessionData(outputTopicName);
            for (auto& w : writeResult) {
                if (w == "key4value4") {
                    success = true;
                    break;
                }
            }
            if (success) {
                break;
            }
            Sleep(TDuration::MilliSeconds(100));
        }
        CancelScriptExecution(operationId);
    }
}

} // namespace NKikimr::NKqp
