#include "pq_helpers.h"

#include <ydb/core/external_sources/external_source.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/kqp/proxy_service/kqp_script_executions.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/federated_query/common/common.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>

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
    TString Query;
    bool SaveState = false;
    NKikimrKqp::TScriptExecutionRetryState::TMapping RetryMapping;
    std::optional<NKikimrKqp::TQueryPhysicalGraph> PhysicalGraph;
    TDuration Timeout = TDuration::Seconds(30);
};

std::pair<TString, TOperation::TOperationId> StartScriptQuery(TTestActorRuntime& runtime, const TScriptQuerySettings& settings) {
    NKikimrKqp::TEvQueryRequest queryProto;
    queryProto.SetUserToken(NACLib::TUserToken(BUILTIN_ACL_ROOT, TVector<NACLib::TSID>{runtime.GetAppData().AllAuthenticatedUsers}).SerializeAsString());
    auto& req = *queryProto.MutableRequest();
    req.SetDatabase("/Root");
    req.SetQuery(settings.Query);
    req.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
    req.SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT);

    auto ev = MakeHolder<TEvKqp::TEvScriptRequest>();
    ev->Record = queryProto;
    ev->ForgetAfter = settings.Timeout;
    ev->ResultsTtl = settings.Timeout;
    ev->RetryMapping = {settings.RetryMapping};
    ev->SaveQueryPhysicalGraph = settings.SaveState;
    ev->QueryPhysicalGraph = settings.PhysicalGraph;

    const auto edgeActor = runtime.AllocateEdgeActor();
    runtime.Send(MakeKqpProxyID(runtime.GetNodeId()), edgeActor, ev.Release());

    const auto reply = runtime.GrabEdgeEvent<TEvKqp::TEvScriptResponse>(edgeActor, settings.Timeout);
    UNIT_ASSERT_C(reply, "CreateScript response is empty");
    UNIT_ASSERT_VALUES_EQUAL_C(reply->Get()->Status, Ydb::StatusIds::SUCCESS, reply->Get()->Issues.ToString());

    const auto& executionId = reply->Get()->ExecutionId;    
    UNIT_ASSERT(executionId);

    return {executionId, TOperation::TOperationId(ScriptExecutionOperationFromExecutionId(executionId))};
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpFederatedQueryDatastreams) {

    Y_UNIT_TEST(CreateExternalDataSource) {
        auto kikimr = NFederatedQueryTest::MakeKikimrRunner(true, nullptr, nullptr, std::nullopt, NYql::NDq::CreateS3ActorsFactory());

        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `sourceName` WITH (
                SOURCE_TYPE="Ydb",
                LOCATION=")" << GetEnv("YDB_ENDPOINT") << R"(",
                DATABASE_NAME=")" << GetEnv("YDB_DATABASE") << R"(",
                AUTH_METHOD="NONE"
            );)";
        auto session = kikimr->GetTableClient().CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        // DataStreams is not allowed.
        auto query2 = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `sourceName2` WITH (
                SOURCE_TYPE="DataStreams",
                LOCATION=")" << GetEnv("YDB_ENDPOINT") << R"(",
                DATABASE_NAME=")" << GetEnv("YDB_DATABASE") << R"(",
                AUTH_METHOD="NONE"
            );)";
        result = session.ExecuteSchemeQuery(query2).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());

        // YdbTopics is not allowed.
        auto query3 = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `sourceName2` WITH (
                SOURCE_TYPE=")" << ToString(NKikimr::NExternalSource::YdbTopicsType) << R"(",
                LOCATION=")" << GetEnv("YDB_ENDPOINT") << R"(",
                DATABASE_NAME=")" << GetEnv("YDB_DATABASE") << R"(",
                AUTH_METHOD="NONE"
            );)";
        result = session.ExecuteSchemeQuery(query3).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(CreateExternalDataSourceBasic) {
        auto kikimr = NFederatedQueryTest::MakeKikimrRunner(true, nullptr, nullptr, std::nullopt, NYql::NDq::CreateS3ActorsFactory());

        auto query = TStringBuilder() << R"(
            CREATE OBJECT secret_local_password (TYPE SECRET) WITH (value = "password");
            CREATE EXTERNAL DATA SOURCE `sourceName` WITH (
                SOURCE_TYPE="Ydb",
                LOCATION=")" << GetEnv("YDB_ENDPOINT") << R"(",
                DATABASE_NAME=")" << GetEnv("YDB_DATABASE") << R"(",
                AUTH_METHOD="BASIC",
                LOGIN="root",
                PASSWORD_SECRET_NAME="secret_local_password"
            );)";
        auto session = kikimr->GetTableClient().CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(FailedWithoutAvailableExternalDataSourcesYdb) {
        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableQueryServiceConfig()->SetAllExternalDataSourcesAreAvailable(false);
        auto kikimr = NFederatedQueryTest::MakeKikimrRunner(true, nullptr, nullptr, appCfg, NYql::NDq::CreateS3ActorsFactory());

        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `sourceName` WITH (
                SOURCE_TYPE="Ydb",
                LOCATION=")" << GetEnv("YDB_ENDPOINT") << R"(",
                DATABASE_NAME=")" << GetEnv("YDB_DATABASE") << R"(",
                AUTH_METHOD="NONE"
            );)";
        auto session = kikimr->GetTableClient().CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(CheckAvailableExternalDataSourcesYdb) {
        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableQueryServiceConfig()->AddAvailableExternalDataSources("Ydb");
        appCfg.MutableQueryServiceConfig()->SetAllExternalDataSourcesAreAvailable(false);
        auto kikimr = NFederatedQueryTest::MakeKikimrRunner(true, nullptr, nullptr, appCfg, NYql::NDq::CreateS3ActorsFactory());

        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `sourceName` WITH (
                SOURCE_TYPE="Ydb",
                LOCATION=")" << GetEnv("YDB_ENDPOINT") << R"(",
                DATABASE_NAME=")" << GetEnv("YDB_DATABASE") << R"(",
                AUTH_METHOD="NONE"
            );)";
        auto session = kikimr->GetTableClient().CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(ReadTopicFailedWithoutAvailableExternalDataSourcesYdbTopics) {
        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableQueryServiceConfig()->AddAvailableExternalDataSources("Ydb");
        appCfg.MutableQueryServiceConfig()->SetAllExternalDataSourcesAreAvailable(false);
        auto kikimr = NFederatedQueryTest::MakeKikimrRunner(true, nullptr, nullptr, appCfg, NYql::NDq::CreateS3ActorsFactory());

        TString sourceName = "sourceName";
        TString topicName = "topicName";
        TString tableName = "tableName";

        NYdb::NTopic::TTopicClientSettings opts;
        opts.DiscoveryEndpoint(GetEnv("YDB_ENDPOINT"))
            .Database(GetEnv("YDB_DATABASE"));

        auto driver = TDriver(TDriverConfig());
        NYdb::NTopic::TTopicClient topicClient(driver, opts);

        auto topicSettings = NYdb::NTopic::TCreateTopicSettings().PartitioningSettings(1, 1);
        auto status = topicClient.CreateTopic(topicName, topicSettings).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `)" << sourceName << R"(` WITH (
                SOURCE_TYPE="Ydb",
                LOCATION=")" << GetEnv("YDB_ENDPOINT") << R"(",
                DATABASE_NAME=")" << GetEnv("YDB_DATABASE") << R"(",
                AUTH_METHOD="NONE"
            );)";

        auto db = kikimr->GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto settings = TExecuteScriptSettings().StatsMode(EStatsMode::Basic);

        const TString sql = fmt::format(R"(
                SELECT * FROM `{source}`.`{topic}`
                    WITH (
                        FORMAT="json_each_row",
                        SCHEMA=(
                            key String NOT NULL,
                            value String NOT NULL
                        ))
                    LIMIT 1;
            )", "source"_a=sourceName, "topic"_a=topicName);

        auto queryClient = kikimr->GetQueryClient();
        auto scriptExecutionOperation = queryClient.ExecuteScript(sql, settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());

        NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
        UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Failed, readyOp.Status().GetIssues().ToString());
    }

    Y_UNIT_TEST(ReadTopic) {
        auto kikimr = NFederatedQueryTest::MakeKikimrRunner(true, nullptr, nullptr, std::nullopt, NYql::NDq::CreateS3ActorsFactory());

        TString sourceName = "sourceName";
        TString topicName = "topicName";
        TString tableName = "tableName";
        ui32 partitionCount = 10;

        NYdb::NTopic::TTopicClientSettings opts;
        opts.DiscoveryEndpoint(GetEnv("YDB_ENDPOINT"))
            .Database(GetEnv("YDB_DATABASE"));
        auto driver = TDriver(TDriverConfig());
        NYdb::NTopic::TTopicClient topicClient(driver, opts);

        auto topicSettings = NYdb::NTopic::TCreateTopicSettings().PartitioningSettings(partitionCount, partitionCount);
        auto status = topicClient.CreateTopic(topicName, topicSettings).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `)" << sourceName << R"(` WITH (
                SOURCE_TYPE="Ydb",
                LOCATION=")" << GetEnv("YDB_ENDPOINT") << R"(",
                DATABASE_NAME=")" << GetEnv("YDB_DATABASE") << R"(",
                AUTH_METHOD="NONE"
            );)";

        auto db = kikimr->GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto settings = TExecuteScriptSettings().StatsMode(EStatsMode::Basic);

        const TString sql = fmt::format(R"(
                SELECT * FROM `{source}`.`{topic}`
                    WITH (
                        FORMAT="json_each_row",
                        SCHEMA=(
                            key String NOT NULL,
                            value String NOT NULL
                        ))
                    LIMIT {partition_count};
            )", "source"_a=sourceName, "topic"_a=topicName, "partition_count"_a=partitionCount);

        auto queryClient = kikimr->GetQueryClient();
        auto scriptExecutionOperation = queryClient.ExecuteScript(sql, settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));

        for (ui32 i = 0; i < partitionCount; ++i) {
            auto writeSettings = NYdb::NTopic::TWriteSessionSettings().Path(topicName).PartitionId(i);
            auto topicSession = topicClient.CreateSimpleBlockingWriteSession(writeSettings);
            topicSession->Write(NYdb::NTopic::TWriteMessage(R"({"key":"key1", "value": "value1"})"));
            topicSession->Close();
        }

        NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
        UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
        TFetchScriptResultsResult results = queryClient.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());

        TResultSetParser resultSet(results.ExtractResultSet());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), partitionCount);
        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetString(), "key1");
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetString(), "value1");
    }

    Y_UNIT_TEST(ReadTopicBasic) {
        auto kikimr = NFederatedQueryTest::MakeKikimrRunner(true, nullptr, nullptr, std::nullopt, NYql::NDq::CreateS3ActorsFactory());

        TString sourceName = "sourceName";
        TString topicName = "topicName";
        TString tableName = "tableName";

        NYdb::NTopic::TTopicClientSettings opts;
        opts.DiscoveryEndpoint(GetEnv("YDB_ENDPOINT"))
            .Database(GetEnv("YDB_DATABASE"));
        auto driver = TDriver(TDriverConfig());
        NYdb::NTopic::TTopicClient topicClient(driver, opts);

        auto topicSettings = NYdb::NTopic::TCreateTopicSettings().PartitioningSettings(1, 1);
        auto status = topicClient.CreateTopic(topicName, topicSettings).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

        auto query = TStringBuilder() << R"(
            CREATE OBJECT secret_local_password (TYPE SECRET) WITH (value = "1234");
            CREATE EXTERNAL DATA SOURCE `)" << sourceName << R"(` WITH (
                SOURCE_TYPE="Ydb",
                LOCATION=")" << GetEnv("YDB_ENDPOINT") << R"(",
                DATABASE_NAME=")" << GetEnv("YDB_DATABASE") << R"(",
                AUTH_METHOD="BASIC",
                LOGIN="root",
                PASSWORD_SECRET_NAME="secret_local_password"
            );)";

        auto db = kikimr->GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto settings = TExecuteScriptSettings().StatsMode(EStatsMode::Basic);

        const TString sql = fmt::format(R"(
                SELECT * FROM `{source}`.`{topic}`
                    WITH (
                        FORMAT="json_each_row",
                        SCHEMA=(
                            key String NOT NULL,
                            value String NOT NULL
                        ))
                    LIMIT 1;
            )", "source"_a=sourceName, "topic"_a=topicName);

        auto queryClient = kikimr->GetQueryClient();
        auto scriptExecutionOperation = queryClient.ExecuteScript(sql, settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));

        auto writeSettings = NYdb::NTopic::TWriteSessionSettings().Path(topicName);
        auto topicSession = topicClient.CreateSimpleBlockingWriteSession(writeSettings);
        topicSession->Write(NYdb::NTopic::TWriteMessage(R"({"key":"key1", "value": "value1"})"));
        topicSession->Close();

        NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
        UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
        TFetchScriptResultsResult results = queryClient.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());

        TResultSetParser resultSet(results.ExtractResultSet());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);
        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetString(), "key1");
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetString(), "value1");
    }

    Y_UNIT_TEST(InsertTopicBasic) {
        auto kikimr = NFederatedQueryTest::MakeKikimrRunner(true, nullptr, nullptr, std::nullopt, NYql::NDq::CreateS3ActorsFactory());

        TString sourceName = "sourceName";
        TString inputTopicName = "inputTopicName";
        TString outputTopicName = "outputTopicName";
        TString tableName = "tableName";

        NYdb::NTopic::TTopicClientSettings opts;
        opts.DiscoveryEndpoint(GetEnv("YDB_ENDPOINT"))
            .Database(GetEnv("YDB_DATABASE"));
        auto driver = TDriver(TDriverConfig());
        NYdb::NTopic::TTopicClient topicClient(driver, opts);

        auto topicSettings = NYdb::NTopic::TCreateTopicSettings().PartitioningSettings(1, 1);
        auto status = topicClient.CreateTopic(outputTopicName, topicSettings).GetValueSync();
        
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));

        status = topicClient.CreateTopic(inputTopicName, topicSettings).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

        auto query = TStringBuilder() << R"(
            CREATE OBJECT secret_local_password (TYPE SECRET) WITH (value = "1234");
            CREATE EXTERNAL DATA SOURCE `)" << sourceName << R"(` WITH (
                SOURCE_TYPE="Ydb",
                LOCATION=")" << GetEnv("YDB_ENDPOINT") << R"(",
                DATABASE_NAME=")" << GetEnv("YDB_DATABASE") << R"(",
                AUTH_METHOD="BASIC",
                LOGIN="root",
                PASSWORD_SECRET_NAME="secret_local_password"
            );)";

        auto db = kikimr->GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        const TString sql = fmt::format(R"(
                $input = SELECT key, value FROM `{source}`.`{input_topic}`
                    WITH (
                        FORMAT="json_each_row",
                        SCHEMA=(
                            key String NOT NULL,
                            value String  NOT NULL
                        ));
                INSERT INTO `{source}`.`{output_topic}`
                    SELECT key || value FROM $input;
            )", "source"_a=sourceName, "input_topic"_a=inputTopicName, "output_topic"_a=outputTopicName);

        auto queryClient = kikimr->GetQueryClient();
        auto settings = TExecuteScriptSettings().StatsMode(EStatsMode::Basic);
        auto scriptExecutionOperation = queryClient.ExecuteScript(sql, settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());

        std::this_thread::sleep_for(std::chrono::milliseconds(1000));

        auto writeSettings = NYdb::NTopic::TWriteSessionSettings().Path(inputTopicName);
        auto topicSession = topicClient.CreateSimpleBlockingWriteSession(writeSettings);
        topicSession->Write(NYdb::NTopic::TWriteMessage(R"({"key":"key1", "value":"value1"})"));
        topicSession->Close();

        NYdb::NTopic::TReadSessionSettings readSettings;
        readSettings
            .WithoutConsumer()
            .AppendTopics(
                NTopic::TTopicReadSettings(outputTopicName).ReadFromTimestamp(TInstant::Now() - TDuration::Seconds(146))
                    .AppendPartitionIds(0));

        readSettings.EventHandlers_.StartPartitionSessionHandler(
            [](NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent& event) {
                event.Confirm(0);
            }
        );

        auto readSession = topicClient.CreateReadSession(readSettings);
        std::vector<std::string> received;
        while (true) {
            auto event = readSession->GetEvent(/*block = */true);
            if (auto dataEvent = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                for (const auto& message : dataEvent->GetMessages()) {
                    received.push_back(message.GetData());
                }
                break;
            }
        }
        UNIT_ASSERT_EQUAL(received.size(), 1);
        UNIT_ASSERT_EQUAL(received[0], "key1value1");

        NYdb::NOperation::TOperationClient client(kikimr->GetDriver());
        status = client.Cancel(scriptExecutionOperation.Id()).ExtractValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }

    Y_UNIT_TEST(RestoreScriptPhysicalGraph) {
        constexpr char writeBucket[] = "test_bucket_restore_script_physical_graph";
        CreateBucket(writeBucket);

        auto kikimr = NFederatedQueryTest::MakeKikimrRunner(true, nullptr, nullptr, std::nullopt, NYql::NDq::CreateS3ActorsFactory());
        auto db = kikimr->GetQueryClient();

        const auto topicDriver = TDriver(TDriverConfig());
        NTopic::TTopicClient topicClient(topicDriver, NTopic::TTopicClientSettings()
            .DiscoveryEndpoint(GetEnv("YDB_ENDPOINT"))
            .Database(GetEnv("YDB_DATABASE")));

        constexpr char topicName[] = "restoreScriptTopic";
        {
            const auto status = topicClient.CreateTopic(topicName).ExtractValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        constexpr char pqSourceName[] = "sourceName";
        constexpr char s3SinkName[] = "s3Sink";
        {
            const auto query = fmt::format(R"(
                CREATE EXTERNAL DATA SOURCE `{s3_sink}` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="{s3_location}",
                    AUTH_METHOD="NONE"
                );
                CREATE EXTERNAL DATA SOURCE `{pq_source}` WITH (
                    SOURCE_TYPE = "Ydb",
                    LOCATION = "{pq_location}",
                    DATABASE_NAME = "{pq_database_name}",
                    AUTH_METHOD = "NONE"
                );)",
                "s3_sink"_a = s3SinkName,
                "s3_location"_a = GetBucketLocation(writeBucket),
                "pq_source"_a = pqSourceName,
                "pq_location"_a = GetEnv("YDB_ENDPOINT"),
                "pq_database_name"_a = GetEnv("YDB_DATABASE")
            );
            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto& runtime = *kikimr->GetTestServer().GetRuntime();

        const auto executeQuery = [&](TScriptQuerySettings settings) {
            settings.Query = fmt::format(R"(
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
            );

            const auto [executionId, operationId] = StartScriptQuery(runtime, settings);
            const auto readyOp = WaitScriptExecutionOperation(operationId, kikimr->GetDriver(), [&]() {
                Sleep(TDuration::Seconds(1));
                auto topicSession = topicClient.CreateSimpleBlockingWriteSession(NTopic::TWriteSessionSettings().Path(topicName));
                topicSession->Write(NTopic::TWriteMessage(R"({"key":"key1", "value": "value1"})"));
                topicSession->Close();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Status().GetStatus(), EStatus::SUCCESS, readyOp.Status().GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());

            return executionId;
        };

        const auto executionId = executeQuery({.SaveState = true});
        const TString sampleResult = "{\"key\":\"key1\",\"value\":\"value1\"}\n";
        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(writeBucket), sampleResult);

        const auto edgeActor = runtime.AllocateEdgeActor();
        runtime.Register(CreateGetScriptExecutionPhysicalGraphActor(edgeActor, "/Root", executionId));
        const auto graph = runtime.GrabEdgeEvent<TEvGetScriptPhysicalGraphResponse>(edgeActor, TDuration::Seconds(10));
        UNIT_ASSERT_C(graph, "Empty graph response");
        UNIT_ASSERT_VALUES_EQUAL_C(graph->Get()->Status, Ydb::StatusIds::SUCCESS, graph->Get()->Issues.ToString());

        {
            const auto query = fmt::format(R"(
                DROP EXTERNAL DATA SOURCE `{s3_sink}`;
                DROP EXTERNAL DATA SOURCE `{pq_source}`;)",
                "s3_sink"_a = s3SinkName,
                "pq_source"_a = pqSourceName
            );
            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        executeQuery({.PhysicalGraph = graph->Get()->PhysicalGraph});
        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(writeBucket), TStringBuilder() << sampleResult << sampleResult);
    }

    Y_UNIT_TEST(RestoreScriptPhysicalGraphOnRetry) {
        constexpr char writeBucket[] = "test_bucket_restore_script_physical_graph_on_retry";
        CreateBucket(writeBucket);

        size_t numberEvents = 0;
        const auto topicEvents = [&](TMockPqSession meta) -> NTopic::TReadSessionEvent::TEvent {
            numberEvents++;

            if (numberEvents == 1) {
                return NTopic::TSessionClosedEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});
            }

            return MakePqMessage(numberEvents, R"({"key":"key1", "value": "value1"})", meta);
        };

        constexpr char pqSourceName[] = "sourceName";
        constexpr char topicName[] = "restoreScriptTopicOnRetry";
        const TMockPqGatewaySettings pqGatewaySettings = {
            .Clusters = {{
                JoinPath({"/Root", pqSourceName}), {
                    .Database = GetEnv("YDB_DATABASE"),
                    .ExpectedQueriesToCluster = 1
                }
            }},
            .Topics = {{
                GetEnv("YDB_DATABASE"), {{
                    topicName, {.EventGen = topicEvents}
                }}
            }}
        };

        auto kikimr = NFederatedQueryTest::MakeKikimrRunner(true, nullptr, nullptr, std::nullopt, NYql::NDq::CreateS3ActorsFactory(), {
            .PqGateway = MakeIntrusive<TMockPqGateway>(pqGatewaySettings)
        });
        auto db = kikimr->GetQueryClient();

        const auto topicDriver = TDriver(TDriverConfig());
        NTopic::TTopicClient topicClient(topicDriver, NTopic::TTopicClientSettings()
            .DiscoveryEndpoint(GetEnv("YDB_ENDPOINT"))
            .Database(GetEnv("YDB_DATABASE")));

        {
            const auto status = topicClient.CreateTopic(topicName).ExtractValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        constexpr char s3SinkName[] = "s3Sink";
        {
            const auto query = fmt::format(R"(
                CREATE EXTERNAL DATA SOURCE `{s3_sink}` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="{s3_location}",
                    AUTH_METHOD="NONE"
                );
                CREATE EXTERNAL DATA SOURCE `{pq_source}` WITH (
                    SOURCE_TYPE = "Ydb",
                    LOCATION = "{pq_location}",
                    DATABASE_NAME = "{pq_database_name}",
                    AUTH_METHOD = "NONE"
                );)",
                "s3_sink"_a = s3SinkName,
                "s3_location"_a = GetBucketLocation(writeBucket),
                "pq_source"_a = pqSourceName,
                "pq_location"_a = GetEnv("YDB_ENDPOINT"),
                "pq_database_name"_a = GetEnv("YDB_DATABASE")
            );
            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto& runtime = *kikimr->GetTestServer().GetRuntime();
        TString executionId;
        TOperation::TOperationId operationId;

        constexpr TDuration backoffDuration = TDuration::Seconds(5);
        {
            NKikimrKqp::TScriptExecutionRetryState::TMapping retryMapping;
            retryMapping.AddStatusCode(Ydb::StatusIds::BAD_REQUEST);
            auto& policy = *retryMapping.MutableBackoffPolicy();
            policy.SetRetryPeriodMs(backoffDuration.MilliSeconds());
            policy.SetBackoffPeriodMs(backoffDuration.MilliSeconds());
            policy.SetRetryRateLimit(1);

            const TScriptQuerySettings settings = {
                .Query = fmt::format(R"(
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
                ),
                .SaveState = true,
                .RetryMapping = retryMapping
            };

            std::tie(executionId, operationId) = StartScriptQuery(runtime, settings);
        }

        NOperation::TOperationClient opClient(kikimr->GetDriver());

        const auto timeout = TInstant::Now() + TDuration::Seconds(10);
        while (true) {
            const auto op = opClient.Get<TScriptExecutionOperation>(operationId).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(op.Status().GetStatus(), EStatus::SUCCESS, op.Status().GetIssues().ToString());
            UNIT_ASSERT_C(!op.Ready(), "Operation unexpectedly finished");
            if (op.Metadata().ExecStatus == EExecStatus::Failed) {
                break;
            }

            if (TInstant::Now() > timeout) {
                UNIT_FAIL("Failed to wait for query to finish (before retry)");
            }

            Sleep(TDuration::MilliSeconds(100));
        }

        {
            const auto query = fmt::format(R"(
                DROP EXTERNAL DATA SOURCE `{s3_sink}`;
                DROP EXTERNAL DATA SOURCE `{pq_source}`;)",
                "s3_sink"_a = s3SinkName,
                "pq_source"_a = pqSourceName
            );
            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        const auto readyOp = WaitScriptExecutionOperation(operationId, kikimr->GetDriver());
        UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Status().GetStatus(), EStatus::SUCCESS, readyOp.Status().GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(writeBucket), "{\"key\":\"key1\",\"value\":\"value1\"}\n");
        UNIT_ASSERT_VALUES_EQUAL(GetUncommittedUploadsCount(writeBucket), 0);
    }
}

} // namespace NKikimr::NKqp
