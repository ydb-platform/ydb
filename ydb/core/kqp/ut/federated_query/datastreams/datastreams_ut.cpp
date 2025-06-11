#include <ydb/core/external_sources/external_source.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>
#include <fmt/format.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NKikimr::NKqp::NFederatedQueryTest;
using namespace fmt::literals;

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
}

} // namespace NKikimr::NKqp
