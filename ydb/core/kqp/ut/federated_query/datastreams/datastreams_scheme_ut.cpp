#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <yql/essentials/utils/log/log.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>

#include <fmt/format.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NKikimr::NKqp::NFederatedQueryTest;
//using namespace NTestUtils;
using namespace fmt::literals;

Y_UNIT_TEST_SUITE(KqpFederatedQueryDatastreams) {
    Y_UNIT_TEST(CreateExternalDataSourceDatastreams) {
        NKikimrConfig::TAppConfig appCfg;
        auto kikimr = NFederatedQueryTest::MakeKikimrRunner(true, nullptr, nullptr, std::nullopt, NYql::NDq::CreateS3ActorsFactory());
        
        auto db = kikimr->GetTableClient();
        // TKikimrRunner kikimr(appCfg);
        // kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        // auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString sourceName = "yds";
        TString topicName = "topic";
        TString tableName = "table";
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `)" << sourceName << R"(` WITH (
                SOURCE_TYPE="DataStreams",
                LOCATION=")" << GetEnv("YDB_ENDPOINT") << R"(",
                DATABASE_NAME=")" << GetEnv("YDB_DATABASE") << R"(",
                AUTH_METHOD="NONE"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        // TODO:
        // auto query2 = TStringBuilder() << R"(
        //     CREATE EXTERNAL TABLE `)" << tableName << R"(` (
        //         key Int64 NOT NULL,
        //         value String NOT NULL ) WITH ( 
        //         DATA_SOURCE=")" << sourceName << R"(",
        //         LOCATION="folder",
        //         AUTH_METHOD="NONE");)";
        // result = session.ExecuteSchemeQuery(query2).GetValueSync();
        // UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());


        Cerr << "YDB_ENDPOINT " << GetEnv("YDB_ENDPOINT") << Endl;
        Cerr << "YDB_DATABASE " << GetEnv("YDB_DATABASE") << Endl;
        Cerr << "YDB_TOKEN " << GetEnv("YDB_TOKEN") << Endl;

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
        UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
        TFetchScriptResultsResult results = queryClient.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());

        // auto driverConfig = TDriverConfig()
        //     .SetEndpoint(GetEnv("YDB_ENDPOINT"))
        //     .SetDatabase(GetEnv("YDB_DATABASE"))
        //     .SetAuthToken(GetEnv("YDB_TOKEN"));

        // TDriver driver(driverConfig);
        // NYdb::NTopic::TTopicClient topicClient(driver);
        // auto topicSettings = NYdb::NTopic::TCreateTopicSettings()
        //     .PartitioningSettings(1, 1);

        // auto status = topicClient
        //     .CreateTopic(topicName, topicSettings)
        //     .GetValueSync();
        // UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    }
}

} // namespace NKikimr::NKqp
