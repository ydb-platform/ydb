#include <fmt/format.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/operation/operation.h>
#include <yql/essentials/utils/log/log.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NKikimr::NKqp::NFederatedQueryTest;
using namespace NTestUtils;
using namespace fmt::literals;

Y_UNIT_TEST_SUITE(KqpScriptExecResults) {
    bool FetchRows(TQueryClient& queryClient, const TOperation& scriptExecutionOperation, TFetchScriptResultsSettings& settings, size_t& rowsFetched, const TString& rowContent) {
        TFetchScriptResultsResult results = queryClient.FetchScriptResults(scriptExecutionOperation.Id(), 0, settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(results.GetStatus(), EStatus::SUCCESS, results.GetIssues().ToOneLineString());

        TResultSetParser resultSet(results.ExtractResultSet());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 2);

        while (resultSet.TryNextRow()) {
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("id").GetUint64(), rowsFetched++);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("data").GetString(), rowContent);
        }

        const auto& fetchToken = results.GetNextFetchToken();
        settings.FetchToken(fetchToken);
        return !fetchToken.empty();
    }

    void ExecuteSelectQuery(const TString& bucket, size_t fileSize, size_t numberRows) {
        using namespace fmt::literals;

        // Create test file
        TString content = "id,data\n";
        const TString rowContent(fileSize / numberRows, 'a');
        for (size_t i = 0; i < numberRows; ++i) {
            content += TStringBuilder() << ToString(i) << "," << rowContent << "\n";
        }

        // Upload test file
        CreateBucketWithObject(bucket, "test_object", content);

        // Create external data source
        const TString externalDataSourceName = "/Root/external_data_source";

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOltpSink(false);
        auto kikimr = NFederatedQueryTest::MakeKikimrRunner(true, nullptr, nullptr, appConfig, NYql::NDq::CreateS3ActorsFactory(), {});

        auto queryClient = kikimr->GetQueryClient();
        const TString schemeQuery = fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE `{external_source}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{location}",
                AUTH_METHOD="NONE"
            ))",
            "external_source"_a = externalDataSourceName,
            "location"_a = GetBucketLocation(bucket)
        );

        const auto schemeResult = queryClient.ExecuteQuery(schemeQuery, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(schemeResult.GetStatus(), EStatus::SUCCESS, schemeResult.GetIssues().ToOneLineString());

        // Execute test query
        const TString query = fmt::format(R"(
            SELECT * FROM `{external_source}`.`/` WITH (
                FORMAT="csv_with_names",
                SCHEMA (
                    id Uint64 NOT NULL,
                    data String NOT NULL
                )
            ))",
            "external_source"_a=externalDataSourceName
        );

        auto scriptExecutionOperation = queryClient.ExecuteScript(query).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToOneLineString());

        // Wait script execution
        NOperation::TOperationClient operationClient(kikimr->GetDriver());

        TFetchScriptResultsSettings settings;
        settings.RowsLimit(0);
        size_t rowsFetched = 0;

        const TInstant waitStart = TInstant::Now();
        while (TInstant::Now() - waitStart <= TDuration::Minutes(1)) {
            scriptExecutionOperation = operationClient.Get<TScriptExecutionOperation>(scriptExecutionOperation.Id()).ExtractValueSync();
            if (scriptExecutionOperation.Ready()) {
                break;
            }

            const auto& meta = scriptExecutionOperation.Metadata();
            UNIT_ASSERT_VALUES_EQUAL(meta.ExecStatus, NYdb::NQuery::EExecStatus::Running);

            if (const auto& resultsMeta = meta.ResultSetsMeta; !resultsMeta.empty()) {
                UNIT_ASSERT_VALUES_EQUAL(resultsMeta.size(), 1);
                const auto& resultMeta = resultsMeta[0];
                Cerr << "Rows saved: " << resultMeta.RowsCount << ", elapsed time: " << TInstant::Now() - waitStart << "\n";

                if (resultMeta.Finished) {
                    UNIT_ASSERT_VALUES_EQUAL(resultMeta.RowsCount, numberRows);
                } else {
                    UNIT_ASSERT_GE(numberRows, resultMeta.RowsCount);
                }
                while (rowsFetched < resultMeta.RowsCount && FetchRows(queryClient, scriptExecutionOperation, settings, rowsFetched, rowContent)) {}
            }

            UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToOneLineString());
            Sleep(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_EQUAL(scriptExecutionOperation.Metadata().ExecStatus, EExecStatus::Completed);
        UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

        // Validate query results
        const auto& resultsMeta = scriptExecutionOperation.Metadata().ResultSetsMeta;
        UNIT_ASSERT_VALUES_EQUAL(resultsMeta.size(), 1);
        const auto& resultMeta = resultsMeta[0];
        UNIT_ASSERT_VALUES_EQUAL(resultMeta.RowsCount, numberRows);
        UNIT_ASSERT(resultMeta.Finished);
        UNIT_ASSERT_C(rowsFetched > 0, "Expected intermediate results");

        if (!settings.FetchToken_.empty()) {
            while (FetchRows(queryClient, scriptExecutionOperation, settings, rowsFetched, rowContent)) {}
        }
        UNIT_ASSERT_VALUES_EQUAL(rowsFetched, numberRows);

        // Test forget operation
        auto status = operationClient.Forget(scriptExecutionOperation.Id()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToOneLineString());

        const TString countResultsQuery = fmt::format(R"(
                SELECT COUNT(*)
                FROM `.metadata/result_sets`
                WHERE execution_id = "{execution_id}" AND expire_at > CurrentUtcTimestamp();
            )", "execution_id"_a=scriptExecutionOperation.Metadata().ExecutionId);

        const TInstant forgetChecksStart = TInstant::Now();
        while (TInstant::Now() - forgetChecksStart <= TDuration::Minutes(1)) {
            const auto result = queryClient.ExecuteQuery(countResultsQuery, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

            auto resultSet = result.GetResultSetParser(0);
            resultSet.TryNextRow();
            const ui64 numberRows = resultSet.ColumnParser(0).GetUint64();
            if (!numberRows) {
                return;
            }

            Cerr << "Rows remains: " << numberRows << ", elapsed time: " << TInstant::Now() - forgetChecksStart << "\n";
            Sleep(TDuration::Seconds(1));
        }
        UNIT_FAIL("Results removing timeout");
    }

    Y_UNIT_TEST(ExecuteScriptWithLargeStrings) {
        ExecuteSelectQuery("test_bucket_execute_script_with_large_strings", 100_MB, 100);
    }

    Y_UNIT_TEST(ExecuteScriptWithLargeFile) {
        ExecuteSelectQuery("test_bucket_execute_script_with_large_file", 65_MB, 500000);
    }

    Y_UNIT_TEST(ExecuteScriptWithThinFile) {
        ExecuteSelectQuery("test_bucket_execute_script_with_large_file", 5_MB, 500000);
    }
}

} // namespace NKikimr::NKqp
