#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <util/system/env.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpFederatedQuery) {
    Y_UNIT_TEST(ExecuteScript) {
        Cerr << "S3 endpoint: " << GetEnv("S3_ENDPOINT") << Endl;
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto executeScrptsResult = db.ExecuteScript(R"(
            SELECT 42
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(executeScrptsResult.Status().GetStatus(), EStatus::SUCCESS, executeScrptsResult.Status().GetIssues().ToString());
        UNIT_ASSERT(executeScrptsResult.Metadata().ExecutionId);

        TMaybe<TFetchScriptResultsResult> results;
        do {
            Sleep(TDuration::MilliSeconds(50));
            TAsyncFetchScriptResultsResult future = db.FetchScriptResults(executeScrptsResult.Metadata().ExecutionId);
            results.ConstructInPlace(future.ExtractValueSync());
            if (!results->IsSuccess()) {
                UNIT_ASSERT_C(results->GetStatus() == NYdb::EStatus::BAD_REQUEST, results->GetStatus());
                UNIT_ASSERT_STRING_CONTAINS(results->GetIssues().ToOneLineString(), "Results are not ready");
            }
        } while (!results->HasResultSet());
        TResultSetParser resultSet(results->ExtractResultSet());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);
        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetInt32(), 42);
    }
}

} // namespace NKqp
} // namespace NKikimr
