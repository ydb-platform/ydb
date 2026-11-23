#include "kqp_effects_guard_helpers.h"
#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_query_helpers.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NEffectsGuardTests;

namespace {

TExecuteQueryResult ExecuteNatively(TKikimrRunner& kikimr, const TString& query) {
    // Fallback is enabled in these tests. Successful execution alone would hide
    // an overly broad guard, so also require native compilation without a retry.
    const auto before = NYqlTests::GetNewRBOCompileCounters(kikimr);
    auto result = kikimr.GetQueryClient().ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    const auto after = NYqlTests::GetNewRBOCompileCounters(kikimr);
    UNIT_ASSERT_C(after.first > before.first, "query must compile with New RBO");
    UNIT_ASSERT_VALUES_EQUAL(after.second, before.second);
    return result;
}

} // namespace

Y_UNIT_TEST_SUITE(KqpRboEffectsGuards) {
    Y_UNIT_TEST(DeterministicSharedInputStaysNative) {
        // Both effects share $rows, but evaluating this deterministic input twice
        // produces the same values and must not trigger the repeatability guard.
        TKikimrRunner kikimr(TKikimrSettings(GuardConfig()).SetWithSampleTables(false));
        CreateTables(kikimr);
        ExecuteNatively(kikimr, R"(
            $rows = SELECT 1ul AS k, 42ul AS v;
            UPSERT INTO A SELECT k, v FROM $rows;
            UPSERT INTO B SELECT k, v FROM $rows;
        )");
        auto result = ExecuteNatively(kikimr, "SELECT k, v FROM A; SELECT k, v FROM B;");
        CompareYson("[[1u;[42u]]]", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson("[[1u;[42u]]]", FormatResultSetYson(result.GetResultSet(1)));
    }

    Y_UNIT_TEST_TWIN(IndependentRandomWritesStayNative, SameArgument) {
        // These statements have separate row producers. With SameArgument=true,
        // annotation may intern their identical scalar RandomNumber calls; that
        // scalar sharing must not be mistaken for a shared non-repeatable input.
        TKikimrRunner kikimr(TKikimrSettings(GuardConfig()).SetWithSampleTables(false));
        CreateTables(kikimr);
        ExecuteNatively(kikimr, TStringBuilder() << R"(
            UPSERT INTO A SELECT 1ul AS k, RandomNumber(1) AS v;
            UPSERT INTO B SELECT 2ul AS k, RandomNumber()" << (SameArgument ? 1 : 2) << ") AS v;");
        auto result = ExecuteNatively(kikimr, "SELECT k, v FROM A; SELECT k, v FROM B;");
        for (const auto& rows : result.GetResultSets()) {
            UNIT_ASSERT_VALUES_EQUAL(rows.RowsCount(), 1);
            UNIT_ASSERT_C(!FormatResultSetYson(rows).Contains("#"), "random value must be stored");
        }
    }

    Y_UNIT_TEST(RandomWriteReturningIsOneRoot) {
        // RETURNING observes the written row rather than independently evaluating
        // its random input. Require native execution and the exact stored value.
        TKikimrRunner kikimr(TKikimrSettings(GuardConfig()).SetWithSampleTables(false));
        CreateTables(kikimr);
        auto written = ExecuteNatively(kikimr, R"(
            UPSERT INTO A SELECT 1ul AS k, RandomNumber(1) AS v RETURNING k, v;
        )");
        auto read = ExecuteNatively(kikimr, "SELECT k, v FROM A;");
        UNIT_ASSERT_VALUES_EQUAL(written.GetResultSet(0).RowsCount(), 1);
        CompareYson(FormatResultSetYson(written.GetResultSet(0)), FormatResultSetYson(read.GetResultSet(0)));
    }
}

} // namespace NKikimr::NKqp
