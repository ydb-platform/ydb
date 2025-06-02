
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/counters/kqp_counters.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <util/system/fs.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

void FillTableWithData(NQuery::TQueryClient& db, ui64 numRows=10) {
    for (ui32 i = 0; i < numRows; ++i) {
        auto result = db.ExecuteQuery(Sprintf(R"(
            --!syntax_v1
            REPLACE INTO `/Root/KeyValue` (Key, Value) VALUES (%d, "%s")
        )", i, TString(10 + i, 'a' + (i % 26)).c_str()), NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
}

} // anonymous namespace


// Currently re2 udf ignores incorrect regexes.
// But this behaviour will be changed in this ticket: https://nda.ya.ru/t/kp3S6IYx7F2jgV.
// There may be queries that use the old behavior, so it is planned to use the feature flag.
// This test checks that the behavior is not changed by default.
Y_UNIT_TEST_SUITE(KqpRe2) {

Y_UNIT_TEST(IncorrectRegexNoError) {
    NKikimrConfig::TAppConfig appCfg;
    TKikimrRunner kikimr(appCfg);

    auto db = kikimr.GetQueryClient();
    auto query = R"(
        select "a[x" REGEXP "a[x";
    )";

    auto explainMode = NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain);
    auto planres = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), explainMode).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(planres.GetStatus(), EStatus::SUCCESS, planres.GetIssues().ToString());

    Cerr << planres.GetStats()->GetAst() << Endl;

    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), NYdb::NQuery::TExecuteQuerySettings()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

Y_UNIT_TEST(IncorrectRegexWithoutExecutionNoError) {
    NKikimrConfig::TAppConfig appCfg;
    TKikimrRunner kikimr(appCfg);

    auto db = kikimr.GetQueryClient();

    FillTableWithData(db);

    auto query = R"(
        select * from `/Root/KeyValue` where Key = 1 or Value regexp "[";
    )";

    auto explainMode = NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain);
    auto planres = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), explainMode).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(planres.GetStatus(), EStatus::SUCCESS, planres.GetIssues().ToString());

    Cerr << planres.GetStats()->GetAst() << Endl;

    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), NYdb::NQuery::TExecuteQuerySettings()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

} // suite

} // namespace NKqp
} // namespace NKikimr
