#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/counters/kqp_counters.h>

#include <util/system/fs.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

TKikimrSettings AppSettings(TStringStream& logStream) {
    TKikimrSettings serverSettings;
    serverSettings.LogStream = &logStream;

    return serverSettings;
}

void FillTableWithData(NQuery::TQueryClient& db, ui64 numRows=300) {
    for (ui32 i = 0; i < numRows; ++i) {
        auto result = db.ExecuteQuery(Sprintf(R"(
            --!syntax_v1
            REPLACE INTO `/Root/KeyValue` (Key, Value) VALUES (%d, "%s")
        )", i, TString(200000 + i, 'a' + (i % 26)).c_str()), NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
}

void RunTestForQuery(const std::string& query, const std::string& expectedLog, bool enabledLogs) {
    TStringStream logsStream;
    {
        Cerr << "cwd: " << NFs::CurrentWorkingDirectory() << Endl;
        TKikimrRunner kikimr(AppSettings(logsStream));

        if (enabledLogs) {
            kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_TASKS_RUNNER, NActors::NLog::PRI_DEBUG);
        }

        auto db = kikimr.GetQueryClient();

        FillTableWithData(db);

        auto explainMode = NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain);
        auto planres = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), explainMode).ExtractValueSync();
        Cerr << planres.GetIssues().ToString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL_C(planres.GetStatus(), EStatus::SUCCESS, planres.GetIssues().ToString());

        Cerr << planres.GetStats()->GetAst() << Endl;

        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), NYdb::NQuery::TExecuteQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        TString output = FormatResultSetYson(result.GetResultSet(0));
        Cout << output << Endl;
    }

    bool hasExpectedLog = false;
    TString line;
    while (logsStream.ReadLine(line)) {
        if (line.Contains(expectedLog)) {
            hasExpectedLog = true;
            break;
        }
    }
    UNIT_ASSERT(hasExpectedLog == enabledLogs);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpScanLogs) {

Y_UNIT_TEST_TWIN(WideCombine, EnabledLogs) {
    auto query = R"(
        --!syntax_v1
        PRAGMA ydb.UseDqHashCombine='true';
        select count(t.Key) from `/Root/KeyValue` as t group by t.Value
    )";

    RunTestForQuery(query, "[DqHashCombine]", EnabledLogs);
}

Y_UNIT_TEST_TWIN(GraceJoin, EnabledLogs) {
    auto query = R"(
        --!syntax_v1
        PRAGMA ydb.CostBasedOptimizationLevel='0';
        PRAGMA ydb.HashJoinMode='graceandself';
        select t1.Key, t1.Value, t2.Key, t2.Value
        from `/Root/KeyValue` as t1 full join `/Root/KeyValue` as t2 on t1.Value = t2.Value
        order by t1.Value
    )";

    RunTestForQuery(query, "[GraceJoin]", EnabledLogs);
}


} // suite

} // namespace NKqp
} // namespace NKikimr
