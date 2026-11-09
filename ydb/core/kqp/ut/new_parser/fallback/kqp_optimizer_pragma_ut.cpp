#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_query_helpers.h>

#include <library/cpp/json/json_reader.h>
#include <util/string/split.h>
#include <util/system/datetime.h>

namespace NKikimr::NKqp {
namespace {

using namespace NYdb;
using namespace NYqlTests;

void CheckNative(const TCapturedLog& logs, size_t start, const TString& traceId, bool success) {
    const TString tag = "TraceId: " + traceId + ", ";
    const auto deadline = TInstant::Now() + TDuration::Seconds(10);
    TString evidence;
    do {
        evidence.clear();
        TVector<TString> lines;
        Split(logs.Snapshot().substr(start), "\n", lines);
        for (const auto& line : lines) {
            if (line.Contains(tag)) { evidence += line + "\n"; }
        }
        UNIT_ASSERT_C(!evidence.Contains(" fallback:") && !evidence.Contains("Legacy YQL graph compilation started"), evidence);
        if (evidence.Contains(success ? "New YQL RBO lowering succeeded" : "New YQL type annotation succeeded")) {
            return;
        }
        Sleep(TDuration::MilliSeconds(10));
    } while (TInstant::Now() < deadline);
    UNIT_FAIL("Missing native frontend evidence for " << traceId << "\n" << evidence);
}

const TString Join = "SELECT a.id, b.value FROM `/Root/a` AS a JOIN `/Root/b` AS b ON a.id = b.id;";

TKikimrSettings Settings(bool native, ui32 defaultLevel, TCapturedLog& logs) {
    NKikimrConfig::TAppConfig app;
    auto* config = app.MutableTableServiceConfig();
    config->SetEnableNewYqlParser(native);
    config->SetEnableNewRBO(true);
    config->SetEnableFallbackToYqlOptimizer(false);
    config->SetEnableFallbackOnMultipleStatements(false);
    config->SetDefaultCostBasedOptimizationLevel(defaultLevel);
    config->SetEnableAstCache(false);
    config->SetCompileQueryCacheSize(0);
    auto settings = TKikimrSettings(app).SetWithSampleTables(false).SetLogCapture(logs);
    settings.LogSettings = TTestLogSettings().AddLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_INFO);
    settings.LogSettings->DefaultLogPriority = NActors::NLog::PRI_CRIT;
    return settings;
}

void CreateTables(TKikimrRunner& kikimr) {
    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    const auto result = session.ExecuteSchemeQuery(
        "CREATE TABLE `/Root/a` (id Int64 NOT NULL, value String, PRIMARY KEY(id));"
        "CREATE TABLE `/Root/b` (id Int64 NOT NULL, value String, PRIMARY KEY(id));").GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

} // namespace

Y_UNIT_TEST_SUITE(KqpNewYqlOptimizerPragma) {
    // Compare both frontend paths and prove both directions of override. Follow
    // every request with an unmodified query to check session/config isolation.
    Y_UNIT_TEST_TWIN(CostBasedOptimizationLevel, Native) {
        for (const ui32 defaultLevel : {0u, 4u}) {
            TCapturedLog logs(true);
            TKikimrRunner kikimr(Settings(Native, defaultLevel, logs));
            CreateTables(kikimr);
            auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
            ui32 request = 0;
            const auto check = [&](const TString& query, bool cbo) {
                const TString traceId = TStringBuilder() << "cbo-pragma-" << defaultLevel << '-' << ++request;
                const auto start = logs.Snapshot().size();
                const auto before = GetNewRBOCompileCounters(kikimr);
                const auto result = session.ExecuteQuery(query, NQuery::TTxControl::NoTx(),
                    NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
                        .TraceId(std::string(traceId))).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), query << "\n" << result.GetIssues().ToString());
                UNIT_ASSERT_C(result.GetStats() && result.GetStats()->GetPlan(), query);
                NJson::TJsonValue plan;
                NJson::ReadJsonTree(TString(*result.GetStats()->GetPlan()), &plan, true);
                const auto& stats = plan.GetMapSafe().at("SimplifiedPlan").GetMapSafe().at("OptimizerStats").GetMapSafe();
                UNIT_ASSERT_VALUES_EQUAL_C(stats.at("CBOTreesTotal").GetUIntegerSafe() > 0, cbo, query << "\n" << plan.GetStringRobust());
                if (!cbo) { UNIT_ASSERT_VALUES_EQUAL(stats.at("CBOTreesOptimized").GetUIntegerSafe(), 0); }
                const auto after = GetNewRBOCompileCounters(kikimr);
                UNIT_ASSERT_VALUES_EQUAL(after.first - before.first, 1);
                UNIT_ASSERT_VALUES_EQUAL(after.second, before.second);
                if (Native) { CheckNative(logs, start, traceId, true); }
            };
            const TVector<std::pair<TString, bool>> cases = {
                {"PRAGMA ydb.CostBasedOptimizationLevel = '0';", false},
                {"PRAGMA ydb.CostBasedOptimizationLevel = '4';", true},
                {"PRAGMA KiKiMr.Cost_Based_Optimization_Level('\\x30');", false},
                {"--!ansi_lexer\nPRAGMA \"ydb\".\"CostBasedOptimizationLevel\" = '4';", true},
                {"PRAGMA ydb.CostBasedOptimizationLevel = '4'; PRAGMA ydb.CostBasedOptimizationLevel = '0';", false},
                {"PRAGMA ydb.CostBasedOptimizationLevel = '0'; PRAGMA ydb.CostBasedOptimizationLevel = '4';", true},
            };
            for (const auto& [pragma, cbo] : cases) {
                check(pragma + Join, cbo);
                check(Join, defaultLevel > 1);
            }
        }
    }

    Y_UNIT_TEST_TWIN(InvalidCostBasedOptimizationLevel, Native) {
        TCapturedLog logs(true);
        TKikimrRunner kikimr(Settings(Native, 4, logs));
        CreateTables(kikimr);
        auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        ui32 request = 0;
        for (const TString value : {"'invalid'", "'-1'", "'4294967296'", "default", "''"}) {
            const TString traceId = TStringBuilder() << "invalid-cbo-pragma-" << ++request;
            const auto start = logs.Snapshot().size();
            const auto result = session.ExecuteQuery("PRAGMA ydb.CostBasedOptimizationLevel = " + value + ";" + Join,
                NQuery::TTxControl::NoTx(), NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
                    .TraceId(std::string(traceId))).GetValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), value);
            TString issues = result.GetIssues().ToString();
            issues.to_lower();
            UNIT_ASSERT_STRING_CONTAINS(issues, "costbasedoptimizationlevel");
            if (Native) { CheckNative(logs, start, traceId, false); }
        }
    }
}

} // namespace NKikimr::NKqp
